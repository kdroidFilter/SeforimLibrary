package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import java.nio.file.Path
import java.sql.Connection

/**
 * Applies a `patch.db` produced by [PatchDbProducer] onto a live `seforim.db`
 * connection.
 *
 * Lifecycle per [apply] (DELTA_UPDATE_PLAN.md §7.3):
 *
 *  1. Snapshot pre-apply FK violation count.
 *  2. `ATTACH 'patch.db' AS patch`.
 *  3. Enable `PRAGMA defer_foreign_keys = ON` so the table-by-table apply
 *     can cross FK cycles (e.g. tocEntry.lineId ↔ line.tocEntryId).
 *  4. Run **migrations** (DDL).
 *  5. For each tracked table in FK order: `INSERT ... SELECT ... FROM
 *     patch.upsert_<table> ON CONFLICT(<pk…>) DO UPDATE SET <cols>` (or
 *     `DO NOTHING` for pure-PK junctions).
 *  6. For each tracked table in REVERSE FK order:
 *     `DELETE FROM <table> WHERE (pk…) IN (SELECT pk… FROM patch.delete_<table>)`.
 *  6a. Replace every optional table the patch snapshots (`optional_<name>`, see
 *      [OPTIONAL_PATCH_TABLES]); a table without a snapshot is left untouched.
 *  6b. Install the patch's `stat1_snapshot`, when present, clear stale STAT4
 *      histograms, and reload planner statistics on the applying connection.
 *  7. Verify the FK violation count did not grow.
 *  8. (Optional) verify logical content hash, and each optional table's hash.
 *  9. COMMIT.
 */
class PatchApplier(
    private val logger: Logger = Logger.withTag("PatchApplier"),
) {

    data class Result(
        val migrationsApplied: Int,
        val upsertCounts: Map<String, Int>,
        val deleteCounts: Map<String, Int>,
        /** Rows written per replaced optional table; kept apart from [upsertCounts]. */
        val optionalTablesReplaced: Map<String, Int> = emptyMap(),
    )

    fun apply(
        conn: Connection,
        patchDb: Path,
        expectedToContentHash: String? = null,
        expectedToSchemaVersion: Int? = null,
        expectedOptionalTableHashes: Map<String, String> = emptyMap(),
    ): Result {
        val wasAutoCommit = conn.autoCommit
        conn.autoCommit = false
        try {
            val preFkCount = countFkViolations(conn)
            attach(conn, patchDb)
            assertPatchSchemaCompatible(conn)
            conn.createStatement().use { it.execute("PRAGMA defer_foreign_keys = ON") }
            val migrations = runMigrations(conn)
            val upserts = runUpserts(conn)
            val deletes = runDeletes(conn)
            val optionalReplaced = replaceOptionalTables(conn)
            applyStat1Snapshot(conn)
            val postFkCount = countFkViolations(conn)
            check(postFkCount <= preFkCount) {
                "Patch introduced ${postFkCount - preFkCount} new FK violations (pre=$preFkCount, post=$postFkCount)"
            }
            if (preFkCount > 0) {
                logger.w { "DB carries $preFkCount pre-existing FK violations — tolerated, not introduced by this patch." }
            }

            if (expectedToContentHash != null) {
                val schemaVersion = expectedToSchemaVersion
                    ?: error("expectedToSchemaVersion is required with expectedToContentHash")
                val actual = LogicalContentHasher.forSchemaVersion(schemaVersion).compute(conn)
                if (actual != expectedToContentHash) {
                    throw IllegalStateException(
                        "Logical content hash after apply ($actual) does not match expected ($expectedToContentHash)",
                    )
                }
            }

            verifyOptionalTables(conn, expectedOptionalTableHashes)

            conn.commit()
            conn.autoCommit = true
            detach(conn)
            logger.i {
                "Patch applied — migrations=$migrations, upserts=$upserts, deletes=$deletes, " +
                    "optional=$optionalReplaced"
            }
            return Result(migrations, upserts, deletes, optionalReplaced)
        } catch (t: Throwable) {
            runCatching { conn.rollback() }
            runCatching { detach(conn) }
            logger.e(t) { "Patch apply failed; rolled back transaction" }
            throw t
        } finally {
            conn.autoCommit = wasAutoCommit
        }
    }

    private fun attach(conn: Connection, patchDb: Path) {
        conn.prepareStatement("ATTACH DATABASE ? AS patch").use { ps ->
            ps.setString(1, patchDb.toAbsolutePath().toString())
            ps.executeUpdate()
        }
    }

    private fun detach(conn: Connection) {
        conn.createStatement().use { it.execute("DETACH DATABASE patch") }
    }

    private fun runMigrations(conn: Connection): Int {
        // Materialise the attached patch rows before executing DDL. Keeping a
        // live sqlite_master-backed result set open while DROP/CREATE changes
        // the main schema can produce SQLITE_LOCKED on SQLite JDBC.
        val migrations = conn.createStatement().use { st ->
            st.executeQuery("SELECT sql FROM patch.migrations ORDER BY version ASC").use { rs ->
                buildList { while (rs.next()) add(rs.getString(1)) }
            }
        }
        for (sql in migrations) conn.createStatement().use { it.execute(sql) }
        return migrations.size
    }

    private fun runUpserts(conn: Connection): Map<String, Int> {
        val counts = LinkedHashMap<String, Int>()
        for (table in PATCH_TABLES_IN_FK_ORDER) {
            if (!patchHasTable(conn, "upsert_${table.name}")) continue
            val cols = PatchDbSchema.readTableInfo(conn, "patch", "upsert_${table.name}").map { it.name }
            if (cols.isEmpty()) continue
            val colsCsv = cols.joinToString(",") { "\"$it\"" }
            val pkCsv = table.primaryKey.joinToString(",") { "\"$it\"" }
            val nonPkCols = cols.filter { it !in table.primaryKey }
            val conflictClause = if (!table.updatable || nonPkCols.isEmpty()) {
                "ON CONFLICT($pkCsv) DO NOTHING"
            } else {
                "ON CONFLICT($pkCsv) DO UPDATE SET " +
                    nonPkCols.joinToString(",") { "\"$it\" = excluded.\"$it\"" }
            }
            val sql = """
                INSERT INTO "${table.name}" ($colsCsv)
                SELECT $colsCsv FROM patch."upsert_${table.name}"
                WHERE true
                $conflictClause
            """.trimIndent()
            val n = conn.createStatement().use { it.executeUpdate(sql) }
            counts[table.name] = n
            if (n > 0) logger.d { "Upserted $n row(s) into ${table.name}" }
        }
        return counts
    }

    /** Full replacement of each known optional table the patch snapshots; unknown snapshots are ignored. */
    private fun replaceOptionalTables(conn: Connection): Map<String, Int> {
        val counts = LinkedHashMap<String, Int>()
        for (spec in OPTIONAL_PATCH_TABLES) {
            val snapshot = optionalSnapshotTable(spec.name)
            if (!patchHasTable(conn, snapshot)) continue
            val ddl = readOptionalTableDdl(conn, spec.name)
                ?: error("patch.db carries $snapshot without its DDL in $OPTIONAL_TABLE_DDL_TABLE")
            val colsCsv = spec.columns.joinToString(",") { "\"$it\"" }
            conn.createStatement().use { st ->
                st.execute(ddl)
                st.execute("DELETE FROM main.\"${spec.name}\"")
                counts[spec.name] = st.executeUpdate(
                    "INSERT INTO main.\"${spec.name}\" ($colsCsv) SELECT $colsCsv FROM patch.\"$snapshot\"",
                )
            }
        }
        return counts
    }

    private fun readOptionalTableDdl(conn: Connection, name: String): String? {
        if (!patchHasTable(conn, OPTIONAL_TABLE_DDL_TABLE)) return null
        conn.prepareStatement("SELECT sql FROM patch.\"$OPTIONAL_TABLE_DDL_TABLE\" WHERE name = ?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return if (rs.next()) rs.getString(1) else null }
        }
    }

    /** Checks every known optional table the manifest carries a hash for; unknown names are not verified. */
    private fun verifyOptionalTables(conn: Connection, expected: Map<String, String>) {
        val tables = OPTIONAL_PATCH_TABLES.map { it.name }.filter { it in expected }
        if (tables.isEmpty()) return
        val actual = LogicalContentHasher(tables).computeReport(conn).tableHashes
        val mismatched = tables.filter { actual[it] != expected[it] }
        check(mismatched.isEmpty()) {
            "Optional table hash after apply does not match optionalTableContentHashes for ${mismatched.joinToString(", ")}"
        }
    }

    /** Installs the patch's planner statistics; a patch without a snapshot keeps the existing ones. */
    private fun applyStat1Snapshot(conn: Connection) {
        if (!patchHasTable(conn, PatchDbSchema.STAT1_SNAPSHOT_TABLE)) return
        conn.createStatement().use { st ->
            // CREATE TABLE sqlite_* is reserved, so ANALYZE creates sqlite_stat1
            // for a client that has never been analyzed.
            if (!mainHasTable(conn, "sqlite_stat1")) st.execute("ANALYZE main.sqlite_schema")
            st.execute("DELETE FROM main.sqlite_stat1")
            st.execute(
                "INSERT INTO main.sqlite_stat1 (tbl, idx, stat) " +
                    "SELECT tbl, idx, stat FROM patch.\"${PatchDbSchema.STAT1_SNAPSHOT_TABLE}\"",
            )
            // The patch carries no STAT4 histograms. Old ones describe the
            // previous DB and can override the fresh stat1 estimates.
            if (mainHasTable(conn, "sqlite_stat4")) st.execute("DELETE FROM main.sqlite_stat4")
            // Direct writes to sqlite_stat1 do not refresh this connection's
            // in-memory query planner statistics.
            st.execute("ANALYZE main.sqlite_schema")
        }
    }

    private fun runDeletes(conn: Connection): Map<String, Int> {
        val counts = LinkedHashMap<String, Int>()
        for (table in PATCH_TABLES_IN_FK_ORDER.asReversed()) {
            if (!patchHasTable(conn, "delete_${table.name}")) continue
            if (table.primaryKey.isEmpty()) continue
            val pkCsv = table.primaryKey.joinToString(",") { "\"$it\"" }
            val sql = if (table.primaryKey.size == 1) {
                val k = "\"${table.primaryKey[0]}\""
                "DELETE FROM \"${table.name}\" WHERE $k IN (SELECT $k FROM patch.\"delete_${table.name}\")"
            } else {
                // SQLite supports tuple IN: WHERE (a,b) IN (SELECT a,b FROM …).
                "DELETE FROM \"${table.name}\" WHERE ($pkCsv) IN (SELECT $pkCsv FROM patch.\"delete_${table.name}\")"
            }
            val n = conn.createStatement().use { it.executeUpdate(sql) }
            counts[table.name] = n
            if (n > 0) logger.d { "Deleted $n row(s) from ${table.name}" }
        }
        return counts
    }

    private fun countFkViolations(conn: Connection): Long {
        var n = 0L
        conn.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM pragma_foreign_key_check").use { rs ->
                if (rs.next()) n = rs.getLong(1)
            }
        }
        return n
    }

    /**
     * Reads `patch.patch_meta.schema_version` and refuses to apply a patch
     * whose format version is outside the supported 1..[PatchDbSchema.CURRENT_VERSION]
     * range. This is the patch artifact format, not the target DB schema from
     * the release manifest. Without
     * this check, an older client could silently mis-apply a future-schema
     * patch.db (new tables ignored, new patch_meta keys not honoured),
     * producing a DB that "passed" the FK check but is semantically wrong.
     */
    private fun assertPatchSchemaCompatible(conn: Connection) {
        val patchSchemaVersion = readPatchMetaInt(conn, "schema_version")
            ?: throw IllegalStateException(
                "patch.db is missing patch_meta.schema_version — refusing to apply " +
                    "(likely a corrupt or hand-built patch).",
            )
        if (patchSchemaVersion !in 1..PatchDbSchema.CURRENT_VERSION) {
            throw IllegalStateException(
                "patch.db carries schema_version=$patchSchemaVersion but this build " +
                    "of the applier only understands versions " +
                    "1..${PatchDbSchema.CURRENT_VERSION}. " +
                    "Upgrade the client before applying this patch.",
            )
        }
    }

    private fun readPatchMetaInt(conn: Connection, key: String): Int? {
        conn.prepareStatement("SELECT value FROM patch.patch_meta WHERE key = ?").use { ps ->
            ps.setString(1, key)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getString(1)?.toIntOrNull() else null
            }
        }
    }

    private fun patchHasTable(conn: Connection, name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM patch.sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }

    private fun mainHasTable(conn: Connection, name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }
}
