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
 *  7. Verify the FK violation count did not grow.
 *  8. (Optional) verify logical content hash.
 *  9. COMMIT.
 */
class PatchApplier(
    private val logger: Logger = Logger.withTag("PatchApplier"),
) {

    data class Result(
        val migrationsApplied: Int,
        val upsertCounts: Map<String, Int>,
        val deleteCounts: Map<String, Int>,
    )

    fun apply(
        conn: Connection,
        patchDb: Path,
        expectedToContentHash: String? = null,
    ): Result {
        val wasAutoCommit = conn.autoCommit
        conn.autoCommit = false
        try {
            val preFkCount = countFkViolations(conn)
            attach(conn, patchDb)
            conn.createStatement().use { it.execute("PRAGMA defer_foreign_keys = ON") }
            val migrations = runMigrations(conn)
            val upserts = runUpserts(conn)
            val deletes = runDeletes(conn)
            val postFkCount = countFkViolations(conn)
            check(postFkCount <= preFkCount) {
                "Patch introduced ${postFkCount - preFkCount} new FK violations (pre=$preFkCount, post=$postFkCount)"
            }
            if (preFkCount > 0) {
                logger.w { "DB carries $preFkCount pre-existing FK violations — tolerated, not introduced by this patch." }
            }

            if (expectedToContentHash != null) {
                val actual = LogicalContentHasher().compute(conn)
                if (actual != expectedToContentHash) {
                    throw IllegalStateException(
                        "Logical content hash after apply ($actual) does not match expected ($expectedToContentHash)",
                    )
                }
            }

            conn.commit()
            conn.autoCommit = true
            detach(conn)
            logger.i { "Patch applied — migrations=$migrations, upserts=$upserts, deletes=$deletes" }
            return Result(migrations, upserts, deletes)
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
        var count = 0
        conn.createStatement().use { st ->
            st.executeQuery("SELECT sql FROM patch.migrations ORDER BY version ASC").use { rs ->
                while (rs.next()) {
                    val sql = rs.getString(1)
                    conn.createStatement().use { it.execute(sql) }
                    count++
                }
            }
        }
        return count
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

    private fun patchHasTable(conn: Connection, name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM patch.sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }
}
