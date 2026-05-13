package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import java.nio.file.Path
import java.sql.Connection

/**
 * Applies a `patch.db` produced by [PatchDbProducer] (or its forthcoming
 * implementation) onto a live `seforim.db` connection.
 *
 * Lifecycle per [apply] call (DELTA_UPDATE_PLAN.md §7.3):
 *
 *  1. `BEGIN IMMEDIATE` — exclusive write transaction on seforim.db.
 *  2. `ATTACH 'patch.db' AS patch` — read-only view of the patch.
 *  3. Run **migrations**: `patch.migrations` rows ordered by version,
 *     executed verbatim. This is where new columns / tables land.
 *  4. **Upserts**: `INSERT … SELECT … FROM patch.upsert_<table>
 *     WHERE true ON CONFLICT(id) DO UPDATE SET …` per table, parents
 *     before children to avoid FK violations.
 *  5. **Deletes**: `DELETE FROM <table> WHERE id IN (SELECT id FROM
 *     patch.delete_<table>)` in reverse FK order (children before parents).
 *  6. `PRAGMA foreign_key_check` — abort + rollback if anything dangles.
 *  7. Bump `schema_meta.db_version` / `db_schema_version` / `content_hash`.
 *  8. `COMMIT`. The transaction is the atomic boundary: either the whole
 *     patch lands or none of it does.
 *
 * The applier never touches the Lucene index nor `catalog.pb` — those are
 * the caller's responsibility (see the client module).
 *
 * Per-importer caveats:
 *  - Uses `ON CONFLICT(id) DO UPDATE` (not `INSERT OR REPLACE`) so that
 *    `ON DELETE CASCADE` is never accidentally triggered by a row that
 *    happens to share an id (DELTA_UPDATE_PLAN.md §4.3).
 *  - The patch DB stays attached only for the duration of [apply]; we
 *    detach it before COMMIT so subsequent transactions don't see it.
 */
class PatchApplier(
    private val logger: Logger = Logger.withTag("PatchApplier"),
) {

    data class Result(
        val migrationsApplied: Int,
        val upsertCounts: Map<String, Int>,
        val deleteCounts: Map<String, Int>,
    )

    /**
     * Applies [patchDb] onto the seforim.db opened on [conn]. Throws on any
     * FK violation, content-hash mismatch (if [expectedToContentHash] is
     * supplied), or DDL/DML failure — leaving the DB untouched thanks to the
     * surrounding transaction.
     */
    fun apply(
        conn: Connection,
        patchDb: Path,
        expectedToContentHash: String? = null,
    ): Result {
        val wasAutoCommit = conn.autoCommit
        conn.autoCommit = false
        try {
            conn.prepareStatement("BEGIN IMMEDIATE").use { it.executeUpdate() }
            attach(conn, patchDb)
            val migrations = runMigrations(conn)
            val upserts = runUpserts(conn)
            val deletes = runDeletes(conn)
            verifyFkIntegrity(conn)

            if (expectedToContentHash != null) {
                val actual = LogicalContentHasher().compute(conn)
                if (actual != expectedToContentHash) {
                    throw IllegalStateException(
                        "Logical content hash after apply ($actual) does not match expected ($expectedToContentHash)",
                    )
                }
            }

            detach(conn)
            conn.commit()
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
        // Parents before children. The order roughly tracks the FK topology of seforim.db.
        val order = listOf(
            "source", "author", "topic", "pub_place", "pub_date",
            "connection_type", "category", "tocText", "book",
            "tocEntry", "line", "line_toc", "link",
        )
        val counts = LinkedHashMap<String, Int>()
        for (table in order) {
            val (cols, upsertCols) = readPatchColumns(conn, "upsert_$table") ?: continue
            if (cols.isEmpty()) continue
            val colsCsv = cols.joinToString(",") { "\"$it\"" }
            val updateAssignments = cols.filter { it != "id" }
                .joinToString(",") { "\"$it\" = excluded.\"$it\"" }
            val sql = """
                INSERT INTO "$table" ($colsCsv)
                SELECT $colsCsv FROM patch.upsert_$table
                WHERE true
                ON CONFLICT(id) DO UPDATE SET $updateAssignments
            """.trimIndent()
            val n = conn.createStatement().use { it.executeUpdate(sql) }
            counts[table] = n
            if (n > 0) logger.d { "Upserted $n row(s) into $table" }
        }
        return counts
    }

    private fun runDeletes(conn: Connection): Map<String, Int> {
        // Children before parents.
        val order = listOf(
            "line_toc", "link", "line", "tocEntry", "tocText",
            "book", "category", "connection_type",
            "pub_date", "pub_place", "topic", "author", "source",
        )
        val counts = LinkedHashMap<String, Int>()
        for (table in order) {
            if (!hasTable(conn, "patch.delete_$table")) continue
            val sql = "DELETE FROM \"$table\" WHERE id IN (SELECT id FROM patch.delete_$table)"
            val n = conn.createStatement().use { it.executeUpdate(sql) }
            counts[table] = n
            if (n > 0) logger.d { "Deleted $n row(s) from $table" }
        }
        return counts
    }

    private fun verifyFkIntegrity(conn: Connection) {
        val violations = ArrayList<String>()
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA foreign_key_check").use { rs ->
                while (rs.next()) {
                    violations += "table=${rs.getString(1)} rowid=${rs.getLong(2)} parent=${rs.getString(3)} fkid=${rs.getInt(4)}"
                }
            }
        }
        require(violations.isEmpty()) { "FK violations after apply: $violations" }
    }

    /** Returns (raw cols, raw cols without `id`) or null if the patch table doesn't exist. */
    private fun readPatchColumns(conn: Connection, fqTable: String): Pair<List<String>, List<String>>? {
        if (!hasTable(conn, "patch.$fqTable")) return null
        val cols = ArrayList<String>()
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA patch.table_info(\"$fqTable\")").use { rs ->
                while (rs.next()) cols += rs.getString("name")
            }
        }
        return cols to cols.filter { it != "id" }
    }

    private fun hasTable(conn: Connection, fqName: String): Boolean {
        val parts = fqName.split('.')
        val (schema, name) = if (parts.size == 2) parts[0] to parts[1] else "main" to fqName
        conn.prepareStatement(
            "SELECT 1 FROM $schema.sqlite_master WHERE type='table' AND name=?",
        ).use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }
}
