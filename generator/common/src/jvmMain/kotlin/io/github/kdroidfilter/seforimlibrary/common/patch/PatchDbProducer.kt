package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager

/**
 * Produces a `patch.db` from two seforim.db snapshots (previous + current).
 *
 * The algorithm is pure SQL once both DBs are attached:
 *
 *   INSERT INTO upsert_<table>
 *   SELECT new.* FROM new.<table> new
 *   LEFT JOIN prev.<table> prev ON new.id = prev.id
 *   WHERE prev.id IS NULL
 *      OR <any non-id column differs>;
 *
 *   INSERT INTO delete_<table>(id)
 *   SELECT prev.id FROM prev.<table> prev
 *   LEFT JOIN new.<table> new ON prev.id = new.id
 *   WHERE new.id IS NULL;
 *
 * Output: a single SQLite file at [outputPath]. Per-book patch splitting
 * (`patch_book_<id>.db`) is a follow-up — for Phase 4 MVP we ship the whole
 * thing in one file, which the client can ATTACH directly.
 *
 * See DELTA_UPDATE_PLAN.md §6.6.
 */
class PatchDbProducer(
    private val logger: Logger = Logger.withTag("PatchDbProducer"),
) {

    data class Output(
        val path: Path,
        val fromVersion: Int,
        val toVersion: Int,
        val upsertCounts: Map<String, Int>,
        val deleteCounts: Map<String, Int>,
    )

    /**
     * @param prevDb path to the previous build's seforim.db
     * @param newDb path to the current build's seforim.db
     * @param outputPath path where the resulting patch.db will be written
     *   (atomically via a `.tmp` rename)
     * @param fromVersion / toVersion pairs of integer build versions
     * @param migrations DDL statements to embed in `patch.migrations`,
     *   produced by [io.github.kdroidfilter.seforimlibrary.common.changes.
     *   computeSchemaMigrations] (TBD) when `db_schema_version` differs.
     */
    fun produce(
        prevDb: Path,
        newDb: Path,
        outputPath: Path,
        fromVersion: Int,
        toVersion: Int,
        migrations: List<Pair<Int, String>> = emptyList(),
    ): Output {
        Files.createDirectories(outputPath.toAbsolutePath().parent)
        val tmp = outputPath.resolveSibling("${outputPath.fileName}.tmp")
        if (Files.exists(tmp)) Files.delete(tmp)

        Class.forName("org.sqlite.JDBC")
        val upsertCounts = LinkedHashMap<String, Int>()
        val deleteCounts = LinkedHashMap<String, Int>()

        DriverManager.getConnection("jdbc:sqlite:${tmp.toAbsolutePath()}").use { conn ->
            conn.autoCommit = false
            applyDdl(conn)
            writeMetadata(conn, fromVersion, toVersion)
            writeMigrations(conn, migrations)

            attach(conn, "prev", prevDb)
            attach(conn, "new", newDb)

            for (table in TABLES_IN_FK_ORDER) {
                upsertCounts[table] = scanUpserts(conn, table)
            }
            for (table in TABLES_IN_FK_ORDER) {
                deleteCounts[table] = scanDeletes(conn, table)
            }

            detach(conn, "new")
            detach(conn, "prev")
            conn.commit()
            conn.autoCommit = true
            conn.createStatement().use { it.execute("VACUUM") }
        }

        Files.move(tmp, outputPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        val totalUpserts = upsertCounts.values.sum()
        val totalDeletes = deleteCounts.values.sum()
        logger.i {
            "Produced patch.db at $outputPath — upserts=$totalUpserts, deletes=$totalDeletes " +
                "(from v$fromVersion to v$toVersion)"
        }
        return Output(outputPath, fromVersion, toVersion, upsertCounts, deleteCounts)
    }

    private fun applyDdl(conn: Connection) {
        conn.createStatement().use { st ->
            PatchDbSchema.statements.forEach { st.executeUpdate(it) }
        }
    }

    private fun writeMetadata(conn: Connection, from: Int, to: Int) {
        conn.prepareStatement("INSERT INTO patch_meta(key, value) VALUES (?, ?)").use { ps ->
            listOf(
                "schema_version" to PatchDbSchema.CURRENT_VERSION.toString(),
                "from_version" to from.toString(),
                "to_version" to to.toString(),
                "generated_at" to java.time.Instant.now().toString(),
            ).forEach { (k, v) -> ps.setString(1, k); ps.setString(2, v); ps.executeUpdate() }
        }
    }

    private fun writeMigrations(conn: Connection, migrations: List<Pair<Int, String>>) {
        if (migrations.isEmpty()) return
        conn.prepareStatement("INSERT INTO migrations(version, sql) VALUES (?, ?)").use { ps ->
            for ((v, sql) in migrations) {
                ps.setInt(1, v); ps.setString(2, sql); ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun attach(conn: Connection, alias: String, path: Path) {
        conn.prepareStatement("ATTACH DATABASE ? AS $alias").use { ps ->
            ps.setString(1, path.toAbsolutePath().toString())
            ps.executeUpdate()
        }
    }

    private fun detach(conn: Connection, alias: String) {
        conn.createStatement().use { it.execute("DETACH DATABASE $alias") }
    }

    /** Returns the number of rows scanned into upsert_<table>. */
    private fun scanUpserts(conn: Connection, table: String): Int {
        val cols = readColumns(conn, "main", "upsert_$table") ?: return 0
        if (!hasTable(conn, "new", table)) return 0
        val colsCsv = cols.joinToString(",") { "\"$it\"" }
        val nonIdCols = cols.filter { it != "id" }
        val diffPredicate = if (nonIdCols.isEmpty()) "FALSE" else nonIdCols.joinToString(" OR ") {
            "COALESCE(new.\"$it\", '') <> COALESCE(prev.\"$it\", '')"
        }
        val sql = """
            INSERT INTO upsert_$table ($colsCsv)
            SELECT ${cols.joinToString(",") { "new.\"$it\"" }}
            FROM new."$table" AS new
            LEFT JOIN prev."$table" AS prev ON new.id = prev.id
            WHERE prev.id IS NULL OR ($diffPredicate)
        """.trimIndent()
        return conn.createStatement().use { it.executeUpdate(sql) }
    }

    /** Returns the number of ids scanned into delete_<table>. */
    private fun scanDeletes(conn: Connection, table: String): Int {
        if (!hasTable(conn, "main", "delete_$table")) return 0
        if (!hasTable(conn, "prev", table)) return 0
        val sql = """
            INSERT INTO delete_$table(id)
            SELECT prev.id FROM prev."$table" AS prev
            LEFT JOIN new."$table" AS new ON prev.id = new.id
            WHERE new.id IS NULL
        """.trimIndent()
        return conn.createStatement().use { it.executeUpdate(sql) }
    }

    private fun readColumns(conn: Connection, schema: String, table: String): List<String>? {
        if (!hasTable(conn, schema, table)) return null
        val cols = ArrayList<String>()
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA $schema.table_info(\"$table\")").use { rs ->
                while (rs.next()) cols += rs.getString("name")
            }
        }
        return cols
    }

    private fun hasTable(conn: Connection, schema: String, name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM $schema.sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }

    companion object {
        // FK order: parents → children. Same order the applier uses for upserts.
        val TABLES_IN_FK_ORDER: List<String> = listOf(
            "source", "author", "topic", "pub_place", "pub_date",
            "connection_type", "category", "tocText", "book",
            "tocEntry", "line", "link",
        )
    }
}
