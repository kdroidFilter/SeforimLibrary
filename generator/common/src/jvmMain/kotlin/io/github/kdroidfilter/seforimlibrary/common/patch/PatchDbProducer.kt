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
 * For each table in [PATCH_TABLES_IN_FK_ORDER]:
 *   1. Reads the column list + PK from the **new** DB.
 *   2. Creates `upsert_<table>` mirroring those columns + PK.
 *   3. Inserts every row of `new.<table>` that either isn't in `prev.<table>`
 *      (joined on the PK) or differs from `prev` on any non-PK column.
 *   4. Creates `delete_<table>` with just the PK columns and inserts every
 *      `(pk…)` tuple present in `prev` but missing from `new`.
 *
 * See `DELTA_UPDATE_PLAN.md` §6.6.
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
            applyBaseDdl(conn)
            writeMetadata(conn, fromVersion, toVersion)
            writeMigrations(conn, migrations)

            attach(conn, "prev", prevDb)
            attach(conn, "new", newDb)

            // Materialise upsert_/delete_ tables based on the new DB's actual
            // schema. The producer is generic — every table in our config list
            // is processed identically.
            for (table in PATCH_TABLES_IN_FK_ORDER) {
                if (!tableExists(conn, "new", table.name)) continue
                PatchDbSchema.createUpsertTable(conn, "new", table)
                PatchDbSchema.createDeleteTable(conn, "new", table)
            }

            for (table in PATCH_TABLES_IN_FK_ORDER) {
                upsertCounts[table.name] = scanUpserts(conn, table)
            }
            for (table in PATCH_TABLES_IN_FK_ORDER) {
                deleteCounts[table.name] = scanDeletes(conn, table)
            }

            // Commit BEFORE detach so SQLite isn't holding locks on the
            // attached DBs through an open transaction.
            conn.commit()
            conn.autoCommit = true
            detach(conn, "new")
            detach(conn, "prev")
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

    private fun applyBaseDdl(conn: Connection) {
        conn.createStatement().use { st ->
            PatchDbSchema.baseStatements.forEach { st.executeUpdate(it) }
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

    private fun scanUpserts(conn: Connection, table: PatchTable): Int {
        val cols = PatchDbSchema.readTableInfo(conn, "new", table.name).map { it.name }
        if (cols.isEmpty()) return 0
        val colsCsv = cols.joinToString(",") { "\"$it\"" }
        val joinCond = table.primaryKey.joinToString(" AND ") { "new.\"$it\" = prev.\"$it\"" }
        val nonPkCols = cols.filter { it !in table.primaryKey }
        val diffPredicate = if (nonPkCols.isEmpty()) "FALSE" else nonPkCols.joinToString(" OR ") {
            "COALESCE(new.\"$it\", '') <> COALESCE(prev.\"$it\", '')"
        }
        // First PK column is enough to detect "prev row absent".
        val firstPk = table.primaryKey.first()
        val sql = """
            INSERT INTO "upsert_${table.name}" ($colsCsv)
            SELECT ${cols.joinToString(",") { "new.\"$it\"" }}
            FROM new."${table.name}" AS new
            LEFT JOIN prev."${table.name}" AS prev ON $joinCond
            WHERE prev."$firstPk" IS NULL OR ($diffPredicate)
        """.trimIndent()
        return conn.createStatement().use { it.executeUpdate(sql) }
    }

    private fun scanDeletes(conn: Connection, table: PatchTable): Int {
        if (table.primaryKey.isEmpty()) return 0
        if (!tableExists(conn, "prev", table.name)) return 0
        val pkCsv = table.primaryKey.joinToString(",") { "\"$it\"" }
        val joinCond = table.primaryKey.joinToString(" AND ") { "new.\"$it\" = prev.\"$it\"" }
        val firstPk = table.primaryKey.first()
        val sql = """
            INSERT INTO "delete_${table.name}" ($pkCsv)
            SELECT ${table.primaryKey.joinToString(",") { "prev.\"$it\"" }}
            FROM prev."${table.name}" AS prev
            LEFT JOIN new."${table.name}" AS new ON $joinCond
            WHERE new."$firstPk" IS NULL
        """.trimIndent()
        return conn.createStatement().use { it.executeUpdate(sql) }
    }

    private fun tableExists(conn: Connection, schema: String, name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM $schema.sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }

    companion object {
        // Kept for backwards-compat with callers (and the docs reference it).
        val TABLES_IN_FK_ORDER: List<String> = PATCH_TABLES_IN_FK_ORDER.map { it.name }
    }
}
