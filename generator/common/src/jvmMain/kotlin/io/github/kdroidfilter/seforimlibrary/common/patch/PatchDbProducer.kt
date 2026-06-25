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
            attach(conn, "prev", prevDb)
            attach(conn, "new", newDb)
            val nextMigrationVersion = (migrations.maxOfOrNull { it.first } ?: 0) + 1
            writeMigrations(conn, migrations + inferCreateTableMigrations(conn, nextMigrationVersion))

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

            // Fail fast on secondary-UNIQUE collisions: catches the case
            // where prev and new were generated from different build_state.db
            // lineages (e.g. same `topic.name` allocated under different ids),
            // which would otherwise blow up mid-transaction in the applier
            // with an opaque "UNIQUE constraint failed" error.
            assertNoSecondaryUniqueCollisions(conn)

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

    private fun inferCreateTableMigrations(conn: Connection, firstVersion: Int): List<Pair<Int, String>> {
        val out = ArrayList<Pair<Int, String>>()
        var version = firstVersion
        for (table in PATCH_TABLES_IN_FK_ORDER) {
            if (!tableExists(conn, "new", table.name)) continue
            if (tableExists(conn, "prev", table.name)) continue

            readCreateSql(conn, "new", "table", table.name)?.let { sql ->
                out += version++ to sql
            }
            readIndexSqlForTable(conn, "new", table.name).forEach { sql ->
                out += version++ to sql
            }
        }
        return out
    }

    private fun readCreateSql(conn: Connection, schemaAlias: String, type: String, name: String): String? {
        conn.prepareStatement(
            "SELECT sql FROM $schemaAlias.sqlite_master WHERE type=? AND name=? AND sql IS NOT NULL",
        ).use { ps ->
            ps.setString(1, type)
            ps.setString(2, name)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getString(1) else null
            }
        }
    }

    private fun readIndexSqlForTable(conn: Connection, schemaAlias: String, table: String): List<String> {
        val out = ArrayList<String>()
        conn.prepareStatement(
            """
            SELECT sql
            FROM $schemaAlias.sqlite_master
            WHERE type='index' AND tbl_name=? AND sql IS NOT NULL
            ORDER BY name
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, table)
            ps.executeQuery().use { rs ->
                while (rs.next()) out += rs.getString(1)
            }
        }
        return out
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
        if (!tableExists(conn, "prev", table.name)) {
            val sql = """
                INSERT INTO "upsert_${table.name}" ($colsCsv)
                SELECT $colsCsv
                FROM new."${table.name}"
            """.trimIndent()
            return conn.createStatement().use { it.executeUpdate(sql) }
        }
        val joinCond = table.primaryKey.joinToString(" AND ") { "new.\"$it\" = prev.\"$it\"" }
        val nonPkCols = cols.filter { it !in table.primaryKey }
        // Use SQLite's `IS NOT` so NULL is treated as a distinct value from
        // '' and from any other column value. The previous COALESCE-based
        // comparison conflated NULL and empty string, silently dropping
        // upserts where a column toggled between NULL and '' (or vice
        // versa) — caught by the real-data v1→v2 e2e on book.heShortDesc.
        val diffPredicate = if (nonPkCols.isEmpty()) "FALSE" else nonPkCols.joinToString(" OR ") {
            "new.\"$it\" IS NOT prev.\"$it\""
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

    /**
     * For every patch table that has at least one secondary UNIQUE index
     * (i.e. a UNIQUE on a non-PK column set), verify that no row in
     * `upsert_<table>` would collide with an existing row in `prev.<table>`
     * on those unique columns at a different primary-key value.
     *
     * Such collisions are almost always caused by feeding the producer two
     * `seforim.db` files that didn't share an `IdAllocator` lineage —
     * surfacing them here gives the operator a clear, actionable error
     * instead of a mid-transaction crash in [PatchApplier].
     */
    private fun assertNoSecondaryUniqueCollisions(conn: Connection) {
        for (table in PATCH_TABLES_IN_FK_ORDER) {
            if (!tableExists(conn, "new", table.name)) continue
            if (!tableExists(conn, "prev", table.name)) continue
            if (!tableExists(conn, "main", "upsert_${table.name}")) continue
            val pkCols = table.primaryKey
            if (pkCols.isEmpty()) continue
            val uniqueGroups = readSecondaryUniqueGroups(conn, "new", table.name, pkCols.toSet())
            for (uniqueCols in uniqueGroups) {
                val firstUnique = uniqueCols.first()
                val joinUnique = uniqueCols.joinToString(" AND ") { "new.\"$it\" = prev.\"$it\"" }
                val pkDiffers = pkCols.joinToString(" OR ") {
                    "new.\"$it\" IS NOT prev.\"$it\""
                }
                val selectCols = (pkCols + uniqueCols).joinToString(",") { "new.\"$it\" AS new_$it" } +
                    "," + pkCols.joinToString(",") { "prev.\"$it\" AS prev_$it" }
                val sql = """
                    SELECT $selectCols
                    FROM main."upsert_${table.name}" AS new
                    JOIN prev."${table.name}" AS prev ON $joinUnique
                    WHERE prev."$firstUnique" IS NOT NULL AND ($pkDiffers)
                    LIMIT 1
                """.trimIndent()
                conn.createStatement().use { st ->
                    st.executeQuery(sql).use { rs ->
                        if (rs.next()) {
                            val meta = rs.metaData
                            val sample = (1..meta.columnCount).joinToString(", ") {
                                "${meta.getColumnLabel(it)}=${rs.getObject(it)}"
                            }
                            error(
                                "Secondary UNIQUE collision detected in '${table.name}' on " +
                                    "(${uniqueCols.joinToString(", ")}): a row exists in prev " +
                                    "with a different PK than the row being upserted. " +
                                    "This usually means prev and new were generated from " +
                                    "different build_state.db lineages. Sample: $sample",
                            )
                        }
                    }
                }
            }
        }
    }

    /**
     * Reads `PRAGMA index_list` / `PRAGMA index_info` for a table and
     * returns the column-name sets of every secondary (non-PK) UNIQUE
     * index. Auto-indexes named `sqlite_autoindex_<table>_1` that back
     * the PRIMARY KEY are filtered out.
     */
    private fun readSecondaryUniqueGroups(
        conn: Connection,
        schemaAlias: String,
        table: String,
        pkColSet: Set<String>,
    ): List<List<String>> {
        val out = ArrayList<List<String>>()
        val indexNames = ArrayList<String>()
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA $schemaAlias.index_list(\"$table\")").use { rs ->
                while (rs.next()) {
                    val unique = rs.getInt("unique") == 1
                    if (unique) indexNames += rs.getString("name")
                }
            }
        }
        for (idx in indexNames) {
            val cols = ArrayList<String>()
            conn.createStatement().use { st ->
                st.executeQuery("PRAGMA $schemaAlias.index_info(\"$idx\")").use { rs ->
                    while (rs.next()) cols += rs.getString("name")
                }
            }
            if (cols.isEmpty()) continue
            // Skip the index that backs the primary key.
            if (cols.toSet() == pkColSet) continue
            out += cols
        }
        return out
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
