package io.github.kdroidfilter.seforimlibrary.common.patch

import java.sql.Connection

/**
 * DDL for the per-release `patch.db` artefact. Mirrors `DELTA_UPDATE_PLAN.md`
 * §5.3 — each `patch.db` contains:
 *
 *  - `patch_meta`     metadata (from_version, to_version, schema_version, …)
 *  - `migrations`     schema DDL executed BEFORE upserts
 *  - `blobs`          auxiliary payloads (catalog.pb, etc.) keyed by name
 *  - `upsert_<table>` one row per upserted row of the target table, with the
 *                     SAME columns + the target table's PK
 *  - `delete_<table>` one row per row to remove, with the target table's PK
 *  - `stat1_snapshot` the target DB's `sqlite_stat1` (optional, see [writeStat1Snapshot])
 *
 * The upsert / delete table shapes are derived dynamically from the target
 * seforim.db schema attached as `prev` (or `new`) at producer time — that way
 * we don't have to maintain a hand-written DDL for every column.
 */
internal object PatchDbSchema {

    const val CURRENT_VERSION: Int = 4

    /**
     * Planner statistics travel here, not in a format bump: released appliers only
     * read `patch_meta`, `migrations` and the `upsert_`/`delete_` tables they know.
     */
    const val STAT1_SNAPSHOT_TABLE: String = "stat1_snapshot"

    /** Fixed-shape tables (metadata + auxiliaries). */
    val baseStatements: List<String> = listOf(
        """
        CREATE TABLE IF NOT EXISTS patch_meta (
            key   TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS migrations (
            version INTEGER PRIMARY KEY NOT NULL,
            sql     TEXT NOT NULL
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS blobs (
            name    TEXT PRIMARY KEY NOT NULL,
            content BLOB NOT NULL
        )
        """.trimIndent(),
    )

    /**
     * Creates `upsert_<table>` mirroring the column list + PK from the source
     * DB referenced by [sourceSchemaAlias] (typically `new` or `main`).
     */
    fun createUpsertTable(
        conn: Connection,
        sourceSchemaAlias: String,
        target: PatchTable,
    ) {
        val cols = readTableInfo(conn, sourceSchemaAlias, target.name)
        if (cols.isEmpty()) return
        val colDdl = cols.joinToString(",\n            ") { c ->
            val nullPart = if (c.notNull) " NOT NULL" else ""
            "\"${c.name}\" ${c.type}$nullPart"
        }
        val pkClause = if (target.primaryKey.isNotEmpty()) {
            ",\n            PRIMARY KEY (${target.primaryKey.joinToString(",") { "\"$it\"" }})"
        } else ""
        conn.createStatement().use {
            it.execute("""
                CREATE TABLE IF NOT EXISTS "upsert_${target.name}" (
                    $colDdl$pkClause
                )
            """.trimIndent())
        }
    }

    /**
     * Creates `delete_<table>` with just the PK columns.
     */
    fun createDeleteTable(
        conn: Connection,
        sourceSchemaAlias: String,
        target: PatchTable,
    ) {
        if (target.primaryKey.isEmpty()) return
        val cols = readTableInfo(conn, sourceSchemaAlias, target.name)
        val pkColsInfo = cols.filter { it.name in target.primaryKey }
            .sortedBy { target.primaryKey.indexOf(it.name) }
        if (pkColsInfo.isEmpty()) return
        val ddl = pkColsInfo.joinToString(",\n            ") { c -> "\"${c.name}\" ${c.type} NOT NULL" }
        val pkClause = "PRIMARY KEY (${target.primaryKey.joinToString(",") { "\"$it\"" }})"
        conn.createStatement().use {
            it.execute("""
                CREATE TABLE IF NOT EXISTS "delete_${target.name}" (
                    $ddl,
                    $pkClause
                )
            """.trimIndent())
        }
    }

    /**
     * Copies `sqlite_stat1` of [sourceSchemaAlias] into [STAT1_SNAPSHOT_TABLE] and returns
     * the row count; 0 without creating the table when the source was never analyzed.
     */
    fun writeStat1Snapshot(conn: Connection, sourceSchemaAlias: String): Int {
        val analyzed = conn.prepareStatement(
            "SELECT 1 FROM $sourceSchemaAlias.sqlite_master WHERE type='table' AND name='sqlite_stat1'",
        ).use { ps -> ps.executeQuery().use { it.next() } }
        if (!analyzed) return 0
        return conn.createStatement().use { st ->
            st.execute("CREATE TABLE $STAT1_SNAPSHOT_TABLE (tbl TEXT NOT NULL, idx TEXT, stat TEXT NOT NULL)")
            st.executeUpdate(
                "INSERT INTO $STAT1_SNAPSHOT_TABLE (tbl, idx, stat) " +
                    "SELECT tbl, idx, stat FROM $sourceSchemaAlias.sqlite_stat1",
            )
        }
    }

    /**
     * Schema info for a single column.
     *
     * [defaultValue] is the raw `dflt_value` text from `PRAGMA table_info`,
     * i.e. an SQL literal already quoted the way the DDL declared it
     * (`'x'`, `0`, `NULL`, …) — or `null` when the column has no DEFAULT.
     * [PatchDbProducer] splices it verbatim into `ALTER TABLE … ADD COLUMN`.
     */
    data class ColumnInfo(
        val name: String,
        val type: String,
        val notNull: Boolean,
        val defaultValue: String? = null,
    )

    /**
     * Reads `PRAGMA <schema>.table_info(<table>)` and returns columns in
     * declaration order.
     */
    fun readTableInfo(conn: Connection, schemaAlias: String, table: String): List<ColumnInfo> {
        val out = ArrayList<ColumnInfo>()
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA $schemaAlias.table_info(\"$table\")").use { rs ->
                while (rs.next()) {
                    out += ColumnInfo(
                        name = rs.getString("name"),
                        type = rs.getString("type").ifBlank { "BLOB" },
                        notNull = rs.getInt("notnull") == 1,
                        defaultValue = rs.getString("dflt_value"),
                    )
                }
            }
        }
        return out
    }
}
