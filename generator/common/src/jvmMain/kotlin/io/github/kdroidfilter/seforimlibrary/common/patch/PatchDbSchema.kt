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
 *
 * The upsert / delete table shapes are derived dynamically from the target
 * seforim.db schema attached as `prev` (or `new`) at producer time — that way
 * we don't have to maintain a hand-written DDL for every column.
 */
internal object PatchDbSchema {

    const val CURRENT_VERSION: Int = 1

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

    /** Schema info for a single column. */
    data class ColumnInfo(val name: String, val type: String, val notNull: Boolean)

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
                    )
                }
            }
        }
        return out
    }
}
