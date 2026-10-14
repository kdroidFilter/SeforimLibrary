package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import java.sql.Connection
import java.security.MessageDigest

/**
 * Computes a **logical** sha256 hash of a seforim.db file (DELTA_UPDATE_PLAN.md §3.7).
 *
 * SQLite does not guarantee that the byte layout of two files is identical
 * even when the row contents are; page order, fragmentation, sqlite_sequence
 * and (post-Phase 1) reused-id holes all break a naive `sha256(file)`. The
 * logical hash dumps each tracked table ordered by its primary key, encodes
 * every cell with an explicit type tag, and feeds the stream into sha256.
 *
 * Two builds whose row contents are semantically identical produce identical
 * logical hashes. The hash is the source of truth for:
 *  - `from_content_hash` / `to_content_hash` in `manifest.json`
 *  - the `verifyApplyChain` CI gate (prev + patch == new ⟺ hashes match)
 *  - the client's post-apply self-check.
 */
class LogicalContentHasher(
    private val tables: List<String> = DEFAULT_TABLES,
    private val logger: Logger = Logger.withTag("LogicalContentHasher"),
) {

    /**
     * Whole-DB hash plus the per-table digest of exactly the bytes each table
     * contributed to it, in hash table order.
     */
    data class Report(
        val wholeHash: String,
        val tableHashes: LinkedHashMap<String, String>,
    )

    fun compute(conn: Connection): String = computeReport(conn).wholeHash

    /**
     * One pass, two digests: whole-DB and per-table (reset at each table boundary),
     * so the client can verify only the tables a patch could have touched.
     */
    fun computeReport(conn: Connection): Report {
        val whole = MessageDigest.getInstance("SHA-256")
        val table = MessageDigest.getInstance("SHA-256")
        val sink = DualDigest(whole, table)
        val tableHashes = LinkedHashMap<String, String>()
        for (t in tables) {
            sink.update(" table:$t ".toByteArray())
            val cols = readColumnsCanonical(conn, t)
            if (cols == null) { // table not present — its stream is just the prefix
                tableHashes[t] = hex(table.digest())
                continue
            }
            sink.update(cols.joinToString(",", prefix = "cols:").toByteArray())
            sink.update(byteArrayOf(0x00))

            val colsSql = cols.joinToString(",") { "\"$it\"" }
            val pkOrder = if ("id" in cols) "id" else cols.joinToString(",") { "\"$it\"" }
            conn.createStatement().use { st ->
                st.executeQuery("SELECT $colsSql FROM \"$t\" ORDER BY $pkOrder").use { rs ->
                    val n = rs.metaData.columnCount
                    while (rs.next()) {
                        for (i in 1..n) encodeCell(sink, rs, i)
                        sink.update(byteArrayOf(0xFF.toByte()))
                    }
                }
            }
            tableHashes[t] = hex(table.digest()) // digest() also resets for the next table
        }
        return Report(hex(whole.digest()), tableHashes)
    }

    /** Feeds the same bytes to the whole-DB digest and the current table's digest. */
    private class DualDigest(private val whole: MessageDigest, private val table: MessageDigest) {
        fun update(bytes: ByteArray) {
            whole.update(bytes)
            table.update(bytes)
        }
    }

    private fun hex(digest: ByteArray): String = digest.joinToString("") { "%02x".format(it) }

    private fun encodeCell(sink: DualDigest, rs: java.sql.ResultSet, i: Int) {
        val obj = rs.getObject(i)
        when {
            obj == null || rs.wasNull() -> sink.update(byteArrayOf(0))
            obj is ByteArray -> { sink.update(byteArrayOf(1)); sink.update(obj) }
            obj is Number -> { sink.update(byteArrayOf(2)); sink.update(obj.toString().toByteArray()) }
            else -> { sink.update(byteArrayOf(3)); sink.update(obj.toString().toByteArray()) }
        }
        sink.update(byteArrayOf(0x1F)) // unit separator between cells
    }

    private fun readColumnsCanonical(conn: Connection, table: String): List<String>? {
        val out = ArrayList<String>()
        conn.prepareStatement("PRAGMA table_info(\"$table\")").use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) out += rs.getString("name")
            }
        }
        return if (out.isEmpty()) null else out.sorted() // canonical order = alphabetical
    }

    companion object {
        /** Schema-2 hash contract. Never append future tables here. */
        val TABLES_SCHEMA_2: List<String> = listOf(
            "source",
            "author",
            "topic",
            "pub_place",
            "pub_date",
            "connection_type",
            "generation",
            "category",
            "category_closure",
            "tocText",
            "book",
            "book_topic",
            "book_author",
            "book_base_text",
            "book_pub_place",
            "book_pub_date",
            "book_generation",
            "tocEntry",
            "line",
            "line_toc",
            "link",
            "link_anchor",
            "link_range",
            "link_coverage",
            "book_has_links",
            "book_version",
            "version_line",
            "book_acronym",
            "alt_toc_structure",
            "alt_toc_entry",
            "line_alt_toc",
            "default_commentator",
            "default_targum",
            "schema_meta",
        )

        /** Schema-1 hash contract predates the book_base_text junction. */
        val TABLES_SCHEMA_1: List<String> = TABLES_SCHEMA_2.filterNot { it == "book_base_text" }

        /** Schema 3 adds the sparse per-side visibility table after link coverage. */
        val TABLES_SCHEMA_3: List<String> = TABLES_SCHEMA_2.toMutableList().apply {
            add(indexOf("link_coverage") + 1, "link_suppressed_side")
        }

        /**
         * Schema 4 adds the canonical line-reference index and the
         * dibbur-hamatchil index right after line_toc.
         */
        val TABLES_SCHEMA_4: List<String> = TABLES_SCHEMA_3.toMutableList().apply {
            add(indexOf("line_toc") + 1, "line_ref")
            add(indexOf("line_ref") + 1, "line_dh")
        }

        /** Schema 5 changes line_dh columns, not the set or order of tables. */
        val TABLES_SCHEMA_5: List<String> = TABLES_SCHEMA_4

        /**
         * Schema 6 moves line text into line_content, hashed right after line, and the
         * dictionary of its zstd frames (zstd_dict) right after that. zstd_dict joined
         * before any release shipped schema 6; from here on the list is frozen.
         */
        val TABLES_SCHEMA_6: List<String> = TABLES_SCHEMA_5.toMutableList().apply {
            add(indexOf("line") + 1, "line_content")
            add(indexOf("line_content") + 1, "zstd_dict")
        }

        /** Schema 7 adds the per-book notices and the per-line indexes right before schema_meta. */
        val TABLES_SCHEMA_7: List<String> = TABLES_SCHEMA_6.toMutableList().apply {
            addAll(indexOf("schema_meta"), listOf("book_banner", "book_protection", "line_divine_name", "line_note"))
        }

        /** Current-schema default for build-time diagnostics and current DB tests. */
        val DEFAULT_TABLES: List<String> = TABLES_SCHEMA_7

        fun tablesForSchemaVersion(schemaVersion: Int): List<String> = when (schemaVersion) {
            1 -> TABLES_SCHEMA_1
            2 -> TABLES_SCHEMA_2
            3 -> TABLES_SCHEMA_3
            4 -> TABLES_SCHEMA_4
            5 -> TABLES_SCHEMA_5
            6 -> TABLES_SCHEMA_6
            7 -> TABLES_SCHEMA_7
            else -> error("Unsupported logical-hash schema version $schemaVersion")
        }

        fun forSchemaVersion(
            schemaVersion: Int,
            logger: Logger = Logger.withTag("LogicalContentHasher"),
        ): LogicalContentHasher = LogicalContentHasher(tablesForSchemaVersion(schemaVersion), logger)
    }
}
