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

    fun compute(conn: Connection): String {
        val md = MessageDigest.getInstance("SHA-256")
        for (table in tables) {
            md.update(" table:$table ".toByteArray())
            val cols = readColumnsCanonical(conn, table) ?: continue // table not present
            md.update(cols.joinToString(",", prefix = "cols:").toByteArray())
            md.update(byteArrayOf(0x00))

            val colsSql = cols.joinToString(",") { "\"$it\"" }
            val pkOrder = if ("id" in cols) "id" else cols.joinToString(",") { "\"$it\"" }
            conn.createStatement().use { st ->
                st.executeQuery("SELECT $colsSql FROM \"$table\" ORDER BY $pkOrder").use { rs ->
                    val meta = rs.metaData
                    val n = meta.columnCount
                    while (rs.next()) {
                        for (i in 1..n) encodeCell(md, rs, i)
                        md.update(byteArrayOf(0xFF.toByte()))
                    }
                }
            }
        }
        val digest = md.digest()
        return digest.joinToString("") { "%02x".format(it) }
    }

    private fun encodeCell(md: MessageDigest, rs: java.sql.ResultSet, i: Int) {
        val obj = rs.getObject(i)
        when {
            obj == null || rs.wasNull() -> md.update(byteArrayOf(0))
            obj is ByteArray -> { md.update(byteArrayOf(1)); md.update(obj) }
            obj is Number -> { md.update(byteArrayOf(2)); md.update(obj.toString().toByteArray()) }
            else -> { md.update(byteArrayOf(3)); md.update(obj.toString().toByteArray()) }
        }
        md.update(byteArrayOf(0x1F)) // unit separator between cells
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
        /** Tables tracked by the logical-hash. Order matters (foreign-key topology). */
        val DEFAULT_TABLES: List<String> = listOf(
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
            "book_pub_place",
            "book_pub_date",
            "book_generation",
            "tocEntry",
            "line",
            "line_toc",
            "link",
            "book_has_links",
            "book_acronym",
            "alt_toc_structure",
            "alt_toc_entry",
            "line_alt_toc",
            "default_commentator",
            "default_targum",
            "schema_meta",
        )
    }
}
