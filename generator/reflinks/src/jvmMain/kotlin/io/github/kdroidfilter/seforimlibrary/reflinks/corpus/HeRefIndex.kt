package io.github.kdroidfilter.seforimlibrary.reflinks.corpus

import io.github.kdroidfilter.seforimlibrary.reflinks.Citation
import io.github.kdroidfilter.seforimlibrary.reflinks.Family
import io.github.kdroidfilter.seforimlibrary.reflinks.HebrewText
import io.github.kdroidfilter.seforimlibrary.reflinks.Section
import java.sql.Connection
import java.sql.DriverManager

/** A resolved target line. */
data class LineRef(val bookId: Long, val lineId: Long, val lineIndex: Int, val heRef: String)

/**
 * Read-only access to seforim.db for citation resolution: `heRef` addresses → first line, Zohar folios and Bavli
 * chapter names from the alt TOCs, and line snippets. Book indexes are built lazily and kept in memory.
 */
class HeRefIndex(dbPath: String) : AutoCloseable {

    private val db: Connection = DriverManager.getConnection("jdbc:sqlite:file:$dbPath?mode=ro&immutable=1")

    val bookIds: Map<String, Long> = db.createStatement().use { st ->
        st.executeQuery("SELECT id, title FROM book").use { rs ->
            buildMap { while (rs.next()) put(rs.getString(2), rs.getLong(1)) }
        }
    }
    val bookTitles: Map<Long, String> = bookIds.entries.associate { (k, v) -> v to k }

    private val books = HashMap<String, Map<List<Section>, LineRef>>()

    /** Section tuple (and every prefix of it) → first line with that address. */
    @Synchronized
    fun book(title: String): Map<List<Section>, LineRef> = books.getOrPut(title) {
        val bookId = bookIds[title] ?: return@getOrPut emptyMap()
        val index = HashMap<List<Section>, LineRef>()
        db.prepareStatement("SELECT id, lineIndex, heRef FROM line WHERE bookId = ? AND heRef IS NOT NULL ORDER BY lineIndex").use { ps ->
            ps.setLong(1, bookId)
            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    val heRef = rs.getString(3)
                    if (!heRef.startsWith(title)) continue
                    val sections = heRef.substring(title.length).trim().trimStart(',').split(',')
                        .map { it.trim() }.filter { it.isNotEmpty() }.map(::section)
                    val ref = LineRef(bookId, rs.getLong(1), rs.getInt(2), heRef)
                    for (k in 1..sections.size) index.putIfAbsent(sections.subList(0, k).toList(), ref)
                }
            }
        }
        index
    }

    fun resolve(citation: Citation): LineRef? = when {
        citation.family == Family.ZOHAR_DAF -> zohar[citation.zoharVolume to citation.zoharDaf]
        citation.sections.isEmpty() -> null
        else -> book(citation.title)[citation.sections]
    }

    /** First [words] words of a line, as plain text (no tags, no nikud). */
    fun snippet(lineId: Long, words: Int = 30): String =
        db.prepareStatement("SELECT content FROM line WHERE id = ?").use { ps ->
            ps.setLong(1, lineId)
            ps.executeQuery().use { rs -> if (rs.next()) HebrewText.plain(rs.getString(1)).split(' ').take(words).joinToString(" ") else "" }
        }

    /** (volume, folio) → first line, from the Zohar's Vilna-pagination alt TOC. */
    private val zohar: Map<Pair<Int?, Section.Daf?>, LineRef> by lazy {
        val zoharId = bookIds["ספר הזהר"] ?: return@lazy emptyMap()
        val sql = """
            WITH RECURSIVE t(id, vol) AS (
              SELECT e.id, tt.text FROM alt_toc_entry e JOIN tocText tt ON tt.id = e.textId
                JOIN alt_toc_structure s ON s.id = e.structureId
               WHERE s.bookId = ? AND e.level = 0 AND tt.text LIKE 'כרך %'
              UNION ALL SELECT e.id, t.vol FROM alt_toc_entry e JOIN t ON e.parentId = t.id)
            SELECT t.vol, tt.text, l.id, l.lineIndex, l.heRef FROM t
              JOIN alt_toc_entry e ON e.id = t.id JOIN tocText tt ON tt.id = e.textId
              JOIN line l ON l.id = e.lineId
             WHERE tt.text LIKE 'דף %'
        """.trimIndent()
        val map = HashMap<Pair<Int?, Section.Daf?>, LineRef>()
        db.prepareStatement(sql).use { ps ->
            ps.setLong(1, zoharId)
            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    val vol = mapOf("כרך א" to 1, "כרך ב" to 2, "כרך ג" to 3)[rs.getString(1)] ?: continue
                    val daf = section(rs.getString(2).removePrefix("דף ").trim()) as? Section.Daf ?: continue
                    map.putIfAbsent(vol to daf, LineRef(zoharId, rs.getLong(3), rs.getInt(4), rs.getString(5) ?: ""))
                }
            }
        }
        map
    }

    /** Bavli chapter names ("כל כתבי", "מרובה") → tractate, from the 'Chapters' alt TOC; ambiguous names dropped. */
    val bavliChapterNames: Map<String, String> by lazy {
        val sql = """
            SELECT tt.text, b.title FROM alt_toc_entry e JOIN alt_toc_structure s ON s.id = e.structureId
              JOIN book b ON b.id = s.bookId JOIN tocText tt ON tt.id = e.textId WHERE s.key = 'Chapters'
        """.trimIndent()
        val seen = HashMap<String, MutableSet<String>>()
        db.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                while (rs.next()) seen.getOrPut(HebrewText.nameKey(rs.getString(1))) { mutableSetOf() } += rs.getString(2)
            }
        }
        seen.filterValues { it.size == 1 }.mapValues { it.value.single() }
    }

    fun rambamTitles(): List<String> = bookIds.keys.filter { it.startsWith("משנה תורה, הלכות") }

    /** Streams every line of the corpus in book order. */
    fun forEachLine(block: (bookId: Long, lineId: Long, content: String) -> Unit) {
        DriverManager.getConnection(db.metaData.url).use { c ->
            c.createStatement().use { st ->
                st.fetchSize = 10_000
                st.executeQuery("SELECT bookId, id, content FROM line ORDER BY bookId, lineIndex").use { rs ->
                    while (rs.next()) block(rs.getLong(1), rs.getLong(2), rs.getString(3))
                }
            }
        }
    }

    override fun close() = db.close()

    companion object {
        private val dafToken = Regex("^([א-ת]+)([.:])$")

        /** heRef token → section: "כא." folio side, "קצג" number, anything else a name (nikud removed). */
        fun section(token: String): Section {
            dafToken.find(token)?.let { m ->
                HebrewText.gematria(m.groupValues[1])?.let {
                    return Section.Daf(it, if (m.groupValues[2] == ".") Section.Side.A else Section.Side.B)
                }
            }
            if (token.all { it in 'א'..'ת' }) HebrewText.gematria(token)?.let { return Section.Num(it) }
            return Section.Name(HebrewText.stripNikud(token).trim())
        }
    }
}
