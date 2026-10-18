package io.github.kdroidfilter.seforimlibrary.common.notes

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.patch.compressLineContent
import io.github.kdroidfilter.seforimlibrary.common.patch.splitLineContent
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LineNoteIndexTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val logger = Logger.withTag("LineNoteIndexTest")

    // Visible: "אב" + sup "1" + note "גד" + "הו" + note "זחט" + "יכ"; the notes cover [3,5) and [7,10).
    private val noted = "אב<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">ג<i>ד</i></i>הו" +
        "<i class=\"footnote\">זחט</i>יכ"

    @Test
    fun `bodies move to line_note, ids stay, charCount is recounted`() {
        val db = db(listOf(noted, "בלי הערות", "<h2>כותרת<sup class=\"footnote-marker\">*</sup><i class=\"footnote\">ה</i></h2>"))
        val report = connect(db).use { rebuildLineNoteIndex(it, logger) }

        assertEquals(2, report.linesChanged)
        assertEquals(3, report.notes)
        assertEquals(1, report.unanchoredNotes)
        val stripped = "אב<sup class=\"footnote-marker\">1</sup>הויכ"
        assertEquals(
            listOf(
                listOf<Any?>(100, stripped, countVisibleChars(stripped)),
                listOf<Any?>(101, "בלי הערות", 0),
                listOf<Any?>(102, "<h2>כותרת<sup class=\"footnote-marker\">*</sup></h2>", 6),
            ),
            query(db, "SELECT id, content, charCount FROM line ORDER BY id"),
        )
        assertEquals(
            listOf(
                listOf<Any?>(100, 0, 3, "1", "ג<i>ד</i>"),
                listOf<Any?>(100, 1, 5, null, "זחט"),
                listOf<Any?>(102, 0, 6, "*", "ה"),
            ),
            query(db, "SELECT lineId, noteIndex, charPos, marker, body FROM line_note ORDER BY lineId, noteIndex"),
        )
    }

    @Test
    fun `charPos is the visible offset of the stripped line right after the marker sup`() {
        val line = "&amp;<b>א</b><sup class=\"footnote-marker\">12</sup> <i class=\"footnote\">x</i>" +
            "<i class=\"footnote\">y</i>ב<sup>3</sup><i class=\"footnote\">z</i>"
        val db = db(listOf(line))
        connect(db).use { rebuildLineNoteIndex(it, logger) }
        val stripped = query(db, "SELECT content FROM line").single().single() as String
        val positions = query(db, "SELECT charPos FROM line_note ORDER BY noteIndex").map { it.single() as Int }
        // "&amp;" counts 1, "א" 1, "12" 2, the space 1; the second note sits where the first one ended.
        assertEquals(listOf(5, 5, 7), positions)
        val sups = Regex("</sup>").findAll(stripped).map { countVisibleChars(stripped, it.range.last + 1) }.toList()
        assertEquals(listOf(4, 7), sups)
    }

    @Test
    fun `the k-th marker sup left in the line belongs to the k-th note with a marker`() {
        val line = "<sup class=\"footnote-marker\">א</sup><i class=\"footnote\">1</i> <i class=\"footnote\">2</i>" +
            " <sup class=\"footnote-marker\">ב</sup><i class=\"footnote\">3</i>"
        val db = db(listOf(line))
        connect(db).use { rebuildLineNoteIndex(it, logger) }
        val content = query(db, "SELECT content FROM line").single().single() as String
        val sups = Regex("<sup class=\"footnote-marker\">(.*?)</sup>").findAll(content).map { it.groupValues[1] }.toList()
        val markers = query(db, "SELECT marker FROM line_note WHERE marker IS NOT NULL ORDER BY noteIndex").map { it.single() }
        assertEquals(listOf("א", "ב"), sups)
        assertEquals(sups, markers)
    }

    @Test
    fun `anchors after a removed body shift, anchors inside one are dropped`() {
        val db = db(listOf(noted, "יעד"))
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO connection_type(id, name) VALUES (1, 'COMMENTARY')")
                st.execute(
                    "INSERT INTO link(id, sourceBookId, targetBookId, sourceLineId, targetLineId, targetLineIndex, " +
                        "targetBookOrderIndex, connectionTypeId) VALUES (1, 1, 1, 100, 101, 1, 0, 1), (2, 1, 1, 101, 100, 0, 0, 1)",
                )
                // side 0 of link 1 and side 1 of link 2 are on line 100; side 1 of link 1 is on line 101.
                st.execute(
                    "INSERT INTO link_anchor(linkId, side, charStart, charEnd, label) VALUES " +
                        "(1, 0, 1, NULL, 'a'), (1, 0, 3, NULL, NULL), (1, 0, 4, NULL, NULL), (1, 0, 5, NULL, NULL), " +
                        "(1, 0, 6, 12, NULL), (1, 0, 8, 11, NULL), (1, 0, 11, NULL, NULL), (1, 1, 5, NULL, NULL), " +
                        "(2, 1, 7, 10, NULL), (2, 1, 2, 11, NULL), (2, 1, 4, 8, NULL), (2, 0, 7, NULL, NULL)",
                )
            }
            val report = rebuildLineNoteIndex(conn, logger)
            assertEquals(5, report.anchorsRemapped)
            assertEquals(2, report.anchorsClipped, "ranges with one end in a body are cut back to its edge")
            assertEquals(2, report.anchorsDroppedInBody, "a point inside a body, a range wholly inside one")
            assertEquals(1, report.anchorsDroppedOnCollision, "5 shifts onto the untouched anchor at 3")
        }
        assertEquals(
            listOf(
                listOf<Any?>(1, 0, 1, null, "a"),
                listOf<Any?>(1, 0, 3, null, null),
                listOf<Any?>(1, 0, 4, 7, null),
                listOf<Any?>(1, 0, 5, 6, null),
                listOf<Any?>(1, 0, 6, null, null),
                listOf<Any?>(1, 1, 5, null, null),
                listOf<Any?>(2, 0, 7, null, null),
                listOf<Any?>(2, 1, 2, 6, null),
                listOf<Any?>(2, 1, 3, 5, null),
            ),
            query(db, "SELECT linkId, side, charStart, charEnd, label FROM link_anchor ORDER BY linkId, side, charStart"),
        )
    }

    @Test
    fun `an unclosed body or a marker without a note leaves the line inline and reported`() {
        val unclosed = "א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">ב <i>ג</i>"
        val orphan = "א<sup class=\"footnote-marker\">1</sup> ב<i class=\"footnote\">ג</i>"
        val db = db(listOf(unclosed, orphan))
        val report = connect(db).use { rebuildLineNoteIndex(it, logger) }
        assertEquals(1, report.unclosedLines)
        assertEquals(1, report.orphanMarkerLines)
        assertEquals(0, report.linesChanged)
        assertEquals(listOf(listOf<Any?>(unclosed), listOf<Any?>(orphan)), query(db, "SELECT content FROM line ORDER BY id"))
        assertTrue(query(db, "SELECT * FROM line_note").isEmpty())
    }

    @Test
    fun `the pre-filter is as broad as the parser and counts lines without a note`() {
        val upper = "א<SUP CLASS = \"footnote-marker\">1</SUP><I CLASS = \"footnote\">גוף</I>"
        val db = db(listOf(upper, "המילה footnote בטקסט", "<sup class=\"footnote-marker\">1</sup>&nbsp;<i class=\"footnote\">ב</i>"))
        val report = connect(db).use { rebuildLineNoteIndex(it, logger) }
        assertEquals(3, report.linesScanned)
        assertEquals(1, report.linesWithoutNote)
        assertEquals(2, report.linesChanged)
        assertEquals(0, report.orphanMarkerLines, "a no-break space keeps the marker adjacent")
        assertEquals(
            listOf(listOf<Any?>(100, 0, 2, "1", "גוף"), listOf<Any?>(102, 0, 2, "1", "ב")),
            query(db, "SELECT lineId, noteIndex, charPos, marker, body FROM line_note ORDER BY lineId"),
        )
    }

    @Test
    fun `a plain sup before a body becomes its classed marker without moving visible text`() {
        val line = "א<sup>1</sup><i class=\"footnote\">גוף</i>ב <sup>2</sup> ג"
        val db = db(listOf(line, "יעד"))
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO connection_type(id, name) VALUES (1, 'COMMENTARY')")
                st.execute(
                    "INSERT INTO link(id, sourceBookId, targetBookId, sourceLineId, targetLineId, targetLineIndex, " +
                        "targetBookOrderIndex, connectionTypeId) VALUES (1, 1, 1, 100, 101, 1, 0, 1)",
                )
                // Visible "א1" + "גוף" + "ב 2 ג": the anchor at 1 sits before the sup text.
                st.execute("INSERT INTO link_anchor(linkId, side, charStart, charEnd) VALUES (1, 0, 1, NULL)")
            }
            val report = rebuildLineNoteIndex(conn, logger)
            assertEquals(0, report.orphanMarkerLines)
            assertEquals(0, report.unanchoredNotes)
        }
        val stripped = "א<sup class=\"footnote-marker\">1</sup>ב <sup>2</sup> ג"
        assertEquals(listOf(listOf<Any?>(stripped, countVisibleChars(stripped))), query(db, "SELECT content, charCount FROM line WHERE id = 100"))
        assertEquals(countVisibleChars("א<sup>1</sup>ב <sup>2</sup> ג"), countVisibleChars(stripped))
        assertEquals(listOf(listOf<Any?>("1", "גוף")), query(db, "SELECT marker, body FROM line_note"))
        assertEquals(listOf(listOf<Any?>(1, null)), query(db, "SELECT charStart, charEnd FROM link_anchor"))
    }

    @Test
    fun `a second run changes nothing`() {
        val db = db(listOf(noted))
        connect(db).use { rebuildLineNoteIndex(it, logger) }
        val before = query(db, "SELECT * FROM line_note") to query(db, "SELECT content, charCount FROM line")
        val again = connect(db).use { rebuildLineNoteIndex(it, logger) }
        assertEquals(0, again.linesChanged)
        assertEquals(before, query(db, "SELECT * FROM line_note") to query(db, "SELECT content, charCount FROM line"))
    }

    @Test
    fun `on the split shape the line_content row changes and inherited versions keep the old text`() {
        val db = db(listOf(noted, "שני"), versions = listOf(100L to noted, 101L to "שונה"))
        connect(db).use { splitLineContent(it) }
        assertEquals(listOf(listOf<Any?>(null), listOf<Any?>("שונה")), query(db, "SELECT content FROM version_line ORDER BY lineId"))

        val report = connect(db).use { rebuildLineNoteIndex(it, logger) }
        assertEquals(1, report.versionRowsMaterialized)
        assertEquals("אב<sup class=\"footnote-marker\">1</sup>הויכ", query(db, "SELECT content FROM line_content WHERE id = 100").single().single())
        assertEquals(listOf(listOf<Any?>(noted), listOf<Any?>("שונה")), query(db, "SELECT content FROM version_line ORDER BY lineId"))
    }

    @Test
    fun `compressed line text is refused`() {
        val db = db(listOf(noted))
        connect(db).use { conn ->
            splitLineContent(conn)
            compressLineContent(conn, threads = 1)
            val error = assertFailsWith<IllegalStateException> { rebuildLineNoteIndex(conn, logger) }
            assertTrue("compressLineContent" in error.message.orEmpty())
        }
    }

    private fun db(lines: List<String>, versions: List<Pair<Long, String>> = emptyList()): Path {
        val path = tmp.newFolder().toPath().resolve("seforim.db")
        JdbcSqliteDriver("jdbc:sqlite:${path.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        connect(path).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'Sefaria')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'cat')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'ספר')")
                st.execute("INSERT INTO book_version(id, bookId, versionTitle, hasContent) VALUES (1, 1, 'A', 1)")
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content, charCount) VALUES (?, 1, ?, ?, 0)").use { ps ->
                lines.forEachIndexed { i, content ->
                    ps.setLong(1, 100L + i); ps.setInt(2, i); ps.setString(3, content); ps.executeUpdate()
                }
            }
            conn.prepareStatement("INSERT INTO version_line(versionId, lineId, content) VALUES (1, ?, ?)").use { ps ->
                for ((lineId, content) in versions) {
                    ps.setLong(1, lineId); ps.setString(2, content); ps.executeUpdate()
                }
            }
        }
        return path
    }

    private fun connect(p: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${p.toAbsolutePath()}")

    private fun query(p: Path, sql: String): List<List<Any?>> = connect(p).use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                val n = rs.metaData.columnCount
                buildList { while (rs.next()) add((1..n).map { rs.getObject(it) }) }
            }
        }
    }
}
