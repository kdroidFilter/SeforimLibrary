package io.github.kdroidfilter.seforimlibrary.common.notes

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.common.notes.LineNoteRehydration.NoteAt
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.sql.DriverManager
import kotlin.test.assertEquals

class LineNoteRehydrationTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `stripping and putting back gives the original line`() {
        val lines = listOf(
            "א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">גוף <i>מוטה</i></i> ב",
            "<sup class=\"footnote-marker\">*</sup> <i class=\"footnote\">x</i><i class=\"footnote\">y</i>&amp; סוף",
            "<h2>כותרת<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">ה</i></h2>",
            "טקסט בסוף<i class=\"footnote\">נספח</i>",
            "א<sup class=\"footnote-marker\">1<i class=\"footnote\"></i></sup><i class=\"footnote\">הערה זרזיר</i> ב",
            "א<sup class=\"footnote-marker\"><b>1</b></sup><i class=\"footnote\">הערה זרזיר</i> ב",
            "א<sup class=\"footnote-marker\">1<i class=\"footnote\"></i></sup><i class=\"footnote\">ראשונה</i><i class=\"footnote\">שנייה</i> ב",
            "<i class=\"footnote\">לפני</i><sup>1</sup> סוף",
            "<sup class=\"footnote-marker\"></sup><i class=\"footnote\">ראשונה</i><i class=\"footnote\">שנייה</i> סוף",
            "<p><b>א</b><i class=\"footnote\">הערה</i></p>המשך",
            "<p>סוף<i class=\"footnote\">הערה</i></p>",
            "<p><b><sup class=\"footnote-marker\"></sup><i class=\"footnote\">הערה זרזיר</i>המשך</b></p>",
            "<p><b><sup class=\"footnote-marker\"></sup><i class=\"footnote\">ראשונה</i><i class=\"footnote\">שנייה</i>המשך</b></p>",
            "<i class=\"footnote\">לפני</i><p><b><sup>1</sup>המשך</b></p>",
        )
        val path = tmp.newFolder().toPath().resolve("seforim.db")
        JdbcSqliteDriver("jdbc:sqlite:${path.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 's')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'c')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'b')")
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content) VALUES (?, 1, ?, ?)").use { ps ->
                lines.forEachIndexed { i, l -> ps.setLong(1, 100L + i); ps.setInt(2, i); ps.setString(3, l); ps.executeUpdate() }
            }
            rebuildLineNoteIndex(conn, Logger.withTag("LineNoteRehydrationTest"))
            lines.forEachIndexed { i, original ->
                val stored = conn.createStatement().use { st ->
                    st.executeQuery("SELECT content FROM line WHERE id = ${100 + i}").use { rs -> rs.next(); rs.getString(1) }
                }
                val notes = conn.createStatement().use { st ->
                    st.executeQuery("SELECT charPos, body, marker FROM line_note WHERE lineId = ${100 + i} ORDER BY noteIndex").use { rs ->
                        buildList { while (rs.next()) add(NoteAt(rs.getInt(1), rs.getString(2), rs.getString(3) != null)) }
                    }
                }
                if (original.startsWith("<sup class=\"footnote-marker\"></sup>")) {
                    assertEquals(listOf(true, false), notes.map { it.hasMarker }, "empty SQL marker is anchored")
                    assertEquals(listOf(0, 0), notes.map { it.charPos }, "adjacent bodies share a position")
                }
                if (original.startsWith("<p><b><sup class=\"footnote-marker\"></sup>")) {
                    assertEquals(List(notes.size) { 0 }, notes.map { it.charPos })
                    assertEquals(listOf(true) + List(notes.size - 1) { false }, notes.map { it.hasMarker })
                }
                assertEquals(original, LineNoteRehydration.rehydrate(stored, notes).text)
            }
        }
    }

    @Test
    fun `a body crosses inline closings and stays before block boundaries`() {
        fun back(stored: String, charPos: Int) = LineNoteRehydration.rehydrate(stored, listOf(NoteAt(charPos, "ה"))).text
        assertEquals("<b>מילה</b><i class=\"footnote\">ה</i> ועוד", back("<b>מילה</b> ועוד", 4))
        assertEquals("<sup>1</sup><i class=\"footnote\">ה</i><b>א</b>", back("<sup>1</sup><b>א</b>", 1))
        assertEquals("<b>סוף</b><i class=\"footnote\">ה</i><br>", back("<b>סוף</b><br>", 3))
        for (block in listOf("p", "DIV", "li", "td", "custom-block")) {
            for (tail in listOf("", "המשך")) {
                assertEquals(
                    "<$block><b><span>אב</span></b><i class=\"footnote\">ה</i></$block>$tail",
                    back("<$block><b><span>אב</span></b></$block>$tail", 2),
                )
            }
        }
        assertEquals("<B>אב</B ><i class=\"footnote\">ה</i></p>", back("<B>אב</B ></p>", 2))
        assertEquals("<b>אב<i class=\"footnote\">ה</i></ b></p>", back("<b>אב</ b></p>", 2))
        assertEquals(countVisibleChars("<b>מילה</b> ועוד") + 1, countVisibleChars(back("<b>מילה</b> ועוד", 4)))
    }

    @Test
    fun `empty markers and unanchored bodies respect sup boundaries and note order`() {
        val body = "<i class=\"footnote\">ה</i>"
        for (marker in listOf(
            "<sup class=\"footnote-marker\">1<i class=\"footnote\"></i></sup>" to 1,
            "<sup class=\"footnote-marker\"><b>1</b></sup>" to 1,
            "<sup class=\"footnote-marker\"></sup>" to 0,
        )) {
            val notes = listOf(NoteAt(marker.second, "ה", true), NoteAt(marker.second, "ה"))
            assertEquals(marker.first + body + body, LineNoteRehydration.rehydrate(marker.first, notes).text)
            if (marker.second > 0) {
                assertEquals(
                    marker.first + body,
                    LineNoteRehydration.rehydrate(marker.first, listOf(NoteAt(marker.second, "ה"))).text,
                    "a raw position strictly inside any sup is outside it even without marker metadata",
                )
            }
        }
        assertEquals(
            body + "<sup>1</sup>",
            LineNoteRehydration.rehydrate("<sup>1</sup>", listOf(NoteAt(0, "ה"))).text,
        )
        assertEquals(
            "<b>א<sup class=\"footnote-marker\"></sup></b>" + body,
            LineNoteRehydration.rehydrate(
                "<b>א<sup class=\"footnote-marker\"></sup></b>", listOf(NoteAt(1, "ה", true)),
            ).text,
        )
        val marker = "<sup class=\"footnote-marker\"></sup>"
        val wrapped = "<p><b>${marker}המשך</b></p>"
        assertEquals(
            "<p><b>$marker$body${body}המשך</b></p>",
            LineNoteRehydration.rehydrate(wrapped, listOf(NoteAt(0, "ה", true), NoteAt(0, "ה"))).text,
        )
        assertEquals(body + wrapped, LineNoteRehydration.rehydrate(wrapped, listOf(NoteAt(0, "ה"))).text)
        for (gap in listOf(" ", "&nbsp;", "א", "<broken")) {
            val stored = "<p>$gap${marker}המשך</p>"
            assertEquals(
                body + stored,
                LineNoteRehydration.rehydrate(stored, listOf(NoteAt(0, "ה", true))).text,
                "anchored metadata cannot skip visible text, entities, whitespace or incomplete tags",
            )
        }
    }

    @Test
    fun `stored ranges skip the inserted bodies and refuse ranges that touch one`() {
        val back = LineNoteRehydration.rehydrate("אב גד", listOf(NoteAt(2, "xx")))
        val body = back.bodies.single()
        assertEquals(2 until 2 + "<i class=\"footnote\">xx</i>".length, body)
        assertEquals(0 to 2, back.storedRange(0, 2))
        assertEquals(null, back.storedRange(1, body.first + 1))
        assertEquals(2 to 5, back.storedRange(body.last + 1, back.text.length))
    }
}
