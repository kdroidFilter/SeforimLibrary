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
                    st.executeQuery("SELECT charPos, body FROM line_note WHERE lineId = ${100 + i} ORDER BY noteIndex").use { rs ->
                        buildList { while (rs.next()) add(NoteAt(rs.getInt(1), rs.getString(2))) }
                    }
                }
                assertEquals(original, LineNoteRehydration.rehydrate(stored, notes).text)
            }
        }
    }

    @Test
    fun `a body goes right after its visible char, past a sup close, or at the very end`() {
        fun back(stored: String, charPos: Int) = LineNoteRehydration.rehydrate(stored, listOf(NoteAt(charPos, "ה"))).text
        assertEquals("<b>מילה<i class=\"footnote\">ה</i></b> ועוד", back("<b>מילה</b> ועוד", 4))
        assertEquals("<sup>1</sup><i class=\"footnote\">ה</i><b>א</b>", back("<sup>1</sup><b>א</b>", 1))
        assertEquals("<b>סוף</b><br><i class=\"footnote\">ה</i>", back("<b>סוף</b><br>", 3))
        assertEquals(countVisibleChars("<b>מילה</b> ועוד") + 1, countVisibleChars(back("<b>מילה</b> ועוד", 4)))
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
