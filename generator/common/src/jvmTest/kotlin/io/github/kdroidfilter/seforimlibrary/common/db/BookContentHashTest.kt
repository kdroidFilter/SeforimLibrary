package io.github.kdroidfilter.seforimlibrary.common.db

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.patch.compressLineContent
import io.github.kdroidfilter.seforimlibrary.common.patch.splitLineContent
import io.github.kdroidfilter.seforimlibrary.common.patch.stampSchemaVersion
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class BookContentHashTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `the value of a fixed book is pinned`() {
        connect(freshDb()).use { conn ->
            assertEquals(GOLDEN_BOOK_1, BookContentHash.compute(conn, 1))
            assertEquals(GOLDEN_EMPTY_BOOK, BookContentHash.compute(conn, 3))
        }
    }

    @Test
    fun `split and compression keep every value`() {
        connect(freshDb()).use { conn ->
            val before = hashes(conn)
            splitLineContent(conn)
            assertEquals(before, hashes(conn))
            compressLineContent(conn, threads = 1)
            assertTrue(LineContentCompression.isCompressed(conn))
            assertEquals(before, hashes(conn))
        }
    }

    @Test
    fun `any change in a line or a note of the book changes only its value`() {
        val edits = listOf(
            "UPDATE line SET content = replace(content, 'ראשונה', 'ראשונה.') WHERE id = 10",
            "UPDATE line SET lineIndex = 5 WHERE id = 11",
            "DELETE FROM line WHERE id = 11",
            "INSERT INTO line (id, bookId, lineIndex, content) VALUES (13, 1, 2, '')",
            "UPDATE line_note SET body = 'גוף אחר' WHERE lineId = 10 AND noteIndex = 0",
            "UPDATE line_note SET charPos = 3 WHERE lineId = 10 AND noteIndex = 0",
            "UPDATE line SET content = replace(content, '>1</sup>', '></sup>') WHERE id = 10",
            "DELETE FROM line_note WHERE lineId = 10 AND noteIndex = 1",
        )
        val base = connect(freshDb()).use { hashes(it) }
        for (edit in edits) {
            val changed = connect(freshDb()).use { conn ->
                conn.createStatement().use { it.execute(edit) }
                hashes(conn)
            }
            assertNotEquals(base.getValue(1), changed.getValue(1), edit)
            assertEquals(base - 1, changed - 1, edit)
        }
    }

    @Test
    fun `a note's marker is read back from its line, and unpaired sups or flags fail`() {
        val unpaired = listOf(
            "UPDATE line_note SET hasMarker = 0 WHERE lineId = 10 AND noteIndex = 0",
            "UPDATE line_note SET hasMarker = 1 WHERE lineId = 10 AND noteIndex = 1",
            "UPDATE line SET content = 'שורה בלי סימן' WHERE id = 10",
            "UPDATE line SET content = content || '<sup class=\"footnote-marker\">2</sup>' WHERE id = 10",
        )
        for (edit in unpaired) {
            connect(freshDb()).use { conn ->
                conn.createStatement().use { it.execute(edit) }
                val error = assertFailsWith<IllegalStateException>(edit) { BookContentHash.compute(conn, 1) }
                assertTrue("line 10" in error.message.orEmpty(), edit)
            }
        }
        connect(freshDb()).use { conn ->
            conn.createStatement().use { it.execute("UPDATE line_note SET hasMarker = 2 WHERE lineId = 10 AND noteIndex = 1") }
            assertFailsWith<IllegalStateException> { BookContentHash.compute(conn, 1) }
        }
    }

    @Test
    fun `ids and derived line columns are not inputs`() {
        val base = connect(freshDb()).use { hashes(it) }
        val changed = connect(freshDb()).use { conn ->
            conn.createStatement().use { st ->
                st.execute("UPDATE line SET heRef = 'אחר', charCount = 99, tocEntryId = NULL WHERE bookId = 1")
                st.execute("UPDATE book SET title = 'שם אחר' WHERE id = 1")
            }
            hashes(conn)
        }
        assertEquals(base, changed)
    }

    @Test
    fun `stamping schema 7 writes every book's value`() {
        connect(freshDb()).use { conn ->
            val expected = hashes(conn)
            splitLineContent(conn)
            stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 7)
            val stored = conn.createStatement().use { st ->
                st.executeQuery("SELECT id, contentHash FROM book ORDER BY id").use { rs ->
                    buildMap { while (rs.next()) put(rs.getLong(1), rs.getLong(2)) }
                }
            }
            assertEquals(expected, stored)
            assertTrue(stored.values.none { it == BookContentHash.UNKNOWN })
            assertTrue(conn.autoCommit)
        }
    }

    @Test
    fun `stamping schema 7 refuses a DB without the column`() {
        connect(freshDb()).use { conn ->
            splitLineContent(conn)
            conn.createStatement().use { it.execute("ALTER TABLE book DROP COLUMN contentHash") }
            val error = assertFailsWith<IllegalArgumentException> {
                stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 7)
            }
            assertTrue("book.contentHash" in error.message.orEmpty())
        }
    }

    private fun hashes(conn: Connection): Map<Long, Long> =
        listOf(1L, 2L, 3L).associateWith { BookContentHash.compute(conn, it) }

    /** Book 1 with two lines and two notes, book 2 with one line, book 3 empty; working (schema 5) shape. */
    private fun freshDb(): Path {
        val db = tmp.newFolder().toPath().resolve("seforim.db")
        JdbcSqliteDriver("jdbc:sqlite:${db.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO category (id, title) VALUES (1, 'תנ\"ך')")
                st.execute("INSERT INTO source (id, name) VALUES (1, 'otzaria')")
                st.execute(
                    "INSERT INTO book (id, categoryId, sourceId, title) VALUES " +
                        "(1, 1, 1, 'א'), (2, 1, 1, 'ב'), (3, 1, 1, 'ג')",
                )
                st.execute(
                    "INSERT INTO line (id, bookId, lineIndex, content, heRef) VALUES " +
                        "(10, 1, 0, '<h1>שורה</h1><sup class=\"footnote-marker\">1</sup> ראשונה', 'א א'), (11, 1, 1, 'שנייה <b>מודגשת</b>', 'א ב'), " +
                        "(12, 2, 0, 'שורה ראשונה', 'ב א')",
                )
                st.execute(
                    "INSERT INTO line_note (lineId, noteIndex, charPos, hasMarker, body) VALUES " +
                        "(10, 0, 4, 1, 'גוף <i>הערה</i>'), (10, 1, 9, 0, 'נספח')",
                )
            }
        }
        return db
    }

    private fun connect(p: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${p.toAbsolutePath()}")

    private companion object {
        // Computed outside Kotlin from the format in BookContentHash's KDoc (note 0's marker is "1").
        const val GOLDEN_BOOK_1 = -8217543383369417224L
        const val GOLDEN_EMPTY_BOOK = 6680358728597189694L
    }
}
