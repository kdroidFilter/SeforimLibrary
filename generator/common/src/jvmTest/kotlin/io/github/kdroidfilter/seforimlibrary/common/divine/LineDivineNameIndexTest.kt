package io.github.kdroidfilter.seforimlibrary.common.divine

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class LineDivineNameIndexTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val logger = Logger.withTag("LineDivineNameIndexTest")

    private fun db(lines: List<String>, heRefs: List<String?> = lines.map { null }, title: String = "ספר"): Path {
        val path = tmp.newFolder().toPath().resolve("seforim.db")
        JdbcSqliteDriver("jdbc:sqlite:${path.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'Sefaria')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'תנ\"ך')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title, isBaseBook) VALUES (1, 1, 1, '$title', 1)")
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content, heRef) VALUES (?, 1, ?, ?, ?)").use { ps ->
                lines.forEachIndexed { i, content ->
                    ps.setLong(1, 100L + i); ps.setInt(2, i); ps.setString(3, content); ps.setString(4, heRefs[i])
                    ps.executeUpdate()
                }
            }
        }
        return path
    }

    private fun rows(path: Path): List<List<Int>> = DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery("SELECT lineId, charStart, span FROM line_divine_name ORDER BY lineId, charStart").use { rs ->
                buildList { while (rs.next()) add(listOf(rs.getInt(1)) + decoded(rs.getInt(2), rs.getLong(3))) }
            }
        }
    }

    private fun span(start: Int, end: Int, kind: Int, source: Int): Int =
        DivineSpanCode.encode(start, end, DivineKind.entries.single { it.code == kind }, DivineSource.entries.single { it.code == source })

    /** (charStart, charEnd, kind, source) of a stored row. */
    private fun decoded(charStart: Int, span: Long): List<Int> =
        DivineSpanCode.decode(charStart, span).let { listOf(charStart, it.charEnd, it.kind.code, it.source.code) }

    @Test
    fun `rebuild writes visible-char spans and replaces the previous rows`() {
        val path = db(listOf("<b>א</b>&nbsp;ה' אלהינו", "ויגביהוהו", "שמע ישראל יהוה"))
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("INSERT INTO line_divine_name VALUES (101, 0, ${span(0, 1, 5, 3)})") }
            val report = rebuildLineDivineNameIndex(conn, logger, overrides = null, requireTanach = false)
            assertEquals(2L, report.total)
            assertEquals(1L, report.appHvyh)
        }
        assertEquals(
            listOf(listOf(100, 5, 11, DivineKind.ELOHIM.code, DivineSource.COLLOCATION.code), listOf(102, 10, 14, 1, 1)),
            rows(path),
        )
    }

    @Test
    fun `note bodies get their own spans, in offsets within the body, replacing the previous rows`() {
        val path = db(listOf("טקסט בלי שם", "שורה"))
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO line_note VALUES (100, 0, 4, 0, '<b>א</b>&nbsp;ה'' אלהינו'), (100, 1, 4, 1, 'שמע ישראל יהוה')")
                st.execute("INSERT INTO line_note VALUES (101, 0, 0, 0, 'בלי שם')")
                st.execute("INSERT INTO line_note_divine_name VALUES (101, 0, 0, ${span(0, 1, 5, 3)})")
            }
            val report = rebuildLineDivineNameIndex(conn, logger, overrides = null, requireTanach = false)
            assertEquals(0L, report.total)
            assertEquals(3L, report.noteBodies)
            assertEquals(2L, report.noteSpans)
        }
        val noteRows = { sql: String ->
            DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
                conn.createStatement().use { st ->
                    st.execute("PRAGMA foreign_keys = ON")
                    if (sql.isNotEmpty()) st.execute(sql)
                    st.executeQuery("SELECT * FROM line_note_divine_name ORDER BY lineId, noteIndex, charStart").use { rs ->
                        buildList { while (rs.next()) add(listOf(rs.getInt(1), rs.getInt(2)) + decoded(rs.getInt(3), rs.getLong(4))) }
                    }
                }
            }
        }
        assertEquals(
            listOf(
                listOf(100, 0, 5, 11, DivineKind.ELOHIM.code, DivineSource.COLLOCATION.code),
                listOf(100, 1, 10, 14, 1, 1),
            ),
            noteRows(""),
        )
        assertEquals(listOf(listOf(100, 1, 10, 14, 1, 1)), noteRows("DELETE FROM line_note WHERE lineId = 100 AND noteIndex = 0"))
    }

    @Test
    fun `a library without every tanach book is refused and the old table survives`() {
        val path = db(listOf("ה' אלהינו"))
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("INSERT INTO line_divine_name VALUES (100, 0, ${span(0, 1, 1, 1)})") }
            assertFailsWith<IllegalStateException> { rebuildLineDivineNameIndex(conn, logger, overrides = null) }
        }
        assertEquals(listOf(listOf(100, 0, 1, 1, 1)), rows(path))
    }

    @Test
    fun `a pre-pack table shape is refused by name and keeps its rows`() {
        val path = db(listOf("x"))
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("DROP TABLE line_divine_name")
                st.execute(
                    "CREATE TABLE line_divine_name (lineId INTEGER NOT NULL, charStart INTEGER NOT NULL, " +
                        "charEnd INTEGER NOT NULL, kind INTEGER NOT NULL, source INTEGER NOT NULL, " +
                        "PRIMARY KEY (lineId, charStart)) WITHOUT ROWID",
                )
                st.execute("INSERT INTO line_divine_name VALUES (100, 0, 1, 1, 1)")
            }
            val failure = assertFailsWith<IllegalStateException> {
                rebuildLineDivineNameIndex(conn, logger, overrides = null, requireTanach = false)
            }
            assertTrue("pre-pack" in failure.message.orEmpty(), failure.message)
            conn.createStatement().use { st ->
                st.executeQuery("SELECT count(*) FROM line_divine_name").use { rs -> assertEquals(1, rs.getInt(1)) }
            }
        }
    }

    @Test
    fun `the report opens the DB read-only and leaves it byte-identical`() {
        val path = db(listOf("ה' אלהינו"))
        val before = Files.readAllBytes(path)
        val out = tmp.newFolder().toPath()
        openReadOnly(path).use { conn ->
            assertFailsWith<Exception> { conn.createStatement().use { it.execute("DELETE FROM line") } }
            // The report requires the full Tanach, which this tiny library lacks.
            assertFailsWith<IllegalStateException> { writeDivineNameReport(conn, out, logger, overrides = null) }
        }
        assertTrue(before.contentEquals(Files.readAllBytes(path)))
    }

    @Test
    fun `tanach verses resolve from heRef`() {
        val path = db(listOf("x"), listOf("בראשית, יט, יח"), title = "בראשית")
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            assertEquals(mapOf(1L to "בראשית"), resolveTanachBooks(conn, requireComplete = false))
        }
    }
}
