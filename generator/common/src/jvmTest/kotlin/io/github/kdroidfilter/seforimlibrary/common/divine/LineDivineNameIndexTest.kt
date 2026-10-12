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
            st.executeQuery("SELECT lineId, charStart, charEnd, kind, source FROM line_divine_name ORDER BY lineId, charStart").use { rs ->
                buildList { while (rs.next()) add((1..5).map { rs.getInt(it) }) }
            }
        }
    }

    @Test
    fun `rebuild writes visible-char spans and replaces the previous rows`() {
        val path = db(listOf("<b>א</b>&nbsp;ה' אלהינו", "ויגביהוהו", "שמע ישראל יהוה"))
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("INSERT INTO line_divine_name VALUES (101, 0, 1, 5, 3)") }
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
    fun `a library without every tanach book is refused and the old table survives`() {
        val path = db(listOf("ה' אלהינו"))
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("INSERT INTO line_divine_name VALUES (100, 0, 1, 1, 1)") }
            assertFailsWith<IllegalStateException> { rebuildLineDivineNameIndex(conn, logger, overrides = null) }
        }
        assertEquals(listOf(listOf(100, 0, 1, 1, 1)), rows(path))
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
