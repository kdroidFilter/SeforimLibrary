package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.StaticConfig
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** ForDB book_banners.csv / book_protection.csv → book_banner / book_protection. */
class SeedBookNoticesTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val logger = Logger(StaticConfig(Severity.Error, emptyList()), "test")

    /** The archive reader hands over physical lines, like readForDbZip. */
    private fun resourceLines(name: String): List<String> =
        javaClass.getResourceAsStream("/fordb_notices/$name")!!.bufferedReader(Charsets.UTF_8).readLines()

    @Test
    fun `the real banner file keeps its multi-line quoted texts`() {
        val rows = parseBookBanners(forDbText(resourceLines("book_banners.csv")))

        val bySource = rows.associateBy { it.sourceName }
        assertEquals(setOf("BeitAharonVeYisraelToOtzaria", "National-LibraryToOtzaria", "wikiJewishBooksToOtzaria"), bySource.keys)
        assertTrue(rows.all { it.bookName.isEmpty() })
        assertEquals(
            "כל הזכויות שמורות למכון בית אהרן וישראל שע\"י מרכז סטאלין קארלין.\n" +
                "אין להעתיק ולשכפל בכל צורה שהיא ללא אישור מפורש בכתב מהמו\"ל",
            bySource.getValue("BeitAharonVeYisraelToOtzaria").value,
        )
        assertContains(bySource.getValue("wikiJewishBooksToOtzaria").value, "\n")
    }

    @Test
    fun `the real protection file parses source defaults and per-book rows`() {
        val rows = parseBookProtection(forDbText(resourceLines("book_protection.csv")))
        assertEquals(
            listOf(
                BookNoticeRow("KSK", "", 1),
                BookNoticeRow("MoreBooks", "שמירת שבת כהלכתה - א", 1),
            ),
            rows,
        )
    }

    @Test
    fun `malformed files fail early`() {
        assertFailsWith<IllegalArgumentException> { parseBookProtection("\"a\",\"b\",\"c\"\n") }
        assertFailsWith<IllegalArgumentException> { parseBookProtection("sourceName,bookName,level\nKSK,,0\n") }
        assertFailsWith<IllegalArgumentException> { parseBookProtection("sourceName,bookName,level\nKSK,,x\n") }
        assertFailsWith<IllegalArgumentException> { parseBookProtection("sourceName,bookName,level\nKSK,,1\nKSK,,2\n") }
        assertFailsWith<IllegalArgumentException> { parseBookBanners("sourceName,bookName,text\n\"S\",\"\",\"\"\n") }
        assertFailsWith<IllegalArgumentException> { parseBookBanners("sourceName,bookName,text\n\"S\",\"\",\"open\n") }
        assertEquals(emptyList(), parseBookBanners(""))
    }

    @Test
    fun `title expands percent-encoded inside a link target and raw elsewhere`() {
        assertEquals(
            "על ספר א (ב) ב&ג [כאן](https://w.org/wiki/%D7%90%20%28%D7%91%29.html) [שוב](x?t=%D7%90%20%28%D7%91%29)",
            expandBannerTitle("על ספר {title} ב&ג [כאן](https://w.org/wiki/{title}.html) [שוב](x?t={title})", "א (ב)"),
        )
        assertEquals("[a](u?q=%23%3F%26%29)", expandBannerTitle("[a](u?q={title})", "#?&)"))
        assertEquals("ללא תבנית", expandBannerTitle("ללא תבנית", "x"))
    }

    @Test
    fun `source defaults expand per book and a per-book row overrides them`() {
        val conn = db()
        conn.use {
            val result = applyBookNotices(
                it,
                BookNoticeRows(
                    banners = listOf(
                        BookNoticeRow("Wiki", "", "באדיבות\n[כאן](https://w.org/{title})"),
                        BookNoticeRow("Wiki", "ספר ב", "מיוחד ל{title}"),
                        BookNoticeRow("Other", "חסר", "לא יימצא"),
                    ),
                    protections = listOf(
                        BookNoticeRow("KSK", "", 1),
                        BookNoticeRow("KSK", "קסק ב", 2),
                    ),
                ),
                logger,
            )
            assertEquals(listOf("Other / חסר"), result.unmatchedBanners)
            assertEquals(emptyList(), result.unmatchedProtection)
            assertEquals(
                listOf(
                    listOf<Any?>(1, "באדיבות\n[כאן](https://w.org/%D7%A1%D7%A4%D7%A8%20%D7%90)"),
                    listOf<Any?>(2, "מיוחד לספר ב"),
                ),
                rows(it, "SELECT bookId, text FROM book_banner ORDER BY bookId"),
            )
            assertEquals(
                listOf(listOf<Any?>(3, 1), listOf<Any?>(4, 2)),
                rows(it, "SELECT bookId, level FROM book_protection ORDER BY bookId"),
            )
        }
    }

    @Test
    fun `a rerun replaces the rows and empty inputs leave the tables empty`() {
        db().use {
            applyBookNotices(it, BookNoticeRows(listOf(BookNoticeRow("Wiki", "", "x")), listOf(BookNoticeRow("KSK", "", 1))), logger)
            applyBookNotices(it, BookNoticeRows(emptyList(), emptyList()), logger)
            assertEquals(emptyList(), rows(it, "SELECT * FROM book_banner"))
            assertEquals(emptyList(), rows(it, "SELECT * FROM book_protection"))
        }
    }

    @Test
    fun `a protection default for an unknown source fails the build and only warns at the ForDB gate`() {
        val input = BookNoticeRows(emptyList(), listOf(BookNoticeRow("Private", "", 1), BookNoticeRow("KSK", "חדש", 1)))
        db().use {
            val error = assertFailsWith<IllegalStateException> { applyBookNotices(it, input, logger) }
            assertContains(error.message.orEmpty(), "Private / *")
        }
        db().use {
            val result = applyBookNotices(it, input, logger, strict = false)
            assertEquals(listOf("Private / *", "KSK / חדש"), result.unmatchedProtection)
            assertEquals(emptyList(), rows(it, "SELECT * FROM book_protection"))
        }
    }

    @Test
    fun `a BOM and CRLF line endings do not change the parsed rows`() {
        val plain = "sourceName,bookName,text\n\"S\",\"\",\"שורה א\nשורה ב\"\n"
        val expected = listOf(BookNoticeRow("S", "", "שורה א\nשורה ב"))
        assertEquals(expected, parseBookBanners(forDbText(plain.lines())))
        // readForDbZip's readLines() strips CR; the BOM survives it and must go.
        val bomCrlf = "\uFEFF" + plain.replace("\n", "\r\n")
        assertEquals(expected, parseBookBanners(forDbText(bomCrlf.byteInputStream().bufferedReader().readLines())))
    }

    @Test
    fun `a protection row whose book is missing fails`() {
        db().use {
            val error = assertFailsWith<IllegalStateException> {
                applyBookNotices(it, BookNoticeRows(emptyList(), listOf(BookNoticeRow("KSK", "אין כזה", 1))), logger)
            }
            assertContains(error.message.orEmpty(), "KSK / אין כזה")
        }
    }

    private fun db(): Connection {
        System.setProperty(GeneratorReport.DIR_PROPERTY, tmp.newFolder().absolutePath)
        val conn = DriverManager.getConnection("jdbc:sqlite:${tmp.newFolder().toPath().resolve("s.db")}")
        conn.createStatement().use { st ->
            st.execute("CREATE TABLE source (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
            st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL, sourceId INTEGER NOT NULL)")
            st.execute("INSERT INTO source VALUES (1, 'Wiki'), (2, 'KSK')")
            st.execute("INSERT INTO book VALUES (1, 'ספר א', 1), (2, 'ספר ב', 1), (3, 'קסק א', 2), (4, 'קסק ב', 2)")
        }
        return conn
    }

    private fun rows(conn: Connection, sql: String): List<List<Any?>> = conn.createStatement().use { st ->
        st.executeQuery(sql).use { rs ->
            val n = rs.metaData.columnCount
            buildList { while (rs.next()) add((1..n).map { rs.getObject(it) }) }
        }
    }
}
