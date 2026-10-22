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

/** ForDB book_banners.csv / book_protection.csv → banner + book_banner / book_protection. */
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

        val defaults = rows.filter { it.bookName.isEmpty() }
        val bySource = defaults.associateBy { it.sourceName }
        assertEquals(setOf("BeitAharonVeYisraelToOtzaria", "National-LibraryToOtzaria", "wikiJewishBooksToOtzaria"), bySource.keys)
        assertEquals(listOf(1L, 2L, 3L), defaults.map { it.value.id })
        // Per-book wiki rows: the default wording with an explicit, encoded page path for {title}.
        val overrides = rows - defaults.toSet()
        assertEquals((4L..12L).toList(), overrides.map { it.value.id })
        val wikiText = bySource.getValue("wikiJewishBooksToOtzaria").value.text
        val wikiPrefix = "https://wiki.jewishbooks.org.il/mediawiki/wiki/"
        val linkTarget = Regex("""\]\(([^)\s]+)\)""")
        for (row in overrides) {
            assertEquals("wikiJewishBooksToOtzaria", row.sourceName)
            val url = linkTarget.find(row.value.text)!!.groupValues[1]
            val page = url.removePrefix(wikiPrefix)
            assertTrue(page != url && wikiPrefix !in page && "{title}" !in page, url)
            assertEquals(wikiText.replace("{title}", page), row.value.text)
        }
        assertEquals(
            "כל הזכויות שמורות למכון בית אהרן וישראל שע\"י מרכז סטאלין קארלין.\n" +
                "אין להעתיק ולשכפל בכל צורה שהיא ללא אישור מפורש בכתב מהמו\"ל",
            bySource.getValue("BeitAharonVeYisraelToOtzaria").value.text,
        )
        assertContains(bySource.getValue("wikiJewishBooksToOtzaria").value.text, "\n")
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
    fun `a ForDB archive without either notices file fails instead of seeding empty tables`() {
        val files = mapOf(
            BOOK_BANNERS_FILE to resourceLines("book_banners.csv"),
            BOOK_PROTECTION_FILE to resourceLines("book_protection.csv"),
        )
        assertEquals(12, loadBookNoticeRows(logger) { requiredForDbFile(files, it) }.banners.size)
        for (missing in files.keys) {
            val error = assertFailsWith<IllegalStateException> {
                loadBookNoticeRows(logger) { requiredForDbFile(files - missing, it) }
            }
            assertContains(error.message.orEmpty(), "is missing required file $missing")
        }
    }

    @Test
    fun `malformed files fail early`() {
        assertFailsWith<IllegalArgumentException> { parseBookProtection("\"a\",\"b\",\"c\"\n") }
        assertFailsWith<IllegalArgumentException> { parseBookProtection("sourceName,bookName,level\nKSK,,0\n") }
        assertFailsWith<IllegalArgumentException> { parseBookProtection("sourceName,bookName,level\nKSK,,x\n") }
        assertFailsWith<IllegalArgumentException> { parseBookProtection("sourceName,bookName,level\nKSK,,1\nKSK,,2\n") }
        assertFailsWith<IllegalArgumentException> { parseBookBanners("id,sourceName,bookName,text\n1,\"S\",\"\",\"\"\n") }
        assertFailsWith<IllegalArgumentException> { parseBookBanners("id,sourceName,bookName,text\n1,\"S\",\"\",\"open\n") }
        assertFailsWith<IllegalArgumentException> { parseBookBanners("sourceName,bookName,text\n\"S\",\"\",\"x\"\n") }
        for (id in listOf("", "0", "-1", " 1", "01", "1.0", "x")) {
            assertFailsWith<IllegalArgumentException>(id) { parseBookBanners("id,sourceName,bookName,text\n\"$id\",S,,x\n") }
        }
        val conflicting = assertFailsWith<IllegalArgumentException> {
            parseBookBanners("id,sourceName,bookName,text\n7,S,,x\n7,S,ב,y\n")
        }
        assertContains(conflicting.message.orEmpty(), "gives id(s) 7 more than one text")
        assertEquals(emptyList(), parseBookBanners(""))
    }

    @Test
    fun `source defaults expand per book and a per-book row overrides them`() {
        val conn = db()
        conn.use {
            val result = applyBookNotices(
                it,
                BookNoticeRows(
                    banners = listOf(
                        BookNoticeRow("Wiki", "", BannerText(5, "באדיבות\n[כאן](https://w.org/{title})")),
                        BookNoticeRow("Wiki", "ספר ב", BannerText(2, "מיוחד ל{title}")),
                        BookNoticeRow("Other", "חסר", BannerText(9, "לא יימצא")),
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
            // Texts stay verbatim under their CSV ids; only texts some book uses are kept.
            assertEquals(
                listOf(listOf<Any?>(2, "מיוחד ל{title}"), listOf<Any?>(5, "באדיבות\n[כאן](https://w.org/{title})")),
                rows(it, "SELECT id, text FROM banner ORDER BY id"),
            )
            assertEquals(
                listOf(listOf<Any?>(1, 5), listOf<Any?>(2, 2)),
                rows(it, "SELECT bookId, bannerId FROM book_banner ORDER BY bookId"),
            )
            assertEquals(
                listOf(listOf<Any?>(3, 1), listOf<Any?>(4, 2)),
                rows(it, "SELECT bookId, level FROM book_protection ORDER BY bookId"),
            )
        }
    }

    @Test
    fun `books that share a text share one banner row`() {
        val shared = parseBookBanners(
            "id,sourceName,bookName,text\n" +
                "\"8\",\"Wiki\",\"ספר א\",\"כל הזכויות\nשמורות\"\n" +
                "\"8\",\"Wiki\",\"ספר ב\",\"כל הזכויות\nשמורות\"\n",
        )
        db().use {
            applyBookNotices(it, BookNoticeRows(shared, emptyList()), logger)
            assertEquals(listOf(listOf<Any?>(8, "כל הזכויות\nשמורות")), rows(it, "SELECT id, text FROM banner"))
            assertEquals(
                listOf(listOf<Any?>(1, 8), listOf<Any?>(2, 8)),
                rows(it, "SELECT bookId, bannerId FROM book_banner ORDER BY bookId"),
            )
        }
    }

    @Test
    fun `a rerun replaces the rows and empty inputs leave the tables empty`() {
        db().use {
            val input = BookNoticeRows(listOf(BookNoticeRow("Wiki", "", BannerText(1, "x"))), listOf(BookNoticeRow("KSK", "", 1)))
            applyBookNotices(it, input, logger)
            applyBookNotices(it, input, logger)
            assertEquals(listOf(listOf<Any?>(1, "x")), rows(it, "SELECT * FROM banner"))
            assertEquals(listOf(listOf<Any?>(1, 1), listOf<Any?>(2, 1)), rows(it, "SELECT * FROM book_banner"))
            applyBookNotices(it, BookNoticeRows(emptyList(), emptyList()), logger)
            assertEquals(emptyList(), rows(it, "SELECT * FROM banner"))
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
        val plain = "id,sourceName,bookName,text\n\"4\",\"S\",\"\",\"שורה א\nשורה ב\"\n"
        val expected = listOf(BookNoticeRow("S", "", BannerText(4, "שורה א\nשורה ב")))
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
