package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SefariaParallelCodeLinksTest {
    private fun range(type: ConnectionType, citation: String, other: String) =
        turShulchanArukhSimanRange(type, canonicalCitation(citation), canonicalCitation(other))

    @Test
    fun widensTheShulchanArukhSideToItsWholeSiman() {
        assertEquals(
            CitationRange(
                startCanonical = "shulchan arukh orach chayim 93:1",
                endCanonical = "shulchan arukh orach chayim 93",
            ),
            range(ConnectionType.COMMENTARY, "Shulchan Arukh, Orach Chayim 93:1", "Tur, Orach Chayim 93:1"),
        )
        // Choshen Mishpat: Sefaria already cites the whole Tur siman as a range.
        assertEquals(
            CitationRange(
                startCanonical = "shulchan arukh choshen mishpat 100:1",
                endCanonical = "shulchan arukh choshen mishpat 100",
            ),
            range(ConnectionType.COMMENTARY, "Shulchan Arukh, Choshen Mishpat 100:1", "Tur, Choshen Mishpat 100:1-7"),
        )
    }

    @Test
    fun leavesEveryOtherRowAlone() {
        val sa = "Shulchan Arukh, Orach Chayim 93:1"
        assertNull(range(ConnectionType.COMMENTARY, "Tur, Orach Chayim 93:1", sa))
        assertNull(range(ConnectionType.RELATED, sa, "Tur, Orach Chayim 93:1"))
        assertNull(range(ConnectionType.COMMENTARY, "Shulchan Arukh, Orach Chayim 93:3", "Tur, Orach Chayim 93:1"))
        assertNull(range(ConnectionType.COMMENTARY, sa, "Tur, Orach Chayim 94:1"))
        assertNull(range(ConnectionType.COMMENTARY, sa, "Tur, Orach Chayim 931:1"))
        assertNull(range(ConnectionType.COMMENTARY, sa, "Tur, Yoreh De'ah 93:1"))
        assertNull(range(ConnectionType.COMMENTARY, sa, "Beit Yosef, Orach Chayim 93:1"))
    }

    @Test
    fun turSimanTieIsStoredAsRelatedOverTheWholeShulchanArukhSiman() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-tur-shulchan-arukh")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"Shulchan Arukh, Orach Chayim 1:1","Tur, Orach Chayim 1:1",commentary
            """.trimMargin()
        )

        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        fun book(id: Long, title: String, lines: Int) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = true, totalLines = lines, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "טור", lines = 2))
        repo.insertBook(book(2, "שולחן ערוך, אורח חיים", lines = 4))
        repo.insertLinesBatch(
            listOf(
                Line(id = 1, bookId = 1, lineIndex = 0, content = "tur 1", heRef = "טור, אורח חיים, א, א"),
                Line(id = 2, bookId = 1, lineIndex = 1, content = "tur 2", heRef = "טור, אורח חיים, ב, א"),
                Line(id = 10, bookId = 2, lineIndex = 0, content = "sa 1:1", heRef = "שולחן ערוך, אורח חיים, א, א"),
                Line(id = 11, bookId = 2, lineIndex = 1, content = "sa 1:2", heRef = "שולחן ערוך, אורח חיים, א, ב"),
                Line(id = 12, bookId = 2, lineIndex = 2, content = "sa 1:3", heRef = "שולחן ערוך, אורח חיים, א, ג"),
                Line(id = 13, bookId = 2, lineIndex = 3, content = "sa 2:1", heRef = "שולחן ערוך, אורח חיים, ב, א"),
            )
        )

        val turPath = "Tur, Orach Chayim"
        val saPath = "Shulchan Arukh, Orach Chayim"
        val refs = listOf(
            RefEntry("Tur, Orach Chayim 1:1", "", turPath, 1),
            RefEntry("Tur, Orach Chayim 2:1", "", turPath, 2),
            RefEntry("Shulchan Arukh, Orach Chayim 1:1", "", saPath, 1),
            RefEntry("Shulchan Arukh, Orach Chayim 1:2", "", saPath, 2),
            RefEntry("Shulchan Arukh, Orach Chayim 1:3", "", saPath, 3),
            RefEntry("Shulchan Arukh, Orach Chayim 2:1", "", saPath, 4),
        )
        val refsByBase = mutableMapOf<String, RefEntry>()
        refs.forEach { e ->
            val base = canonicalBase(e.ref)
            val existing = refsByBase[base]
            if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
        }
        val lineKeyToId = mapOf(
            turPath to 0 to 1L, turPath to 1 to 2L,
            saPath to 0 to 10L, saPath to 1 to 11L, saPath to 2 to 12L, saPath to 3 to 13L,
        )
        val lineIdToBookId = lineKeyToId.values.associateWith { if (it < 10L) 1L else 2L }
        // The Tur ranks first, which made it the Shulchan Arukh's "base" under COMMENTARY.
        val bookMeta = mapOf(
            1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
            2L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 1),
        )

        val bindings = io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings(
            io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator.load(path = null),
            repo,
        )
        SefariaLinksImporter(repo, bindings, Logger.withTag("SefariaParallelCodeLinksTest")).processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = refs.groupBy { canonicalCitation(it.ref) },
            refsByBase = refsByBase,
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMeta,
            headingLineIds = emptySet(),
            refsByPath = refs.groupBy { it.path },
        )

        fun query(sql: String): List<List<String>> =
            driver.getConnection().createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    val out = mutableListOf<List<String>>()
                    while (rs.next()) out += (1..rs.metaData.columnCount).map { rs.getString(it) }
                    out
                }
            }

        assertEquals(
            listOf(listOf("10", "1", "RELATED")),
            query(
                "SELECT l.sourceLineId, l.targetLineId, c.name FROM link l " +
                    "JOIN connection_type c ON c.id = l.connectionTypeId"
            ),
        )
        assertEquals(listOf(listOf("0", "12")), query("SELECT side, endLineId FROM link_range"))
        assertEquals(
            listOf(listOf("11", "0"), listOf("12", "0")),
            query("SELECT lineId, side FROM link_coverage ORDER BY lineId"),
        )
        repo.close()
    }
}
