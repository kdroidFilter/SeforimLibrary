package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * A library file saved with a UTF-8 BOM (51 MoreBooks/ToratEmet books in v30):
 * `readText` keeps it as U+FEFF, so line 0 was `"﻿<h1>…"`, the heading was
 * not detected and the book's title never reached the TOC. The reader strips a
 * leading BOM from book files, companion notes and JSON inputs.
 */
class OtzariaBomTest {

    private val bom = "﻿"

    private data class ImportedBook(val toc: List<TocEntry>, val lines: List<Line>)

    private fun newRepo(): SeforimRepository {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        return SeforimRepository(":memory:", driver)
    }

    private suspend fun importBook(sourceDir: Path, allocator: InMemoryIdAllocator): ImportedBook {
        val repo = newRepo()
        try {
            writeFilesManifest(sourceDir)
            DatabaseGenerator(sourceDirectory = sourceDir, repository = repo, allocator = allocator)
                .generateLinesOnly()
            val book = repo.getAllBooks().single { it.title == "ספר" }
            return ImportedBook(repo.getBookToc(book.id), repo.getLines(book.id, 0, Int.MAX_VALUE))
        } finally {
            repo.close()
        }
    }

    private fun <T> withBook(text: String, action: suspend (Path) -> T): T = runBlocking {
        val sourceDir = Files.createTempDirectory("otzaria-bom")
        try {
            val bookDir = Files.createDirectories(sourceDir.resolve("אוצריא").resolve("מוסר"))
            Files.writeString(bookDir.resolve("ספר.txt"), text)
            action(sourceDir)
        } finally {
            sourceDir.toFile().deleteRecursively()
        }
    }

    private val bookLines = listOf("<h1>ספר</h1>", "<h2>פרק א</h2>", "טקסט", "<h2>פרק ב</h2>", "עוד טקסט")

    /** TOC as (text, level, parent text) — the tree the app shows. */
    private fun ImportedBook.tree(): List<Triple<String, Int, String?>> {
        val textById = toc.associate { it.id to it.text }
        return toc.map { Triple(it.text, it.level, it.parentId?.let(textById::get)) }
    }

    @Test
    fun bomBeforeH1NoLongerHidesTheTitleFromTheToc() {
        val withBom = withBook(bom + bookLines.joinToString("\n")) { importBook(it, InMemoryIdAllocator.load(null)) }
        val withoutBom = withBook(bookLines.joinToString("\n")) { importBook(it, InMemoryIdAllocator.load(null)) }

        assertEquals(
            listOf(
                Triple("ספר", 1, null),
                Triple("פרק א", 2, "ספר"),
                Triple("פרק ב", 2, "ספר"),
            ),
            withBom.tree(),
        )
        assertEquals("<h1>ספר</h1>", withBom.lines.first().content)
        assertNull(withBom.lines.first().heRef, "a heading line carries no heRef, like every other h1")
        assertTrue(withBom.lines.none { it.content.contains(bom) })
        // Byte-for-byte the same book as the one saved without a BOM.
        assertEquals(withoutBom, withBom)
    }

    @Test
    fun aBomInsideTheTextIsNotABomAndStays() {
        val text = listOf("<h1>ספר</h1>", "טקסט ${bom}באמצע", "${bom}שורה").joinToString("\n")
        val book = withBook(text) { importBook(it, InMemoryIdAllocator.load(null)) }
        assertEquals(listOf("<h1>ספר</h1>", "טקסט ${bom}באמצע", "${bom}שורה"), book.lines.map { it.content })
    }

    @Test
    fun lineZeroKeepsTheIdABomCarryingBuildGaveIt() {
        withBook(bom + bookLines.joinToString("\n")) { sourceDir ->
            val allocator = InMemoryIdAllocator.load(null)
            val first = importBook(sourceDir, allocator)
            val state = sourceDir.resolve("allocator.db")
            allocator.snapshotTo(state, mapOf("run" to "bom-test"))

            // Rewrite the seed the way a pre-fix build left it: line 0 keyed on
            // its BOM-carrying content.
            val stripped = IdAllocatorBindings.normalisedContentHash(bookLines[0])
            val legacy = IdAllocatorBindings.normalisedContentHash(bom + bookLines[0])
            DriverManager.getConnection("jdbc:sqlite:$state").use { conn ->
                conn.prepareStatement("UPDATE id_line SET content_hash = ? WHERE content_hash = ?").use { ps ->
                    ps.setBytes(1, legacy)
                    ps.setBytes(2, stripped)
                    assertEquals(1, ps.executeUpdate())
                }
            }

            val rebuilt = importBook(sourceDir, InMemoryIdAllocator.load(state))
            assertEquals(first, rebuilt, "every line, TOC and text id must survive, line 0 included")
        }
    }

    @Test
    fun bomInCompanionNotesAndLinksJsonStillMerges() = runBlocking {
        val sourceDir = Files.createTempDirectory("otzaria-bom-hearot")
        try {
            val bookDir = Files.createDirectories(sourceDir.resolve("אוצריא").resolve("מוסר"))
            Files.writeString(bookDir.resolve("ספר.txt"), "$bom<h1>ספר</h1>\nגוף<sup>1</sup> המשך")
            Files.writeString(bookDir.resolve("הערות על ספר.txt"), "$bom<sup>1</sup> גוף ההערה")
            val linksDir = Files.createDirectories(sourceDir.resolve("links"))
            Files.writeString(
                linksDir.resolve("ספר_links.json"),
                bom + """[{"line_index_1": 2, "heRef_2": "הערות", "path_2": "הערות על ספר.txt",
                    |"line_index_2": 1, "Conection Type": "commentary"}]""".trimMargin(),
            )
            val repo = newRepo()
            try {
                val generator = DatabaseGenerator(sourceDirectory = sourceDir, repository = repo)
                generator.generateLinesOnly()
                generator.generateLinksOnly()
                val base = assertNotNull(repo.getAllBooks().find { it.title == "ספר" })
                assertNull(repo.getAllBooks().find { it.title == "הערות על ספר" }, "companion must merge")
                assertEquals(
                    listOf(
                        "<h1>ספר</h1>",
                        "גוף<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">גוף ההערה</i> המשך",
                    ),
                    repo.getLines(base.id, 0, Int.MAX_VALUE).map { it.content },
                )
                assertEquals(listOf("ספר"), repo.getBookToc(base.id).map { it.text })
            } finally {
                repo.close()
            }
        } finally {
            sourceDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun bomInLinksAndAltTocJsonIsParsed() = runBlocking {
        val repo = newRepo()
        val sourceId = repo.insertSource("Tashma")
        val catId = repo.insertCategory(Category(0, null, "הלכה", level = 0, order = 1))
        fun book(id: Long, title: String) = Book(
            id = id, categoryId = catId, sourceId = sourceId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = 2, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "מקור"))
        repo.insertBook(book(2, "מפרש"))
        repo.insertLinesBatch(
            listOf(
                Line(id = 100, bookId = 1, lineIndex = 0, content = "א", heRef = "מקור 1"),
                Line(id = 101, bookId = 1, lineIndex = 1, content = "ב", heRef = "מקור 2"),
                Line(id = 200, bookId = 2, lineIndex = 0, content = "ג", heRef = "מפרש 1"),
                Line(id = 201, bookId = 2, lineIndex = 1, content = "ד", heRef = "מפרש 2"),
            )
        )
        val sourceDir = Files.createTempDirectory("otzaria-bom-json")
        try {
            val linksDir = Files.createDirectories(sourceDir.resolve("links"))
            Files.writeString(
                linksDir.resolve("מקור_links.json"),
                bom + """[{"line_index_1": 2, "heRef_2": "מפרש 2", "path_2": "מפרש.txt",
                    |"line_index_2": 2, "Conection Type": "commentary"}]""".trimMargin(),
            )
            val altTocDir = Files.createDirectories(sourceDir.resolve("alt_toc"))
            Files.writeString(
                altTocDir.resolve("מקור_alt_toc.json"),
                bom + """[{"key": "Chapters", "heTitle": "פרקים", "nodes": [{"heTitle": "פרק", "line": 1}]}]""",
            )

            DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

            assertEquals(1, repo.getLinkIdsBetweenLines(101, 201).size)
            assertEquals(listOf("פרקים"), repo.getAltTocStructuresForBook(1).map { it.heTitle })
        } finally {
            repo.close()
            sourceDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun stripperOnlyTouchesALeadingMark() {
        val clean = listOf("<h1>א</h1>", "ב")
        assertSame(clean, Utf8Bom.stripFirstLine(clean))
        assertEquals(emptyList(), Utf8Bom.stripFirstLine(emptyList()))
        assertEquals(listOf("<h1>א</h1>", "${bom}ב"), Utf8Bom.stripFirstLine(listOf("$bom<h1>א</h1>", "${bom}ב")))
        assertEquals(listOf(""), Utf8Bom.stripFirstLine(listOf(bom)))
        assertEquals("[]", Utf8Bom.strip("$bom[]"))
        assertEquals("[$bom]", Utf8Bom.strip("[$bom]"))
        assertEquals("$bom[]", Utf8Bom.strip("$bom$bom[]"), "only one leading mark is a BOM")
        assertEquals("$bom<h1>א</h1>", Utf8Bom.legacyFirstLineContent("<h1>א</h1>"))
    }
}
