package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** A heading's TOC parent is the nearest preceding heading of a shallower level. */
class OtzariaTocParentTest {

    private data class ImportedBook(val toc: List<TocEntry>, val lines: List<Line>)

    // Each build after the last one, as in a release.
    private var builds = 0

    private suspend fun importBook(sourceDir: Path, allocator: InMemoryIdAllocator): ImportedBook {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
            writeFilesManifest(sourceDir)
            DatabaseGenerator(sourceDirectory = sourceDir, repository = repo, allocator = allocator, buildVersion = ++builds)
                .generateLinesOnly()
            val book = repo.getAllBooks().single { it.title == "ספר" }
            return ImportedBook(repo.getBookToc(book.id), repo.getLines(book.id, 0, Int.MAX_VALUE))
        } finally {
            repo.close()
        }
    }

    private fun <T> withSource(bookLines: String, action: suspend (Path) -> T): T = runBlocking {
        val sourceDir = Files.createTempDirectory("otzaria-toc-parent")
        try {
            val bookDir = Files.createDirectories(sourceDir.resolve("אוצריא").resolve("מוסר"))
            Files.writeString(bookDir.resolve("ספר.txt"), bookLines)
            action(sourceDir)
        } finally {
            sourceDir.toFile().deleteRecursively()
        }
    }

    private fun parentsOf(bookLines: String): Map<String, String?> = withSource(bookLines) { sourceDir ->
        val toc = importBook(sourceDir, InMemoryIdAllocator.load(null)).toc
        val textById = toc.associate { it.id to it.text }
        toc.associate { it.text to it.parentId?.let(textById::get) }
    }

    @Test
    fun levelSkipAfterShallowerHeadingDoesNotAdoptEarlierSectionHeading() {
        val parents = parentsOf(
            listOf(
                "<h1>ספר</h1>",
                "<h3>הערה</h3>",
                "טקסט",
                "<h2>פרק א</h2>",
                "<h4>ברכה</h4>",
                "טקסט",
                "<h2>פרק ב</h2>",
                "<h4>ניסן</h4>",
                "טקסט",
            ).joinToString("\n"),
        )

        assertEquals("ספר", parents["הערה"])
        assertEquals("פרק א", parents["ברכה"])
        assertEquals("פרק ב", parents["ניסן"])
    }

    @Test
    fun emptyShallowerHeadingStillClosesDeeperLevels() {
        val parents = parentsOf(
            listOf(
                "<h1>ספר</h1>",
                "<h3>הערה</h3>",
                "טקסט",
                "<h2></h2>",
                "<h4>ברכה</h4>",
                "טקסט",
            ).joinToString("\n"),
        )

        assertEquals("ספר", parents["ברכה"])
    }

    @Test
    fun correctedTreeFlagsAndPersistentIdsSurviveFreshRebuild() {
        val lines = listOf(
            "<h1>ספר</h1>", "<h3>הערה</h3>", "טקסט",
            "<h2>פרק א</h2>", "<h4>ברכה</h4>", "טקסט",
            "<h2></h2>", "<h5>סיום</h5>", "טקסט",
            "<h1>נספח</h1>", "<h6>תוספת</h6>", "טקסט",
        )
        withSource(lines.joinToString("\n")) { sourceDir ->
            val allocator = InMemoryIdAllocator.load(null)
            val first = importBook(sourceDir, allocator)
            val entries = first.toc.associateBy { it.text }
            val expectedChildren = mapOf(
                "ספר" to listOf("הערה", "פרק א", "סיום"),
                "פרק א" to listOf("ברכה"),
                "נספח" to listOf("תוספת"),
            )
            for ((parent, children) in expectedChildren) {
                val parentEntry = entries.getValue(parent)
                assertTrue(parentEntry.hasChildren, parent)
                for ((index, child) in children.withIndex()) {
                    val childEntry = entries.getValue(child)
                    assertEquals(parentEntry.id, childEntry.parentId, child)
                    assertEquals(index == children.lastIndex, childEntry.isLastChild, child)
                }
            }
            for (leaf in listOf("הערה", "ברכה", "סיום", "תוספת")) {
                assertFalse(entries.getValue(leaf).hasChildren, leaf)
            }
            assertFalse(entries.getValue("ספר").isLastChild)
            assertTrue(entries.getValue("נספח").isLastChild)
            assertEquals(lines.indices.filter { it != 6 }, first.lines.map { it.lineIndex })
            val state = sourceDir.resolve("allocator.db")
            allocator.snapshotTo(state, mapOf("run" to "toc-parent-test"))
            val rebuilt = importBook(sourceDir, InMemoryIdAllocator.load(state))
            assertEquals(first, rebuilt, "a fresh rebuild must preserve every TOC, text and line ID and flag")
        }
    }
}
