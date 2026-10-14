package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * 'הערות על <title>' companion files whose links fully cover the notes are
 * merged into the base book as inline footnotes (the Sefaria idiom the app's
 * 'הערות' pane renders) and are NOT imported as standalone books. Partially
 * linked companions stay on the standalone-book mechanism.
 */
class OtzariaHearotBooksTest {

    private fun newRepo(): SeforimRepository {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        return SeforimRepository(":memory:", driver)
    }

    private fun writeLibrary(
        baseLines: String,
        hearotLines: String?,
        linksJson: String?,
        baseName: String = "מזור ותרופה",
        hearotName: String = "הערות על מזור ותרופה",
    ): Path {
        val sourceDir = Files.createTempDirectory("otzaria-hearot")
        val bookDir = Files.createDirectories(sourceDir.resolve("אוצריא").resolve("מוסר"))
        Files.writeString(bookDir.resolve("$baseName.txt"), baseLines)
        if (hearotLines != null) Files.writeString(bookDir.resolve("$hearotName.txt"), hearotLines)
        if (linksJson != null) {
            val linksDir = Files.createDirectories(sourceDir.resolve("links"))
            Files.writeString(linksDir.resolve("${baseName}_links.json"), linksJson)
        }
        writeFilesManifest(sourceDir)
        return sourceDir
    }

    private fun linksEntry(i1: Int, i2: Int, hearotName: String = "הערות על מזור ותרופה") =
        """{"line_index_1": $i1, "heRef_2": "הערות", "path_2": "$hearotName.txt",
           |"line_index_2": $i2, "Conection Type": "commentary"}""".trimMargin()

    @Test
    fun fullyLinkedCompanionMergesInlineAndIsNotABook() = runBlocking {
        val repo = newRepo()
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nגוף הספר<sup>1</sup> המשך",
            hearotLines = "<h1>הערות על מזור ותרופה</h1>\n<sup>1</sup> גוף ההערה",
            linksJson = "[${linksEntry(i1 = 2, i2 = 2)}]",
        )

        val generator = DatabaseGenerator(sourceDirectory = sourceDir, repository = repo)
        generator.generateLinesOnly()
        generator.generateLinksOnly()

        val base = repo.getAllBooks().find { it.title == "מזור ותרופה" }
        assertNotNull(base, "base book missing")
        assertNull(
            repo.getAllBooks().find { it.title == "הערות על מזור ותרופה" },
            "merged companion must not become a standalone book",
        )
        assertEquals(
            "גוף הספר<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">גוף ההערה</i> המשך",
            repo.getLines(base.id, 1, 1).single().content,
        )
        assertEquals(0, repo.countLinks(), "companion links are consumed by the merge")
        assertEquals(2, repo.getBook(base.id)!!.totalLines, "merge must preserve line count")
    }

    @Test
    fun phaseTwoOnFreshInstanceSkipsMergedCompanions() = runBlocking {
        val repo = newRepo()
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nגוף הספר<sup>1</sup> המשך",
            hearotLines = "<h1>הערות על מזור ותרופה</h1>\n<sup>1</sup> גוף ההערה",
            linksJson = "[${linksEntry(i1 = 2, i2 = 2)}]",
        )
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinesOnly()
        // Production runs phase 2 in a fresh process — no in-memory plan state.
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

        assertNull(repo.getAllBooks().find { it.title == "הערות על מזור ותרופה" })
        assertEquals(0, repo.countLinks(), "merged companion links must be skipped in a fresh phase-2 instance")
    }

    @Test
    fun iTagNoteDemotesPairToStandalone() = runBlocking {
        val repo = newRepo()
        // <i> inside a note breaks the app's footnote regex — the pair must
        // degrade to the standalone mechanism at PLAN time, not crash the build.
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nגוף<sup>1</sup>",
            hearotLines = "<sup>1</sup> גוף עם <i>הדגשה</i>",
            linksJson = "[${linksEntry(i1 = 2, i2 = 1)}]",
        )
        val generator = DatabaseGenerator(sourceDirectory = sourceDir, repository = repo)
        generator.generateLinesOnly()
        generator.generateLinksOnly()

        val base = repo.getAllBooks().find { it.title == "מזור ותרופה" }!!
        assertNotNull(repo.getAllBooks().find { it.title == "הערות על מזור ותרופה" })
        assertEquals("גוף<sup>1</sup>", repo.getLines(base.id, 1, 1).single().content)
        assertEquals(1, repo.countLinks())
    }

    @Test
    fun sefariaShadowedBaseDemotesPairToStandalone() = runBlocking<Unit> {
        val repo = newRepo()
        // The importer skips a base whose title already exists from Sefaria —
        // its companion must not be consumed (the notes would vanish).
        val sefariaSourceId = repo.insertSource("Sefaria")
        val catId = repo.insertCategory(Category(title = "ספריא"))
        repo.insertBook(
            Book(
                id = 500_000,
                categoryId = catId,
                sourceId = sefariaSourceId,
                title = "מזור ותרופה",
                heRef = "מזור ותרופה",
            ),
        )
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nגוף<sup>1</sup>",
            hearotLines = "<sup>1</sup> גוף ההערה",
            linksJson = "[${linksEntry(i1 = 2, i2 = 1)}]",
        )
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinesOnly()

        assertNotNull(
            repo.getAllBooks().find { it.title == "הערות על מזור ותרופה" },
            "companion of a Sefaria-shadowed base must stay a standalone book",
        )
    }

    @Test
    fun wordAnchoredLinkOnNoteLineDemotesPair() = runBlocking {
        val repo = newRepo()
        // Word-anchor offsets are measured against the original line; an
        // in-place merge would shift them — the pair must stay standalone.
        val anchored = """{"line_index_1": 2, "heRef_2": "אחר", "path_2": "ספר אחר.txt",
            |"line_index_2": 1, "Conection Type": "commentary", "start": 3, "end": 6}""".trimMargin()
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nגוף<sup>1</sup>",
            hearotLines = "<sup>1</sup> גוף ההערה",
            linksJson = "[${linksEntry(i1 = 2, i2 = 1)},\n$anchored]",
        )
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinesOnly()

        val base = repo.getAllBooks().find { it.title == "מזור ותרופה" }!!
        assertNotNull(repo.getAllBooks().find { it.title == "הערות על מזור ותרופה" })
        assertEquals("גוף<sup>1</sup>", repo.getLines(base.id, 1, 1).single().content)
    }

    @Test
    fun rangedCompanionLinkDemotesPair() = runBlocking<Unit> {
        val repo = newRepo()
        val ranged = """{"line_index_1": 2, "heRef_2": "הערות", "path_2": "הערות על מזור ותרופה.txt",
            |"line_index_2": 1, "line_index_2_end": 2, "Conection Type": "commentary"}""".trimMargin()
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nגוף<sup>1</sup>",
            hearotLines = "<sup>1</sup> חלק ראשון\nחלק שני",
            linksJson = "[$ranged]",
        )
        val generator = DatabaseGenerator(sourceDirectory = sourceDir, repository = repo)
        generator.generateLinesOnly()

        assertNotNull(
            repo.getAllBooks().find { it.title == "הערות על מזור ותרופה" },
            "ranged companion links must degrade to standalone, not crash",
        )
    }

    @Test
    fun anchorPicksLeftmostAcrossGrammars() {
        // A line mixing marker styles must consume the leftmost occurrence first.
        val merged = HearotCompanionMerge.mergeLines(
            bookTitle = "ספר",
            baseLines = listOf("""א <sup style="color:blue;">1</sup> ב <sup>1</sup> ג"""),
            notesByLine = mapOf(0 to listOf("<sup>1</sup> ראשונה", "<sup>1</sup> שנייה")),
        )
        assertEquals(
            """א <sup class="footnote-marker">1</sup><i class="footnote">ראשונה</i>""" +
                """ ב <sup class="footnote-marker">1</sup><i class="footnote">שנייה</i> ג""",
            merged[0],
        )
    }

    @Test
    fun restartedNumberingConsumesMarkersInOrder() = runBlocking {
        val repo = newRepo()
        // Footnote numbering restarts between sections: the same <sup>1</sup>
        // appears twice in one line and maps to two different notes.
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nראשון<sup>1</sup> שני<sup>1</sup> סוף",
            hearotLines = "<sup>1</sup> הערה א\n<sup>1</sup> הערה ב",
            linksJson = "[${linksEntry(i1 = 2, i2 = 1)},\n${linksEntry(i1 = 2, i2 = 2)}]",
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinesOnly()

        val base = repo.getAllBooks().find { it.title == "מזור ותרופה" }!!
        assertEquals(
            "ראשון<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">הערה א</i>" +
                " שני<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">הערה ב</i> סוף",
            repo.getLines(base.id, 1, 1).single().content,
        )
    }

    @Test
    fun unanchorableNoteIsAppendedWhole() = runBlocking {
        val repo = newRepo()
        // No marker in the base line — the full note line is appended, lossless.
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nשורה בלי סימון",
            hearotLines = "הערה חופשית בלי מספור",
            linksJson = "[${linksEntry(i1 = 2, i2 = 1)}]",
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinesOnly()

        val base = repo.getAllBooks().find { it.title == "מזור ותרופה" }!!
        assertEquals(
            "שורה בלי סימון<i class=\"footnote\">הערה חופשית בלי מספור</i>",
            repo.getLines(base.id, 1, 1).single().content,
        )
    }

    @Test
    fun noteOnHeadingLineStaysOutOfTocText() = runBlocking {
        val repo = newRepo()
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\n<h2>פרק א<sup>1</sup></h2>\nתוכן",
            hearotLines = "<sup>1</sup> הערת כותרת",
            linksJson = "[${linksEntry(i1 = 2, i2 = 1)}]",
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinesOnly()

        val base = repo.getAllBooks().find { it.title == "מזור ותרופה" }!!
        val chapterEntry = repo.getTocEntriesForBook(base.id).find { it.level == 2 }
        assertNotNull(chapterEntry, "chapter TOC entry missing")
        assertEquals("פרק א", chapterEntry.text, "merged note must not leak into TOC text")
        assertEquals(
            "<h2>פרק א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">הערת כותרת</i></h2>",
            repo.getLines(base.id, 1, 1).single().content,
        )
    }

    @Test
    fun partiallyLinkedCompanionStaysAStandaloneBook() = runBlocking {
        val repo = newRepo()
        // Second real note line has no link — merging would silently lose it,
        // so the pair stays on the standalone-book mechanism.
        val sourceDir = writeLibrary(
            baseLines = "<h1>מזור ותרופה</h1>\nגוף הספר<sup>1</sup>",
            hearotLines = "<sup>1</sup> הערה מקושרת\n<sup>2</sup> הערה בלי קישור",
            linksJson = "[${linksEntry(i1 = 2, i2 = 1)}]",
        )

        val generator = DatabaseGenerator(sourceDirectory = sourceDir, repository = repo)
        generator.generateLinesOnly()
        generator.generateLinksOnly()

        val base = repo.getAllBooks().find { it.title == "מזור ותרופה" }!!
        val hearot = repo.getAllBooks().find { it.title == "הערות על מזור ותרופה" }
        assertNotNull(hearot, "partially linked companion must stay a standalone book")
        assertEquals("גוף הספר<sup>1</sup>", repo.getLines(base.id, 1, 1).single().content)
        assertEquals(1, repo.countLinks(), "the existing link mechanism must keep working")
    }

    @Test
    fun fullyLinkedHavroutaHearotMergeLikeAnyCompanion() = runBlocking {
        val repo = newRepo()
        val sourceDir = writeLibrary(
            baseLines = "<h1>חברותא על ברכות</h1>\nגוף <small>(1)</small> ועוד <small>(1*)</small> סוף",
            hearotLines = "<h1>חברותא  הערות</h1>\n<b>(1)</b> הערת חברותא\n<b>(1*)</b> הוספה",
            linksJson = "[${havroutaEntry(2, 2)}, ${havroutaEntry(2, 3)}]",
            baseName = "חברותא על ברכות",
            hearotName = "הערות על חברותא על ברכות",
        )

        val generator = DatabaseGenerator(sourceDirectory = sourceDir, repository = repo)
        generator.generateLinesOnly()
        generator.generateLinksOnly()

        val base = repo.getAllBooks().find { it.title == "חברותא על ברכות" }!!
        assertNull(repo.getAllBooks().find { it.title == "הערות על חברותא על ברכות" })
        assertEquals(
            "גוף <sup class=\"footnote-marker\">(1)</sup><i class=\"footnote\">הערת חברותא</i> ועוד " +
                "<sup class=\"footnote-marker\">(1*)</sup><i class=\"footnote\">הוספה</i> סוף",
            repo.getLines(base.id, 1, 1).single().content,
        )
        assertEquals(0, repo.countLinks())
    }

    @Test
    fun havroutaHearotWithAnUnlinkedAttributionLineStayStandalone() = runBlocking {
        val repo = newRepo()
        // Every real Havrouta notes file repeats the base's author line, linked to nothing.
        val sourceDir = writeLibrary(
            baseLines = "<h1>חברותא על ברכות</h1>\nהרב פלוני\nגוף <small>(1)</small>",
            hearotLines = "<h1>חברותא  הערות</h1>\nהרב פלוני\n<b>(1)</b> הערת חברותא",
            linksJson = "[${havroutaEntry(3, 3)}]",
            baseName = "חברותא על ברכות",
            hearotName = "הערות על חברותא על ברכות",
        )

        val generator = DatabaseGenerator(sourceDirectory = sourceDir, repository = repo)
        generator.generateLinesOnly()
        generator.generateLinksOnly()

        assertNotNull(repo.getAllBooks().find { it.title == "הערות על חברותא על ברכות" })
        val base = repo.getAllBooks().find { it.title == "חברותא על ברכות" }!!
        assertEquals("גוף <small>(1)</small>", repo.getLines(base.id, 2, 2).single().content)
        assertEquals(1, repo.countLinks())
    }

    private fun havroutaEntry(i1: Int, i2: Int) =
        """{"line_index_1": $i1, "heRef_2": "הערות על חברותא על ברכות", "path_2": "הערות על חברותא על ברכות.txt",
           |"line_index_2": $i2, "Conection Type": "footnotes"}""".trimMargin()

    @Test
    fun noteCarryingItalicMarkupFailsLoudly() {
        assertFailsWith<IllegalStateException> {
            HearotCompanionMerge.mergeLines(
                bookTitle = "ספר",
                baseLines = listOf("שורה<sup>1</sup>"),
                notesByLine = mapOf(0 to listOf("<sup>1</sup> גוף עם <i>הדגשה</i>")),
            )
        }
    }

    @Test
    fun markerFamiliesAnchorInPlace() {
        // Styled sup (Dicta), small-wrapped sup, and bare-number (מיקרופדיה) markers.
        val merged = HearotCompanionMerge.mergeLines(
            bookTitle = "ספר",
            baseLines = listOf(
                """טקסט <sup style="color:blue;">(א)</sup> המשך""",
                """<small><sup>3</sup> (חו"מ)</small> טקסט""",
                """כותרת <small style="color: gray;">7</small> סוף""",
            ),
            notesByLine = mapOf(
                0 to listOf("""<sup style="color:blue;">(א)</sup> הערת דיקטה"""),
                1 to listOf("""<small><sup>3</sup> הערה קטנה</small>"""),
                2 to listOf("""7 ב', עמ' כה."""),
            ),
        )
        assertEquals(
            """טקסט <sup class="footnote-marker">(א)</sup><i class="footnote">הערת דיקטה</i> המשך""",
            merged[0],
        )
        assertEquals(
            """<small><sup class="footnote-marker">3</sup><i class="footnote">הערה קטנה</i> (חו"מ)</small> טקסט""",
            merged[1],
        )
        assertEquals(
            """כותרת <sup class="footnote-marker">7</sup><i class="footnote">ב', עמ' כה.</i> סוף""",
            merged[2],
        )
    }
}
