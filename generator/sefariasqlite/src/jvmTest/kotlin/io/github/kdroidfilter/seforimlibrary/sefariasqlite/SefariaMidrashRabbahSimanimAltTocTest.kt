package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The Midrash Rabbah family is stored in Sefaria as a plain depth-N JaggedArray
 * with NO alt-struct. The main-TOC `depth > 1` rule therefore flattens the
 * innermost siman/paragraph level into running text — the book is navigable only
 * by parasha, never "by letter" as on Sefaria. [SefariaAltTocBuilder] synthesizes
 * a siman-level alt-TOC for these books (and only these) so each siman becomes a
 * navigable node, without inserting or moving any content line.
 */
class SefariaMidrashRabbahSimanimAltTocTest {

    /** A tiny Pesikta-DeRav-Kahana-shaped book: two sections (h2), simanim as plain
     *  leaf lines. Expected: a "Simanim" alt-struct, parashot as level-0 nodes,
     *  simanim as level-1 children labelled א, ב, … under their parasha. */
    @Test
    fun curatedMidrashGetsSynthesizedSimanimStructure() = runBlocking {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        val repository = SeforimRepository(":memory:", driver)

        val categoryId = repository.insertCategory(
            Category(id = 0, parentId = null, title = "מדרש רבה", level = 0, order = 0)
        )
        val sourceId = repository.insertSource("Sefaria")
        val bookId = repository.insertBook(
            Book(
                id = 0,
                categoryId = categoryId,
                sourceId = sourceId,
                title = "פסיקתא דרב כהנא",
                heShortDesc = null,
                notesContent = null,
                order = 0f,
                totalLines = 6,
                isBaseBook = true,
                hasAltStructures = false
            )
        )

        val bookPath = "פסיקתא דרב כהנא"
        // 0: <h1> book   1: <h2>פרק א   2,3: simanim   4: <h2>פרק ב   5: siman
        val contents = listOf(
            "<h1>פסיקתא דרב כהנא</h1>",
            "<h2>פרק א</h2>",
            "siman 1:1",
            "siman 1:2",
            "<h2>פרק ב</h2>",
            "siman 2:1",
        )
        val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>()
        contents.forEachIndexed { i, content ->
            val lineId = repository.insertLine(
                Line(id = 0, bookId = bookId, lineIndex = i, content = content, heRef = null)
            )
            lineKeyToId[bookPath to i] = lineId
        }

        // Heading.lineIndex is 0-based; RefEntry.lineIndex is 1-based (0-based + 1).
        val headings = listOf(
            Heading(title = "פסיקתא דרב כהנא", level = 0, lineIndex = 0),
            Heading(title = "פרק א", level = 1, lineIndex = 1),
            Heading(title = "פרק ב", level = 1, lineIndex = 4),
        )
        val refEntries = listOf(
            RefEntry(ref = "Pesikta DeRav Kahana 1:1", heRef = "פסיקתא דרב כהנא א, א", path = bookPath, lineIndex = 3),
            RefEntry(ref = "Pesikta DeRav Kahana 1:2", heRef = "פסיקתא דרב כהנא א, ב", path = bookPath, lineIndex = 4),
            RefEntry(ref = "Pesikta DeRav Kahana 2:1", heRef = "פסיקתא דרב כהנא ב, א", path = bookPath, lineIndex = 6),
        )

        val payload = midrashPayload(
            heTitle = "פסיקתא דרב כהנא",
            enTitle = "Pesikta DeRav Kahana",
            contents = contents,
            headings = headings,
            refEntries = refEntries,
        )

        // A foreign row above the allocator's counter: an entry still taking an
        // implicit rowid would land above it instead of at 1..n.
        repository.executeRawQuery(
            "INSERT INTO alt_toc_entry (id, structureId, parentId, textId, level, lineId, " +
                "isLastChild, hasChildren) VALUES (5000, 9999, NULL, 1, 0, NULL, 0, 0)",
        )
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repository)
        val builder = SefariaAltTocBuilder(repository, bindings)
        val result = builder.buildAltTocStructuresForBook(
            payload = payload,
            bookId = bookId,
            bookPath = bookPath,
            lineKeyToId = lineKeyToId,
            totalLines = contents.size
        )
        assertTrue(result, "synthetic siman alt-struct should be generated for a curated midrash")

        val structures = repository.getAltTocStructuresForBook(bookId)
        assertEquals(1, structures.size, "exactly one synthetic structure expected")
        assertEquals("Simanim", structures.first().key)
        assertEquals("סימנים", structures.first().heTitle)

        val entries = repository.getAltTocEntriesForStructure(structures.first().id)
        assertTrue(
            entries.all { it.id < 5000 },
            "every synthesized siman entry must take an allocator id, not an implicit rowid",
        )

        // Level-0: the two parashot, pointing at their heading lines.
        val parashaA = entries.single { it.text == "פרק א" }
        val parashaB = entries.single { it.text == "פרק ב" }
        assertEquals(0, parashaA.level)
        assertEquals(0, parashaB.level)
        assertEquals(lineKeyToId[bookPath to 1], parashaA.lineId)
        assertEquals(lineKeyToId[bookPath to 4], parashaB.lineId)
        assertTrue(parashaA.hasChildren, "פרק א should have siman children")

        // Level-1: simanim nested under the right parasha, labelled א/ב.
        val simanimOfA = entries.filter { it.parentId == parashaA.id }.sortedBy { it.lineId }
        assertEquals(listOf("א", "ב"), simanimOfA.map { it.text }, "simanim of פרק א should be א, ב")
        assertEquals(lineKeyToId[bookPath to 2], simanimOfA[0].lineId)
        assertEquals(lineKeyToId[bookPath to 3], simanimOfA[1].lineId)

        val simanimOfB = entries.filter { it.parentId == parashaB.id }
        assertEquals(listOf("א"), simanimOfB.map { it.text }, "the second parasha's siman ordinal resets to א")
        assertEquals(lineKeyToId[bookPath to 5], simanimOfB.single().lineId)

        // Every entry maps to an existing content line — nothing was invented.
        assertTrue(entries.all { it.lineId != null }, "every synthetic entry points at a real line")

        driver.close()
    }

    /** A structurally IDENTICAL depth-2 book that is NOT in the family (Tanakh:
     *  chapter → verse). It must NOT receive the synthetic structure — proving the
     *  gate is a curated allowlist, not a structural rule (verses are not simanim). */
    @Test
    fun tanakhShapedBookDoesNotGetSimanimStructure() = runBlocking {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        val repository = SeforimRepository(":memory:", driver)

        val categoryId = repository.insertCategory(
            Category(id = 0, parentId = null, title = "תורה", level = 0, order = 0)
        )
        val sourceId = repository.insertSource("Sefaria")
        val bookId = repository.insertBook(
            Book(
                id = 0,
                categoryId = categoryId,
                sourceId = sourceId,
                title = "בראשית",
                heShortDesc = null,
                notesContent = null,
                order = 0f,
                totalLines = 6,
                isBaseBook = true,
                hasAltStructures = false
            )
        )

        val bookPath = "בראשית"
        val contents = listOf(
            "<h1>בראשית</h1>",
            "<h2>פרק א</h2>",
            "verse 1:1",
            "verse 1:2",
            "<h2>פרק ב</h2>",
            "verse 2:1",
        )
        val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>()
        contents.forEachIndexed { i, content ->
            val lineId = repository.insertLine(
                Line(id = 0, bookId = bookId, lineIndex = i, content = content, heRef = null)
            )
            lineKeyToId[bookPath to i] = lineId
        }

        val headings = listOf(
            Heading(title = "בראשית", level = 0, lineIndex = 0),
            Heading(title = "פרק א", level = 1, lineIndex = 1),
            Heading(title = "פרק ב", level = 1, lineIndex = 4),
        )
        val refEntries = listOf(
            RefEntry(ref = "Genesis 1:1", heRef = "בראשית א, א", path = bookPath, lineIndex = 3),
            RefEntry(ref = "Genesis 1:2", heRef = "בראשית א, ב", path = bookPath, lineIndex = 4),
            RefEntry(ref = "Genesis 2:1", heRef = "בראשית ב, א", path = bookPath, lineIndex = 6),
        )

        val payload = midrashPayload(
            heTitle = "בראשית",
            enTitle = "Genesis",
            contents = contents,
            headings = headings,
            refEntries = refEntries,
        )

        // A foreign row above the allocator's counter: an entry still taking an
        // implicit rowid would land above it instead of at 1..n.
        repository.executeRawQuery(
            "INSERT INTO alt_toc_entry (id, structureId, parentId, textId, level, lineId, " +
                "isLastChild, hasChildren) VALUES (5000, 9999, NULL, 1, 0, NULL, 0, 0)",
        )
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repository)
        val builder = SefariaAltTocBuilder(repository, bindings)
        val result = builder.buildAltTocStructuresForBook(
            payload = payload,
            bookId = bookId,
            bookPath = bookPath,
            lineKeyToId = lineKeyToId,
            totalLines = contents.size
        )

        assertFalse(result, "a non-family depth-2 book must not get a synthetic siman structure")
        assertTrue(
            repository.getAltTocStructuresForBook(bookId).isEmpty(),
            "no alt structure should be written for Tanakh"
        )

        driver.close()
    }

    /** Otzaria: the Midrash Rabbah books index every paragraph in their main TOC
     *  (upstream 602d9502), so they no longer get the synthetic structure. */
    @Test
    fun numberedParagraphBookDoesNotGetSimanimStructure() = runBlocking {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        val repository = SeforimRepository(":memory:", driver)

        val categoryId = repository.insertCategory(
            Category(id = 0, parentId = null, title = "מדרש רבה", level = 0, order = 0)
        )
        val sourceId = repository.insertSource("Sefaria")
        val bookId = repository.insertBook(
            Book(
                id = 0,
                categoryId = categoryId,
                sourceId = sourceId,
                title = "בראשית רבה",
                heShortDesc = null,
                notesContent = null,
                order = 0f,
                totalLines = 6,
                isBaseBook = true,
                hasAltStructures = false
            )
        )

        val bookPath = "בראשית רבה"
        val contents = listOf(
            "<h1>בראשית רבה</h1>",
            "<h2>פרק א</h2>",
            "verse 1:1",
            "verse 1:2",
            "<h2>פרק ב</h2>",
            "verse 2:1",
        )
        val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>()
        contents.forEachIndexed { i, content ->
            val lineId = repository.insertLine(
                Line(id = 0, bookId = bookId, lineIndex = i, content = content, heRef = null)
            )
            lineKeyToId[bookPath to i] = lineId
        }

        val headings = listOf(
            Heading(title = "בראשית רבה", level = 0, lineIndex = 0),
            Heading(title = "פרק א", level = 1, lineIndex = 1),
            Heading(title = "פרק ב", level = 1, lineIndex = 4),
        )
        val refEntries = listOf(
            RefEntry(ref = "Bereshit Rabbah 1:1", heRef = "בראשית רבה, א, א", path = bookPath, lineIndex = 3),
            RefEntry(ref = "Bereshit Rabbah 1:2", heRef = "בראשית רבה, א, ב", path = bookPath, lineIndex = 4),
            RefEntry(ref = "Bereshit Rabbah 2:1", heRef = "בראשית רבה, ב, א", path = bookPath, lineIndex = 6),
        )

        val payload = midrashPayload(
            heTitle = "בראשית רבה",
            enTitle = "Bereshit Rabbah",
            contents = contents,
            headings = headings,
            refEntries = refEntries,
        )

        // A foreign row above the allocator's counter: an entry still taking an
        // implicit rowid would land above it instead of at 1..n.
        repository.executeRawQuery(
            "INSERT INTO alt_toc_entry (id, structureId, parentId, textId, level, lineId, " +
                "isLastChild, hasChildren) VALUES (5000, 9999, NULL, 1, 0, NULL, 0, 0)",
        )
        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repository)
        val builder = SefariaAltTocBuilder(repository, bindings)
        val result = builder.buildAltTocStructuresForBook(
            payload = payload,
            bookId = bookId,
            bookPath = bookPath,
            lineKeyToId = lineKeyToId,
            totalLines = contents.size
        )

        assertFalse(result, "a numbered-paragraph book must not get a synthetic siman structure")
        assertTrue(
            repository.getAltTocStructuresForBook(bookId).isEmpty(),
            "no alt structure should be written for Midrash Rabbah"
        )

        driver.close()
    }

    private fun midrashPayload(
        heTitle: String,
        enTitle: String,
        contents: List<String>,
        headings: List<Heading>,
        refEntries: List<RefEntry>,
    ) = BookPayload(
        heTitle = heTitle,
        enTitle = enTitle,
        categoriesHe = listOf("מדרש", "מדרש אגדה", "מדרש רבה"),
        lines = contents,
        refEntries = refEntries,
        headings = headings,
        authors = emptyList(),
        description = null,
        heShortDesc = null,
        pubDates = emptyList(),
        altStructures = emptyList(),
    )
}
