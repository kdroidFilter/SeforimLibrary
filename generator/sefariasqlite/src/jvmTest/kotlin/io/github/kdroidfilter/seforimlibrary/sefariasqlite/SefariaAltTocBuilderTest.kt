package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SefariaAltTocBuilderTest {

    /**
     * Test that alt TOC entries from different sections of a multi-section book (like Tur)
     * do not resolve to the same line due to aggressive tail fallback.
     *
     * Before the fix, citations like "Tur, Orach Chaim 1" and "Tur, Yoreh De'ah 1"
     * would both resolve to the same line because canonicalTail("Tur, Orach Chaim 1") = "1"
     * and canonicalTail("Tur, Yoreh De'ah 1") = "1", and the lookup map prefers earlier lines.
     *
     * After the fix, tail fallback is disabled for multi-section books (Tur, Shulchan Arukh).
     */
    @Test
    fun multiSectionBookAltTocEntriesShouldNotShareLineIds() = runBlocking {
        // Create in-memory database
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        val repository = SeforimRepository(":memory:", driver)

        // Create a category for טור
        val categoryId = repository.insertCategory(
            Category(
                id = 0,
                parentId = null,
                title = "טור",
                level = 0,
                order = 0
            )
        )

        // Create a source
        val sourceId = repository.insertSource("Sefaria")

        // Create the book
        val bookId = repository.insertBook(
            Book(
                id = 0,
                categoryId = categoryId,
                sourceId = sourceId,
                title = "טור",
                heShortDesc = null,
                notesContent = null,
                order = 0f,
                totalLines = 100,
                isBaseBook = true,
                hasAltStructures = true
            )
        )

        val bookPath = "טור"

        // Create lines for the book
        // Lines 0-49: אורח חיים section
        // Lines 50-99: יורה דעה section
        val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>()
        for (i in 0 until 100) {
            val content = when {
                i == 0 -> "<h2>אורח חיים</h2>"
                i == 1 -> "<h3>סימן א</h3>"
                i in 2..49 -> "Content line $i in Orach Chaim"
                i == 50 -> "<h2>יורה דעה</h2>"
                i == 51 -> "<h3>סימן א</h3>"
                else -> "Content line $i in Yoreh De'ah"
            }
            val lineId = repository.insertLine(
                Line(
                    id = 0,
                    bookId = bookId,
                    lineIndex = i,
                    content = content,
                    heRef = null
                )
            )
            lineKeyToId[bookPath to i] = lineId
        }

        // Create refEntries - simulating refs for each section
        // Both sections have "Siman 1" but at different line indices
        val refEntries = listOf(
            // Orach Chaim refs
            RefEntry(ref = "Tur, Orach Chaim", heRef = "טור, אורח חיים", path = bookPath, lineIndex = 1),
            RefEntry(ref = "Tur, Orach Chaim 1", heRef = "טור, אורח חיים א", path = bookPath, lineIndex = 2),
            RefEntry(ref = "Tur, Orach Chaim 1:1", heRef = "טור, אורח חיים א:א", path = bookPath, lineIndex = 2),
            // Yoreh De'ah refs
            RefEntry(ref = "Tur, Yoreh De'ah", heRef = "טור, יורה דעה", path = bookPath, lineIndex = 51),
            RefEntry(ref = "Tur, Yoreh De'ah 1", heRef = "טור, יורה דעה א", path = bookPath, lineIndex = 52),
            RefEntry(ref = "Tur, Yoreh De'ah 1:1", heRef = "טור, יורה דעה א:א", path = bookPath, lineIndex = 52)
        )

        // Create alt structure with nodes for each section
        val altStructures = listOf(
            AltStructurePayload(
                key = "Topic",
                title = "Tur",
                heTitle = "טור",
                nodes = listOf(
                    // Orach Chaim section
                    AltNodePayload(
                        title = "Orach Chaim",
                        heTitle = "אורח חיים",
                        wholeRef = null,
                        refs = emptyList(),
                        addressTypes = listOf("Siman"),
                        childLabel = "סימן",
                        addresses = emptyList(),
                        skippedAddresses = emptyList(),
                        startingAddress = null,
                        offset = null,
                        children = listOf(
                            AltNodePayload(
                                title = "Laws of Morning Conduct",
                                heTitle = "הלכות הנהגת האדם בבוקר",
                                wholeRef = "Tur, Orach Chaim 1",
                                refs = listOf("Tur, Orach Chaim 1:1"),
                                addressTypes = listOf("Siman"),
                                childLabel = null,
                                addresses = emptyList(),
                                skippedAddresses = emptyList(),
                                startingAddress = null,
                                offset = null,
                                children = emptyList()
                            )
                        )
                    ),
                    // Yoreh De'ah section
                    AltNodePayload(
                        title = "Yoreh De'ah",
                        heTitle = "יורה דעה",
                        wholeRef = null,
                        refs = emptyList(),
                        addressTypes = listOf("Siman"),
                        childLabel = "סימן",
                        addresses = emptyList(),
                        skippedAddresses = emptyList(),
                        startingAddress = null,
                        offset = null,
                        children = listOf(
                            AltNodePayload(
                                title = "Laws of Slaughter",
                                heTitle = "הלכות שחיטה",
                                wholeRef = "Tur, Yoreh De'ah 1",
                                refs = listOf("Tur, Yoreh De'ah 1:1"),
                                addressTypes = listOf("Siman"),
                                childLabel = null,
                                addresses = emptyList(),
                                skippedAddresses = emptyList(),
                                startingAddress = null,
                                offset = null,
                                children = emptyList()
                            )
                        )
                    )
                )
            )
        )

        val payload = BookPayload(
            heTitle = "טור",
            enTitle = "Tur",
            categoriesHe = listOf("טור"), // This triggers isTurCode = true
            lines = (0 until 100).map { "line $it" },
            refEntries = refEntries,
            headings = emptyList(),
            authors = emptyList(),
            description = null,
            pubDates = emptyList(),
            altStructures = altStructures
        )

        // Build alt TOC structures
        val builder = SefariaAltTocBuilder(repository)
        val result = builder.buildAltTocStructuresForBook(
            payload = payload,
            bookId = bookId,
            bookPath = bookPath,
            lineKeyToId = lineKeyToId,
            totalLines = 100
        )

        // Verify alt structures were generated
        assertTrue(result, "Alt structures should have been generated")

        // Get the alt structure
        val structures = repository.getAltTocStructuresForBook(bookId)
        assertEquals(1, structures.size, "Should have one alt structure")
        val structureId = structures.first().id

        // Get all entries for the structure
        val entries = repository.getAltTocEntriesForStructure(structureId)

        // Find entries by text
        val orachChaimEntry = entries.find { it.text == "הלכות הנהגת האדם בבוקר" }
        val yorehDeahEntry = entries.find { it.text == "הלכות שחיטה" }

        assertNotNull(orachChaimEntry, "Orach Chaim entry should exist")
        assertNotNull(yorehDeahEntry, "Yoreh De'ah entry should exist")

        // The critical assertion: they should NOT have the same lineId
        assertNotEquals(
            orachChaimEntry.lineId,
            yorehDeahEntry.lineId,
            "Alt TOC entries from different sections should have different lineIds. " +
                    "Orach Chaim lineId=${orachChaimEntry.lineId}, Yoreh De'ah lineId=${yorehDeahEntry.lineId}"
        )

        // Verify correct line assignments
        // lineIndex is 1-based in RefEntry, so "Tur, Orach Chaim 1" at lineIndex=2 maps to 0-based index 1
        val expectedOrachChaimLineId = lineKeyToId[bookPath to 1] // lineIndex 2 -> 0-based index 1
        val expectedYorehDeahLineId = lineKeyToId[bookPath to 51] // lineIndex 52 -> 0-based index 51

        assertEquals(
            expectedOrachChaimLineId,
            orachChaimEntry.lineId,
            "Orach Chaim entry should point to line in Orach Chaim section"
        )
        assertEquals(
            expectedYorehDeahLineId,
            yorehDeahEntry.lineId,
            "Yoreh De'ah entry should point to line in Yoreh De'ah section"
        )

        driver.close()
    }
}
