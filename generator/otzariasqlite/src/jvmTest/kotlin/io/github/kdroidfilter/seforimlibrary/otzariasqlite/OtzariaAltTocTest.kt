package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Alt-toc structures from `alt_toc/<book>_alt_toc.json` files (parasha/aliyah
 * shape): parashot at level 0, aliyot as their children at level 1, each
 * anchored to a 1-based line. Every book line must map to the nearest
 * preceding anchor with the deepest entry winning a shared line; out-of-range
 * nodes are dropped; re-running the phase must not duplicate entries.
 */
class OtzariaAltTocTest {
    @Test
    fun altTocFileBuildsStructureEntriesAndLineMapping() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Tashma")
        val sefariaSourceId = repo.insertSource("Sefaria")
        val catId = repo.insertCategory(Category(0, null, "תורה", level = 0, order = 1))
        fun book(id: Long, title: String, srcId: Long, totalLines: Int) = Book(
            id = id, categoryId = catId, sourceId = srcId, title = title, heRef = title,
            authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
            heShortDesc = null, notesContent = null, order = id.toFloat(), topics = emptyList(),
            isBaseBook = false, totalLines = totalLines, hasAltStructures = false,
            hasTeamim = false, hasNekudot = false,
        )
        repo.insertBook(book(1, "חומש", sourceId, 8))
        repo.insertBook(book(2, "ילקוט", sefariaSourceId, 2))
        repo.insertLinesBatch(
            (0 until 8).map { idx ->
                Line(id = 100L + idx, bookId = 1, lineIndex = idx, content = "שורה ${idx + 1}", heRef = "חומש ${idx + 1}")
            } + (0 until 2).map { idx ->
                Line(id = 200L + idx, bookId = 2, lineIndex = idx, content = "ילקוט ${idx + 1}", heRef = "ילקוט ${idx + 1}")
            }
        )

        val sourceDir = Files.createTempDirectory("otzaria-alt-toc")
        val altTocDir = Files.createDirectories(sourceDir.resolve("alt_toc"))
        // Two parashot with aliyot; one node out of range (line 99) must be dropped.
        Files.writeString(
            altTocDir.resolve("חומש_alt_toc.json"),
            """
            |[
            | {"key": "Parasha", "heTitle": "פרשה",
            |  "nodes": [
            |   {"heTitle": "בראשית", "line": 1, "children": [
            |     {"heTitle": "עליה א", "line": 1},
            |     {"heTitle": "עליה ב", "line": 4}
            |   ]},
            |   {"heTitle": "נח", "line": 6, "children": [
            |     {"heTitle": "עליה א", "line": 6},
            |     {"heTitle": "מחוץ לתחום", "line": 99}
            |   ]}
            |  ]}
            |]
            """.trimMargin()
        )
        // A file targeting a Sefaria-sourced book must be ignored entirely.
        Files.writeString(
            altTocDir.resolve("ילקוט_alt_toc.json"),
            """[{"key": "Chapters", "heTitle": "פרקים", "nodes": [{"heTitle": "פרק", "line": 1}]}]""",
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

        val structures = repo.getAltTocStructuresForBook(1)
        assertEquals(1, structures.size)
        assertEquals("Parasha", structures.single().key)
        assertEquals("פרשה", structures.single().heTitle)
        val structureId = structures.single().id

        // 5 entries survive (the out-of-range node dropped): 2 parashot + 3 aliyot.
        val entries = repo.getAltTocEntriesForStructure(structureId).sortedBy { it.id }
        assertEquals(5, entries.size)
        val (bereshit, aliyaA, aliyaB, noach, noachAliyaA) = entries
        assertEquals("בראשית" to 0, bereshit.text to bereshit.level)
        assertEquals("עליה א" to 1, aliyaA.text to aliyaA.level)
        assertEquals("עליה ב" to 1, aliyaB.text to aliyaB.level)
        assertEquals("נח" to 0, noach.text to noach.level)
        assertEquals("עליה א" to 1, noachAliyaA.text to noachAliyaA.level)
        assertEquals(bereshit.id, aliyaA.parentId)
        assertEquals(bereshit.id, aliyaB.parentId)
        assertEquals(noach.id, noachAliyaA.parentId)
        assertEquals(listOf(100L, 100L, 103L, 105L, 105L), entries.map { it.lineId })

        assertTrue(bereshit.hasChildren && noach.hasChildren)
        assertTrue(!aliyaA.hasChildren && !aliyaB.hasChildren && !noachAliyaA.hasChildren)
        // Last child per parent bucket: נח (root), עליה ב (בראשית), עליה א (נח).
        assertEquals(
            listOf(false, false, true, true, true),
            entries.map { it.isLastChild },
        )

        // Every line maps to the nearest preceding anchor; on a shared line the
        // deepest entry wins (line 1 → עליה א, not בראשית).
        val mappings = repo.getLineAltTocMappings(structureId).associate { it.lineId to it.altTocEntryId }
        assertEquals(
            mapOf(
                100L to aliyaA.id, 101L to aliyaA.id, 102L to aliyaA.id,
                103L to aliyaB.id, 104L to aliyaB.id,
                105L to noachAliyaA.id, 106L to noachAliyaA.id, 107L to noachAliyaA.id,
            ),
            mappings,
        )

        assertTrue(repo.getBook(1)!!.hasAltStructures)

        // The Sefaria-sourced book's file was ignored — no structures, flag untouched.
        assertTrue(repo.getAltTocStructuresForBook(2).isEmpty())
        assertTrue(!repo.getBook(2)!!.hasAltStructures)

        // Re-running the phase rebuilds wholesale — no duplicated entries.
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()
        assertEquals(1, repo.getAltTocStructuresForBook(1).size)
        assertEquals(5, repo.getAltTocEntriesForStructure(structureId).size)
        assertEquals(8, repo.getLineAltTocMappings(structureId).size)

        // The file is authoritative: dropping נח removes its subtree, remaps lines,
        // and an edited heTitle refreshes the structure row (full rebuild).
        Files.writeString(
            altTocDir.resolve("חומש_alt_toc.json"),
            """
            |[
            | {"key": "Parasha", "heTitle": "פרשות",
            |  "nodes": [
            |   {"heTitle": "בראשית", "line": 1, "children": [
            |     {"heTitle": "עליה א", "line": 1},
            |     {"heTitle": "עליה ב", "line": 4}
            |   ]}
            |  ]}
            |]
            """.trimMargin()
        )
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()
        assertEquals("פרשות", repo.getAltTocStructuresForBook(1).single().heTitle)
        val remaining = repo.getAltTocEntriesForStructure(structureId).sortedBy { it.id }
        assertEquals(listOf("בראשית", "עליה א", "עליה ב"), remaining.map { it.text })
        // Lines 6-8 now fall under עליה ב — the nearest remaining preceding anchor.
        val remainingAliyaB = remaining.single { it.text == "עליה ב" }
        val remapped = repo.getLineAltTocMappings(structureId).associate { it.lineId to it.altTocEntryId }
        assertEquals(remainingAliyaB.id, remapped[105L])
        assertEquals(remainingAliyaB.id, remapped[107L])

        // Emptying nodes removes the whole structure and clears the book flag.
        Files.writeString(
            altTocDir.resolve("חומש_alt_toc.json"),
            """[{"key": "Parasha", "heTitle": "פרשה", "nodes": []}]""",
        )
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()
        assertTrue(repo.getAltTocStructuresForBook(1).isEmpty())
        assertEquals(0, repo.getLineAltTocMappings(structureId).size)
        assertTrue(!repo.getBook(1)!!.hasAltStructures)

        // Deleting the file entirely also cleans the DB: restore first, then delete.
        Files.writeString(
            altTocDir.resolve("חומש_alt_toc.json"),
            """[{"key": "Parasha", "heTitle": "פרשה", "nodes": [{"heTitle": "בראשית", "line": 1}]}]""",
        )
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()
        assertEquals(1, repo.getAltTocStructuresForBook(1).size)
        assertTrue(repo.getBook(1)!!.hasAltStructures)
        Files.delete(altTocDir.resolve("חומש_alt_toc.json"))
        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()
        assertTrue(repo.getAltTocStructuresForBook(1).isEmpty())
        assertTrue(!repo.getBook(1)!!.hasAltStructures)

        repo.close()
    }

    /**
     * Siblings may share a line: in ספר העיטור several page markers sit inside one
     * paragraph (Otzaria/otzaria#3181). Every sibling is kept, and the shared line
     * maps to the last of them.
     */
    @Test
    fun siblingsSharingALineAreAllKept() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Dicta")
        val catId = repo.insertCategory(Category(0, null, "Cat", level = 0, order = 1))
        repo.insertBook(
            Book(
                id = 1, categoryId = catId, sourceId = sourceId, title = "Book", heRef = "Book",
                authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
                heShortDesc = null, notesContent = null, order = 1f, topics = emptyList(),
                isBaseBook = false, totalLines = 3, hasAltStructures = false,
                hasTeamim = false, hasNekudot = false,
            ),
        )
        repo.insertLinesBatch(
            (0 until 3).map { idx ->
                Line(id = 100L + idx, bookId = 1, lineIndex = idx, content = "l${idx + 1}", heRef = "l${idx + 1}")
            },
        )

        val sourceDir = Files.createTempDirectory("otzaria-alt-toc-shared")
        val altTocDir = Files.createDirectories(sourceDir.resolve("alt_toc"))
        Files.writeString(
            altTocDir.resolve("Book_alt_toc.json"),
            """[{"key": "Dapim", "heTitle": "D", "nodes": [{"heTitle": "Part", "line": 1, "children": [
               {"heTitle": "P1", "line": 1}, {"heTitle": "P2", "line": 2}, {"heTitle": "P3", "line": 2},
               {"heTitle": "P4", "line": 3}]}]}]""",
        )

        DatabaseGenerator(sourceDirectory = sourceDir, repository = repo).generateLinksOnly()

        val structureId = repo.getAltTocStructuresForBook(1).single().id
        val entries = repo.getAltTocEntriesForStructure(structureId).sortedBy { it.id }
        assertEquals(listOf("Part", "P1", "P2", "P3", "P4"), entries.map { it.text })
        assertEquals(listOf(100L, 100L, 101L, 101L, 102L), entries.map { it.lineId })
        val byText = entries.associateBy { it.text }
        val mappings = repo.getLineAltTocMappings(structureId).associate { it.lineId to it.altTocEntryId }
        assertEquals(
            mapOf(100L to byText.getValue("P1").id, 101L to byText.getValue("P3").id, 102L to byText.getValue("P4").id),
            mappings,
        )

        repo.close()
    }

    /**
     * Entry ids must come from the persistent allocator, not from the rowid the
     * wholesale rebuild happens to free up. Reverting the `addNode` insert to
     * `repository.insertAltTocEntry` fails this: `id_alt_toc_entry` stays empty
     * and the second run renumbers from the squatter's rowid.
     */
    @Test
    fun altTocEntryIdsAreReproducedFromTheBuildState() = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Tashma")
        val catId = repo.insertCategory(Category(0, null, "Cat", level = 0, order = 1))
        repo.insertBook(
            Book(
                id = 1, categoryId = catId, sourceId = sourceId, title = "Book", heRef = "Book",
                authors = emptyList(), pubPlaces = emptyList(), pubDates = emptyList(),
                heShortDesc = null, notesContent = null, order = 1f, topics = emptyList(),
                isBaseBook = false, totalLines = 4, hasAltStructures = false,
                hasTeamim = false, hasNekudot = false,
            ),
        )
        repo.insertLinesBatch(
            (0 until 4).map { idx ->
                Line(id = 100L + idx, bookId = 1, lineIndex = idx, content = "l${idx + 1}", heRef = "l${idx + 1}")
            },
        )
        // A foreign row far above the rebuild's rowid range: an implicit-rowid
        // writer would renumber past it on the second run, the allocator will not.
        repo.executeRawQuery(
            "INSERT INTO alt_toc_entry (id, structureId, parentId, textId, level, lineId, isLastChild, hasChildren) " +
                "VALUES (5000, 9999, NULL, 1, 0, 100, 0, 0)",
        )

        val sourceDir = Files.createTempDirectory("otzaria-alt-toc-ids")
        val altTocDir = Files.createDirectories(sourceDir.resolve("alt_toc"))
        Files.writeString(
            altTocDir.resolve("Book_alt_toc.json"),
            """[{"key": "Parasha", "heTitle": "P", "nodes": [
               {"heTitle": "A", "line": 1, "children": [{"heTitle": "A1", "line": 1}, {"heTitle": "A2", "line": 2}]},
               {"heTitle": "B", "line": 3, "children": [{"heTitle": "B1", "line": 3}]}]}]""",
        )

        val state = Files.createTempDirectory("otzaria-alt-toc-state").resolve("build_state.db")
        fun run(): List<Pair<String, Long>> = runBlocking {
            val allocator = InMemoryIdAllocator.load(state.takeIf { Files.exists(it) })
            // appendOtzaria registers books through the allocator; without it the
            // snapshot GC drops this structure's keys as orphans.
            allocator.bookId("Tashma", "Book")
            DatabaseGenerator(sourceDirectory = sourceDir, repository = repo, allocator = allocator)
                .generateLinksOnly()
            allocator.snapshotTo(state)
            val structureId = repo.getAltTocStructuresForBook(1).single().id
            repo.getAltTocEntriesForStructure(structureId).sortedBy { it.id }.map { it.text to it.id }
        }

        val first = run()
        assertEquals(listOf("A", "A1", "A2", "B", "B1"), first.map { it.first })
        assertEquals(first, run(), "alt_toc_entry ids must be reproduced from the build state")

        val snapshot = BuildStateReader().read(state)
        assertEquals(5, snapshot.altTocEntries.size)
        assertEquals(first.map { it.second }.toSet(), snapshot.altTocEntries.values.toSet())

        repo.close()
    }
}
