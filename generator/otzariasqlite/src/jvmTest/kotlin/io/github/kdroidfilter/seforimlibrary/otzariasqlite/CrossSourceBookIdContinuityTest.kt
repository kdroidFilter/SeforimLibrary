package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.changes.BookIdContinuity
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/** Otzaria: book ids of books moving between Sefaria and Otzaria over several builds. */
class CrossSourceBookIdContinuityTest {

    private val body = (1..20).map { "line $it" }
    private val other = BookFile("A", "x", "Other", body.map { "other $it" })

    private data class BookFile(val source: String, val folder: String, val title: String, val lines: List<String>)

    private fun library(files: List<BookFile>): Path {
        val root = Files.createTempDirectory("cross-source")
        val manifest = files.joinToString(",\n", "{\n", "\n}") { f ->
            "  \"${f.source}/אוצריא/${f.folder}/${f.title}.txt\": { \"hash\": \"x\" }"
        }
        Files.writeString(root.resolve("files_manifest_new.json"), manifest)
        for (f in files) {
            val dir = Files.createDirectories(root.resolve("אוצריא").resolve(f.folder))
            Files.writeString(dir.resolve("${f.title}.txt"), (listOf("<h1>${f.title}</h1>") + f.lines).joinToString("\n"))
        }
        return root
    }

    private class Built(val ids: Map<Pair<String, String>, Long>, val lines: Map<Long, Long>)

    /**
     * One build: the Sefaria step as the Sefaria importer registers aliases and allocates
     * [sefaria] (no line hashes: they never match Otzaria's), then Otzaria phase 1 on [files].
     */
    private fun build(state: Path, version: Int, files: List<BookFile>, sefaria: List<String> = emptyList()): Built = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
            val allocator = InMemoryIdAllocator.load(state.takeIf { Files.exists(it) })
            val sourceId = repo.insertSource("Sefaria")
            val categoryId = repo.insertCategory(Category(title = "Sefaria"))
            val keys = sefaria.mapTo(LinkedHashSet()) { BookKey("Sefaria", it) }
            val continuity = BookIdContinuity(allocator)
            continuity.registerAliases(
                currentKeys = keys,
                inScope = { it.sourceName == "Sefaria" || it.canonicalHeTitle in sefaria },
                atVersion = version,
                currentLineHashes = { null },
            )
            continuity.settleCrossSourceAliases(keys, version)
            sefaria.forEach { title ->
                val id = allocator.bookId("Sefaria", title)
                repo.insertBook(Book(id = id, categoryId = categoryId, sourceId = sourceId, title = title, heRef = title))
            }
            DatabaseGenerator(
                sourceDirectory = library(files).also(::writeFilesManifest),
                repository = repo,
                allocator = allocator,
                buildVersion = version,
            ).generateLinesOnly()
            allocator.snapshotTo(state)
            val sources = repo.getAllSources().associate { it.id to it.name }
            val ids = repo.getAllBooks().associate { (sources.getValue(it.sourceId) to it.title) to it.id }
            val lines = HashMap<Long, Long>()
            driver.executeQuery(null, "SELECT bookId, count(*) FROM line GROUP BY bookId", { c ->
                while (c.next().value) lines[c.getLong(0)!!] = c.getLong(1)!!
                QueryResult.Unit
            }, 0)
            Built(ids, lines)
        } finally {
            repo.close()
        }
    }

    private fun newState(): Path = Files.createTempDirectory("cross-source-state").resolve("build_state.db")

    @Test
    fun aBookMovingBackAndForthKeepsItsId() {
        val state = newState()
        val moreBooks = BookFile("MoreBooks", "x", "Book", body)
        val id = build(state, 1, listOf(other), sefaria = listOf("Book")).ids.getValue("Sefaria" to "Book")
        assertEquals(id, build(state, 2, listOf(other, moreBooks)).ids.getValue("MoreBooks" to "Book"))
        assertEquals(id, build(state, 3, listOf(other, moreBooks), sefaria = listOf("Book")).ids.getValue("Sefaria" to "Book"))
        assertEquals(id, build(state, 4, listOf(other, moreBooks)).ids.getValue("MoreBooks" to "Book"))
        assertEquals(id, build(state, 5, listOf(other, moreBooks.copy(source = "KSK"))).ids.getValue("KSK" to "Book"))
        assertEquals(id, build(state, 6, listOf(other, moreBooks.copy(source = "KSK")), sefaria = listOf("Book")).ids.getValue("Sefaria" to "Book"))
        assertEquals(id, build(state, 7, listOf(other, moreBooks)).ids.getValue("MoreBooks" to "Book"))
    }

    @Test
    fun anOtzariaBookTitledApartFromADroppedSefariaBookIsNotAliasedToIt() {
        // Matched by exact title only, as the Otzaria importer tells Sefaria's books: otherwise
        // the Sefaria book back later would follow the alias to the Otzaria book's id.
        val state = newState()
        val gershayim = BookFile("MoreBooks", "x", "R״M", body)
        val first = build(state, 1, listOf(other), sefaria = listOf("R\"M"))
        val second = build(state, 2, listOf(other, gershayim))
        assertNotEquals(first.ids.getValue("Sefaria" to "R\"M"), second.ids.getValue("MoreBooks" to "R״M"))
        val third = build(state, 3, listOf(other, gershayim), sefaria = listOf("R\"M"))
        assertEquals(first.ids.getValue("Sefaria" to "R\"M"), third.ids.getValue("Sefaria" to "R\"M"))
        assertEquals(second.ids.getValue("MoreBooks" to "R״M"), third.ids.getValue("MoreBooks" to "R״M"))
    }

    @Test
    fun anOtzariaRenameKeepsItsIdBeforeADroppedSefariaTitle() {
        val state = newState()
        val old = BookFile("MoreBooks", "x", "Old", body)
        val first = build(state, 1, listOf(other, old), sefaria = listOf("Dead"))
        build(state, 2, listOf(other, old))
        val renamed = build(state, 3, listOf(other, old.copy(title = "Dead")))
        assertEquals(first.ids.getValue("MoreBooks" to "Old"), renamed.ids.getValue("MoreBooks" to "Dead"))
    }

    @Test
    fun aSefariaBookBackBesideTheRenamedBookItsAliasLeadsToTakesANewId() {
        val state = newState()
        val id = build(state, 1, listOf(other), sefaria = listOf("Book")).ids.getValue("Sefaria" to "Book")
        assertEquals(id, build(state, 2, listOf(other, BookFile("MoreBooks", "x", "Book", body))).ids.getValue("MoreBooks" to "Book"))
        val renamed = BookFile("MoreBooks", "x", "Book renamed", body)
        assertEquals(id, build(state, 3, listOf(other, renamed)).ids.getValue("MoreBooks" to "Book renamed"))
        val back = build(state, 4, listOf(other, renamed), sefaria = listOf("Book"))
        assertEquals(id, back.ids.getValue("MoreBooks" to "Book renamed"))
        assertNotEquals(id, back.ids.getValue("Sefaria" to "Book"))
        assertEquals(body.size + 1L, back.lines.getValue(id))
    }

    @Test
    fun anOtzariaBookBackBesideTheSefariaBookItsAliasLeadsToTakesANewId() {
        // Sefaria takes the book over, then fixes its title: the Otzaria file is no longer
        // skipped for it, and is a book of its own again.
        val state = newState()
        val file = BookFile("MoreBooks", "x", "Book A", body)
        val id = build(state, 1, listOf(other, file)).ids.getValue("MoreBooks" to "Book A")
        assertEquals(id, build(state, 2, listOf(other, file), sefaria = listOf("Book A")).ids.getValue("Sefaria" to "Book A"))
        val fixed = build(state, 3, listOf(other, file), sefaria = listOf("Book  A"))
        assertEquals(id, fixed.ids.getValue("Sefaria" to "Book  A"))
        assertNotEquals(id, fixed.ids.getValue("MoreBooks" to "Book A"))
        assertEquals(body.size + 1L, fixed.lines.getValue(fixed.ids.getValue("MoreBooks" to "Book A")))
    }

    @Test
    fun aBookBlacklistedInAndOutOfSefariaKeepsItsIdBesideADeadKeyOfItsTitle() {
        // The dead (A, T) makes the title ambiguous for registerAliases; the alias still
        // gives the id back to Sefaria, which the Otzaria importer skips (C, T) for.
        val state = newState()
        val t = { source: String -> BookFile(source, "x", "T", body) }
        build(state, 1, listOf(other, t("A")))
        build(state, 2, listOf(other))
        build(state, 3, listOf(other, t("C")))
        val id = build(state, 4, listOf(other, t("C")), sefaria = listOf("T")).ids.getValue("Sefaria" to "T")
        assertEquals(id, build(state, 5, listOf(other, t("C"))).ids.getValue("C" to "T"))
        assertEquals(id, build(state, 6, listOf(other, t("C")), sefaria = listOf("T")).ids.getValue("Sefaria" to "T"))
        assertEquals(id, build(state, 7, listOf(other, t("C"))).ids.getValue("C" to "T"))
        assertEquals(id, build(state, 8, listOf(other, t("C")), sefaria = listOf("T")).ids.getValue("Sefaria" to "T"))
    }
}
