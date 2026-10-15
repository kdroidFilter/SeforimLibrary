package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
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
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/** Otzaria: book ids across builds of our importer (upstream #118, adapted). */
class OtzariaBookIdContinuityTest {

    private val body = (1..20).map { "שורה $it" }

    /** One library file: its source, its folder under אוצריא, and its title. */
    private data class BookFile(val source: String, val folder: String, val title: String, val lines: List<String>)

    private fun library(files: List<BookFile>): Path {
        val root = Files.createTempDirectory("otzaria-continuity")
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

    /**
     * Builds [files] against [state], on a DB already holding the Sefaria books [sefaria],
     * and returns each book's (source, title) with its id. files_manifest.json leaves out
     * the book titled [unhashed]. [sefariaKeyed] are Sefaria books the build state holds,
     * key title to display title, as the Sefaria importer leaves them.
     */
    private fun build(
        state: Path,
        version: Int,
        files: List<BookFile>,
        sefaria: List<String> = emptyList(),
        unhashed: String? = null,
        sefariaKeyed: Map<String, String> = emptyMap(),
    ): Map<Pair<String, String>, Long> = runBlocking {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        try {
            if (sefaria.isNotEmpty()) {
                val sourceId = repo.insertSource("Sefaria")
                val categoryId = repo.insertCategory(Category(title = "ספריא"))
                sefaria.forEachIndexed { i, title ->
                    repo.insertBook(Book(id = 500_000L + i, categoryId = categoryId, sourceId = sourceId, title = title, heRef = title))
                }
            }
            val allocator = InMemoryIdAllocator.load(state.takeIf { Files.exists(it) })
            if (sefariaKeyed.isNotEmpty()) {
                val sourceId = repo.insertSource("Sefaria")
                val categoryId = repo.insertCategory(Category(title = "Sefaria"))
                sefariaKeyed.forEach { (key, display) ->
                    val id = allocator.bookId("Sefaria", key)
                    repo.insertBook(Book(id = id, categoryId = categoryId, sourceId = sourceId, title = display, heRef = key))
                }
            }
            val sourceDir = library(files).also(::writeFilesManifest)
            if (unhashed != null) {
                val manifest = sourceDir.resolve("files_manifest.json")
                Files.write(manifest, Files.readAllLines(manifest).filterNot { "/$unhashed.txt\"" in it })
            }
            DatabaseGenerator(
                sourceDirectory = sourceDir,
                repository = repo,
                allocator = allocator,
                buildVersion = version,
            ).generateLinesOnly()
            allocator.snapshotTo(state)
            val sources = repo.getAllSources().associate { it.id to it.name }
            repo.getAllBooks().associate { (sources.getValue(it.sourceId) to it.title) to it.id }
        } finally {
            repo.close()
        }
    }

    private fun newState(): Path = Files.createTempDirectory("otzaria-continuity-state").resolve("build_state.db")

    @Test
    fun aBookMovedToAnotherSourceKeepsItsId() {
        val state = newState()
        val before = build(state, 1, listOf(BookFile("MoreBooks", "חסידות", "ספר", body)))
        val after = build(state, 2, listOf(BookFile("KSK", "חסידות", "ספר", body)))
        assertEquals(before.getValue("MoreBooks" to "ספר"), after.getValue("KSK" to "ספר"))
        // And again from the state that build wrote.
        assertEquals(after, build(state, 3, listOf(BookFile("KSK", "חסידות", "ספר", body))))
    }

    @Test
    fun aRenamedFileKeepsItsId() {
        val state = newState()
        val before = build(state, 1, listOf(BookFile("A", "חסידות", "ספר - מחבר", body)))
        val after = build(state, 2, listOf(BookFile("A", "חסידות", "ספר", body)))
        assertEquals(before.getValue("A" to "ספר - מחבר"), after.getValue("A" to "ספר"))
    }

    @Test
    fun anOldKeyBackBesideItsNewOneFailsTheBuild() {
        val state = newState()
        build(state, 1, listOf(BookFile("MoreBooks", "חסידות", "ספר", body)))
        build(state, 2, listOf(BookFile("KSK", "חסידות", "ספר", body)))
        val error = assertFailsWith<IllegalStateException> {
            build(
                state, 3,
                listOf(
                    BookFile("KSK", "חסידות", "ספר", body),
                    BookFile("MoreBooks", "מוסר", "ספר", body.map { "אחר $it" }),
                ),
            )
        }
        assertTrue("went to both" in error.message.orEmpty(), error.message)
    }

    @Test
    fun aBookGoneForABuildComesBackWithANewId() {
        val state = newState()
        val first = build(state, 1, listOf(BookFile("A", "חסידות", "ספר", body), BookFile("A", "מוסר", "אחר", body.map { "ב $it" })))
        build(state, 2, listOf(BookFile("A", "מוסר", "אחר", body.map { "ב $it" })))
        val back = build(state, 3, listOf(BookFile("C", "חסידות", "ספר", body), BookFile("A", "מוסר", "אחר", body.map { "ב $it" })))
        assertNotEquals(first.getValue("A" to "ספר"), back.getValue("C" to "ספר"))
    }

    @Test
    fun aDeadKeyOfTheSameTitleDoesNotBlockAMove() {
        // Without the previous build's books only, (A, ספר) and (B, ספר) would both be
        // candidates for (C, ספר), no alias would be unique and the build would fail.
        val state = newState()
        val other = BookFile("A", "מוסר", "אחר", body.map { "ב $it" })
        build(state, 1, listOf(BookFile("A", "חסידות", "ספר", body), other))
        build(state, 2, listOf(other))
        val returned = build(state, 3, listOf(BookFile("B", "חסידות", "ספר", body), other))
        val moved = build(state, 4, listOf(BookFile("C", "חסידות", "ספר", body), other))
        assertEquals(returned.getValue("B" to "ספר"), moved.getValue("C" to "ספר"))
    }

    @Test
    fun aBookSefariaNowCoversIsLeftToTheLibraryCheck() {
        val state = newState()
        build(state, 1, listOf(BookFile("A", "חסידות", "ספר", body)))
        // The Otzaria file is skipped for Sefaria's edition; its id is gone, the build goes on.
        val after = build(state, 2, listOf(BookFile("A", "חסידות", "ספר", body)), sefaria = listOf("ספר"))
        assertEquals(setOf("Sefaria" to "ספר"), after.keys)
    }

    @Test
    fun aBookImportedWithoutASourceHashFailsTheBuild() {
        // The next build would take it for a book that is gone, and its key for a free one.
        val error = assertFailsWith<IllegalStateException> {
            build(newState(), 1, listOf(BookFile("A", "חסידות", "ספר", body)), unhashed = "ספר")
        }
        assertTrue("no source hash" in error.message.orEmpty(), error.message)
    }

    @Test
    fun aBuildNotAfterThePreviousOneFailsTheBuild() {
        val state = newState()
        val book = BookFile("A", "חסידות", "ספר", body)
        build(state, 2, listOf(book))
        val error = assertFailsWith<IllegalStateException> { build(state, 2, listOf(book)) }
        assertTrue("is not after" in error.message.orEmpty(), error.message)
    }

    @Test
    fun aBookRenamedToAKeyAnOlderBuildHadKeepsItsId() {
        val state = newState()
        val live = BookFile("A", "חסידות", "ספר חי", body)
        val gone = BookFile("A", "מוסר", "שם ישן", body.map { "אחר $it" })
        val first = build(state, 1, listOf(live, gone))
        build(state, 2, listOf(live))
        val renamed = build(state, 3, listOf(live.copy(title = gone.title)))
        assertEquals(first.getValue("A" to live.title), renamed.getValue("A" to gone.title))
        // The gone book's id is retired: a new book does not get it.
        val next = build(state, 4, listOf(live.copy(title = gone.title), BookFile("A", "מוסר", "ספר חדש", body.map { "חדש $it" })))
        assertEquals(renamed.getValue("A" to gone.title), next.getValue("A" to gone.title))
        assertTrue(first.getValue("A" to gone.title) !in next.values, "$next")
    }

    @Test
    fun aGoneBookBackUnderItsOwnKeyKeepsItsOldId() {
        // Not a rename: the other book is still there.
        val state = newState()
        val live = BookFile("A", "חסידות", "ספר חי", body)
        val gone = BookFile("A", "מוסר", "שם ישן", body.map { "אחר $it" })
        val first = build(state, 1, listOf(live, gone))
        build(state, 2, listOf(live))
        assertEquals(first, build(state, 3, listOf(live, gone)))
    }

    @Test
    fun aBookSefariaDroppedKeepsItsIdInOtzaria() {
        val state = newState()
        val before = build(state, 1, listOf(BookFile("A", "x", "y", body)), sefariaKeyed = mapOf("Dropped" to "Dropped"))
        val after = build(state, 2, listOf(BookFile("A", "x", "y", body), BookFile("MoreBooks", "x", "Dropped", body)))
        assertEquals(before.getValue("Sefaria" to "Dropped"), after.getValue("MoreBooks" to "Dropped"))
    }

    @Test
    fun aLiveSefariaBookTitledApartFromItsKeyKeepsItsId() {
        // Upstream compared keys with DB titles: this book looked gone, and the Otzaria
        // book of the same normalised title would have taken its id.
        val state = newState()
        val sefaria = mapOf("Maharsha \"A\"" to "Maharsha display")
        build(state, 1, listOf(BookFile("A", "x", "y", body)), sefariaKeyed = sefaria)
        val after = build(
            state, 2,
            listOf(BookFile("A", "x", "y", body), BookFile("MoreBooks", "x", "Maharsha ״A״", body)),
            sefariaKeyed = sefaria,
        )
        assertNotEquals(after.getValue("Sefaria" to "Maharsha display"), after.getValue("MoreBooks" to "Maharsha ״A״"))
    }
}
