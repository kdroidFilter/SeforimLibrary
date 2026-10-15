package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.LegacyLineKey
import io.github.kdroidfilter.seforimlibrary.common.ids.LineOccurrenceCounter
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Otzaria: book ids across Sefaria renames and Sefaria/Otzaria moves (upstream 6a10f729, adapted). */
class SefariaBookIdContinuityTest {

    private val body = (1..20).map { "line $it of the book" }
    private val other = body.map { "other $it" }

    private fun payload(title: String, lines: List<String>) = BookPayload(
        heTitle = title, enTitle = title, categoriesHe = listOf("Cat"),
        lines = lines, refEntries = emptyList(), headings = emptyList(), authors = emptyList(),
        description = null, heShortDesc = null, pubDates = emptyList(), altStructures = emptyList(),
    ).precomputeLineData()

    private fun newState(): Path = Files.createTempDirectory("sefaria-continuity-state").resolve("build_state.db")

    /**
     * One build on [state]: the Sefaria import of [sefaria] (title to lines) as the importer
     * allocates it, then the Otzaria import of [otzaria] keys, which records a source hash for
     * each. Returns every allocated key with its id.
     */
    private fun build(
        state: Path,
        version: Int,
        sefaria: Map<String, List<String>>,
        otzaria: List<BookKey> = emptyList(),
    ): Map<BookKey, Long> {
        val allocator = InMemoryIdAllocator.load(state.takeIf { Files.exists(it) })
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)
        val out = LinkedHashMap<BookKey, Long>()
        try {
            val payloads = sefaria.map { (title, lines) -> payload(title, lines) }
            SefariaDirectImporter(Files.createTempDirectory("export"), repo, allocator, version).registerBookAliases(payloads)
            for (p in payloads) {
                // As the insert loop: the book id, then its line ids.
                val bookId = allocator.bookId("Sefaria", p.heTitle)
                val pre = requireNotNull(p.precomputed)
                val occ = LineOccurrenceCounter()
                val legacyOcc = LineOccurrenceCounter()
                p.lines.indices.forEach { idx ->
                    val hash = pre.lineKeyHashes!![idx]
                    val legacyHash = pre.legacyLineKeyHashes!![idx]
                    allocator.lineId(bookId, hash, occ.next(bookId, hash), LegacyLineKey(legacyHash, legacyOcc.next(bookId, legacyHash)))
                }
                out[BookKey("Sefaria", p.heTitle)] = bookId
            }
            for (key in otzaria) {
                out[key] = allocator.bookId(key.sourceName, key.canonicalHeTitle)
                allocator.recordSourceHash(key, BookSourceHash(ByteArray(32), version))
            }
            allocator.snapshotTo(state)
        } finally {
            repo.close()
        }
        return out
    }

    private fun sefaria(title: String) = BookKey("Sefaria", title)

    @Test
    fun aSefariaTitleFixKeepsItsId() {
        val state = newState()
        val before = build(state, 1, mapOf("Book  A" to body))
        val after = build(state, 2, mapOf("Book A" to body))
        assertEquals(before.getValue(sefaria("Book  A")), after.getValue(sefaria("Book A")))
    }

    @Test
    fun aSefariaRenameKeepsItsIdByContent() {
        val state = newState()
        val before = build(state, 1, mapOf("Old name" to body, "Kept" to other))
        val after = build(state, 2, mapOf("New name" to body, "Kept" to other))
        assertEquals(before.getValue(sefaria("Old name")), after.getValue(sefaria("New name")))
        assertEquals(before.getValue(sefaria("Kept")), after.getValue(sefaria("Kept")))
    }

    @Test
    fun aBookSefariaTakesOverFromOtzariaKeepsItsId() {
        val state = newState()
        val otzaria = BookKey("MoreBooks", "Taken")
        val before = build(state, 1, emptyMap(), listOf(otzaria))
        val after = build(state, 2, mapOf("Taken" to body))
        assertEquals(before.getValue(otzaria), after.getValue(sefaria("Taken")))
    }

    @Test
    fun aSefariaBookBackWithoutACompetitorKeepsItsOldId() {
        val state = newState()
        val first = build(state, 1, mapOf("Back" to body))
        build(state, 2, emptyMap(), listOf(BookKey("MoreBooks", "Unrelated")))
        val back = build(state, 3, mapOf("Back" to body))
        assertEquals(first.getValue(sefaria("Back")), back.getValue(sefaria("Back")))
    }

    @Test
    fun anOtzariaKeyHashedBesideALiveSefariaBookDoesNotTakeItsId() {
        // Builds before #77 recorded a hash for skipped Otzaria books too, so the build
        // state can hold an Otzaria key, hashed by the last build, under a live Sefaria title.
        val state = newState()
        val first = build(state, 1, mapOf("Both" to body), listOf(BookKey("MoreBooks", "Both")))
        val after = build(state, 2, mapOf("Both" to body))
        assertEquals(first.getValue(sefaria("Both")), after.getValue(sefaria("Both")))
    }
}
