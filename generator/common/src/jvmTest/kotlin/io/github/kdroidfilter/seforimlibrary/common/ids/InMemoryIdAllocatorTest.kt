package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.security.MessageDigest
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class InMemoryIdAllocatorTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun sha1(s: String): ByteArray =
        MessageDigest.getInstance("SHA-1").digest(s.toByteArray())

    @Test
    fun `fresh allocator hands out sequential ids per table starting at 1`() {
        val allocator = InMemoryIdAllocator.load(path = null)

        assertEquals(1L, allocator.sourceId("Sefaria"))
        assertEquals(2L, allocator.sourceId("Otzaria"))
        assertEquals(1L, allocator.authorId("Rashi")) // independent counter
        assertEquals(1L, allocator.bookId("Sefaria", "בראשית"))
        assertEquals(2L, allocator.bookId("Sefaria", "שמות"))
    }

    @Test
    fun `same natural key returns same id across calls`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val first = allocator.bookId("Sefaria", "בראשית")
        val second = allocator.bookId("Sefaria", "בראשית")
        assertEquals(first, second)
    }

    @Test
    fun `ids survive a snapshot+reload roundtrip`() {
        val statePath = tmp.newFolder().toPath().resolve("build_state.db")

        val first = InMemoryIdAllocator.load(path = null)
        val sefariaId = first.sourceId("Sefaria")
        val genesisId = first.bookId("Sefaria", "בראשית")
        val exodusId = first.bookId("Sefaria", "שמות")
        val hash = sha1("hello world").copyOf(20)
        val lineGenesis1 = first.lineId(genesisId, hash, 0)
        val tocId = first.tocEntryId(genesisId, "1/2")
        val typeId = first.connectionTypeId("commentary")
        val linkId = first.linkId(lineGenesis1, lineGenesis1 + 100, typeId)
        first.snapshotTo(statePath, mapOf("build_version" to "1"))

        val second = InMemoryIdAllocator.load(statePath)
        assertEquals(sefariaId, second.sourceId("Sefaria"))
        assertEquals(genesisId, second.bookId("Sefaria", "בראשית"))
        assertEquals(exodusId, second.bookId("Sefaria", "שמות"))
        assertEquals(lineGenesis1, second.lineId(genesisId, hash, 0))
        assertEquals(tocId, second.tocEntryId(genesisId, "1/2"))
        assertEquals(typeId, second.connectionTypeId("commentary"))
        assertEquals(linkId, second.linkId(lineGenesis1, lineGenesis1 + 100, typeId))

        // A new book gets the next free id, not a colliding one.
        val newBookId = second.bookId("Sefaria", "ויקרא")
        assertNotEquals(genesisId, newBookId)
        assertNotEquals(exodusId, newBookId)
        assertTrue(newBookId > exodusId)
    }

    @Test
    fun `book alias preserves id under new natural key`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val oldId = allocator.bookId("Sefaria", "Old Title")

        val oldKey = BookKey("Sefaria", "Old Title")
        val newKey = BookKey("Sefaria", "New Title")
        allocator.registerBookAlias(oldKey, newKey, atVersion = 5)

        // Lookup with new title returns the original id.
        assertEquals(oldId, allocator.bookId("Sefaria", "New Title"))
        // Old title still resolves too (alias indirection).
        assertEquals(oldId, allocator.bookId("Sefaria", "Old Title"))
    }

    @Test
    fun `concurrent lookups of same key never split into two ids`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        val pool = Executors.newFixedThreadPool(16)
        val ids = java.util.concurrent.ConcurrentHashMap.newKeySet<Long>()
        val tasks = (1..10_000).map {
            Runnable { ids.add(allocator.bookId("Sefaria", "racy")) }
        }
        tasks.forEach { pool.submit(it) }
        pool.shutdown()
        pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)
        assertEquals(1, ids.size, "racy key was assigned multiple ids: $ids")
    }

    @Test
    fun `counters skip used ids after partial snapshot`() {
        // Simulate a snapshot where someone manually inserted a high id externally:
        // counter says 5 but we already have id 100 mapped — allocator must avoid 100.
        val state = io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot(
            schemaVersion = io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSchema.CURRENT_VERSION,
            meta = emptyMap(),
            counters = mapOf(IdTable.BOOK to 5L),
            lookups = emptyMap(),
            books = mapOf(BookKey("Sefaria", "X") to 100L),
            lines = emptyMap(),
            tocEntries = emptyMap(),
            altTocStructures = emptyMap(),
            altTocEntries = emptyMap(),
            links = emptyMap(),
            bookAliases = emptyList(),
            sourceHashes = emptyMap(),
        )
        val allocator = InMemoryIdAllocator.fromSnapshot(state)
        val freshId = allocator.bookId("Sefaria", "Y")
        assertTrue(freshId > 100L, "fresh id $freshId must not collide with existing 100")
    }

    @Test
    fun `peekBookId returns null for unknown key`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        allocator.bookId("Sefaria", "Known")
        assertNull(allocator.peekBookId("Sefaria", "Unknown"))
    }

    @Test
    fun `stats track reused vs fresh`() {
        val allocator = InMemoryIdAllocator.load(path = null)
        allocator.bookId("Sefaria", "A") // fresh
        allocator.bookId("Sefaria", "B") // fresh
        allocator.bookId("Sefaria", "A") // reused
        val stats = allocator.stats().perTable.getValue(IdTable.BOOK)
        assertEquals(2L, stats.freshlyAllocated)
        assertEquals(1L, stats.reused)
        assertEquals(3L, stats.total)
    }
}
