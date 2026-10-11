package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BookIdContinuityTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun sha1(s: String): ByteArray = MessageDigest.getInstance("SHA-1").digest(s.toByteArray())

    private fun lines(prefix: String, count: Int = 20) = (1..count).map { "$prefix line $it" }

    /** Build 1: allocates [books] with their lines and snapshots, returning the ids. */
    private fun previousBuild(state: Path, books: Map<BookKey, List<String>>): Map<BookKey, Long> {
        val allocator = InMemoryIdAllocator.load(path = null)
        val ids = books.mapValues { (key, content) ->
            allocator.bookId(key.sourceName, key.canonicalHeTitle).also { id ->
                content.forEach { allocator.lineId(id, sha1(it), 0) }
            }
        }
        allocator.snapshotTo(state)
        return ids
    }

    private fun newState(): Path = tmp.newFolder().toPath().resolve("build_state.db")

    private fun BookIdContinuity.register(
        current: Map<BookKey, List<String>>,
        inScope: (BookKey) -> Boolean = { it.sourceName != "Sefaria" },
    ) = registerAliases(current.keys, inScope, atVersion = 2) { key -> current[key]?.map(::sha1) }

    @Test
    fun `book moved to another source keeps its id`() {
        val state = newState()
        val old = BookKey("Unknown", "ספר")
        val ids = previousBuild(state, mapOf(old to lines("a")))

        val allocator = InMemoryIdAllocator.load(state)
        val moved = BookKey("DictaToOtzaria", "ספר")
        val aliases = BookIdContinuity(allocator).register(mapOf(moved to lines("a")))

        assertEquals(mapOf(old to moved), aliases)
        assertEquals(ids.getValue(old), allocator.bookId(moved.sourceName, moved.canonicalHeTitle))
    }

    @Test
    fun `whitespace and quote fixes keep the id even when content changed`() {
        val state = newState()
        val spaced = BookKey("Unknown", " דברי חמודות על ברכות")
        val quoted = BookKey("Unknown", "שו\"ת")
        val ids = previousBuild(state, mapOf(spaced to lines("a"), quoted to lines("b")))

        val allocator = InMemoryIdAllocator.load(state)
        val trimmed = BookKey("Unknown", "דברי חמודות על ברכות")
        val gershayim = BookKey("KSK", "שו״ת")
        BookIdContinuity(allocator).register(mapOf(trimmed to lines("x"), gershayim to lines("y")))

        assertEquals(ids.getValue(spaced), allocator.bookId(trimmed.sourceName, trimmed.canonicalHeTitle))
        assertEquals(ids.getValue(quoted), allocator.bookId(gershayim.sourceName, gershayim.canonicalHeTitle))
    }

    @Test
    fun `renamed book is matched by its lines`() {
        val state = newState()
        val old = BookKey("Unknown", "שם ישן")
        val other = BookKey("Unknown", "אחר")
        val ids = previousBuild(state, mapOf(old to lines("a"), other to lines("b")))

        val allocator = InMemoryIdAllocator.load(state)
        val renamed = BookKey("Unknown", "שם חדש")
        val unrelated = BookKey("Unknown", "חדש לגמרי")
        BookIdContinuity(allocator).register(
            mapOf(renamed to lines("a") + "one more line", unrelated to lines("c"), other to lines("b")),
        )

        assertEquals(ids.getValue(old), allocator.bookId(renamed.sourceName, renamed.canonicalHeTitle))
        assertTrue(allocator.bookId(unrelated.sourceName, unrelated.canonicalHeTitle) !in ids.values)
    }

    @Test
    fun `ambiguous titles fall back to line hashes`() {
        val state = newState()
        val first = BookKey("A", "ספר")
        val second = BookKey("B", "ספר")
        val ids = previousBuild(state, mapOf(first to lines("first"), second to lines("second")))

        val allocator = InMemoryIdAllocator.load(state)
        val newSecond = BookKey("C", "ספר")
        val newFirst = BookKey("D", "ספר")
        BookIdContinuity(allocator).register(mapOf(newFirst to lines("first"), newSecond to lines("second")))

        assertEquals(ids.getValue(first), allocator.bookId(newFirst.sourceName, newFirst.canonicalHeTitle))
        assertEquals(ids.getValue(second), allocator.bookId(newSecond.sourceName, newSecond.canonicalHeTitle))
    }

    @Test
    fun `out of scope and still used ids are never reused`() {
        val state = newState()
        val sefaria = BookKey("Sefaria", "בראשית")
        val kept = BookKey("Unknown", "ספר")
        val ids = previousBuild(state, mapOf(sefaria to lines("a"), kept to lines("b")))

        val allocator = InMemoryIdAllocator.load(state)
        val otzariaGenesis = BookKey("Unknown", "בראשית")
        val sameTitle = BookKey("KSK", "ספר")
        val aliases = BookIdContinuity(allocator).register(
            mapOf(otzariaGenesis to lines("a"), kept to lines("b"), sameTitle to lines("b")),
        )

        assertTrue(aliases.isEmpty())
        val freshIds = listOf(otzariaGenesis, sameTitle).map { allocator.bookId(it.sourceName, it.canonicalHeTitle) }
        assertTrue(freshIds.none { it in ids.values })
    }

    @Test
    fun `lost ids are reported only when the title survived`() {
        val state = newState()
        val moved = BookKey("Unknown", "ספר")
        val removed = BookKey("Unknown", "הוסר")
        val aliased = BookKey("MoreBooks", "כוזרי")
        val ids = previousBuild(state, mapOf(moved to lines("a"), removed to lines("b"), aliased to lines("c")))

        val allocator = InMemoryIdAllocator.load(state)
        val continuity = BookIdContinuity(allocator)
        // Out of scope, so not aliased: simulates a key change the planner missed.
        continuity.register(mapOf(BookKey("KSK", "כוזרי") to lines("c")), inScope = { it != aliased })
        val movedId = allocator.bookId("Dicta", "ספר")
        val khuzariId = allocator.bookId("KSK", "כוזרי")

        val lost = continuity.findLostIds(mapOf(movedId to "ספר", khuzariId to "כוזרי"))

        assertEquals(
            setOf(aliased to ids.getValue(aliased), moved to ids.getValue(moved)),
            lost.mapTo(HashSet()) { it.key to it.id },
        )
    }
}
