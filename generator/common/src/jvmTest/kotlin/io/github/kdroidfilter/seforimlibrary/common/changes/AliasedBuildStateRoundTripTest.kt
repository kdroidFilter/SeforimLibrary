package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import kotlin.test.assertEquals

/** Otzaria: an aliased book must survive the build state it is written to. */
class AliasedBuildStateRoundTripTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `an aliased book keeps its id across a snapshot and reload`() {
        val state = tmp.newFolder().toPath().resolve("build_state.db")
        val old = BookKey("MoreBooks", "ספר")
        val new = BookKey("KSK", "ספר")
        val build1 = InMemoryIdAllocator.load(path = null)
        val id = build1.bookId(old.sourceName, old.canonicalHeTitle)
        build1.snapshotTo(state)

        val build2 = InMemoryIdAllocator.load(state)
        build2.registerBookAlias(old, new, atVersion = 2)
        assertEquals(id, build2.bookId(new.sourceName, new.canonicalHeTitle))
        build2.snapshotTo(state)

        val build3 = InMemoryIdAllocator.load(state)
        assertEquals(id, build3.bookId(new.sourceName, new.canonicalHeTitle))
    }

    @Test
    fun `a book renamed twice keeps its id and every former key`() {
        val state = tmp.newFolder().toPath().resolve("build_state.db")
        val a = BookKey("A", "שם ראשון")
        val b = BookKey("A", "שם שני")
        val c = BookKey("A", "שם שלישי")
        val build1 = InMemoryIdAllocator.load(path = null)
        val id = build1.bookId(a.sourceName, a.canonicalHeTitle)
        build1.snapshotTo(state)

        val build2 = InMemoryIdAllocator.load(state)
        build2.registerBookAlias(a, b, atVersion = 2)
        build2.snapshotTo(state)

        val build3 = InMemoryIdAllocator.load(state)
        build3.registerBookAlias(b, c, atVersion = 3)
        build3.snapshotTo(state)

        val build4 = InMemoryIdAllocator.load(state)
        assertEquals(mapOf(a to id, b to id, c to id), build4.knownBookKeys())
        assertEquals(id, build4.bookId(a.sourceName, a.canonicalHeTitle))
        assertEquals(id, build4.bookId(c.sourceName, c.canonicalHeTitle))
    }

    @Test
    fun `a book renamed back to a former title keeps its id`() {
        val state = tmp.newFolder().toPath().resolve("build_state.db")
        val a = BookKey("A", "שם ראשון")
        val b = BookKey("A", "שם שני")
        val build1 = InMemoryIdAllocator.load(path = null)
        val id = build1.bookId(a.sourceName, a.canonicalHeTitle)
        build1.snapshotTo(state)

        val build2 = InMemoryIdAllocator.load(state)
        build2.registerBookAlias(a, b, atVersion = 2)
        build2.snapshotTo(state)

        // The alias a -> b goes with the rename back, or a -> b -> a would loop.
        val build3 = InMemoryIdAllocator.load(state)
        build3.registerBookAlias(b, a, atVersion = 3)
        build3.snapshotTo(state)

        val build4 = InMemoryIdAllocator.load(state)
        assertEquals(mapOf(a to id, b to id), build4.knownBookKeys())
        assertEquals(id, build4.bookId(b.sourceName, b.canonicalHeTitle))
    }
}
