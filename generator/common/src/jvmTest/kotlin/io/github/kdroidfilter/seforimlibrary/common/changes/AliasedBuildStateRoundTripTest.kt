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
}
