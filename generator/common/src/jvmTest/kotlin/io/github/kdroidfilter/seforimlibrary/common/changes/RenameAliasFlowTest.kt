package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/**
 * Glue test that mirrors what an importer's `main()` will do at the top of a
 * build: detect touched/removed/added, run the rename detector on
 * `added × removed`, register the resulting aliases on the IdAllocator,
 * then snapshot. On the next run the alias is loaded back and the renamed
 * book keeps its original id.
 */
class RenameAliasFlowTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun sha1(s: String): ByteArray =
        MessageDigest.getInstance("SHA-1").digest(s.toByteArray())

    private fun hash32(seed: Byte) = BookSourceHash(ByteArray(32) { seed }, 1)

    private fun lineHashSet(vararg contents: String): BookRenameDetector.LineHashSet =
        BookRenameDetector.LineHashSet(contents.map { BookRenameDetector.ContentHash(sha1(it)) }.toSet())

    @Test
    fun `rename preserves book id across builds`() {
        val statePath = tmp.newFolder().toPath().resolve("build_state.db")

        // ── Build 1: original universe ──────────────────────────────────────
        val allocator1 = InMemoryIdAllocator.load(path = null)
        val oldKey = BookKey("Sefaria", "Old Title")
        val unrelatedKey = BookKey("Sefaria", "Unrelated")
        val oldId = allocator1.bookId(oldKey.sourceName, oldKey.canonicalHeTitle)
        val unrelatedId = allocator1.bookId(unrelatedKey.sourceName, unrelatedKey.canonicalHeTitle)
        // Record book line natural keys (via the standard lineId API so they end up in id_line).
        val sharedHashes = (1..10).map { sha1("shared $it") }
        for ((i, h) in sharedHashes.withIndex()) {
            allocator1.lineId(oldId, h, 0)
            allocator1.lineId(unrelatedId, sha1("unrelated $i"), 0)
        }
        // Record source hashes
        allocator1.recordSourceHash(oldKey, hash32(1))
        allocator1.recordSourceHash(unrelatedKey, hash32(9))
        allocator1.snapshotTo(statePath, mapOf("build_version" to "1"))

        // ── Build 2: oldKey is renamed to newKey; same line content ─────────
        val allocator2 = InMemoryIdAllocator.load(statePath)
        val newKey = BookKey("Sefaria", "New Title")

        // 1. Compute current source hashes (newKey is "added", oldKey is "removed",
        //    unrelatedKey is "unchanged").
        val currentHashes = mapOf(
            newKey to hash32(2),       // new hash (different content path)
            unrelatedKey to hash32(9), // identical to previous → unchanged
        )

        // 2. Touched-book detection.
        val previousHashes = mapOf(
            oldKey to hash32(1),
            unrelatedKey to hash32(9),
        )
        val classification = TouchedBookDetector.classify(previousHashes, currentHashes)
        assertEquals(setOf(unrelatedKey), classification.unchanged)
        assertEquals(setOf(newKey), classification.added)
        assertEquals(setOf(oldKey), classification.removed)

        // 3. Rename detector reads previous-build line hashes from the snapshot.
        val addedLines = mapOf(newKey to lineHashSet(*(1..10).map { "shared $it" }.toTypedArray()))
        val previousSnapshot = io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader()
            .read(statePath)
        val aliases = BookRenameDetector().detectFromSnapshot(
            previous = previousSnapshot,
            addedSources = addedLines,
            removedKeys = classification.removed,
            atVersion = 2,
        )
        assertEquals(1, aliases.size)
        assertEquals(oldKey, aliases[0].oldKey)
        assertEquals(newKey, aliases[0].newKey)

        // 4. Register the alias on the allocator and resolve newKey.
        aliases.forEach { allocator2.registerBookAlias(it.oldKey, it.newKey, atVersion = 2) }
        val resolvedNewId = allocator2.bookId(newKey.sourceName, newKey.canonicalHeTitle)

        assertEquals(oldId, resolvedNewId, "renamed book must keep its original id")

        // Unrelated book unchanged.
        assertEquals(unrelatedId, allocator2.bookId(unrelatedKey.sourceName, unrelatedKey.canonicalHeTitle))

        // 5. Without an alias, a brand-new book would get a fresh id.
        val freshId = allocator2.bookId("Sefaria", "Totally Fresh Book")
        assertNotEquals(oldId, freshId)
        assertNotEquals(unrelatedId, freshId)
    }

    @Test
    fun `source hashes round-trip through snapshot reload`() {
        val statePath = tmp.newFolder().toPath().resolve("build_state.db")
        val alloc1 = InMemoryIdAllocator.load(path = null)
        val key = BookKey("Otzaria", "Foo")
        alloc1.recordSourceHash(key, hash32(42))
        alloc1.snapshotTo(statePath)

        val alloc2 = InMemoryIdAllocator.load(statePath)
        val reloaded = alloc2.previousSourceHash(key)
        assertEquals(hash32(42), reloaded)
    }
}
