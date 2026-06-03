package io.github.kdroidfilter.seforimlibrary.common.buildstate

import org.junit.Test
import org.junit.rules.TemporaryFolder
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class BuildStateRoundtripTest {
    @JvmField
    @org.junit.Rule
    val tmp = TemporaryFolder()

    @Test
    fun `read returns empty snapshot when file is missing`() {
        val path = tmp.newFolder().toPath().resolve("missing.db")
        val snapshot = BuildStateReader().read(path)
        assertEquals(BuildStateSnapshot.empty(), snapshot)
    }

    @Test
    fun `write then read produces identical snapshot`() {
        val original = BuildStateSnapshot(
            schemaVersion = BuildStateSchema.CURRENT_VERSION,
            meta = mapOf("build_version" to "42", "generator_commit" to "abcdef"),
            counters = mapOf(
                IdTable.BOOK to 1001L,
                IdTable.LINE to 50_001L,
                IdTable.CATEGORY to 17L,
            ),
            lookups = mapOf(
                IdTable.SOURCE to mapOf("Sefaria" to 1L, "Otzaria" to 2L),
                IdTable.AUTHOR to mapOf("Rashi" to 10L, "Rambam" to 11L),
            ),
            books = mapOf(
                BookKey("Sefaria", "בראשית") to 100L,
                BookKey("Otzaria", "תהילים") to 101L,
            ),
            lines = mapOf(
                LineKey(100L, ByteArray(20) { it.toByte() }, 0) to 5000L,
                LineKey(100L, ByteArray(20) { (it + 1).toByte() }, 1) to 5001L,
            ),
            tocEntries = mapOf(
                TocEntryKey(100L, "1") to 9000L,
                TocEntryKey(100L, "1/2") to 9001L,
            ),
            altTocStructures = mapOf(
                AltTocStructureKey(100L, "Parasha") to 700L,
            ),
            altTocEntries = mapOf(
                AltTocEntryKey(700L, "1") to 7000L,
            ),
            links = mapOf(
                LinkKey(5000L, 5001L, 3L) to 80_000L,
            ),
            bookAliases = listOf(
                BookAlias(
                    oldKey = BookKey("Sefaria", "Old"),
                    newKey = BookKey("Sefaria", "New"),
                    detectedAtVersion = 42,
                ),
            ),
            sourceHashes = mapOf(
                BookKey("Sefaria", "בראשית") to BookSourceHash(ByteArray(32) { 1 }, 41),
                BookKey("Sefaria", "שמות")   to BookSourceHash(ByteArray(32) { 2 }, 42),
            ),
        )

        val target = tmp.newFolder().toPath().resolve("build_state.db")
        BuildStateWriter().write(original, target)
        assertTrue(java.nio.file.Files.exists(target))

        val reloaded = BuildStateReader().read(target)

        assertEquals(original.schemaVersion, reloaded.schemaVersion)
        assertEquals(original.meta, reloaded.meta)
        assertEquals(original.counters, reloaded.counters)
        assertEquals(original.lookups, reloaded.lookups)
        assertEquals(original.books, reloaded.books)
        assertEquals(original.tocEntries, reloaded.tocEntries)
        assertEquals(original.altTocStructures, reloaded.altTocStructures)
        assertEquals(original.altTocEntries, reloaded.altTocEntries)
        assertEquals(original.links, reloaded.links)
        assertEquals(original.bookAliases, reloaded.bookAliases)

        // LineKey uses a custom equals on the byte array; check via keys() comparison.
        assertEquals(original.lines.size, reloaded.lines.size)
        for ((k, v) in original.lines) {
            assertEquals(v, reloaded.lines[k], "line id mismatch for $k")
        }

        // BookSourceHash also has structural equals on the byte array.
        assertEquals(original.sourceHashes.size, reloaded.sourceHashes.size)
        for ((k, v) in original.sourceHashes) {
            assertEquals(v, reloaded.sourceHashes[k], "source hash mismatch for $k")
        }
    }

    @Test
    fun `write is atomic — tmp file removed after success`() {
        val target = tmp.newFolder().toPath().resolve("build_state.db")
        BuildStateWriter().write(BuildStateSnapshot.empty(), target)
        val tmpFile = target.resolveSibling("${target.fileName}.tmp")
        assertTrue(java.nio.file.Files.exists(target))
        assertTrue(!java.nio.file.Files.exists(tmpFile), "tmp file should be cleaned up")
    }

    @Test
    fun `LineKey equality is structural on the byte array`() {
        val a = LineKey(1L, byteArrayOf(1, 2, 3), 0)
        val b = LineKey(1L, byteArrayOf(1, 2, 3), 0)
        val c = LineKey(1L, byteArrayOf(1, 2, 4), 0)
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertNotEquals(a, c)
    }
}
