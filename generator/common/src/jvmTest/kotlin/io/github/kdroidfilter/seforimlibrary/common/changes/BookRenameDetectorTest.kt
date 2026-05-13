package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import org.junit.Test
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class BookRenameDetectorTest {

    private fun sha1(s: String): BookRenameDetector.ContentHash =
        BookRenameDetector.ContentHash(MessageDigest.getInstance("SHA-1").digest(s.toByteArray()))

    private fun hashes(vararg contents: String): BookRenameDetector.LineHashSet =
        BookRenameDetector.LineHashSet(contents.map { sha1(it) }.toCollection(HashSet()))

    private val oldKey = BookKey("Sefaria", "Old Title")
    private val newKey = BookKey("Sefaria", "New Title")
    private val unrelatedKey = BookKey("Sefaria", "Unrelated")

    @Test
    fun `90 percent overlap is detected as rename`() {
        // 9/10 lines match → jaccard = 9 / (10) = 0.9 ≥ 0.8 → rename
        val common = (1..9).map { "line $it" }.toTypedArray()
        val oldLines = hashes(*common, "old only")
        val newLines = hashes(*common, "new only")
        val det = BookRenameDetector()
        val aliases = det.detect(
            removed = mapOf(oldKey to oldLines),
            added = mapOf(newKey to newLines),
            atVersion = 1,
        )
        assertEquals(1, aliases.size)
        assertEquals(oldKey, aliases[0].oldKey)
        assertEquals(newKey, aliases[0].newKey)
        assertTrue(aliases[0].jaccard >= 0.8)
    }

    @Test
    fun `low overlap is NOT detected as rename`() {
        // 2/10 lines match → below default 0.8
        val oldLines = hashes("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
        val newLines = hashes("a", "b", "X", "Y", "Z", "W", "V", "U", "T", "S")
        val aliases = BookRenameDetector().detect(
            removed = mapOf(oldKey to oldLines),
            added = mapOf(newKey to newLines),
            atVersion = 1,
        )
        assertTrue(aliases.isEmpty(), "expected no rename, got $aliases")
    }

    @Test
    fun `best match wins among multiple candidates`() {
        val oldLines = hashes("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
        // Candidate 1: 50 % overlap (below threshold)
        val mediocre = hashes("a", "b", "c", "d", "e", "X", "Y", "Z", "W", "V")
        // Candidate 2: 90 % overlap (above threshold) — should be picked
        val perfect = hashes("a", "b", "c", "d", "e", "f", "g", "h", "i", "QQ")
        val aliases = BookRenameDetector().detect(
            removed = mapOf(oldKey to oldLines),
            added = mapOf(
                BookKey("Sefaria", "Mediocre") to mediocre,
                BookKey("Sefaria", "Perfect") to perfect,
            ),
            atVersion = 5,
        )
        assertEquals(1, aliases.size)
        assertEquals(BookKey("Sefaria", "Perfect"), aliases[0].newKey)
    }

    @Test
    fun `a matched added book is not reused for another removed book`() {
        val identical = hashes(*(1..10).map { "shared $it" }.toTypedArray())
        // Both removed books have identical line sets; one new book also has it.
        // Only one alias should be emitted; the second removed book should be left alone.
        val aliases = BookRenameDetector().detect(
            removed = mapOf(
                BookKey("Sefaria", "OldA") to identical,
                BookKey("Sefaria", "OldB") to identical,
            ),
            added = mapOf(BookKey("Sefaria", "New") to identical),
            atVersion = 1,
        )
        assertEquals(1, aliases.size)
    }

    @Test
    fun `empty removed or added produces empty result`() {
        val det = BookRenameDetector()
        assertTrue(
            det.detect(emptyMap(), mapOf(newKey to hashes("a")), 1).isEmpty(),
        )
        assertTrue(
            det.detect(mapOf(oldKey to hashes("a")), emptyMap(), 1).isEmpty(),
        )
    }

    @Test
    fun `custom threshold raises bar`() {
        // 9/10 match → 0.9 — passes default 0.8 but not 0.95.
        val oldLines = hashes(*(1..9).map { "L$it" }.toTypedArray(), "old")
        val newLines = hashes(*(1..9).map { "L$it" }.toTypedArray(), "new")
        val strict = BookRenameDetector(threshold = 0.95)
        assertTrue(strict.detect(mapOf(oldKey to oldLines), mapOf(newKey to newLines), 1).isEmpty())
        val relaxed = BookRenameDetector(threshold = 0.5)
        assertEquals(1, relaxed.detect(mapOf(oldKey to oldLines), mapOf(newKey to newLines), 1).size)
    }

    @Test
    fun `ContentHash equals is byte-structural`() {
        val a = sha1("hello")
        val b = sha1("hello")
        val c = sha1("world")
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertNotEquals(a, c)
    }
}
