package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TouchedBookDetectorTest {

    private fun hash(seed: Byte): BookSourceHash = BookSourceHash(ByteArray(32) { seed }, 1)

    private val genesis = BookKey("Sefaria", "בראשית")
    private val exodus = BookKey("Sefaria", "שמות")
    private val leviticus = BookKey("Sefaria", "ויקרא")
    private val numbers = BookKey("Sefaria", "במדבר")

    @Test
    fun `classifies unchanged, touched, added, removed`() {
        val previous = mapOf(
            genesis to hash(1),
            exodus to hash(2),
            leviticus to hash(3), // will be removed
        )
        val current = mapOf(
            genesis to hash(1),    // unchanged
            exodus to hash(99),    // touched (hash differs)
            numbers to hash(4),    // added
        )

        val c = TouchedBookDetector.classify(previous, current)
        assertEquals(setOf(genesis), c.unchanged)
        assertEquals(setOf(exodus), c.touched)
        assertEquals(setOf(numbers), c.added)
        assertEquals(setOf(leviticus), c.removed)
    }

    @Test
    fun `empty previous and current produces empty classification`() {
        val c = TouchedBookDetector.classify(emptyMap(), emptyMap())
        assertTrue(c.unchanged.isEmpty() && c.touched.isEmpty() && c.added.isEmpty() && c.removed.isEmpty())
    }

    @Test
    fun `first build classifies everything as added`() {
        val current = mapOf(genesis to hash(1), exodus to hash(2))
        val c = TouchedBookDetector.classify(previous = emptyMap(), current = current)
        assertEquals(setOf(genesis, exodus), c.added)
        assertTrue(c.unchanged.isEmpty() && c.touched.isEmpty() && c.removed.isEmpty())
    }

    @Test
    fun `summary string reports all four bucket sizes`() {
        val c = TouchedBookDetector.classify(
            previous = mapOf(genesis to hash(1), exodus to hash(2)),
            current = mapOf(genesis to hash(99), numbers to hash(3)),
        )
        val s = c.summary()
        assertTrue("touched=1" in s, s)
        assertTrue("added=1" in s, s)
        assertTrue("removed=1" in s, s)
        assertTrue("unchanged=0" in s, s)
    }

    @Test
    fun `hash comparison is byte-structural even with same logical hash but distinct arrays`() {
        // Two arrays with identical content but distinct identities — classifier must use contentEquals.
        val previous = mapOf(genesis to BookSourceHash(ByteArray(32) { 7 }, 1))
        val current = mapOf(genesis to BookSourceHash(ByteArray(32) { 7 }, 2)) // same hash bytes, different version
        val c = TouchedBookDetector.classify(previous, current)
        assertEquals(setOf(genesis), c.unchanged)
    }
}
