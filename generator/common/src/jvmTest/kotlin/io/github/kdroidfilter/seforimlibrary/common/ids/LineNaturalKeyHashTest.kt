package io.github.kdroidfilter.seforimlibrary.common.ids

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals

class LineNaturalKeyHashTest {

    @Test
    fun `heRef key is stable across content reformatting`() {
        // Same heRef, different rendered prefix: should yield the same hash.
        val a = IdAllocatorBindings.lineNaturalKeyHash(
            content = "(א) בְּרֵאשִׁית בָּרָא אֱלֹהִים",
            heRef = "Genesis 1:1",
        )
        val b = IdAllocatorBindings.lineNaturalKeyHash(
            content = "(ב) בְּרֵאשִׁית בָּרָא אֱלֹהִים", // prefix shifted by an insertion
            heRef = "Genesis 1:1",                       // citation unchanged
        )
        assertEquals(a.toList(), b.toList(), "heRef-keyed hash must ignore rendered content")
    }

    @Test
    fun `distinct heRef produces distinct hash`() {
        val verse1 = IdAllocatorBindings.lineNaturalKeyHash("foo", "Genesis 1:1")
        val verse2 = IdAllocatorBindings.lineNaturalKeyHash("foo", "Genesis 1:2")
        assertNotEquals(verse1.toList(), verse2.toList())
    }

    @Test
    fun `content-hash fallback when heRef is null`() {
        val headingA = IdAllocatorBindings.lineNaturalKeyHash("<h1>בראשית</h1>", null)
        val headingB = IdAllocatorBindings.lineNaturalKeyHash("<h1>בראשית</h1>", null)
        assertEquals(headingA.toList(), headingB.toList())

        val differentContent = IdAllocatorBindings.lineNaturalKeyHash("<h2>פרק א</h2>", null)
        assertNotEquals(headingA.toList(), differentContent.toList())
    }

    @Test
    fun `ref and content namespaces don't collide`() {
        // If "REF:foo" and "CT:foo" both hashed naively, they could collide.
        // The discriminator prefix prevents that.
        val viaRef = IdAllocatorBindings.lineNaturalKeyHash("anything", "foo")
        val viaContent = IdAllocatorBindings.lineNaturalKeyHash("foo", null)
        assertNotEquals(viaRef.toList(), viaContent.toList())
    }

    @Test
    fun `hash is always 20 bytes`() {
        assertEquals(20, IdAllocatorBindings.lineNaturalKeyHash("x", "y").size)
        assertEquals(20, IdAllocatorBindings.lineNaturalKeyHash("x", null).size)
    }

    @Test
    fun `empty content with heRef still keyed by heRef`() {
        val a = IdAllocatorBindings.lineNaturalKeyHash("", "Genesis 1:1")
        val b = IdAllocatorBindings.lineNaturalKeyHash("some text", "Genesis 1:1")
        assertEquals(a.toList(), b.toList())
    }

    @Test
    fun `Otzaria-style raw content path is byte-stable across calls`() {
        val a = IdAllocatorBindings.lineNaturalKeyHash("<p>שלום</p>", null)
        val b = IdAllocatorBindings.lineNaturalKeyHash("<p>שלום</p>", null)
        assertEquals(a.toList(), b.toList())
        // And differs from the legacy normalisedContentHash (which doesn't prefix).
        val legacy = IdAllocatorBindings.normalisedContentHash("<p>שלום</p>")
        assertFalse(a.toList() == legacy.toList(), "new hash must be namespaced (CT: prefix)")
    }
}
