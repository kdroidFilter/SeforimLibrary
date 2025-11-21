package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SefariaCitationParserTest {

    private val parser = SefariaCitationParser()

    @Test
    fun `parse simple citation with chapter and verse`() {
        val citation = parser.parse("Genesis 1:1")
        assertNotNull(citation)
        assertEquals("Genesis", citation.bookTitle)
        assertEquals(null, citation.section)
        assertEquals(listOf(1, 1), citation.references)
    }

    @Test
    fun `parse citation with section and references`() {
        val citation = parser.parse("Beit Yosef, Orach Chayim 325:34:1")
        assertNotNull(citation)
        assertEquals("Beit Yosef", citation.bookTitle)
        assertEquals("Orach Chayim", citation.section)
        assertEquals(listOf(325, 34, 1), citation.references)
    }

    @Test
    fun `parse citation with section but no references - Introduction case`() {
        val citation = parser.parse("Tur, Orach Chayim, Introduction")
        assertNotNull(citation)
        assertEquals("Tur", citation.bookTitle)
        assertEquals("Orach Chayim, Introduction", citation.section)
        assertEquals(emptyList(), citation.references)
    }

    @Test
    fun `parse citation with Yoreh De'ah Introduction`() {
        val citation = parser.parse("Tur, Yoreh De'ah, Introduction")
        assertNotNull(citation)
        assertEquals("Tur", citation.bookTitle)
        assertEquals("Yoreh De'ah, Introduction", citation.section)
        assertEquals(emptyList(), citation.references)
    }

    @Test
    fun `parse citation with Introduction and paragraph number`() {
        val citation = parser.parse("Tur, Orach Chayim, Introduction 3")
        assertNotNull(citation)
        assertEquals("Tur", citation.bookTitle)
        assertEquals("Orach Chayim, Introduction", citation.section)
        assertEquals(listOf(3), citation.references)
    }

    @Test
    fun `parse Talmud citation with page reference`() {
        val citation = parser.parse("Shabbat 45b:3")
        assertNotNull(citation)
        assertEquals("Shabbat", citation.bookTitle)
        assertEquals(null, citation.section)
        // 45b = page 45 side b = (45 * 2 + 1) = 91
        assertEquals(listOf(91, 3), citation.references)
    }

    @Test
    fun `parse citation with just book title`() {
        val citation = parser.parse("Tanakh")
        assertNotNull(citation)
        assertEquals("Tanakh", citation.bookTitle)
        assertEquals(null, citation.section)
        assertEquals(emptyList(), citation.references)
    }

    @Test
    fun `ignore incidental letters a-b in section names`() {
        val citation = parser.parse("Tur, Orach Chayim")
        assertNotNull(citation)
        assertEquals("Tur", citation.bookTitle)
        assertEquals("Orach Chayim", citation.section)
        assertEquals(emptyList(), citation.references, "section-only links must not fabricate a 0 ref")
    }
}
