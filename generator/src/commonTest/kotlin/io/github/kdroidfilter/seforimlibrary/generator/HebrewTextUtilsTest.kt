package io.github.kdroidfilter.seforimlibrary.generator

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Tests for [HebrewTextUtils] functionality.
 */
class HebrewTextUtilsTest {

    // Test data
    private val textWithNikud = "בְּרֵאשִׁית בָּרָא אֱלֹהִים אֵת הַשָּׁמַיִם וְאֵת הָאָרֶץ"
    private val textWithoutNikud = "בראשית ברא אלהים את השמים ואת הארץ"
    private val textWithTeamim = "בְּרֵאשִׁ֖ית בָּרָ֣א אֱלֹהִ֑ים אֵ֥ת הַשָּׁמַ֖יִם וְאֵ֥ת הָאָֽרֶץ׃"
    private val textWithMaqaf = "אֵל־שַׁדַּי"

    @Test
    fun testRemoveNikud() {
        // Test with nikud
        assertEquals(textWithoutNikud, HebrewTextUtils.removeNikud(textWithNikud))
        
        // Test with null and empty string
        assertEquals("", HebrewTextUtils.removeNikud(null))
        assertEquals("", HebrewTextUtils.removeNikud(""))
        
        // Test with text that doesn't have nikud
        val textWithoutNikudOriginal = "בראשית ברא אלהים"
        assertEquals(textWithoutNikudOriginal, HebrewTextUtils.removeNikud(textWithoutNikudOriginal))
    }

    @Test
    fun testRemoveTeamim() {
        // Test with teamim
        val expectedWithoutTeamim = "בְּרֵאשִׁית בָּרָא אֱלֹהִים אֵת הַשָּׁמַיִם וְאֵת הָאָרֶץ׃"
        assertEquals(expectedWithoutTeamim, HebrewTextUtils.removeTeamim(textWithTeamim))
        
        // Test with null and empty string
        assertEquals("", HebrewTextUtils.removeTeamim(null))
        assertEquals("", HebrewTextUtils.removeTeamim(""))
        
        // Test with text that doesn't have teamim
        assertEquals(textWithNikud, HebrewTextUtils.removeTeamim(textWithNikud))
    }

    @Test
    fun testRemoveAllDiacritics() {
        // Test with both nikud and teamim
        assertEquals(textWithoutNikud, HebrewTextUtils.removeAllDiacritics(textWithTeamim))
        
        // Test with only nikud
        assertEquals(textWithoutNikud, HebrewTextUtils.removeAllDiacritics(textWithNikud))
        
        // Test with null and empty string
        assertEquals("", HebrewTextUtils.removeAllDiacritics(null))
        assertEquals("", HebrewTextUtils.removeAllDiacritics(""))
        
        // Test with plain text
        assertEquals(textWithoutNikud, HebrewTextUtils.removeAllDiacritics(textWithoutNikud))
    }

    @Test
    fun testContainsNikud() {
        // Test with nikud
        assertTrue(HebrewTextUtils.containsNikud(textWithNikud))
        
        // Test without nikud
        assertFalse(HebrewTextUtils.containsNikud(textWithoutNikud))
        
        // Test with null and empty string
        assertFalse(HebrewTextUtils.containsNikud(null))
        assertFalse(HebrewTextUtils.containsNikud(""))
    }

    @Test
    fun testContainsTeamim() {
        // Test with teamim
        assertTrue(HebrewTextUtils.containsTeamim(textWithTeamim))
        
        // Test without teamim
        assertFalse(HebrewTextUtils.containsTeamim(textWithNikud))
        
        // Test with null and empty string
        assertFalse(HebrewTextUtils.containsTeamim(null))
        assertFalse(HebrewTextUtils.containsTeamim(""))
    }

    @Test
    fun testContainsMaqaf() {
        // Test with maqaf
        assertTrue(HebrewTextUtils.containsMaqaf(textWithMaqaf))
        
        // Test without maqaf
        assertFalse(HebrewTextUtils.containsMaqaf(textWithNikud))
        
        // Test with null and empty string
        assertFalse(HebrewTextUtils.containsMaqaf(null))
        assertFalse(HebrewTextUtils.containsMaqaf(""))
    }

    @Test
    fun testReplaceMaqaf() {
        // Test default replacement (space)
        assertEquals("אֵל שַׁדַּי", HebrewTextUtils.replaceMaqaf(textWithMaqaf))
        
        // Test custom replacement (dash)
        assertEquals("אֵל-שַׁדַּי", HebrewTextUtils.replaceMaqaf(textWithMaqaf, "-"))
        
        // Test with null and empty string
        assertEquals("", HebrewTextUtils.replaceMaqaf(null))
        assertEquals("", HebrewTextUtils.replaceMaqaf(""))
        
        // Test with text that doesn't have maqaf
        assertEquals(textWithNikud, HebrewTextUtils.replaceMaqaf(textWithNikud))
    }
}