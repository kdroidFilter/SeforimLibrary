package io.github.kdroidfilter.seforimlibrary.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFalse

class HebrewTextUtilsTest {

    // --- normalizeHebrew tests ---

    @Test
    fun `normalizeHebrew returns empty string for blank input`() {
        assertEquals("", HebrewTextUtils.normalizeHebrew(""))
        assertEquals("", HebrewTextUtils.normalizeHebrew("   "))
        assertEquals("", HebrewTextUtils.normalizeHebrew("\t\n"))
    }

    @Test
    fun `normalizeHebrew removes nikud vowel points`() {
        // בְּרֵאשִׁית -> בראשית
        val withNikud = "בְּרֵאשִׁית"
        val expected = "בראשית"
        assertEquals(expected, HebrewTextUtils.normalizeHebrew(withNikud))
    }

    @Test
    fun `normalizeHebrew removes teamim cantillation marks`() {
        // Text with teamim (U+0591-U+05AF range)
        val withTeamim = "בְּרֵאשִׁ֖ית בָּרָ֣א אֱלֹהִ֑ים"
        val result = HebrewTextUtils.normalizeHebrew(withTeamim)
        // Should not contain any teamim
        assertFalse(result.any { it.code in 0x0591..0x05AF })
    }

    @Test
    fun `normalizeHebrew replaces maqaf with space`() {
        // מַה־טֹּבוּ -> מה טבו
        val withMaqaf = "מה־טבו"
        val result = HebrewTextUtils.normalizeHebrew(withMaqaf)
        assertTrue(result.contains(' '))
        assertFalse(result.contains('\u05BE'))
    }

    @Test
    fun `normalizeHebrew removes gershayim and geresh`() {
        val withGershayim = "רש\"י"
        val withGeresh = "ר'"
        // Gershayim U+05F4 and geresh U+05F3
        val text = "רש\u05F4י ר\u05F3"
        val result = HebrewTextUtils.normalizeHebrew(text)
        assertFalse(result.contains('\u05F4'))
        assertFalse(result.contains('\u05F3'))
    }

    @Test
    fun `normalizeHebrew converts final letters to base forms`() {
        // ך -> כ, ם -> מ, ן -> נ, ף -> פ, ץ -> צ
        val withFinals = "מלך שלום אמן סוף ארץ"
        val result = HebrewTextUtils.normalizeHebrew(withFinals)
        assertEquals("מלכ שלומ אמנ סופ ארצ", result)
    }

    @Test
    fun `normalizeHebrew collapses multiple spaces`() {
        val withSpaces = "שלום    עולם"
        val result = HebrewTextUtils.normalizeHebrew(withSpaces)
        assertEquals("שלומ עולמ", result)
    }

    @Test
    fun `normalizeHebrew trims whitespace`() {
        val withWhitespace = "  שלום  "
        val result = HebrewTextUtils.normalizeHebrew(withWhitespace)
        assertEquals("שלומ", result)
    }

    @Test
    fun `normalizeHebrew handles mixed Hebrew and ASCII`() {
        val mixed = "Hello שלום World"
        val result = HebrewTextUtils.normalizeHebrew(mixed)
        assertEquals("Hello שלומ World", result)
    }

    // --- replaceFinalsWithBase tests ---

    @Test
    fun `replaceFinalsWithBase converts all final letters`() {
        assertEquals("כ", HebrewTextUtils.replaceFinalsWithBase("ך"))
        assertEquals("מ", HebrewTextUtils.replaceFinalsWithBase("ם"))
        assertEquals("נ", HebrewTextUtils.replaceFinalsWithBase("ן"))
        assertEquals("פ", HebrewTextUtils.replaceFinalsWithBase("ף"))
        assertEquals("צ", HebrewTextUtils.replaceFinalsWithBase("ץ"))
    }

    @Test
    fun `replaceFinalsWithBase preserves non-final letters`() {
        val text = "אבגדהוזחטיכלמנסעפצקרשת"
        val result = HebrewTextUtils.replaceFinalsWithBase(text)
        assertEquals(text, result)
    }

    @Test
    fun `replaceFinalsWithBase handles word with final letter at end`() {
        assertEquals("מלכ", HebrewTextUtils.replaceFinalsWithBase("מלך"))
        assertEquals("שלומ", HebrewTextUtils.replaceFinalsWithBase("שלום"))
    }

    // --- isNikudOrTeamim tests ---

    @Test
    fun `isNikudOrTeamim returns true for nikud characters`() {
        // Shva, Patach, Kamatz, etc.
        val nikudChars = listOf('\u05B0', '\u05B1', '\u05B2', '\u05B3', '\u05B4',
                                '\u05B5', '\u05B6', '\u05B7', '\u05B8', '\u05B9',
                                '\u05BB', '\u05BC', '\u05BD')
        nikudChars.forEach { char ->
            assertTrue(HebrewTextUtils.isNikudOrTeamim(char), "Expected $char to be nikud")
        }
    }

    @Test
    fun `isNikudOrTeamim returns true for teamim characters`() {
        // Some teamim characters
        val teamimChars = listOf('\u0591', '\u0592', '\u0593', '\u05A0', '\u05AF')
        teamimChars.forEach { char ->
            assertTrue(HebrewTextUtils.isNikudOrTeamim(char), "Expected $char to be teamim")
        }
    }

    @Test
    fun `isNikudOrTeamim returns false for regular Hebrew letters`() {
        val hebrewLetters = "אבגדהוזחטיכלמנסעפצקרשת"
        hebrewLetters.forEach { char ->
            assertFalse(HebrewTextUtils.isNikudOrTeamim(char), "Expected $char to NOT be nikud/teamim")
        }
    }

    @Test
    fun `isNikudOrTeamim returns false for ASCII characters`() {
        ('a'..'z').forEach { char ->
            assertFalse(HebrewTextUtils.isNikudOrTeamim(char))
        }
        ('0'..'9').forEach { char ->
            assertFalse(HebrewTextUtils.isNikudOrTeamim(char))
        }
    }

    // --- stripDiacriticsWithMap tests ---

    @Test
    fun `stripDiacriticsWithMap removes all diacritics`() {
        val withDiacritics = "בְּרֵאשִׁית"
        val (plain, _) = HebrewTextUtils.stripDiacriticsWithMap(withDiacritics)
        assertEquals("בראשית", plain)
    }

    @Test
    fun `stripDiacriticsWithMap returns correct index mapping`() {
        // Simple case: "אָב" -> "אב" with mapping [0, 2]
        val input = "א\u05B8ב" // א with kamatz, then ב
        val (plain, map) = HebrewTextUtils.stripDiacriticsWithMap(input)
        assertEquals("אב", plain)
        assertEquals(2, map.size)
        assertEquals(0, map[0]) // 'א' at position 0 in original
        assertEquals(2, map[1]) // 'ב' at position 2 in original (after kamatz)
    }

    @Test
    fun `stripDiacriticsWithMap handles text without diacritics`() {
        val plain = "שלום"
        val (result, map) = HebrewTextUtils.stripDiacriticsWithMap(plain)
        assertEquals(plain, result)
        assertEquals(4, map.size)
        // Each character maps to itself
        for (i in map.indices) {
            assertEquals(i, map[i])
        }
    }

    @Test
    fun `stripDiacriticsWithMap handles empty string`() {
        val (plain, map) = HebrewTextUtils.stripDiacriticsWithMap("")
        assertEquals("", plain)
        assertEquals(0, map.size)
    }

    // --- stripDiacritics tests ---

    @Test
    fun `stripDiacritics removes diacritics without mapping`() {
        val withDiacritics = "בְּרֵאשִׁית"
        val result = HebrewTextUtils.stripDiacritics(withDiacritics)
        assertEquals("בראשית", result)
    }

    @Test
    fun `stripDiacritics preserves text without diacritics`() {
        val plain = "שלום עולם"
        assertEquals(plain, HebrewTextUtils.stripDiacritics(plain))
    }

    @Test
    fun `stripDiacritics handles empty string`() {
        assertEquals("", HebrewTextUtils.stripDiacritics(""))
    }

    // --- mapToOrigIndex tests ---

    @Test
    fun `mapToOrigIndex returns correct index for valid input`() {
        val map = intArrayOf(0, 2, 4, 6)
        assertEquals(0, HebrewTextUtils.mapToOrigIndex(map, 0))
        assertEquals(2, HebrewTextUtils.mapToOrigIndex(map, 1))
        assertEquals(4, HebrewTextUtils.mapToOrigIndex(map, 2))
        assertEquals(6, HebrewTextUtils.mapToOrigIndex(map, 3))
    }

    @Test
    fun `mapToOrigIndex clamps out of bounds index`() {
        val map = intArrayOf(0, 2, 4)
        // Index too high should return last valid
        assertEquals(4, HebrewTextUtils.mapToOrigIndex(map, 10))
        // Negative index should return first
        assertEquals(0, HebrewTextUtils.mapToOrigIndex(map, -1))
    }

    @Test
    fun `mapToOrigIndex returns plainIndex for empty map`() {
        val emptyMap = intArrayOf()
        assertEquals(5, HebrewTextUtils.mapToOrigIndex(emptyMap, 5))
    }

    // --- SOFIT_MAP tests ---

    @Test
    fun `SOFIT_MAP contains all five final letter mappings`() {
        assertEquals(5, HebrewTextUtils.SOFIT_MAP.size)
        assertEquals('כ', HebrewTextUtils.SOFIT_MAP['ך'])
        assertEquals('מ', HebrewTextUtils.SOFIT_MAP['ם'])
        assertEquals('נ', HebrewTextUtils.SOFIT_MAP['ן'])
        assertEquals('פ', HebrewTextUtils.SOFIT_MAP['ף'])
        assertEquals('צ', HebrewTextUtils.SOFIT_MAP['ץ'])
    }

    // --- Edge cases and regression tests ---

    @Test
    fun `normalizeHebrew handles real Torah verse`() {
        // Genesis 1:1 with full nikud and teamim
        val verse = "בְּרֵאשִׁ֖ית בָּרָ֣א אֱלֹהִ֑ים אֵ֥ת הַשָּׁמַ֖יִם וְאֵ֥ת הָאָֽרֶץ׃"
        val normalized = HebrewTextUtils.normalizeHebrew(verse)
        // Should be plain Hebrew without any diacritics
        assertFalse(normalized.any { HebrewTextUtils.isNikudOrTeamim(it) })
        // Should contain the main words
        assertTrue(normalized.contains("בראשית"))
        assertTrue(normalized.contains("ברא"))
        assertTrue(normalized.contains("אלהימ"))
    }

    @Test
    fun `normalizeHebrew handles Divine Name variants`() {
        // Various representations of the Divine Name
        val names = listOf("יהוה", "יְהוָה", "ה׳", "ה'")
        names.forEach { name ->
            val result = HebrewTextUtils.normalizeHebrew(name)
            assertTrue(result.isNotEmpty(), "Normalized name should not be empty for: $name")
        }
    }
}
