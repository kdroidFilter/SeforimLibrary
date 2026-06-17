package io.github.kdroidfilter.seforimlibrary.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SearchQueryParserTest {

    @Test
    fun `query without quotes is all free text`() {
        val parsed = SearchQueryParser.parse("מלך דוד")
        assertEquals("מלך דוד", parsed.freeText)
        assertTrue(parsed.exactPhrases.isEmpty())
        assertFalse(parsed.hasExactPhrases)
    }

    @Test
    fun `single quoted phrase`() {
        val parsed = SearchQueryParser.parse("\"מלך דוד\"")
        assertEquals(listOf("מלך דוד"), parsed.exactPhrases)
        assertEquals("", parsed.freeText)
        assertTrue(parsed.hasExactPhrases)
    }

    @Test
    fun `mixed quoted phrase and free text`() {
        val parsed = SearchQueryParser.parse("ירושלים \"מלך דוד\" עיר")
        assertEquals(listOf("מלך דוד"), parsed.exactPhrases)
        assertEquals("ירושלים עיר", parsed.freeText)
    }

    @Test
    fun `multiple quoted phrases`() {
        val parsed = SearchQueryParser.parse("\"בראשית ברא\" \"אלהים את\"")
        assertEquals(listOf("בראשית ברא", "אלהים את"), parsed.exactPhrases)
        assertEquals("", parsed.freeText)
    }

    @Test
    fun `acronym with ascii quote is not a delimiter`() {
        // רש"י (Rashi) is written with an ASCII double quote between the last two letters.
        val parsed = SearchQueryParser.parse("רש\"י")
        assertTrue(parsed.exactPhrases.isEmpty())
        assertEquals("רש\"י", parsed.freeText)
    }

    @Test
    fun `acronym is preserved alongside a real quoted phrase`() {
        val parsed = SearchQueryParser.parse("רש\"י \"בראשית ברא\"")
        assertEquals(listOf("בראשית ברא"), parsed.exactPhrases)
        assertEquals("רש\"י", parsed.freeText)
    }

    @Test
    fun `common hebrew acronyms are never treated as delimiters`() {
        for (acronym in listOf("רמב\"ם", "שו\"ע", "תנ\"ך", "תשפ\"א", "י\"ד")) {
            val parsed = SearchQueryParser.parse(acronym)
            assertTrue(parsed.exactPhrases.isEmpty(), "Acronym should not produce phrases: $acronym")
            assertEquals(acronym, parsed.freeText)
        }
    }

    @Test
    fun `single gershayim-quoted phrase`() {
        val parsed = SearchQueryParser.parse("״מלך דוד״")
        assertEquals(listOf("מלך דוד"), parsed.exactPhrases)
        assertEquals("", parsed.freeText)
        assertTrue(parsed.hasExactPhrases)
    }

    @Test
    fun `mixed gershayim phrase and free text`() {
        val parsed = SearchQueryParser.parse("ירושלים ״מלך דוד״ עיר")
        assertEquals(listOf("מלך דוד"), parsed.exactPhrases)
        assertEquals("ירושלים עיר", parsed.freeText)
    }

    @Test
    fun `gershayim acronym is not a delimiter`() {
        // רש״י written with the Hebrew gershayim (U+05F4) between the last two letters.
        val parsed = SearchQueryParser.parse("רש״י")
        assertTrue(parsed.exactPhrases.isEmpty())
        assertEquals("רש״י", parsed.freeText)
    }

    @Test
    fun `common hebrew acronyms with gershayim are not delimiters`() {
        for (acronym in listOf("רמב״ם", "שו״ע", "תנ״ך", "תשפ״א", "י״ד")) {
            val parsed = SearchQueryParser.parse(acronym)
            assertTrue(parsed.exactPhrases.isEmpty(), "Acronym should not produce phrases: $acronym")
            assertEquals(acronym, parsed.freeText)
        }
    }

    @Test
    fun `ascii and gershayim quotes are interchangeable delimiters`() {
        val parsed = SearchQueryParser.parse("\"מלך דוד״")
        assertEquals(listOf("מלך דוד"), parsed.exactPhrases)
        assertEquals("", parsed.freeText)
    }

    @Test
    fun `unclosed quote treats remainder as a phrase`() {
        val parsed = SearchQueryParser.parse("\"מלך דוד")
        assertEquals(listOf("מלך דוד"), parsed.exactPhrases)
        assertEquals("", parsed.freeText)
    }

    @Test
    fun `empty quotes produce no phrase`() {
        val parsed = SearchQueryParser.parse("מלך \"\" דוד")
        assertTrue(parsed.exactPhrases.isEmpty())
        assertEquals("מלך דוד", parsed.freeText)
    }

    @Test
    fun `whitespace inside and around phrases is collapsed`() {
        val parsed = SearchQueryParser.parse("  ירושלים   \"מלך    דוד\"  ")
        assertEquals(listOf("מלך דוד"), parsed.exactPhrases)
        assertEquals("ירושלים", parsed.freeText)
    }

    @Test
    fun `blank input yields empty free text and no phrases`() {
        val parsed = SearchQueryParser.parse("")
        assertEquals("", parsed.freeText)
        assertTrue(parsed.exactPhrases.isEmpty())
    }
}
