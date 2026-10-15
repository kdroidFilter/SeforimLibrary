package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LinkerAnchorBracketsTest {
    private fun extended(prefix: String, suffix: String): String {
        val content = "לפני " + prefix + suffix
        return content.substring(5, closeDanglingBrackets(content, 5, 5 + prefix.length))
    }

    @Test
    fun nestedBracketsCloseInTheirActualOrder() {
        for ((prefix, suffix) in listOf(
            "([א, ב" to "]) שאחריו",
            "[(א, ב" to ")] שאחריו",
            "((א, ב" to ")) שאחריו",
            "[[א, ב" to "]] שאחריו",
            "([([א, ב" to "])]) שאחריו",
        )) {
            val closers = suffix.substringBefore(' ')
            assertEquals(prefix + closers, extended(prefix, suffix))
        }
    }

    @Test
    fun whitespaceAndEncodedBracketsKeepTheirRawOffsets() {
        for (space in listOf(" ", "\t", "\n", "\r", "\u00a0", "\u2002", "&nbsp;", "&ensp;", "&#160;", "&#xA0;")) {
            assertEquals("(א, ב$space)", extended("(א, ב", "$space) text"))
        }
        assertEquals("&#40;א, ב&#41;", extended("&#40;א, ב", "&#41; text"))
        assertEquals("&lsqb;א, ב&rsqb;", extended("&lsqb;א, ב", "&rsqb; text"))
        assertEquals("&#x+28;א, ב", extended("&#x+28;א, ב", ")"))
        assertEquals("&#-40;א, ב", extended("&#-40;א, ב", ")"))
    }

    @Test
    fun onlyMatchingClosersAreAddedAndTrailingFormattingIsLeftOut() {
        assertEquals("(א, ב", extended("(א, ב", "] text)"))
        assertEquals("([א, ב", extended("([א, ב", ")]"))
        assertEquals("([א, ב]", extended("([א, ב", "] text)"))
        assertEquals("(א, ב", extended("(א, ב", " text)"))
        assertEquals("(א, ב)", extended("(א, ב)", ")"))
        assertEquals("א, ב", extended("א, ב", ")"))
        assertEquals("(א, ב )", extended("(א, ב", " ) </b> "))
        assertEquals(") ראה (א, ב)", extended(") ראה (א, ב", ")"))
        assertEquals("([)] (א, ב", extended("([)] (א, ב", ")"))
    }

    @Test
    fun notesLinksMarkersAndStructuralTagsAreBarriers() {
        for (tag in listOf(
            "<i class=\"footnote\">", "<I CLASS='other footnote extra'>", "<span class=footnote>",
            "<sup class=\"footnote-marker\">", "<a href=\"x\">", "<br>", "<img src=\"x\">",
            "<i data-commentator=\"X\">", "<i-frame>", "<b:custom>",
        )) {
            assertEquals("(א, ב", extended("(א, ב", "$tag)</i>)"), tag)
            assertEquals("(א, ב", extended("(א, ב", "$tag</i>)"), tag)
        }
    }

    @Test
    fun ordinaryFormattingDoesNotContributeBracketsFromAttributes() {
        assertEquals("<b title=\"(\">א, ב", extended("<b title=\"(\">א, ב", "</b>)"))
        assertEquals("(א, ב</b> <em>)", extended("(א, ב", "</b> <em>)</em>"))
        assertEquals("(א, ב<span title=\"class='footnote'\">)", extended("(א, ב", "<span title=\"class='footnote'\">)"))
        assertEquals("(א, ב", extended("(א, ב", "<b title=\"unterminated>)"))
        assertEquals("(א, ב", extended("(א, ב", "<b )"))
    }

    @Test
    fun rawBoundariesCannotBeCrossedEvenWithinTagsOrEntities() {
        for (suffix in listOf(")", " )", "</b>)", "&nbsp;)", "&#41;")) {
            val text = "(א, ב$suffix"
            for (limit in 5 until text.length) {
                val result = closeDanglingBrackets(text, 0, 5, limit)
                assertTrue(result in 5..limit)
            }
            assertEquals(text.length, closeDanglingBrackets(text, 0, 5))
        }
    }

    @Test
    fun deepNestingDoesNotRequireRecursion() {
        val prefix = "[".repeat(10_000) + "א"
        val content = prefix + "]".repeat(10_000)
        assertEquals(content.length, closeDanglingBrackets(content, 0, prefix.length))
    }

    @Test
    fun extensionIsIdempotentAndAlwaysWithinTheLine() {
        for (text in listOf("(א, ב)", "([א, ב])", "((א, ב))", "(א, ב</b> )", "([א, ב] text)", "א, ב")) {
            for (end in 1..text.length) {
                val result = closeDanglingBrackets(text, 0, end)
                assertTrue(result in end..text.length)
                assertEquals(result, closeDanglingBrackets(text, 0, result), "$text end=$end")
            }
        }
        assertEquals(0, closeDanglingBrackets("", 0, 0))
    }
}
