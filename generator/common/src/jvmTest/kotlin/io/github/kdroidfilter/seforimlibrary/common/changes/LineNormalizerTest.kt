package io.github.kdroidfilter.seforimlibrary.common.changes

import org.junit.Test
import kotlin.test.assertEquals

class LineNormalizerTest {

    @Test
    fun `strips html tags`() {
        // Note: sofit is normalised (ם → מ) — see `final letters become base form` test.
        assertEquals(
            "שלומ",
            LineNormalizer.normalize("<p style='color:red'>שלום</p>"),
        )
    }

    @Test
    fun `removes nikud and teamim and normalises sofit`() {
        val withDiacritics = "בְּרֵאשִׁ֖ית בָּרָ֣א אֱלֹהִ֑ים"
        val normalized = LineNormalizer.normalize(withDiacritics)
        // After normalisation: consonants only, sofit normalised (ם → מ).
        assertEquals("בראשית ברא אלהימ", normalized)
    }

    @Test
    fun `final letters become base form`() {
        // א ך ב ם → א כ ב מ
        assertEquals(
            "א כ ב מ",
            LineNormalizer.normalize("א ך ב ם"),
        )
    }

    @Test
    fun `collapses whitespace runs`() {
        assertEquals(
            "א ב ג",
            LineNormalizer.normalize("  א    ב\t\nג  "),
        )
    }

    @Test
    fun `null and blank input return empty string`() {
        assertEquals("", LineNormalizer.normalize(null))
        assertEquals("", LineNormalizer.normalize(""))
        assertEquals("", LineNormalizer.normalize("   "))
    }

    @Test
    fun `html-only input returns empty string`() {
        assertEquals("", LineNormalizer.normalize("<br/><p></p>"))
    }

    @Test
    fun `the verse-prefix shift that broke phase 1 test C is now matched`() {
        // The Genesis 1:1 verse rendered with prefix "(א)" before insertion,
        // "(ב)" after. The bulk of the line is byte-identical except for the
        // parenthesised marker; that's enough for the LineMatcher's MODIFY
        // merge (60 % token similarity) to collapse them into a single
        // MODIFY op, preserving the line id.
        val before = LineNormalizer.normalize("(א) <big>בְּ</big>רֵאשִׁ֖ית בָּרָ֣א אֱלֹהִ֑ים")
        val after = LineNormalizer.normalize("(ב) <big>בְּ</big>רֵאשִׁ֖ית בָּרָ֣א אֱלֹהִ֑ים")
        // Note: sofit is normalised (ם → מ).
        assertEquals("(א) בראשית ברא אלהימ", before)
        assertEquals("(ב) בראשית ברא אלהימ", after)
    }

    @Test
    fun `nbsp and html entities are decoded`() {
        assertEquals(
            "א ב",
            LineNormalizer.normalize("א&nbsp;ב"),
        )
    }
}
