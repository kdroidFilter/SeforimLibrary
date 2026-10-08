package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SefariaCleanSefariaLineTest {
    @Test
    fun stripsOtzarMarkupAtLineStart() {
        // Real example from Maaseh Rokeach on Vessels (chapter 5, halacha 3)
        assertEquals("והנקטמון", cleanSefariaLine("@04והנקטמון}"))
    }

    @Test
    fun stripsOtzarMarkupInline() {
        assertEquals(
            "לא ידעתי לפרש דברי רבינו וכן",
            cleanSefariaLine("לא ידעתי לפרש דברי רבינו @04וכן}")
        )
    }

    @Test
    fun preservesCurlyBraceWhenNotFromMarker() {
        // Sefaria texts sometimes contain legitimate braces (parentheses in old formats).
        // Only the @NN...} pattern should be stripped.
        assertEquals("{word} text", cleanSefariaLine("{word} text"))
    }

    @Test
    fun stripsMultipleMarkers() {
        assertEquals(
            "one and two",
            cleanSefariaLine("@04one} and @44two}")
        )
    }

    @Test
    fun stripsNewlinesAsBefore() {
        assertEquals("a b", cleanSefariaLine("a\nb".replace("b", " b")))
    }

    @Test
    fun passThroughWhenNoMarkup() {
        assertEquals("בראשית ברא אלהים", cleanSefariaLine("בראשית ברא אלהים"))
    }

    @Test
    fun keepsInlineBrByDefault() {
        // Even Ha'azel: each <br> separates a dibbur from the next one.
        assertEquals(
            "<b>כותרת ההלכה.</b><br><b>דיבור</b> פירוש.<br>המשך",
            cleanSefariaLine("<b>כותרת ההלכה.</b><br><b>דיבור</b> פירוש.<br/>המשך")
        )
    }

    @Test
    fun collapsesBrTagsIntoSpacesWhenAsked() {
        // Sample from Tikkunei Zohar daf יז (idx=33 in merged.json): the
        // paragraph is broken mid-sentence into short lines.
        assertEquals(
            "ובר מינך לית יחודא בעלאי ותתאי. ואנת אשתמודע אדון על כלא.",
            cleanSefariaLine(
                "ובר מינך לית יחודא <br>בעלאי ותתאי. <br>ואנת אשתמודע אדון על כלא.",
                collapseInlineBreaks = true,
            )
        )
    }

    @Test
    fun handlesSelfClosingAndUppercaseBr() {
        assertEquals("a b c", cleanSefariaLine("a<br/>b<BR />c", collapseInlineBreaks = true))
        assertEquals("a<br>b<br>c", cleanSefariaLine("a<br/>b<BR />c"))
    }

    @Test
    fun collapsesOnlyListedBooks() {
        assertTrue(collapsesInlineBreaks("Tikkunei Zohar"))
        assertTrue(collapsesInlineBreaks("The Koren Yom Kippur Mahzor; Ashkenaz"))
        assertFalse(collapsesInlineBreaks("Even Ha'azel on Mishneh Torah, Sabbath"))
    }

    @Test
    fun keptBreaksCollapseToTheFormerlyCleanedLine() {
        // The line key hashes the collapsed form, so it must match what the
        // collapsing cleaner produced — otherwise every such line gets a new id.
        for (raw in listOf("a <br> b", "<br>a<br><br>b <br>", "a<BR/>  b<br>", "<b>x</b><br>y")) {
            assertEquals(
                cleanSefariaLine(raw, collapseInlineBreaks = true),
                collapseInlineLineBreaks(cleanSefariaLine(raw)),
                raw,
            )
        }
    }

    @Test
    fun keepsTrailingBrOfOpenParasha() {
        // MAM Deuteronomy 6:3 — the trailing <br> after {פ} is the open-parasha
        // break; without it the next verse runs on in continuous reading.
        assertEquals(
            """חָלָב וּדְבָשׁ׃&nbsp;<span class="mam-spi-pe">{פ}</span><br>""",
            cleanSefariaLine("""חָלָב וּדְבָשׁ׃&nbsp;<span class="mam-spi-pe">{פ}</span><br>""")
        )
    }

    @Test
    fun collapsesInnerBrButKeepsTrailingOne() {
        assertEquals("a b<br>", cleanSefariaLine("a<br>b<br>", collapseInlineBreaks = true))
        assertEquals("a<br>b<br>", cleanSefariaLine("a<br>b<br>"))
    }

    @Test
    fun dropsBrOnlyLine() {
        assertEquals("", cleanSefariaLine("<br><br>"))
        assertEquals("", cleanSefariaLine("<br>\u00a0"))
        assertEquals("", cleanSefariaLine("<br>\u2003"))
        assertEquals("", cleanSefariaLine("<br>\u00a0<br>"))
    }

    @Test
    fun keepsFootnotesByDefault() {
        val line = """א<sup class="footnote-marker">*</sup><i class="footnote">הגה״ה</i>. ב"""
        assertEquals(line, cleanSefariaLine(line))
    }

    @Test
    fun stripsFootnotesWithNestedItalics() {
        // Shape of Noda BiYehudah I, Orach Chaim 1:20 (Landa's translator footnotes)
        val line = "ע\"ש<sup class=\"footnote-marker\">38</sup><i class=\"footnote\">הלכות תפילין ג:א<br>" +
            "<b>There are eight requirements in the making of <i>tefillin</i>.</b> See <i>Sukkah</i> 8a.</i> " +
            "והנה<sup class=\"footnote-marker\">39</sup><i class=\"footnote\">See note 47</i>, שכן נראה"
        assertEquals("ע\"ש והנה, שכן נראה", cleanSefariaLine(line, stripFootnotes = true))
    }

    @Test
    fun translatorFootnotesAreScopedToNodaBiYehudahOrachChaim() {
        assertTrue(hasTranslatorFootnotes("Noda BiYehudah I, Orach Chaim,  1 20 "))
        assertFalse(hasTranslatorFootnotes("Noda BiYehudah I, Author's Introduction,  1 "))
        assertFalse(hasTranslatorFootnotes("Noda BiYehudah II, Orach Chaim,  1 "))
    }
}
