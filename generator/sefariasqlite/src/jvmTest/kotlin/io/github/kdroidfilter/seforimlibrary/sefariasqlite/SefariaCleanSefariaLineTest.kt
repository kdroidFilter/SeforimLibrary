package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals

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
    fun collapsesBrTagsIntoSpaces() {
        // Sample from Tikkunei Zohar daf יז (idx=33 in merged.json): the
        // paragraph is internally split by <br> tags, which the app used to
        // render as short broken lines.
        assertEquals(
            "ובר מינך לית יחודא בעלאי ותתאי. ואנת אשתמודע אדון על כלא.",
            cleanSefariaLine("ובר מינך לית יחודא <br>בעלאי ותתאי. <br>ואנת אשתמודע אדון על כלא.")
        )
    }

    @Test
    fun handlesSelfClosingAndUppercaseBr() {
        assertEquals("a b c", cleanSefariaLine("a<br/>b<BR />c"))
    }
}
