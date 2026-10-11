package io.github.kdroidfilter.seforimlibrary.reflinks

import io.github.kdroidfilter.seforimlibrary.reflinks.Section.Daf
import io.github.kdroidfilter.seforimlibrary.reflinks.Section.Num
import io.github.kdroidfilter.seforimlibrary.reflinks.Section.Side
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CitationParserTest {

    private val parser = CitationParser(
        listOf("משנה תורה, הלכות שבת", "משנה תורה, הלכות מכירה", "משנה תורה, הלכות שלוחין ושותפין"),
    )

    private fun parse(s: String) = parser.parse(s)?.citation

    @Test
    fun gematria() {
        assertEquals(193, HebrewText.gematria("קצג"))
        assertEquals(15, HebrewText.gematria("ט\"ו"))
        assertEquals(251, HebrewText.gematria("רנ\"א"))
        assertNull(HebrewText.gematria("אמר"), "ascending letters are a word, not a numeral")
        assertEquals(13, HebrewText.markerNumber("פי\"ג", "פ"))
        assertEquals(3, HebrewText.markerNumber("פ\"ג", "פ"))
    }

    @Test
    fun tanakh() {
        assertEquals(listOf(Num(1), Num(4)), parse("בראשית א, ד")?.sections)
        assertEquals(Precision.CHAPTER, parse("בראשית לד")?.precision)
        assertEquals(listOf(Num(119), Num(105)), parse("עיין תהלים קיט, קה")?.sections)
        assertEquals(5, parse("ישעיה מ, ג-ה")?.rangeEnd)
        assertEquals("ישעיהו", parse("ישעיה מ, ג-ה")?.title)
        assertEquals(listOf(Num(29), Num(26)), parse("משלי כ״ט:כ״ו")?.sections)
        assertEquals("שמואל א", parse("שמואל א ב, ו")?.title)
    }

    @Test
    fun ibidAndProseAreNotBooks() {
        assertNull(parse("שם"))
        assertNull(parse("הובא למעלה פ\"א הערה 1820"))
        assertNull(parse("דף כא:"), "a folio without tractate needs the context")
    }

    @Test
    fun bavli() {
        assertEquals(listOf(Daf(21, Side.B)), parse("שבת דף כא ע\"ב")?.sections)
        assertEquals(listOf(Daf(22, Side.B)), parse("ב\"ק כב:")?.sections)
        assertEquals(listOf(Daf(8, Side.A)), parse("ברכות ח א")?.sections)
        assertEquals(listOf(Daf(30, Side.B)), parse("שבת ל' ע\"ב")?.sections, "single-letter folio with geresh")
        assertEquals(listOf(Daf(115, Side.A)), parse("ב\"מ קט\"ו א'")?.sections)
        assertEquals(listOf(Daf(85, Side.A)), parse("חולין פ\"ה.")?.sections, "פ\"ה. is folio 85, not chapter 5")
    }

    @Test
    fun firstSegmentAndLongestPrefix() {
        assertEquals(listOf(Daf(42, Side.B)), parse("גיטין מב ב; טוש\"ע יו\"ד רסז סב")?.sections)
        val rashi = parse("רש\"י סוכה מ ב ד\"ה אדם")!!
        assertEquals("רש\"י על סוכה", rashi.title)
        assertEquals(2, rashi.trailingTokens)
    }

    @Test
    fun mishna() {
        assertEquals(listOf(Num(3), Num(13)), parse("אבות פ\"ג מי\"ג")?.sections)
        assertEquals("משנה כלים", parse("כלים פ' י\"ז מי\"ג")?.title)
        assertEquals(listOf(Num(4), Num(27)), parse("אבות ד כז")?.sections)
    }

    @Test
    fun bareChapterMarkerIsAmbiguous() {
        val p = parser.parse("שבת פ\"ב")!!
        assertEquals(Ambiguity.PEREK_OR_DAF, p.ambiguity)
        assertEquals("משנה שבת", p.citation.title)
        assertEquals(listOf(Daf(82, Side.A)), p.alternatives.single().sections, "the alternative is folio 82, not 2")
    }

    @Test
    fun rambam() {
        assertEquals(listOf(Num(2), Num(3)), parse("רמב\"ם הל' שבת פ\"ב ה\"ג")?.sections)
        assertEquals(listOf(Num(12), Num(11)), parse("רמב\"ם פי\"ב מהל' שבת הי\"א")?.sections)
        val hl = parse("רמב\"ם פרק כב מהלכות מכירה הל\"ט")!!
        assertEquals(listOf(Num(22), Num(9)), hl.sections, "הל\"ט is halacha 9")
        assertEquals("משנה תורה, הלכות שלוחין ושותפין", parse("רמב\"ם פ\"ג מהל' שלוחין ה\"ז")?.title)
    }

    @Test
    fun shulchanArukh() {
        val p = parser.parse("או\"ח סי' רנא ס\"ב")!!
        assertEquals(listOf(Num(251), Num(2)), p.citation.sections)
        assertEquals(Ambiguity.TUR_PART_ONLY, p.ambiguity)
        val explicit = parser.parse("שו\"ע או\"ח סי' רנא ס\"ב")!!
        assertNull(explicit.ambiguity, "naming the Shulchan Arukh removes the ambiguity")
        assertEquals(listOf(Num(168)), parse("או\"ח סי' קס\"ח ס\"ק ב'")?.sections, "a sub-seif is never read as seif 100")
        assertEquals(listOf(Section.Name("אורח חיים"), Num(448)), parse("טור או\"ח סי' תמח")?.sections)
    }

    @Test
    fun kingsOrCommentary() {
        val p = parser.parse("מ\"א ב, ו")!!
        assertEquals("מלכים א", p.citation.title)
        assertEquals(Ambiguity.KINGS_OR_COMMENTARY, p.ambiguity)
        assertEquals("מגן אברהם", p.alternatives.single().title)
    }

    @Test
    fun otherFamilies() {
        assertEquals(listOf(Num(8)), parse("ב\"ר פ\"ח")?.sections)
        assertEquals(listOf(Num(21), Num(1)), parse("במ\"ר פכ\"א א'")?.sections)
        assertEquals("תלמוד ירושלמי ברכות", parse("ירושלמי ברכות פ\"א ה\"ב")?.title)
        val zohar = parse("זח\"א דף כה.")!!
        assertEquals(1, zohar.zoharVolume)
        assertEquals(Daf(25, Side.A), zohar.zoharDaf)
        assertEquals(listOf(Section.Name("וירא"), Num(9)), parse("תנחומא וירא ט")?.sections)
    }

    @Test
    fun nameBeforeParentheses() {
        assertEquals(listOf(Num(40), Num(3)), parser.parseWithName("בישעיה", "מ, ג")?.citation?.sections)
        assertEquals(listOf(Daf(17, Side.A)), parser.parseWithName("בביצה", "דף יז")?.citation?.sections)
        assertNull(parser.parseWithName("בביצה", "דף יז ועוד דברים"), "trailing words are not accepted")
    }
}
