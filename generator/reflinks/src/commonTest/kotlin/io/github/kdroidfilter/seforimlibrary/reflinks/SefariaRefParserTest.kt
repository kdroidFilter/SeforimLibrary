package io.github.kdroidfilter.seforimlibrary.reflinks

import io.github.kdroidfilter.seforimlibrary.reflinks.Section.Daf
import io.github.kdroidfilter.seforimlibrary.reflinks.Section.Name
import io.github.kdroidfilter.seforimlibrary.reflinks.Section.Num
import io.github.kdroidfilter.seforimlibrary.reflinks.Section.Side
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SefariaRefParserTest {

    private val parser = SefariaRefParser(
        listOf(
            SefariaRefParser.Book(listOf("Tosafot on Bava Kamma"), "תוספות על בבא קמא"),
            SefariaRefParser.Book(
                listOf("Kol Yaakov on Shulchan Arukh", "Kol Yaakov"),
                "קול יעקב על שולחן ערוך",
                mapOf("Orach Chayim" to "אורח חיים", "Yoreh De'ah" to "יורה דעה"),
            ),
            SefariaRefParser.Book(listOf("Derashat Shabbat HaGadol"), "דרשת שבת הגדול"),
        ),
    )

    @Test
    fun talmudCommentary() {
        val ref = parser.parse("Tosafot on Bava Kamma 22b:2")!!
        assertEquals("תוספות על בבא קמא", ref.heTitle)
        assertEquals(listOf(Daf(22, Side.B), Num(2)), ref.sections)
    }

    @Test
    fun complexTextWithTitleVariant() {
        val ref = parser.parse("Kol Yaakov, Yoreh De'ah 276:67:1")!!
        assertEquals("קול יעקב על שולחן ערוך", ref.heTitle)
        assertEquals(listOf(Name("יורה דעה"), Num(276), Num(67), Num(1)), ref.sections)
    }

    @Test
    fun wholeBookAndUnderscores() {
        assertEquals(emptyList(), parser.parse("Derashat_Shabbat_HaGadol")?.sections)
    }

    @Test
    fun unknownBook() {
        assertNull(parser.parse("Some Unknown Book 3:4"))
    }
}
