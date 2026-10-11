package io.github.kdroidfilter.seforimlibrary.reflinks

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ShulchanArukhCommentatorsTest {

    private fun named(before: String, part: String) = ShulchanArukhCommentators.named(before.split(' '), part)

    @Test
    fun commentatorNamedBeforeTheCitation() {
        assertEquals("טורי זהב על שולחן ערוך אורח חיים", named("וכן כתב הט\"ז", "אורח חיים")?.title)
        assertEquals("שפתי כהן על שולחן ערוך יורה דעה", named("עיין בש\"ך", "יורה דעה")?.title)
        assertEquals("מגן אברהם", named("כמ\"ש המג\"א", "אורח חיים")?.title)
        val beitYosef = named("וכתב הב\"י", "חושן משפט")!!
        assertEquals("בית יוסף", beitYosef.title)
        assertEquals(true, beitYosef.partSection)
    }

    @Test
    fun unknownOrWrongPart() {
        assertNull(named("ואמר רבא", "אורח חיים"))
        assertNull(named("המג\"א", "יורה דעה"), "the Magen Avraham only covers Orach Chaim")
    }
}
