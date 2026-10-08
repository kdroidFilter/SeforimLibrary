package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/440: the Maharsha's
 * commentaries were listed as "חידושי הלכות" / "חידושי אגדות" without his name.
 */
class SefariaBookTitlesTest {
    @Test
    fun prefixesMaharshaCollections() {
        assertEquals(
            "מהרש\"א - חידושי אגדות על חולין",
            sefariaDisplayTitle("חידושי אגדות על חולין", "Chidushei Agadot"),
        )
        assertEquals(
            "מהרש\"א - חידושי הלכות על שבת",
            sefariaDisplayTitle("חידושי הלכות על שבת", "Chidushei Halachot"),
        )
    }

    @Test
    fun keepsOtherTitles() {
        assertEquals("רש\"י על ברכות", sefariaDisplayTitle("רש\"י על ברכות", "Rashi"))
        assertEquals("ברכות", sefariaDisplayTitle("ברכות", null))
    }
}
