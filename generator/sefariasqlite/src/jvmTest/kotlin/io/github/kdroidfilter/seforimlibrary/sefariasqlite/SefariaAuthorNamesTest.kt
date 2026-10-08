package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/444: Sefaria ships
 * Admat Kodesh's author with an empty `he` title, which produced a blank author.
 */
class SefariaAuthorNamesTest {
    private fun resolve(json: String) = resolveSefariaAuthorName(Json.parseToJsonElement(json))

    @Test
    fun keepsSefariaHebrewName() {
        assertEquals("רש״י", resolve("""{"en": "Rashi", "he": "רש״י", "slug": "rashi"}"""))
    }

    @Test
    fun usesOverrideWhenHebrewNameIsEmpty() {
        assertEquals(
            "ניסים חיים משה מזרחי",
            resolve("""{"en": "Nissim Chaim Moshe Mizrachi", "he": "", "slug": "nissim-chaim-moshe-mizrachi"}"""),
        )
    }

    @Test
    fun fallsBackToEnglishForUnknownSlug() {
        assertEquals("Someone", resolve("""{"en": "Someone", "he": " ", "slug": "someone"}"""))
    }

    @Test
    fun dropsAuthorWithoutAnyName() {
        assertNull(resolve("""{"en": "", "he": "", "slug": "nobody"}"""))
    }
}
