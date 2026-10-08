package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/501: the Epistle of
 * Rav Sherira Gaon ships `heSectionNames=["", "פסקה"]` with `sectionNames=["Question", ...]`,
 * and the unmapped English "Question" leaked into the Hebrew headings.
 */
class SefariaSectionNameMappingTest {
    @Test
    fun mapsEnglishSectionNamesFoundInSefariaSchemas() {
        val expected = mapOf(
            "Question" to "שאלה",
            "Comment" to "פירוש",
            "'Comment'" to "פירוש",
            "DH" to "דיבור המתחיל",
            "Ot" to "אות",
            "Sheilta" to "שאילתא",
            "Gate" to "שער",
            "Sha'ar Ha'Gemul" to "שער",
            "Se'if" to "סעיף",
            "Passuk" to "פסוק",
            "Room" to "חדר",
            "Letter" to "אות",
            "Principle" to "עיקר",
            "Statement" to "סימן",
        )
        expected.forEach { (en, he) -> assertEquals(he, mapSectionNameToHebrew(en), "mapping for '$en'") }
    }

    @Test
    fun shortTokensDoNotMatchAsSubstrings() {
        // "ot" must not hijack names that merely contain it
        assertEquals("הערה", mapSectionNameToHebrew("Footnote"))
        assertEquals("Manuscript", mapSectionNameToHebrew("Manuscript"))
    }

    @Test
    fun integerLevelIsUnnamed() {
        assertNull(mapSectionNameToHebrew("Integer"))
    }

    /**
     * Sefaria shows "Integer" levels by their bare number (its Hebrew refs read
     * "א׳:א׳:א׳"), as in Sha'arei Torat Eretz Yisrael (Chapter/Halakhah/Integer/Integer).
     */
    @Test
    fun integerLevelHeadingIsTheBareNumber() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-integer")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve("Sample"))
        Files.writeString(
            schemaDir.resolve("Sample.json"),
            """
            {
              "title": "Sample",
              "heTitle": "דוגמה",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 4,
                "addressTypes": ["Perek", "Halakhah", "Integer", "Integer"],
                "sectionNames": ["Chapter", "Halakhah", "Integer", "Integer"],
                "title": "Sample",
                "heTitle": "דוגמה"
              }
            }
            """.trimIndent()
        )
        Files.writeString(
            bookDir.resolve("merged.json"),
            """{"title": "Sample", "heTitle": "דוגמה", "text": [[[["a1", "a2"], ["b1"]]]]}"""
        )

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaSectionNameMappingTest")
        )
        val payload = reader.readBooksInParallel(
            tempDir.resolve("json"), schemaDir, reader.buildSchemaLookup(schemaDir)
        ).single()
        val titles = payload.headings.map { it.title }

        assertEquals(listOf("דוגמה", "פרק א", "הלכה א", "א", "ב"), titles)
        assertTrue(payload.lines.none { it.contains("Integer") }, "no English label in ${payload.lines}")
    }
}
