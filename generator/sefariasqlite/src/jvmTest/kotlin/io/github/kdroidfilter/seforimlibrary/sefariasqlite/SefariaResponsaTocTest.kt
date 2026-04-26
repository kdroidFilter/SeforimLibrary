package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/392 (responsa TOC):
 *
 * Sefaria schemas for Chavot Yair, Teshuvot HaRi Migash, Ohr HaSekhel,
 * Shut Chatam Sofer, Noda BiYehuda, Shut HaRashba etc. ship
 * `heSectionNames=["", "פסקה"]` — the top-level Hebrew label ("תשובה",
 * "סימן", "חלק"...) is left empty even though `sectionNames=["Teshuva", ...]`
 * is populated. The old importer read `heSectionNames` strictly, so it
 * emitted blank `<h2></h2>` and generated no TOC at all.
 */
class SefariaResponsaTocTest {
    @Test
    fun blankHeSectionNameFallsBackToEnglishTranslation() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-responsa")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Havot Yair"))

        Files.writeString(schemaDir.resolve("Havot_Yair.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaResponsaTocTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        // Sanity: content is present
        assertTrue(payload.lines.any { it.contains("text of teshuva 1") })

        // Before the fix, there was no heading for "Teshuva N" because heSectionNames[0] == "".
        val teshuvaA = payload.headings.firstOrNull { it.title == "תשובה א" }
        val teshuvaB = payload.headings.firstOrNull { it.title == "תשובה ב" }
        assertTrue(teshuvaA != null, "expected <h2>תשובה א</h2> heading (got ${payload.headings.map { it.title }})")
        assertTrue(teshuvaB != null, "expected <h2>תשובה ב</h2> heading (got ${payload.headings.map { it.title }})")

        // heRef for the first paragraph of the first teshuva should use the Hebrew label chain
        assertTrue(
            payload.refEntries.any { it.heRef == "חוות יאיר א, א" },
            "expected refEntry heRef='חוות יאיר א, א'"
        )
    }

    companion object {
        // Simulates the real Havot Yair schema: heSectionNames present but first
        // entry is "" (empty) — a recurring Sefaria pattern for responsa.
        private val schemaJson = """
            {
              "title": "Havot Yair",
              "heTitle": "חוות יאיר",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Siman", "Seif"],
                "sectionNames": ["Teshuva", "Paragraph"],
                "heSectionNames": ["", "פסקה"],
                "title": "Havot Yair",
                "heTitle": "חוות יאיר"
              }
            }
        """.trimIndent()

        private val mergedJson = """
            {
              "title": "Havot Yair",
              "heTitle": "חוות יאיר",
              "text": [
                ["text of teshuva 1 paragraph 1", "text of teshuva 1 paragraph 2"],
                ["text of teshuva 2 paragraph 1"]
              ]
            }
        """.trimIndent()
    }
}
