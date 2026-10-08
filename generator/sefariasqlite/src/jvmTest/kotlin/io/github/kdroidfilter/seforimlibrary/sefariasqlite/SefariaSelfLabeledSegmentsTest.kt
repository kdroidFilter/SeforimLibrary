package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Regression for kdroidFilter/SeforimLibrary#73 and #74: the Mishnah Berurah's
 * segments carry their own seif katan marker, so the generated "(letter) " label
 * doubled it, and labelled the kuntresim (Mishnat Soferim) that have none.
 */
class SefariaSelfLabeledSegmentsTest {
    @Test
    fun selfLabeledBookKeepsOnlyItsOwnMarkers() {
        val payload = readBook("Mishnah Berurah", "משנה ברורה")

        assertEquals(
            listOf(
                "<h1>משנה ברורה</h1>",
                "<h2>סימן א</h2>",
                "(א) נהגו לתייג",
                "<b>משנת סופרים</b>",
                "תפילין ומזוזות",
                "<h2>סימן ב</h2>",
                "(א) אין מראין",
                "(ב) לראות",
            ),
            payload.lines,
        )
        // heRefs (the line id natural keys) are unchanged
        assertEquals(
            listOf("משנה ברורה א, א", "משנה ברורה א, ב", "משנה ברורה א, ג", "משנה ברורה ב, א", "משנה ברורה ב, ב"),
            payload.refEntries.map { it.heRef.replace(Regex(" +"), " ") },
        )
    }

    @Test
    fun otherBooksStillGetGeneratedLabels() {
        val payload = readBook("FakeBook", "ספר")

        assertEquals("(א) (א) נהגו לתייג", payload.lines[2])
        assertEquals("(ב) <b>משנת סופרים</b>", payload.lines[3])
        assertEquals("(א) {א} אין מראין", payload.lines[6])
    }

    private fun readBook(title: String, heTitle: String) = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-self-labeled")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve(title))
        Files.writeString(schemaDir.resolve("${title.replace(' ', '_')}.json"), schemaJson(title, heTitle))
        Files.writeString(bookDir.resolve("merged.json"), mergedJson(title, heTitle))

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaSelfLabeledSegmentsTest"),
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        reader.readBooksInParallel(tempDir.resolve("json"), schemaDir, schemaLookup).single()
    }

    private fun schemaJson(title: String, heTitle: String) = """
        {
          "title": "$title",
          "heTitle": "$heTitle",
          "schema": {
            "nodeType": "JaggedArrayNode",
            "depth": 2,
            "addressTypes": ["Siman", "Seif Katan"],
            "sectionNames": ["Siman", "Seif Katan"],
            "heSectionNames": ["סימן", "סעיף קטן"],
            "title": "$title",
            "heTitle": "$heTitle"
          }
        }
    """.trimIndent()

    private fun mergedJson(title: String, heTitle: String) = """
        {
          "title": "$title",
          "heTitle": "$heTitle",
          "text": [
            ["(א) נהגו לתייג", "<b>משנת סופרים</b>", "תפילין ומזוזות"],
            ["{א} אין מראין", "{ב} לראות"]
          ]
        }
    """.trimIndent()
}
