package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Regression for kdroidFilter/Zayit#509: the Midrash Rabbah's paragraphs (אותיות)
 * are unnamed "Integer" segments, so they had neither a number nor a TOC entry.
 */
class SefariaNumberedParagraphsTest {
    @Test
    fun midrashRabbahParagraphsGetLabelAndTocEntry() {
        val payload = readBook("Bereshit Rabbah", "בראשית רבה")

        assertEquals(
            listOf(
                "<h1>בראשית רבה</h1>",
                "<h2>פרק א</h2>",
                "(א) רבי הושעיה רבה פתח",
                "(ב) רבי יהושע דסכנין",
                "<h2>פרק ב</h2>",
                "(א) והארץ היתה תהו",
            ),
            payload.lines,
        )
        assertEquals(
            listOf(
                Heading("בראשית רבה", 0, 0),
                Heading("פרק א", 1, 1),
                Heading("אות א", 2, 2),
                Heading("אות ב", 2, 3),
                Heading("פרק ב", 1, 4),
                Heading("אות א", 2, 5),
            ),
            payload.headings,
        )
        // heRefs (the line id natural keys) are unchanged
        assertEquals(
            listOf("בראשית רבה א, א", "בראשית רבה א, ב", "בראשית רבה ב, א"),
            payload.refEntries.map { it.heRef.replace(Regex(" +"), " ") },
        )
    }

    @Test
    fun otherBooksKeepUnlabeledIntegerSegments() {
        val payload = readBook("FakeBook", "ספר")

        assertEquals("רבי הושעיה רבה פתח", payload.lines[2])
        assertEquals(listOf(0, 1, 1), payload.headings.map { it.level })
    }

    private fun readBook(title: String, heTitle: String) = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-numbered-paragraphs")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve(title))
        Files.writeString(schemaDir.resolve("${title.replace(' ', '_')}.json"), schemaJson(title, heTitle))
        Files.writeString(bookDir.resolve("merged.json"), mergedJson(title, heTitle))

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaNumberedParagraphsTest"),
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
            "addressTypes": ["Perek", "Integer"],
            "sectionNames": ["Chapter", "Paragraph"],
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
            ["רבי הושעיה רבה פתח", "רבי יהושע דסכנין"],
            ["והארץ היתה תהו"]
          ]
        }
    """.trimIndent()
}
