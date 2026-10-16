package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContains

// Shulchan Arukh HaRav's Hilchot Ribit and Choshen Mishpat are "פסקה" leaves that the print numbers.
class SefariaNumberedParagraphSectionsTest {

    private fun readBook(): BookPayload = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val bookDir = Files.createDirectories(tempDir.resolve("json").resolve("Shulchan Arukh HaRav"))
        Files.writeString(schemaDir.resolve("Shulchan_Arukh_HaRav.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)
        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaNumberedParagraphSectionsTest")
        )
        reader.readBooksInParallel(tempDir.resolve("json"), schemaDir, reader.buildSchemaLookup(schemaDir)).single()
    }

    @Test
    fun choshenMishpatAndHilchotRibitParagraphsAreNumbered() {
        val lines = readBook().lines
        assertContains(lines, "(א) הלואה ראשון")
        assertContains(lines, "(ב) הלואה שני")
        assertContains(lines, "(א) רבית ראשון")
        assertContains(lines, "(ב) רבית שני")
    }

    @Test
    fun introductionParagraphsStayBare() {
        val lines = readBook().lines
        assertContains(lines, "הקדמה ראשון")
        assertContains(lines, "הקדמה שני")
    }

    companion object {
        private fun paragraphNode(title: String) = """
            {"nodeType": "JaggedArrayNode", "depth": 1, "addressTypes": ["Integer"],
             "sectionNames": ["Paragraph"], "heSectionNames": ["פסקה"],
             "title": "$title", "heTitle": "$title", "key": "$title"}
        """

        private val schemaJson = """
            {"title": "Shulchan Arukh HaRav", "heTitle": "שולחן ערוך הרב",
             "schema": {"title": "Shulchan Arukh HaRav", "heTitle": "שולחן ערוך הרב",
               "key": "Shulchan Arukh HaRav", "nodes": [
               ${paragraphNode("Introduction by the Author's Sons")},
               {"title": "Yoreh Deah", "heTitle": "יורה דעה", "key": "Yoreh Deah", "nodes": [
                 ${paragraphNode("Laws of Interest and Iska")}
               ]},
               {"title": "Choshen Mishpat", "heTitle": "חושן משפט", "key": "Choshen Mishpat", "nodes": [
                 ${paragraphNode("Laws of Loans")}
               ]}
             ]}}
        """.trimIndent()

        private val mergedJson = """
            {"title": "Shulchan Arukh HaRav", "heTitle": "שולחן ערוך הרב", "text": {
              "Introduction by the Author's Sons": ["הקדמה ראשון", "הקדמה שני"],
              "Yoreh Deah": {"Laws of Interest and Iska": ["רבית ראשון", "רבית שני"]},
              "Choshen Mishpat": {"Laws of Loans": ["הלואה ראשון", "הלואה שני"]}
            }}
        """.trimIndent()
    }
}
