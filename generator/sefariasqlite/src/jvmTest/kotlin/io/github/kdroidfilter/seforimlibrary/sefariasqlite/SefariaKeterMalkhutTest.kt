package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/392 (Keter Malkhut):
 * the book's schema declares `depth=2` (Chapter × Paragraph) but the merged
 * `text` is effectively 1-D — most chapters are a single string rather than a
 * paragraph array. Before the fix, the importer hit the `text !is JsonArray`
 * guard at depth=1 and silently dropped every stanza, leaving only the
 * `<h2>פרק N</h2>` headings.
 */
class SefariaKeterMalkhutTest {
    @Test
    fun chapterStanzaStoredAsStringStillBecomesContentLine() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-keter")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Keter Malkhut"))

        Files.writeString(schemaDir.resolve("Keter_Malkhut.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaKeterMalkhutTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        val perekA = payload.headings.firstOrNull { it.title == "פרק א" }
        val perekB = payload.headings.firstOrNull { it.title == "פרק ב" }
        assertTrue(perekA != null, "expected <h2>פרק א</h2> heading")
        assertTrue(perekB != null, "expected <h2>פרק ב</h2> heading")

        // Stanza content must survive the schema-vs-data mismatch
        assertTrue(
            payload.lines.any { it.contains("נִפְלָאִים") },
            "stanza from chapter 2 should be present as a content line"
        )
        assertTrue(
            payload.lines.any { it.contains("בִּתְפִלָּתִי") },
            "stanza from chapter 1 should be present as a content line"
        )

        // Chapter 1 stanza is referenced as 'כתר מלכות א'
        val refA = payload.refEntries.firstOrNull { it.heRef == "כתר מלכות א" }
        assertTrue(refA != null, "chapter 1 should have heRef 'כתר מלכות א'")
    }

    @Test
    fun legitimate2dChapterStillExpandsToParagraphs() = runBlocking {
        // Guard that the primitive-fallback doesn't short-circuit books where
        // the data actually matches the schema.
        val tempDir = Files.createTempDirectory("seforim-2d")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("FakeBook"))

        Files.writeString(schemaDir.resolve("FakeBook.json"), schema2dJson)
        Files.writeString(bookDir.resolve("merged.json"), merged2dJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaKeterMalkhutTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        // 2 chapters × 2 paragraphs → 4 paragraph refs
        assertEquals(4, payload.refEntries.size)
    }

    companion object {
        private val schemaJson = """
            {
              "title": "Keter Malkhut",
              "heTitle": "כתר מלכות",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Perek", "Integer"],
                "sectionNames": ["Chapter", "Paragraph"],
                "heSectionNames": ["פרק", "פסקה"],
                "title": "Keter Malkhut",
                "heTitle": "כתר מלכות"
              }
            }
        """.trimIndent()

        // Real shape: outer array is chapters, most are a primitive string
        private val mergedJson = """
            {
              "title": "Keter Malkhut",
              "heTitle": "כתר מלכות",
              "text": [
                "בִּתְפִלָּתִי יִסְכָּן גָּבֶר",
                "נִפְלָאִים מַעֲשֶׂיךָ",
                "אַתָּה אֶחָד"
              ]
            }
        """.trimIndent()

        private val schema2dJson = """
            {
              "title": "FakeBook",
              "heTitle": "ספר",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Perek", "Integer"],
                "sectionNames": ["Chapter", "Verse"],
                "heSectionNames": ["פרק", "פסוק"],
                "title": "FakeBook",
                "heTitle": "ספר"
              }
            }
        """.trimIndent()

        private val merged2dJson = """
            {
              "title": "FakeBook",
              "heTitle": "ספר",
              "text": [
                ["ch1 p1", "ch1 p2"],
                ["ch2 p1", "ch2 p2"]
              ]
            }
        """.trimIndent()
    }
}
