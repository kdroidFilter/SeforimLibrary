package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/392 (Zohar vol 2
 * Tetzaveh: daf קפו: missing — jumps from קפו. straight to קפז).
 *
 * Root cause: Sefaria refs for Zohar use `chapter:global_paragraph_index`
 * (cumulative within a parasha), driven by `index_offsets_by_depth`. The old
 * importer stored per-chapter-local paragraph refs ("14:1"…"14:14"), so the
 * alt-struct `Daf` refs like `"Zohar, Tetzaveh 14:118-127"` fell back to the
 * same line ("14:14"), got deduplicated, and daf 186a (the 14th sub-ref) was
 * dropped.
 */
class SefariaZoharIndexOffsetsTest {
    @Test
    fun paragraphRefsUseGlobalIndexWhenIndexOffsetsByDepthPresent() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-zohar")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Zohar"))

        Files.writeString(schemaDir.resolve("Zohar.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaZoharIndexOffsetsTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        // Chapter 1 (offset 0): paragraphs 1..3 → refs chapter 1 global 1..3
        assertTrue(
            payload.refEntries.any { it.ref.contains("Tetzaveh") && it.ref.endsWith("1:1") },
            "expected ref ending 'Tetzaveh 1:1' (got ${payload.refEntries.take(6).map { it.ref }})"
        )
        assertTrue(payload.refEntries.any { it.ref.contains("Tetzaveh") && it.ref.endsWith("1:3") })

        // Chapter 2 (offset 3): first paragraph → global index 4
        assertTrue(
            payload.refEntries.any { it.ref.contains("Tetzaveh") && it.ref.endsWith("2:4") },
            "expected ref ending 'Tetzaveh 2:4' (chapter 2 starts at global paragraph 4)"
        )

        // Chapter 3 (offset 6): first paragraph → global index 7
        assertTrue(
            payload.refEntries.any { it.ref.contains("Tetzaveh") && it.ref.endsWith("3:7") },
            "expected ref ending 'Tetzaveh 3:7' (chapter 3 starts at global paragraph 7)"
        )

        // Per-chapter Hebrew letter labels remain local (א/ב/ג at each chapter)
        // — offsets only touch the English numeric refs.
        val chapter3HeRefs = payload.refEntries.filter { it.heRef.contains("תצוה,") && it.heRef.contains("ג,") }
        assertTrue(
            chapter3HeRefs.any { it.heRef.endsWith("ג, א") },
            "Hebrew heRef should still use per-chapter letter (got ${payload.refEntries.map { it.heRef }})"
        )
    }

    @Test
    fun bookWithoutIndexOffsetsByDepthStillUsesLocalIndex() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-plain")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Plain"))

        Files.writeString(schemaDir.resolve("Plain.json"), plainSchemaJson)
        Files.writeString(bookDir.resolve("merged.json"), plainMergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaZoharIndexOffsetsTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        // Chapter 2 paragraph 1 → ref ends "2:1" (no global offset applied)
        assertTrue(
            payload.refEntries.any { it.ref.contains("Plain") && it.ref.endsWith("2:1") },
            "without offsets, refs stay per-chapter local (got ${payload.refEntries.map { it.ref }})"
        )
        assertTrue(payload.refEntries.none { it.ref.contains("Plain") && it.ref.endsWith("2:4") })
        assertEquals(6, payload.refEntries.size)
    }

    companion object {
        // Faithful to the real Zohar → Tetzaveh node pattern (simplified to 3
        // chapters with offsets [0, 3, 6]).
        private val schemaJson = """
            {
              "title": "Zohar",
              "heTitle": "ספר הזהר",
              "schema": {
                "nodeType": "SchemaNode",
                "title": "Zohar",
                "heTitle": "ספר הזהר",
                "nodes": [
                  {
                    "nodeType": "JaggedArrayNode",
                    "depth": 2,
                    "addressTypes": ["Integer", "Integer"],
                    "sectionNames": ["Chapter", "Paragraph"],
                    "heSectionNames": ["פרק", "פסקה"],
                    "title": "Tetzaveh",
                    "heTitle": "תצוה",
                    "key": "Tetzaveh",
                    "index_offsets_by_depth": { "2": [0, 3, 6] }
                  }
                ]
              }
            }
        """.trimIndent()

        private val mergedJson = """
            {
              "title": "Zohar",
              "heTitle": "ספר הזהר",
              "text": {
                "Tetzaveh": [
                  ["ch1 p1", "ch1 p2", "ch1 p3"],
                  ["ch2 p1", "ch2 p2", "ch2 p3"],
                  ["ch3 p1", "ch3 p2", "ch3 p3"]
                ]
              }
            }
        """.trimIndent()

        private val plainSchemaJson = """
            {
              "title": "Plain",
              "heTitle": "ספר רגיל",
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 2,
                "addressTypes": ["Integer", "Integer"],
                "sectionNames": ["Chapter", "Paragraph"],
                "heSectionNames": ["פרק", "פסקה"],
                "title": "Plain",
                "heTitle": "ספר רגיל"
              }
            }
        """.trimIndent()

        private val plainMergedJson = """
            {
              "title": "Plain",
              "heTitle": "ספר רגיל",
              "text": [
                ["p1", "p2", "p3"],
                ["p1", "p2", "p3"]
              ]
            }
        """.trimIndent()
    }
}
