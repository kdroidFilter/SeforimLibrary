package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SefariaBookPayloadReaderTest {
    @Test
    fun defaultNodeWithoutTitleKeepsSimanimAtSameLevelAsIntroduction() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-test")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Tur"))

        Files.writeString(schemaDir.resolve("Tur.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaBookPayloadReaderTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        val intro = payload.headings.firstOrNull { it.title == "הקדמה" }
        val siman = payload.headings.firstOrNull { it.title == "סימן א" }

        assertNotNull(intro)
        assertNotNull(siman)
        assertEquals(2, intro.level)
        assertEquals(2, siman.level)
        assertTrue(payload.lines.any { it == "<h3>סימן א</h3>" })
        assertTrue(payload.lines.none { it == "<h4>סימן א</h4>" })
    }

    companion object {
        private val schemaJson = """
            {
              "title": "Tur",
              "heTitle": "טור",
              "schema": {
                "title": "Tur",
                "heTitle": "טור",
                "nodes": [
                  {
                    "nodeType": "SchemaNode",
                    "title": "Orach Chayim",
                    "heTitle": "אורח חיים",
                    "key": "Orach Chaim",
                    "nodes": [
                      {
                        "nodeType": "JaggedArrayNode",
                        "depth": 1,
                        "addressTypes": [
                          "Integer"
                        ],
                        "sectionNames": [
                          "Paragraph"
                        ],
                        "title": "Introduction",
                        "heTitle": "הקדמה",
                        "heSectionNames": [
                          "פסקה"
                        ],
                        "key": "Introduction"
                      },
                      {
                        "nodeType": "JaggedArrayNode",
                        "depth": 2,
                        "addressTypes": [
                          "Siman",
                          "Seif"
                        ],
                        "sectionNames": [
                          "Siman",
                          "Seif"
                        ],
                        "title": "",
                        "heTitle": "",
                        "heSectionNames": [
                          "סימן",
                          "סעיף"
                        ],
                        "key": "default",
                        "default": true
                      }
                    ]
                  }
                ]
              }
            }
        """.trimIndent()

        private val mergedJson = """
            {
              "title": "Tur",
              "heTitle": "טור",
              "text": {
                "Orach Chayim": {
                  "Introduction": [
                    "intro paragraph"
                  ],
                  "": [
                    [
                      "siman text"
                    ]
                  ]
                }
              }
            }
        """.trimIndent()
    }
}
