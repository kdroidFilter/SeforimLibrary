package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Regression for https://github.com/kdroidFilter/Zayit/issues/392 (Jerusalem
 * Talmud Venice/Vilna daf headings).
 *
 * The Yerushalmi's main schema is chapter × halakhah × segment; the printed-
 * edition daf navigation lives in the `Venice` and `Vilna` alt-structs.
 * The old importer tagged every tractate whose category contains "תלמוד" as
 * `isTalmudTractate`, which suppresses alt-struct child enumeration — so
 * Yerushalmi ended up with no daf headings from either printing.
 */
class SefariaYerushalmiAltTocTest {
    @Test
    fun yerushalmiGetsAltTocEntriesForVeniceAndVilna() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-yeru")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        val jsonDir = Files.createDirectories(tempDir.resolve("json"))
        val bookDir = Files.createDirectories(jsonDir.resolve("Jerusalem Talmud Berakhot"))

        Files.writeString(schemaDir.resolve("Jerusalem_Talmud_Berakhot.json"), schemaJson)
        Files.writeString(bookDir.resolve("merged.json"), mergedJson)

        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaYerushalmiAltTocTest")
        )
        val schemaLookup = reader.buildSchemaLookup(schemaDir)
        val payload = reader.readBooksInParallel(jsonDir, schemaDir, schemaLookup).single()

        // Precondition: schema parsing picked up the Hebrew categorisation
        assertTrue(payload.categoriesHe.any { it.contains("ירושלמי") })

        // Build alt-toc entries through a real (in-memory) SQLite so we can
        // inspect what gets persisted for Venice/Vilna.
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(
            io.github.kdroidfilter.seforimlibrary.core.models.Category(
                id = 0,
                parentId = null,
                title = "ירושלמי",
                level = 0,
                order = 1
            )
        )
        val book = io.github.kdroidfilter.seforimlibrary.core.models.Book(
            id = 1,
            categoryId = catId,
            sourceId = sourceId,
            title = payload.heTitle,
            heRef = payload.heTitle,
            authors = emptyList(),
            pubPlaces = emptyList(),
            pubDates = emptyList(),
            heShortDesc = null,
            notesContent = null,
            order = 1f,
            topics = emptyList(),
            isBaseBook = true,
            totalLines = payload.lines.size,
            hasAltStructures = false,
            hasTeamim = false,
            hasNekudot = false
        )
        repo.insertBook(book)

        val lineKeyToId = ConcurrentHashMap<Pair<String, Int>, Long>()
        payload.lines.forEachIndexed { idx, content ->
            val lineId = (idx + 1).toLong()
            repo.insertLinesBatch(
                listOf(
                    io.github.kdroidfilter.seforimlibrary.core.models.Line(
                        id = lineId,
                        bookId = 1,
                        lineIndex = idx,
                        content = content,
                        heRef = payload.refEntries.getOrNull(idx)?.heRef
                    )
                )
            )
            lineKeyToId["Jerusalem Talmud Berakhot" to idx] = lineId
        }

        val altTocBuilder = SefariaAltTocBuilder(repo)
        val ok = altTocBuilder.buildAltTocStructuresForBook(
            payload = payload,
            bookId = 1,
            bookPath = "Jerusalem Talmud Berakhot",
            lineKeyToId = lineKeyToId,
            totalLines = payload.lines.size
        )

        assertTrue(ok, "expected alt-struct TOC entries to have been generated")

        // Read back structures — both Venice and Vilna must exist
        val structs = repo.getAltTocStructuresForBook(1)
        assertTrue(structs.any { it.key == "Venice" }, "Venice alt-struct not persisted")
        assertTrue(structs.any { it.key == "Vilna" }, "Vilna alt-struct not persisted")

        // Entries under Venice should include column headings from the refs
        val venice = structs.first { it.key == "Venice" }
        val veniceEntries = repo.getAltTocEntriesForStructure(venice.id)
        assertTrue(
            veniceEntries.isNotEmpty(),
            "Venice structure has no entries — alt-struct children were skipped"
        )

        repo.close()
        // Unused but reminds future readers why we built a real DB
        @Suppress("UNUSED_VARIABLE") val _unused = UUID.randomUUID()
    }

    companion object {
        // Simplified but schema-faithful: main chapter × halakhah × segment
        // schema with Venice and Vilna alt-structs (each a single chapter
        // node with 3 column refs).
        private val schemaJson = """
            {
              "title": "Jerusalem Talmud Berakhot",
              "heTitle": "תלמוד ירושלמי ברכות",
              "heCategories": ["תלמוד", "ירושלמי", "סדר זרעים"],
              "categories": ["Talmud", "Yerushalmi", "Seder Zeraim"],
              "schema": {
                "nodeType": "JaggedArrayNode",
                "depth": 3,
                "addressTypes": ["Perek", "Halakhah", "Integer"],
                "sectionNames": ["Chapter", "Halakhah", "Segment"],
                "heSectionNames": ["פרק", "הלכה", "קטע"],
                "title": "Jerusalem Talmud Berakhot",
                "heTitle": "תלמוד ירושלמי ברכות"
              },
              "alts": {
                "Venice": {
                  "nodes": [
                    {
                      "nodeType": "ArrayMapNode",
                      "depth": 1,
                      "wholeRef": "Jerusalem Talmud Berakhot 1:1:1-1:2:2",
                      "addressTypes": ["Folio"],
                      "sectionNames": ["Column"],
                      "refs": [
                        "Jerusalem Talmud Berakhot 1:1:1",
                        "Jerusalem Talmud Berakhot 1:1:2",
                        "Jerusalem Talmud Berakhot 1:2:1"
                      ],
                      "startingAddress": "2a",
                      "offset": 4,
                      "title": "Chapter 1",
                      "heTitle": "מאימתי"
                    }
                  ]
                },
                "Vilna": {
                  "nodes": [
                    {
                      "nodeType": "ArrayMapNode",
                      "depth": 1,
                      "wholeRef": "Jerusalem Talmud Berakhot 1:1:1-1:2:2",
                      "addressTypes": ["Folio"],
                      "sectionNames": ["Daf"],
                      "refs": [
                        "Jerusalem Talmud Berakhot 1:1:1",
                        "Jerusalem Talmud Berakhot 1:1:2"
                      ],
                      "startingAddress": "2a",
                      "title": "Chapter 1",
                      "heTitle": "מאימתי"
                    }
                  ]
                }
              }
            }
        """.trimIndent()

        private val mergedJson = """
            {
              "title": "Jerusalem Talmud Berakhot",
              "heTitle": "תלמוד ירושלמי ברכות",
              "text": [
                [
                  ["seg 1-1-1", "seg 1-1-2"],
                  ["seg 1-2-1", "seg 1-2-2"]
                ]
              ]
            }
        """.trimIndent()
    }
}
