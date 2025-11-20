package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SefariaMergedText
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SefariaSchema
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SchemaNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject

class SefariaTextParserTest {
    private val parser = SefariaTextParser()

    @Test
    fun `parses default child branches under parent section`() {
        val schema = SefariaSchema(
            title = "Tur Test",
            heTitle = "טור",
            categories = listOf("Halakhah", "Tur"),
            schema = SchemaNode(
                nodeType = "SchemaNode",
                nodes = listOf(
                    SchemaNode(
                        nodeType = "SchemaNode",
                        key = "Orach Chayim",
                        heTitle = "אורח חיים",
                        nodes = listOf(
                            SchemaNode(
                                nodeType = "JaggedArrayNode",
                                key = "Introduction",
                                heTitle = "הקדמה",
                                depth = 1,
                                heSectionNames = listOf("פסקה")
                            ),
                            SchemaNode(
                                nodeType = "JaggedArrayNode",
                                key = "default",
                                heTitle = "",
                                depth = 2,
                                heSectionNames = listOf("סימן", "סעיף")
                            )
                        )
                    )
                )
            )
        )

        val merged = SefariaMergedText(
            title = "Tur Test",
            language = "he",
            versionTitle = "test",
            versionSource = "local",
            schema = schema.schema,
            sectionNames = null,
            categories = schema.categories,
            text = buildJsonObject {
                put(
                    "Orach Chayim",
                    buildJsonObject {
                        put("Introduction", buildJsonArray { add(JsonPrimitive("intro")) })
                        put(
                            "",
                            buildJsonArray {
                                add(buildJsonArray {
                                    add(JsonPrimitive("Siman 1 Seif 1"))
                                    add(JsonPrimitive("Siman 1 Seif 2"))
                                })
                                add(buildJsonArray {
                                    add(JsonPrimitive("Siman 2 Seif 1"))
                                })
                            }
                        )
                    }
                )
            }
        )

        val parsed = parser.parse(bookId = 143, mergedText = merged, schema = schema)

        val root = parsed.tocEntries.firstOrNull { it.text == "אורח חיים" }
        assertNotNull(root, "root section entry should be created")
        assertTrue(parsed.tocEntries.size > 3, "TOC should include children, not just root sections")

        val firstSiman = parsed.tocEntries.firstOrNull { it.text == "סימן א" }
        assertNotNull(firstSiman, "first siman should be present in TOC")
        assertEquals(root.id, firstSiman.parentId, "siman entries should be attached to the parent section")

        val firstSeif = parsed.tocEntries.firstOrNull { it.text == "סעיף א" }
        assertNotNull(firstSeif, "first seif should be present in TOC")
        assertEquals(firstSiman.id, firstSeif.parentId, "seif entries should be nested under their siman")
    }
}
