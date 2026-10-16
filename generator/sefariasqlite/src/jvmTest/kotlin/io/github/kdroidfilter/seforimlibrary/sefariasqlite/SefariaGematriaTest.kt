package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SefariaGematriaTest {

    @Test
    fun numbersBelowTwelveHundredRepeatTav() {
        assertEquals("תתקצט", toGematria(999))
        assertEquals("תתר", toGematria(1000))
        assertEquals("תתרא", toGematria(1001))
        assertEquals("תתרטו", toGematria(1015))
        assertEquals("תתרפה", toGematria(1085))
        assertEquals("תתשצט", toGematria(1199))
    }

    @Test
    fun legacyFormIsTheOldThousandsSpelling() {
        assertEquals("א ", legacyThousandsGematria(1000))
        assertEquals("א א", legacyThousandsGematria(1001))
        assertEquals("א טו", legacyThousandsGematria(1015))
        assertNull(legacyThousandsGematria(999))
        assertNull(legacyThousandsGematria(1200))
    }

    private fun strings(values: List<String>) = JsonArray(values.map(::JsonPrimitive))

    @Test
    fun headingsAboveNineHundredNinetyNineKeepTheirLineKeys() = runBlocking {
        val root = Files.createTempDirectory("sefaria-gematria-keys")
        try {
            Files.createDirectories(root.resolve("schemas"))
            val schema = buildJsonObject {
                put("title", "Fixture Remazim"); put("heTitle", "רמזים לבדיקה")
                put("schema", buildJsonObject {
                    put("title", "Fixture Remazim"); put("heTitle", "רמזים לבדיקה")
                    put("nodeType", "JaggedArrayNode"); put("depth", 2)
                    put("sectionNames", strings(listOf("Remez", "Paragraph")))
                    put("heSectionNames", strings(listOf("רמז", "פסקה")))
                    put("addressTypes", strings(listOf("Integer", "Integer")))
                })
                put("categories", strings(listOf("Midrash")))
            }
            Files.writeString(root.resolve("schemas/Fixture_Remazim.json"), schema.toString())
            val merged = buildJsonObject {
                put("title", "Fixture Remazim"); put("heTitle", "רמזים לבדיקה"); put("language", "he")
                put("versions", JsonArray(listOf(strings(listOf("Fixture Edition", "")))))
                put("text", JsonArray((1..1002).map { strings(listOf("תוכן רמז $it")) }))
            }
            Files.writeString(
                Files.createDirectories(root.resolve("json/Fixture Remazim")).resolve("merged.json"),
                merged.toString(),
            )

            val reader = SefariaBookPayloadReader(Json { ignoreUnknownKeys = true }, Logger.withTag("GematriaTest"))
            val payload = reader.readBooksInParallel(
                root.resolve("json"), root.resolve("schemas"), reader.buildSchemaLookup(root.resolve("schemas")),
            ).single()
            val keys = payload.precomputed!!.lineKeyHashes!!

            fun keyOf(remez: Int): ByteArray {
                val index = payload.lines.indexOfFirst { it.startsWith("<h") && it.contains("רמז ${toGematria(remez)}<") }
                return keys[index]!!
            }
            fun headingWith(label: String) = payload.lines.first { it.startsWith("<h") && it.contains("רמז ") }
                .replace(Regex("רמז [^<]+"), "רמז $label")

            assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(headingWith("תתקצט")), keyOf(999))
            assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(headingWith("א ")), keyOf(1000))
            assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(headingWith("א א")), keyOf(1001))
            assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(headingWith("א ב")), keyOf(1002))
        } finally {
            Files.walk(root).use { paths -> paths.sorted(java.util.Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }
}
