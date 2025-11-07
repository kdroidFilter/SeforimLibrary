package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Small ad-hoc runner to validate object-wrapped Sefaria texts (e.g., Chayei Moharan).
 * Loads a specific book's merged.json and schema, invokes the builder's internal
 * content flattener via reflection, and prints the number of output lines.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("TestSingle")

    val rootCandidates = listOf(
        Paths.get("generator", "build", "Sefaria").toAbsolutePath(),
        Paths.get("build", "Sefaria").toAbsolutePath(),
        Paths.get("..", "generator", "build", "Sefaria").toAbsolutePath(),
        Paths.get("..", "build", "Sefaria").toAbsolutePath(),
    )
    val sefariaRoot = rootCandidates.firstOrNull { Files.isDirectory(it) }
        ?: error("Sefaria root not found; checked: ${rootCandidates.joinToString()} ")

    // Test with Chayei Moharan
    val textPath = sefariaRoot.resolve("json/Chasidut/Breslov/Chayei Moharan/Hebrew/merged.json")
    val schemaPath = sefariaRoot.resolve("export_schemas/schemas/Chayei_Moharan.json")
    require(Files.exists(textPath)) { "Missing text: $textPath" }
    require(Files.exists(schemaPath)) { "Missing schema: $schemaPath" }

    val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
    val textRoot = json.parseToJsonElement(Files.readString(textPath))
    val schemaRoot = json.parseToJsonElement(Files.readString(schemaPath)) as JsonObject

    val builder = SefariaToOtzariaBuilder(logger)
    val m = SefariaToOtzariaBuilder::class.java.getDeclaredMethod(
        "buildContentWithRefsFromCached",
        JsonElement::class.java,
        JsonObject::class.java
    )
    m.isAccessible = true
    @Suppress("UNCHECKED_CAST")
    val pair = m.invoke(builder, textRoot, schemaRoot) as Pair<List<String>, List<Any?>>
    val lines = pair.first
    println("Chayei Moharan lines: ${lines.size}")
    lines.take(20).forEach { print(it) }

    // Test with Likutei Halakhot
    val textPath2 = sefariaRoot.resolve("json/Chasidut/Breslov/Likutei Halakhot/Hebrew/merged.json")
    val schemaPath2 = sefariaRoot.resolve("export_schemas/schemas/Likutei_Halakhot.json")
    if (Files.exists(textPath2) && Files.exists(schemaPath2)) {
        val textRoot2 = json.parseToJsonElement(Files.readString(textPath2))
        val schemaRoot2 = json.parseToJsonElement(Files.readString(schemaPath2)) as JsonObject
        @Suppress("UNCHECKED_CAST")
        val pair2 = m.invoke(builder, textRoot2, schemaRoot2) as Pair<List<String>, List<Any?>>
        val lines2 = pair2.first
        println("Likutei Halakhot lines: ${lines2.size}")
        lines2.take(10).forEach { print(it) }
    }
}
