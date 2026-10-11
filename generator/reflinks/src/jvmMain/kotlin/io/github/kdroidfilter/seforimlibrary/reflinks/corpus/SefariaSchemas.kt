package io.github.kdroidfilter.seforimlibrary.reflinks.corpus

import io.github.kdroidfilter.seforimlibrary.reflinks.SefariaRefParser
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path

/** Reads the English and Hebrew titles of every book from the Sefaria export (the JSON files of `export/schemas`). */
object SefariaSchemas {

    private val json = Json { ignoreUnknownKeys = true }

    fun load(schemasDir: Path): List<SefariaRefParser.Book> {
        if (!Files.isDirectory(schemasDir)) return emptyList()
        return Files.list(schemasDir).use { files ->
            files.filter { it.toString().endsWith(".json") }.toList().mapNotNull { file ->
                runCatching { book(json.parseToJsonElement(Files.readString(file)).jsonObject) }.getOrNull()
            }
        }
    }

    private fun book(root: JsonObject): SefariaRefParser.Book? {
        val heTitle = root.string("heTitle") ?: return null
        val schema = root["schema"] as? JsonObject
        val enTitles = buildSet {
            root.string("title")?.let(::add)
            (root["titleVariants"] as? JsonArray)?.forEach { it.jsonPrimitive.contentOrNull?.let(::add) }
            schema?.let { addAll(titles(it, "en")) }
        }
        val nodes = HashMap<String, String>()
        schema?.let { collectNodes(it, nodes) }
        return SefariaRefParser.Book(enTitles.toList(), heTitle, nodes)
    }

    /** English → Hebrew names of every named node, at any depth (the default unnamed node has none). */
    private fun collectNodes(node: JsonObject, into: MutableMap<String, String>) {
        (node["nodes"] as? JsonArray)?.forEach { child ->
            val obj = child as? JsonObject ?: return@forEach
            val he = titles(obj, "he").firstOrNull() ?: obj.string("heTitle")
            if (he != null) titles(obj, "en").forEach { into.putIfAbsent(it, he) }
            collectNodes(obj, into)
        }
    }

    private fun titles(node: JsonObject, lang: String): List<String> =
        (node["titles"] as? JsonArray).orEmpty().mapNotNull { t ->
            val o = t.jsonObject
            o.string("text")?.takeIf { o.string("lang") == lang }
        }

    private fun JsonObject.string(key: String): String? =
        this[key]?.let { runCatching { it.jsonPrimitive.contentOrNull }.getOrNull() }
}
