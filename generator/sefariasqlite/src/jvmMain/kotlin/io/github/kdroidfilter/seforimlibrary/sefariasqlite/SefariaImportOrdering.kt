package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.isDirectory

/**
 * Parse `table_of_contents.json` to extract category and book orders.
 */
internal fun parseTableOfContentsOrders(
    dbRoot: Path,
    json: Json,
    logger: Logger
): Pair<Map<String, Int>, Map<String, Int>> {
    val tocFile = dbRoot.resolve("table_of_contents.json")
    if (!Files.exists(tocFile)) {
        logger.w { "table_of_contents.json not found, using default ordering" }
        return Pair(emptyMap(), emptyMap())
    }

    val categoryOrders = ConcurrentHashMap<String, Int>()
    val bookOrders = ConcurrentHashMap<String, Int>()

    try {
        val tocJson = Files.readString(tocFile)
        val tocEntries = json.parseToJsonElement(tocJson).jsonArray

        fun processTocItem(item: JsonObject, categoryPath: List<String> = emptyList()) {
            val title = item["title"]?.jsonPrimitive?.contentOrNull
            val heTitle = item["heTitle"]?.jsonPrimitive?.contentOrNull
            // Use order if available, otherwise fall back to base_text_order (for commentaries)
            val order = item["order"]?.jsonPrimitive?.intOrNull
                ?: item["base_text_order"]?.jsonPrimitive?.intOrNull
                ?: item["base_text_order"]?.jsonPrimitive?.doubleOrNull?.toInt()
            if (title != null && order != null) {
                bookOrders[title] = order
            }
            if (heTitle != null && order != null) {
                bookOrders[heTitle] = order
                bookOrders[sanitizeFolder(heTitle)] = order
            }

            val category = item["category"]?.jsonPrimitive?.contentOrNull
            val heCategory = item["heCategory"]?.jsonPrimitive?.contentOrNull
            if (order != null && categoryPath.isNotEmpty()) {
                if (category != null) {
                    val fullPath = (categoryPath + category).joinToString("/")
                    categoryOrders[fullPath] = order
                    categoryOrders[sanitizeFolder(fullPath)] = order
                }
                if (heCategory != null) {
                    val fullPath = (categoryPath + heCategory).joinToString("/")
                    categoryOrders[fullPath] = order
                    categoryOrders[sanitizeFolder(fullPath)] = order
                }
            }

            item["contents"]?.jsonArray?.forEach { subItem ->
                val newPath = when {
                    heCategory != null -> categoryPath + heCategory
                    category != null -> categoryPath + category
                    else -> categoryPath
                }
                processTocItem(subItem.jsonObject, newPath)
            }
        }

        tocEntries.forEach { categoryEntry ->
            val obj = categoryEntry.jsonObject
            val catNameEn = obj["category"]?.jsonPrimitive?.contentOrNull
            val catNameHe = obj["heCategory"]?.jsonPrimitive?.contentOrNull
            val order = obj["order"]?.jsonPrimitive?.intOrNull ?: return@forEach

            if (catNameEn != null) {
                categoryOrders[catNameEn] = order
                categoryOrders[sanitizeFolder(catNameEn)] = order
            }
            if (catNameHe != null) {
                categoryOrders[catNameHe] = order
                categoryOrders[sanitizeFolder(catNameHe)] = order
            }

            val pathKey = catNameHe ?: catNameEn ?: return@forEach
            obj["contents"]?.jsonArray?.forEach { item ->
                processTocItem(item.jsonObject, listOf(pathKey))
            }
        }

        logger.i { "Parsed TOC orders: ${categoryOrders.size} categories, ${bookOrders.size} books" }
    } catch (e: Exception) {
        logger.e(e) { "Error parsing table_of_contents.json" }
    }

    return Pair(categoryOrders, bookOrders)
}

internal fun normalizePriorityEntry(raw: String): String {
    var entry = raw.trim().replace('\\', '/')
    if (entry.startsWith("/")) entry = entry.removePrefix("/")
    return entry.split('/').filter { it.isNotBlank() }.joinToString("/") { sanitizeFolder(it) }
}

internal fun normalizedBookPath(categories: List<String>, heTitle: String): String =
    (categories.map { sanitizeFolder(it) } + sanitizeFolder(heTitle)).joinToString("/")

internal fun buildBookPath(categories: List<String>, title: String): String =
    (categories + title).joinToString(separator = "/")

internal fun loadPriorityList(classLoader: ClassLoader?, logger: Logger): List<String> = try {
    val stream = classLoader?.getResourceAsStream("priority.txt") ?: return emptyList()
    stream.bufferedReader(Charsets.UTF_8).useLines { lines ->
        lines.map { it.trim() }
            .filter { it.isNotEmpty() && !it.startsWith("#") }
            .map { normalizePriorityEntry(it) }
            .filter { it.isNotEmpty() }
            .toList()
    }
} catch (e: Exception) {
    logger.w(e) { "Unable to read Sefaria priority list, continuing with default order" }
    emptyList()
}

internal fun applyPriorityOrdering(
    payloads: List<BookPayload>,
    priorityEntries: List<String>
): Pair<List<BookPayload>, List<String>> {
    if (priorityEntries.isEmpty()) return payloads to emptyList()

    val lookup = payloads.associateBy { normalizedBookPath(it.categoriesHe, it.heTitle) }
    val used = mutableSetOf<String>()
    val ordered = mutableListOf<BookPayload>()
    val missing = mutableListOf<String>()

    priorityEntries.forEach { entry ->
        val normalized = normalizePriorityEntry(entry)
        val payload = lookup[normalized]
        if (payload != null && used.add(normalized)) {
            ordered += payload
        } else if (payload == null) {
            missing += entry
        }
    }

    val remaining = payloads.filter { normalizedBookPath(it.categoriesHe, it.heTitle) !in used }
    return (ordered + remaining) to missing
}
