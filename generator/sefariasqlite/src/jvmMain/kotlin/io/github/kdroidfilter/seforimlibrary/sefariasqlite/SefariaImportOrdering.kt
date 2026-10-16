package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.text.normalizeCategoryPathSegment
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

internal data class CategoryDescriptions(
    val heShortDesc: String?,
    val heDesc: String?,
)

internal data class ParsedTableOfContents(
    val categoryOrders: Map<String, Int>,
    val bookOrders: Map<String, Int>,
    val categoryDescriptions: Map<String, CategoryDescriptions>,
)

/** Parses category descriptions and display ordering from `table_of_contents.json`. */
internal fun parseTableOfContentsMetadata(
    dbRoot: Path,
    json: Json,
    logger: Logger,
): ParsedTableOfContents {
    val tocFile = dbRoot.resolve("table_of_contents.json")
    require(Files.isRegularFile(tocFile)) { "Required table_of_contents.json not found at $tocFile" }

    val tocEntries = try {
        json.parseToJsonElement(Files.readString(tocFile)).jsonArray
    } catch (e: Exception) {
        throw IllegalArgumentException("Invalid table_of_contents.json at $tocFile", e)
    }
    val categoryOrders = mutableMapOf<String, Int>()
    val bookOrders = mutableMapOf<String, Int>()
    val categoryDescriptions = mutableMapOf<String, CategoryDescriptions>()

    fun putDescription(path: String, value: CategoryDescriptions) {
        val previous = categoryDescriptions[path]
        check(previous == null || previous == value) {
            "Conflicting category descriptions for '$path': existing=$previous, new=$value"
        }
        categoryDescriptions.putIfAbsent(path, value)
    }

    fun processTocItem(
        item: JsonObject,
        parentOrderingPath: List<String>,
        parentHebrewPath: List<String>?,
    ) {
        val title = item["title"]?.jsonPrimitive?.contentOrNull
        val heTitle = item["heTitle"]?.jsonPrimitive?.contentOrNull
        val category = item["category"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
        val heCategory = item["heCategory"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }
        val heShortDesc = item["heShortDesc"]?.jsonPrimitive?.contentOrNull?.trim()?.takeIf { it.isNotEmpty() }
        val heDesc = item["heDesc"]?.jsonPrimitive?.contentOrNull?.trim()?.takeIf { it.isNotEmpty() }
        val order = item["order"]?.jsonPrimitive?.intOrNull
            ?: item["base_text_order"]?.jsonPrimitive?.intOrNull
            ?: item["base_text_order"]?.jsonPrimitive?.doubleOrNull?.toInt()

        if (title != null && order != null) bookOrders[title] = order
        if (heTitle != null && order != null) {
            bookOrders[heTitle] = order
            bookOrders[sanitizeFolder(heTitle)] = order
        }

        if (order != null) {
            listOfNotNull(category, heCategory).forEach { segment ->
                val fullPath = flattenTalmudCategories(
                    parentOrderingPath.map(::normalizeCategoryPathSegment) +
                        normalizeCategoryPathSegment(segment),
                ).joinToString("/")
                categoryOrders[fullPath] = order
                categoryOrders[sanitizeFolder(fullPath)] = order
            }
        }

        val isCategoryNode = category != null || heCategory != null
        if (isCategoryNode && (heShortDesc != null || heDesc != null)) {
            require(heCategory != null) {
                "Hebrew category description without heCategory in node category='$category', title='$title'"
            }
            require(parentHebrewPath != null) {
                "Hebrew category description under an ancestor without heCategory: '$heCategory'"
            }
            val rawPath = parentHebrewPath.map(::normalizeCategoryPathSegment) +
                normalizeCategoryPathSegment(heCategory)
            val canonicalPath = flattenTalmudCategories(rawPath).joinToString("/")
            putDescription(canonicalPath, CategoryDescriptions(heShortDesc, heDesc))
        }

        val childOrderingPath = when {
            heCategory != null -> parentOrderingPath + heCategory
            category != null -> parentOrderingPath + category
            else -> parentOrderingPath
        }
        val childHebrewPath = when {
            heCategory != null && parentHebrewPath != null -> parentHebrewPath + heCategory
            category != null -> null
            else -> parentHebrewPath
        }
        item["contents"]?.jsonArray?.forEach { child ->
            processTocItem(child.jsonObject, childOrderingPath, childHebrewPath)
        }
    }

    tocEntries.forEach { entry ->
        processTocItem(entry.jsonObject, emptyList(), emptyList())
    }
    logger.i {
        "Parsed TOC metadata: ${categoryOrders.size} category orders, " +
            "${bookOrders.size} book orders, ${categoryDescriptions.size} descriptions"
    }
    categoryOrders.putAll(CATEGORY_ORDER_OVERRIDES)
    return ParsedTableOfContents(
        categoryOrders = categoryOrders.toMap(),
        bookOrders = bookOrders.toMap(),
        categoryDescriptions = categoryDescriptions.toMap(),
    )
}

// Sefaria lists Bavli reference works before the Rishonim; we place them after all commentators.
internal val CATEGORY_ORDER_OVERRIDES = mapOf("תלמוד בבלי/ספרות עזר" to 65)

internal fun normalizePriorityEntry(raw: String): String {
    var entry = raw.trim().replace('\\', '/')
    if (entry.startsWith("/")) entry = entry.removePrefix("/")
    val parts = entry.split('/').filter { it.isNotBlank() }.map { sanitizeFolder(it) }
    return flattenTalmudCategories(parts).joinToString("/")
}

internal fun flattenTalmudCategories(parts: List<String>): List<String> {
    if (parts.isEmpty()) return parts
    val flattened = ArrayList<String>(parts.size)
    var idx = 0
    while (idx < parts.size) {
        val part = parts[idx]
        if (part == "תלמוד" && idx + 1 < parts.size) {
            val next = parts[idx + 1]
            when (next) {
                "בבלי" -> {
                    flattened += "תלמוד בבלי"
                    idx += 2
                    continue
                }
                "ירושלמי" -> {
                    flattened += "תלמוד ירושלמי"
                    idx += 2
                    continue
                }
            }
        }
        flattened += part
        idx += 1
    }
    return flattened
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
