package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.name
import kotlin.io.path.readText

internal class SefariaBookPayloadReader(
    private val json: Json,
    private val logger: Logger
) {
    fun buildSchemaLookup(schemaDir: Path): Map<String, Path> {
        val lookup = ConcurrentHashMap<String, Path>()
        Files.newDirectoryStream(schemaDir) { it.fileName.toString().endsWith(".json") }.use { ds ->
            for (schemaPath in ds) {
                runCatching {
                    val obj = json.parseToJsonElement(schemaPath.readText()).jsonObject
                    val schemaObj = obj["schema"]?.jsonObject ?: return@runCatching
                    listOf(schemaObj["title"]?.stringOrNull(), schemaObj["heTitle"]?.stringOrNull()).forEach { key ->
                        normalizeTitleKey(key)?.let { normalized ->
                            lookup.putIfAbsent(normalized, schemaPath)
                        }
                    }
                }
            }
        }
        return lookup
    }

    /**
     * Read and parse book files in parallel using coroutines.
     */
    suspend fun readBooksInParallel(
        jsonDir: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>
    ): List<BookPayload> = coroutineScope {
        val mergedFiles = Files.walk(jsonDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.name.equals("merged.json", ignoreCase = true) }
                .toList()
        }

        logger.i { "Found ${mergedFiles.size} merged.json files to process" }

        // Process files in parallel with limited concurrency
        val semaphore = Semaphore(SefariaImportTuning.FILE_PARALLELISM)

        mergedFiles.map { textPath ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    parseBookFile(textPath, schemaDir, schemaLookup)
                }
            }
        }.awaitAll().filterNotNull()
    }

    private fun parseBookFile(
        textPath: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>
    ): BookPayload? {
        return runCatching {
            val textJson = json.parseToJsonElement(textPath.readText()).jsonObject
            val fileTitle = textJson["title"]?.stringOrNull()
            val fileHeTitle = textJson["heTitle"]?.stringOrNull()
            val folderName = textPath.parent?.fileName?.name

            val schemaPath = resolveSchemaPath(
                title = fileTitle,
                heTitle = fileHeTitle,
                folderName = folderName,
                schemaDir = schemaDir,
                lookup = schemaLookup
            ) ?: return@runCatching null

            val schemaJson = json.parseToJsonElement(schemaPath.readText()).jsonObject
            val schemaObj = schemaJson["schema"]?.jsonObject ?: return@runCatching null
            val englishTitle = schemaObj["title"]?.stringOrNull() ?: fileTitle ?: folderName ?: return@runCatching null
            val hebrewTitle = schemaObj["heTitle"]?.stringOrNull() ?: fileHeTitle ?: englishTitle

            val textElement = textJson["text"] ?: return@runCatching null
            val categories = schemaJson["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: schemaObj["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: textJson["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: emptyList()

            val authors = schemaJson["authors"]?.jsonArray?.mapNotNull { author ->
                author.jsonObject["he"]?.stringOrNull()
            } ?: emptyList()

            val (lines, refs, headings) = buildBookContent(
                schemaObj = schemaObj,
                textElement = textElement,
                bookHeTitle = hebrewTitle,
                bookEnTitle = englishTitle,
                authors = authors
            )
            val description = extractDescription(schemaJson, schemaObj)
            val pubDates = extractPubDates(schemaJson, schemaObj)
            val altStructures = parseAltStructures(schemaJson)

            BookPayload(
                heTitle = hebrewTitle,
                enTitle = englishTitle,
                categoriesHe = categories.map { sanitizeFolder(it) },
                lines = lines,
                refEntries = refs,
                headings = headings,
                authors = authors,
                description = description,
                pubDates = pubDates,
                altStructures = altStructures
            )
        }.onFailure { e ->
            logger.w(e) { "Failed to prepare book from $textPath" }
        }.getOrNull()
    }

    private fun resolveSchemaPath(
        title: String?,
        heTitle: String?,
        folderName: String?,
        schemaDir: Path,
        lookup: Map<String, Path>
    ): Path? {
        val candidates = listOfNotNull(title, heTitle, folderName?.replace('_', ' '), folderName)
        for (candidate in candidates) {
            val normalized = normalizeTitleKey(candidate)
            if (normalized != null) {
                val fromLookup = lookup[normalized]
                if (fromLookup != null) return fromLookup
            }
            val path = schemaDir.resolve("${candidate.replace(" ", "_")}.json")
            if (path.exists()) return path
        }
        return null
    }

    private fun extractDescription(schemaJson: JsonObject, schemaObj: JsonObject): String? {
        return schemaJson["heDesc"]?.stringOrNull()
            ?: schemaObj["heDesc"]?.stringOrNull()
            ?: schemaJson["description"]?.stringOrNull()
            ?: schemaObj["description"]?.stringOrNull()
            ?: schemaJson["heDescription"]?.stringOrNull()
            ?: schemaObj["heDescription"]?.stringOrNull()
    }

    private fun extractPubDates(schemaJson: JsonObject, schemaObj: JsonObject): List<PubDate> {
        val dates = mutableListOf<String>()
        fun collect(key: String, obj: JsonObject) {
            obj[key]?.let { el ->
                when (el) {
                    is JsonArray -> dates += el.mapNotNull { it.jsonPrimitive.contentOrNull }
                    is JsonPrimitive -> el.contentOrNull?.let { dates += it }
                    else -> {}
                }
            }
        }
        collect("pubDate", schemaJson)
        collect("pubDate", schemaObj)
        return dates.distinct().map { PubDate(date = it) }
    }

    private fun parseAltStructures(schemaJson: JsonObject): List<AltStructurePayload> {
        val altsObj = schemaJson["alts"]?.jsonObject
            ?: schemaJson["alt_structs"]?.jsonObject
            ?: return emptyList()
        return altsObj.mapNotNull { (key, value) ->
            val altObj = value.jsonObject
            val nodesArray = altObj["nodes"]?.jsonArray ?: return@mapNotNull null
            val nodes = nodesArray.mapNotNull { parseAltNode(it.jsonObject) }
            AltStructurePayload(
                key = key,
                title = altObj["title"]?.stringOrNull(),
                heTitle = altObj["heTitle"]?.stringOrNull(),
                nodes = nodes
            )
        }
    }

    private fun parseAltNode(obj: JsonObject): AltNodePayload {
        val title = obj["title"]?.stringOrNull()
        val heTitle = obj["heTitle"]?.stringOrNull()
        val wholeRef = obj["wholeRef"]?.stringOrNull()
        val refs = obj["refs"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
        val addressTypes = obj["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
        val addresses = obj["addresses"]?.jsonArray?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList()
        val skippedAddresses = obj["skipped_addresses"]?.jsonArray?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList()
        val startingAddress = obj["startingAddress"]?.stringOrNull()
        val offset = obj["offset"]?.jsonPrimitive?.intOrNullSafe()
        val childLabel = obj["heSectionNames"]?.jsonArray?.firstOrNull()?.jsonPrimitive?.contentOrNull
            ?: obj["sectionNames"]?.jsonArray?.firstOrNull()?.jsonPrimitive?.contentOrNull
        val children = obj["nodes"]?.jsonArray?.mapNotNull { parseAltNode(it.jsonObject) } ?: emptyList()
        return AltNodePayload(
            title = title,
            heTitle = heTitle,
            wholeRef = wholeRef,
            refs = refs,
            addressTypes = addressTypes,
            childLabel = childLabel,
            addresses = addresses,
            skippedAddresses = skippedAddresses,
            startingAddress = startingAddress,
            offset = offset,
            children = children
        )
    }

    private fun buildBookContent(
        schemaObj: JsonObject,
        textElement: JsonElement,
        bookHeTitle: String,
        bookEnTitle: String,
        authors: List<String>
    ): Triple<List<String>, List<RefEntry>, List<Heading>> {
        // Pre-allocate with estimated capacity
        val output = ArrayList<String>(1000)
        val refs = ArrayList<RefEntry>(1000)
        val headings = ArrayList<Heading>(100)

        fun headingTagForLevel(level: Int): Pair<String, String> = when (level) {
            0 -> "<h1>" to "</h1>"
            1 -> "<h2>" to "</h2>"
            2 -> "<h3>" to "</h3>"
            3 -> "<h4>" to "</h4>"
            else -> "<h5>" to "</h5>"
        }

        val tag = headingTagForLevel(0)
        output += "${tag.first}$bookHeTitle${tag.second}"
        authors.forEach { output += it }
        headings += Heading(title = bookHeTitle, level = 0, lineIndex = 0)

        fun processNode(
            node: JsonObject,
            text: JsonElement?,
            level: Int,
            refPrefix: String,
            heRefPrefix: String
        ) {
            val nodeTitle = node["heTitle"]?.stringOrNull()?.takeIf { it.isNotBlank() }
                ?: node["title"]?.stringOrNull()?.takeIf { it.isNotBlank() }
            val hasTitle = nodeTitle != null
            if (hasTitle && level > 0) {
                val tag = headingTagForLevel(level)
                output += "${tag.first}$nodeTitle${tag.second}"
                headings += Heading(
                    title = nodeTitle,
                    level = level,
                    lineIndex = output.size - 1
                )
            }

            if (text !is JsonArray && text !is JsonPrimitive && text !is JsonObject) return

            if (node.containsKey("nodes")) {
                val children = node["nodes"]?.jsonArray ?: JsonArray(emptyList())
                for (child in children) {
                    val childNode = child.jsonObject
                    val childText = selectNodeText(childNode, text)
                    val childTitle = childNode["title"]?.stringOrNull().orEmpty()
                    val childHeTitle = childNode["heTitle"]?.stringOrNull().orEmpty()
                    val key = childNode["key"]?.stringOrNull()
                    val nextRefPrefix = buildString {
                        append(refPrefix)
                        if (!key.equals("default", ignoreCase = true) && childTitle.isNotBlank()) append(childTitle).append(", ")
                    }
                    val nextHeRefPrefix = buildString {
                        append(heRefPrefix)
                        if (!key.equals("default", ignoreCase = true) && childHeTitle.isNotBlank()) append(childHeTitle).append(", ")
                    }
                    processNode(childNode, childText, level + 1, nextRefPrefix, nextHeRefPrefix)
                }
            } else {
                val sectionNames = node["heSectionNames"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
                val depth = node["depth"]?.jsonPrimitive?.intOrNullSafe() ?: sectionNames.size
                val addressTypes = node["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
                val referenceableSections = node["referenceableSections"]?.jsonArray?.mapNotNull { it.jsonPrimitive.booleanOrNull } ?: emptyList()
                val nextLevel = if (hasTitle) level + 1 else level
                recursiveSections(
                    sectionNames = sectionNames,
                    text = text,
                    depth = depth,
                    level = nextLevel,
                    output = output,
                    refEntries = refs,
                    refPrefix = "$refPrefix ",
                    heRefPrefix = "$heRefPrefix ",
                    bookEnTitle = bookEnTitle,
                    bookHeTitle = bookHeTitle,
                    headings = headings,
                    addressTypes = addressTypes,
                    referenceableSections = referenceableSections
                )
            }
        }

        if (schemaObj.containsKey("nodes")) {
            val nodes = schemaObj["nodes"]?.jsonArray ?: JsonArray(emptyList())
            for (nodeElement in nodes) {
                val node = nodeElement.jsonObject
                val nodeText = selectNodeText(node, textElement)
                val nodeTitle = node["title"]?.stringOrNull().orEmpty()
                val nodeHeTitle = node["heTitle"]?.stringOrNull().orEmpty()
                val key = node["key"]?.stringOrNull()
                val refPrefix = buildString {
                    append(bookEnTitle)
                    append(", ")
                    if (!key.equals("default", ignoreCase = true) && nodeTitle.isNotBlank()) append(nodeTitle).append(", ")
                }
                val heRefPrefix = buildString {
                    append(bookHeTitle)
                    append(", ")
                    if (!key.equals("default", ignoreCase = true) && nodeHeTitle.isNotBlank()) append(nodeHeTitle).append(", ")
                }
                processNode(node, nodeText, 1, refPrefix, heRefPrefix)
            }
        } else {
            val sectionNames = schemaObj["heSectionNames"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
            val depth = schemaObj["depth"]?.jsonPrimitive?.intOrNullSafe() ?: sectionNames.size
            val addressTypes = schemaObj["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
            val referenceableSections = schemaObj["referenceableSections"]?.jsonArray?.mapNotNull { it.jsonPrimitive.booleanOrNull } ?: emptyList()
            recursiveSections(
                sectionNames = sectionNames,
                text = textElement,
                depth = depth,
                level = 1,
                output = output,
                refEntries = refs,
                refPrefix = "$bookEnTitle ",
                heRefPrefix = "$bookHeTitle ",
                bookEnTitle = bookEnTitle,
                bookHeTitle = bookHeTitle,
                headings = headings,
                addressTypes = addressTypes,
                referenceableSections = referenceableSections
            )
        }

        return Triple(output, refs, headings)
    }

    private fun recursiveSections(
        sectionNames: List<String>,
        text: JsonElement?,
        depth: Int,
        level: Int,
        output: MutableList<String>,
        refEntries: MutableList<RefEntry>,
        refPrefix: String,
        heRefPrefix: String,
        bookEnTitle: String,
        bookHeTitle: String,
        headings: MutableList<Heading>,
        linePrefix: String = "",
        addressTypes: List<String> = emptyList(),
        referenceableSections: List<Boolean> = emptyList()
    ) {
        if (depth == 0) {
            val primitive = text as? JsonPrimitive
            val content = primitive?.takeIf { it.isString }?.content
            if (!content.isNullOrEmpty()) {
                output += linePrefix + content.replace("\n", "")
                val cleanRef = trimTrailingSeparators(refPrefix)
                val cleanHeRef = trimTrailingSeparators(heRefPrefix)
                refEntries.add(
                    RefEntry(
                        ref = cleanRef,
                        heRef = cleanHeRef,
                        path = "",
                        lineIndex = output.size
                    )
                )
            }
            return
        }
        if (text !is JsonArray) return
        val index = (sectionNames.size - depth).coerceAtLeast(0)
        val sectionName = sectionNames.getOrNull(index) ?: ""

        val nonEmptyCount = if (depth == 1) {
            text.count { !it.isTriviallyEmpty() }
        } else {
            0
        }

        text.forEachIndexed { idx, item ->
            if (item.isTriviallyEmpty()) return@forEachIndexed

            val currentAddressType = addressTypes.getOrNull(addressTypes.size - depth)
            val letter = when (currentAddressType) {
                "Talmud" -> toDaf(idx + 1)
                "Integer" -> toGematria(idx + 1)
                else -> toGematria(idx + 1)
            }

            val sectionIndex = sectionNames.size - depth
            val isReferenceable = referenceableSections.getOrNull(sectionIndex) ?: true
            val nextLinePrefix = if (depth == 1 && isReferenceable && currentAddressType != "Integer" && nonEmptyCount > 1) {
                "($letter) "
            } else {
                ""
            }

            if (depth > 1 && sectionName.isNotBlank() && isReferenceable) {
                val tag = when (level) {
                    1 -> "<h2>" to "</h2>"
                    2 -> "<h3>" to "</h3>"
                    3 -> "<h4>" to "</h4>"
                    else -> "<h5>" to "</h5>"
                }
                output += "${tag.first}$sectionName $letter${tag.second}"
                headings += Heading(
                    title = "$sectionName $letter",
                    level = level,
                    lineIndex = output.size - 1
                )
            }

            val newRefPrefix = buildString {
                append(refPrefix)
                val refNumber = when (currentAddressType) {
                    "Talmud" -> toEnglishDaf(idx + 1)
                    else -> (idx + 1).toString()
                }
                append(refNumber)
                append(":")
            }
            val newHeRefPrefix = buildString {
                append(heRefPrefix)
                append(letter)
                append(", ")
            }

            recursiveSections(
                sectionNames = sectionNames,
                text = item,
                depth = depth - 1,
                level = level + 1,
                output = output,
                refEntries = refEntries,
                refPrefix = newRefPrefix,
                heRefPrefix = newHeRefPrefix,
                bookEnTitle = bookEnTitle,
                bookHeTitle = bookHeTitle,
                headings = headings,
                linePrefix = nextLinePrefix,
                addressTypes = addressTypes,
                referenceableSections = referenceableSections
            )
        }
    }

    private fun selectNodeText(node: JsonObject, text: JsonElement?): JsonElement? {
        val key = node["key"]?.stringOrNull()
        val title = node["title"]?.stringOrNull().orEmpty()
        val obj = text as? JsonObject ?: return null
        return if (!key.equals("default", ignoreCase = true) && title.isNotBlank()) {
            obj[title]
        } else {
            obj[""] ?: obj[title]
        }
    }
}
