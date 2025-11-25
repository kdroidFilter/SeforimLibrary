package io.github.kdroidfilter.seforimlibrary.sefaria

import co.touchlab.kermit.Logger
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.ArrayDeque
import kotlin.io.path.exists
import kotlin.io.path.invariantSeparatorsPathString
import kotlin.io.path.isDirectory
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.io.path.writeLines
import kotlin.io.path.writeText

/**
 * Converts the raw Sefaria export into an Otzaria-like folder with Hebrew .txt books.
 * The resulting structure mirrors the output produced by the legacy Python scripts.
 */
class SefariaToOtzariaConverter(
    private val exportRoot: Path,
    private val outputRoot: Path,
    private val logger: Logger = Logger.withTag("SefariaConverter")
) {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
    private val headingTags = listOf(
        "<h1>" to "</h1>",
        "<h2>" to "</h2>",
        "<h3>" to "</h3>",
        "<h4>" to "</h4>",
        "<h5>" to "</h5>"
    )
    private val inlineSkipSections = setOf("שורה", "פירוש", "פסקה")

    fun convert(generateRefs: Boolean = true, copyMetadata: Boolean = true): ConversionResult {
        val dbRoot = findDatabaseExportRoot(exportRoot)
        val jsonDir = dbRoot.resolve("json")
        val schemaDir = dbRoot.resolve("schemas")
        require(jsonDir.isDirectory() && schemaDir.isDirectory()) {
            "Expected json and schemas directories under $dbRoot"
        }

        val blacklist = loadBlacklist()
        val schemaLookup = buildSchemaLookup(schemaDir)
        val libraryRoot = outputRoot.resolve("אוצריא")
        Files.createDirectories(libraryRoot)

        val refsRoot = if (generateRefs) outputRoot.resolve("refs").resolve("אוצריא") else null
        refsRoot?.let { Files.createDirectories(it) }

        val createdBooks = mutableListOf<Path>()
        val refEntries = if (generateRefs) mutableListOf<RefEntry>() else null

        Files.walk(jsonDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.name.equals("merged.json", ignoreCase = true) }
                .forEach { textPath ->
                    val textJson = runCatching { json.parseToJsonElement(textPath.readText()) }.getOrElse { e ->
                        logger.w(e) { "Failed to parse text at $textPath" }
                        return@forEach
                    }.jsonObject
                    val fileTitle = textJson["title"]?.stringOrNull()
                    val fileHeTitle = textJson["heTitle"]?.stringOrNull()
                    val folderName = textPath.parent?.fileName?.name

                    val schemaPath = resolveSchemaPath(
                        title = fileTitle,
                        heTitle = fileHeTitle,
                        folderName = folderName,
                        schemaDir = schemaDir,
                        lookup = schemaLookup
                    ) ?: run {
                        logger.w { "Schema missing for ${fileTitle ?: folderName} under ${schemaDir.toAbsolutePath()}" }
                        return@forEach
                    }

                    val schemaJson = runCatching { json.parseToJsonElement(schemaPath.readText()) }.getOrElse { e ->
                        logger.w(e) { "Failed to parse schema for ${fileTitle ?: folderName}" }
                        return@forEach
                    }.jsonObject
                    val schemaObj = schemaJson["schema"]?.jsonObject ?: run {
                        logger.w { "Schema object missing in $schemaPath" }
                        return@forEach
                    }
                    val englishTitle = schemaObj["title"]?.stringOrNull() ?: fileTitle ?: folderName ?: return@forEach
                    if (englishTitle in blacklist) {
                        logger.i { "Skipping $englishTitle (blacklisted)" }
                        return@forEach
                    }
                    val hebrewTitle = schemaObj["heTitle"]?.stringOrNull() ?: fileHeTitle ?: englishTitle
                    val textElement = textJson["text"] ?: run {
                        logger.w { "Text missing in $textPath" }
                        return@forEach
                    }
                    val authors = schemaJson["authors"]?.jsonArray?.mapNotNull { author ->
                        author.jsonObject["he"]?.stringOrNull()
                    } ?: emptyList()
                    val categories = schemaJson["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                        ?: schemaObj["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                        ?: textJson["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                        ?: emptyList()
                    val relativeDir = categories.joinToString("/") { sanitizeFolder(it) }.trim('/')
                    val libraryDir = if (relativeDir.isBlank()) libraryRoot else libraryRoot.resolve(relativeDir)
                    Files.createDirectories(libraryDir)
                    val fileName = "${sanitizeFileName(hebrewTitle)}.txt"
                    val outputFile = libraryDir.resolve(fileName)

                    val refFile = refsRoot?.let { refs ->
                        val dir = if (relativeDir.isBlank()) refs else refs.resolve(relativeDir)
                        Files.createDirectories(dir)
                        dir.resolve(fileName)
                    }
                    val refPathRelative = refFile?.let { refsRoot.parent.relativize(it).invariantSeparatorsPathString }

                    val bookOutput = buildBookOutput(
                        schemaObj = schemaObj,
                        textElement = textElement,
                        authors = authors,
                        bookHeTitle = hebrewTitle,
                        bookEnTitle = englishTitle,
                        mode = OutputMode.LIBRARY,
                        refEntries = null,
                        refPath = null
                    )
                    outputFile.writeLines(bookOutput)
                    createdBooks.add(outputFile)

                    if (generateRefs && refEntries != null && refFile != null && refPathRelative != null) {
                        val bookRefEntries = mutableListOf<RefEntry>()
                        val refOutput = buildBookOutput(
                            schemaObj = schemaObj,
                            textElement = textElement,
                            authors = authors,
                            bookHeTitle = hebrewTitle,
                            bookEnTitle = englishTitle,
                            mode = OutputMode.REFS,
                            refEntries = bookRefEntries,
                            refPath = refPathRelative
                        )
                        refFile.writeLines(refOutput)
                        refEntries.addAll(bookRefEntries)
                    }

                    logger.i { "Processed $hebrewTitle" }
                }
        }

        val manifestPath = writeManifest(createdBooks, outputRoot)
        val refsCsv = if (generateRefs && refEntries != null && refsRoot != null) {
            writeRefsCsv(refEntries, refsRoot.parent)
        } else null
        val linksRoot = if (generateRefs && refEntries != null && refsRoot != null) {
            writeLinksJson(
                linksDir = dbRoot.resolve("links"),
                refEntries = refEntries,
                outputRoot = outputRoot
            )
        } else null

        if (copyMetadata) copyMetadataIfAvailable(dbRoot, outputRoot)

        return ConversionResult(
            libraryRoot = libraryRoot,
            refsCsv = refsCsv,
            manifestPath = manifestPath,
            linksRoot = linksRoot
        )
    }

    private fun buildBookOutput(
        schemaObj: JsonObject,
        textElement: JsonElement,
        authors: List<String>,
        bookHeTitle: String,
        bookEnTitle: String,
        mode: OutputMode,
        refEntries: MutableList<RefEntry>?,
        refPath: String?
    ): List<String> {
        val output = mutableListOf<String>()
        val tag = headingTagForLevel(0)
        output += "${tag.first}$bookHeTitle${tag.second}\n"
        authors.forEach { output += "$it\n" }

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
                processNode(
                    node = node,
                    text = nodeText,
                    output = output,
                    level = 1,
                    mode = mode,
                    refEntries = refEntries,
                    refPath = refPath,
                    refPrefix = refPrefix,
                    heRefPrefix = heRefPrefix,
                    bookEnTitle = bookEnTitle,
                    bookHeTitle = bookHeTitle
                )
            }
        } else {
            val sectionNames = schemaObj["heSectionNames"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
            val depth = schemaObj["depth"]?.jsonPrimitive?.intOrNull() ?: sectionNames.size
            recursiveSections(
                sectionNames = sectionNames,
                text = textElement,
                depth = depth,
                level = 1,
                output = output,
                mode = mode,
                refEntries = refEntries,
                refPath = refPath,
                refPrefix = "$bookEnTitle ",
                heRefPrefix = "$bookHeTitle "
            )
        }
        return output
    }

    private fun processNode(
        node: JsonObject,
        text: JsonElement?,
        output: MutableList<String>,
        level: Int,
        mode: OutputMode,
        refEntries: MutableList<RefEntry>?,
        refPath: String?,
        refPrefix: String,
        heRefPrefix: String,
        bookEnTitle: String,
        bookHeTitle: String
    ) {
        val heTitle = node["heTitle"]?.stringOrNull().orEmpty()
        val tag = headingTagForLevel(level)
        output += "${tag.first}$heTitle${tag.second}\n"

        if (node.containsKey("nodes")) {
            val children = node["nodes"]?.jsonArray ?: JsonArray(emptyList())
            val textObject = text as? JsonObject
            for (childElement in children) {
                val child = childElement.jsonObject
                val key = child["key"]?.stringOrNull()
                val childTitle = child["title"]?.stringOrNull().orEmpty()
                val childHeTitle = child["heTitle"]?.stringOrNull().orEmpty()
                val childText = if (textObject != null) {
                    if (!key.equals("default", ignoreCase = true) && childTitle.isNotBlank()) {
                        textObject[childTitle]
                    } else {
                        textObject[""] ?: textObject[childTitle]
                    }
                } else null
                val newRefPrefix = buildString {
                    append(refPrefix)
                    if (!key.equals("default", ignoreCase = true) && childTitle.isNotBlank()) append(childTitle).append(", ")
                }
                val newHeRefPrefix = buildString {
                    append(heRefPrefix)
                    if (!key.equals("default", ignoreCase = true) && childHeTitle.isNotBlank()) append(childHeTitle).append(", ")
                }
                processNode(
                    node = child,
                    text = childText,
                    output = output,
                    level = level + 1,
                    mode = mode,
                    refEntries = refEntries,
                    refPath = refPath,
                    refPrefix = newRefPrefix,
                    heRefPrefix = newHeRefPrefix,
                    bookEnTitle = bookEnTitle,
                    bookHeTitle = bookHeTitle
                )
            }
        } else {
            val sectionNames = node["heSectionNames"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
            val depth = node["depth"]?.jsonPrimitive?.intOrNull() ?: sectionNames.size
            recursiveSections(
                sectionNames = sectionNames,
                text = text,
                depth = depth,
                level = level + 1,
                output = output,
                mode = mode,
                refEntries = refEntries,
                refPath = refPath,
                refPrefix = "$refPrefix ",
                heRefPrefix = "$heRefPrefix "
            )
        }
    }

    private fun recursiveSections(
        sectionNames: List<String>,
        text: JsonElement?,
        depth: Int,
        level: Int,
        output: MutableList<String>,
        mode: OutputMode,
        refEntries: MutableList<RefEntry>?,
        refPath: String?,
        refPrefix: String,
        heRefPrefix: String
    ) {
        if (depth == 0) {
            val primitive = text as? JsonPrimitive
            val content = primitive?.takeIf { it.isString }?.content
            if (!content.isNullOrEmpty()) {
                when (mode) {
                    OutputMode.LIBRARY -> output += content.replace("\n", "") + "\n"
                    OutputMode.REFS -> {
                        val cleanRef = trimTrailingSeparators(refPrefix)
                        val cleanHeRef = trimTrailingSeparators(heRefPrefix)
                        output += "ref: $cleanRef| heRef: $cleanHeRef| text: ${content.replace("\n", "")}\n"
                        refEntries?.add(
                            RefEntry(
                                ref = cleanRef,
                                heRef = cleanHeRef,
                                path = refPath ?: "",
                                lineIndex = output.size
                            )
                        )
                    }
                }
            }
            return
        }
        if (text !is JsonArray) return
        val index = (sectionNames.size - depth).coerceAtLeast(0)
        val sectionName = sectionNames.getOrNull(index) ?: ""

        text.forEachIndexed { idx, item ->
            if (item.isTriviallyEmpty()) return@forEachIndexed
            val letter = if (sectionName == "דף") toDaf(idx + 1) else toGematria(idx + 1)
            if (depth > 1) {
                val tag = headingTagForLevel(level)
                output += "${tag.first}$sectionName $letter${tag.second}\n"
            } else if (mode == OutputMode.LIBRARY && sectionName !in inlineSkipSections) {
                output += "($letter) "
            }

            val newRefPrefix = buildString {
                append(refPrefix)
                append(if (sectionName == "דף") toEnglishDaf(idx + 1) else (idx + 1).toString())
                append(":")
            }
            val newHeRefPrefix = buildString {
                append(heRefPrefix)
                append(if (sectionName == "דף") toDaf(idx + 1) else toGematria(idx + 1))
                append(", ")
            }

            recursiveSections(
                sectionNames = sectionNames,
                text = item,
                depth = depth - 1,
                level = level + 1,
                output = output,
                mode = mode,
                refEntries = refEntries,
                refPath = refPath,
                refPrefix = newRefPrefix,
                heRefPrefix = newHeRefPrefix
            )
        }
    }

    private fun writeManifest(createdBooks: List<Path>, root: Path): Path {
        val manifestPath = root.resolve("files_manifest.json")
        if (createdBooks.isEmpty()) {
            manifestPath.writeText("{}")
            return manifestPath
        }
        val manifestEntries = createdBooks.associate { path ->
            val rel = root.relativize(path).invariantSeparatorsPathString
            "sefaria/$rel" to ManifestEntry(hash = null)
        }
        manifestPath.writeText(json.encodeToString(manifestEntries))
        return manifestPath
    }

    private fun writeRefsCsv(entries: List<RefEntry>, refsRoot: Path): Path {
        val csvPath = refsRoot.resolve("refs.csv")
        Files.createDirectories(csvPath.parent)
        val builder = StringBuilder()
        builder.append("ref,heRef,path,line_index\n")
        entries.forEach { entry ->
            builder.append(csvEscape(entry.ref)).append(',')
            builder.append(csvEscape(entry.heRef)).append(',')
            builder.append(csvEscape(entry.path)).append(',')
            builder.append(entry.lineIndex).append('\n')
        }
        csvPath.writeText(builder.toString())
        return csvPath
    }

    private fun writeLinksJson(
        linksDir: Path,
        refEntries: List<RefEntry>,
        outputRoot: Path
    ): Path? {
        if (!linksDir.isDirectory()) {
            logger.w { "links directory not found at $linksDir; skipping links generation" }
            return null
        }

        val refsByCitation = refEntries.groupBy { it.ref }
        val linksByBook = mutableMapOf<String, MutableList<LinkOutputEntry>>()

        Files.list(linksDir)
            .filter { it.fileName.toString().endsWith(".csv") }
            .sorted()
            .use { stream ->
                stream.forEach { file ->
                    Files.newBufferedReader(file).use { reader ->
                        val lines = reader.lineSequence().iterator()
                        if (!lines.hasNext()) return@use
                        val headers = parseCsvLine(lines.next()).map { it.trim() }
                        val idxCitation1 = headers.indexOf("Citation 1")
                        val idxCitation2 = headers.indexOf("Citation 2")
                        val idxConnection = headers.indexOf("Conection Type")
                        if (idxCitation1 < 0 || idxCitation2 < 0 || idxConnection < 0) return@use

                        while (lines.hasNext()) {
                            val row = parseCsvLine(lines.next())
                            if (row.isEmpty()) continue
                            val citation1 = row.getOrNull(idxCitation1)?.trim().orEmpty()
                            val citation2 = row.getOrNull(idxCitation2)?.trim().orEmpty()
                            if (citation1.isEmpty() || citation2.isEmpty()) continue
                            val connectionType = row.getOrNull(idxConnection)?.trim().orEmpty()

                            val fromRefs = refsByCitation[citation1].orEmpty()
                            val toRefs = refsByCitation[citation2].orEmpty()
                            if (fromRefs.isEmpty() || toRefs.isEmpty()) continue

                            for (from in fromRefs) {
                                for (to in toRefs) {
                                    addLinkEntry(linksByBook, from, to, connectionType)
                                    addLinkEntry(linksByBook, to, from, connectionType)
                                }
                            }
                        }
                    }
                }
            }

        if (linksByBook.isEmpty()) return null

        val linksRoot = outputRoot.resolve("links")
        Files.createDirectories(linksRoot)
        for ((bookKey, entries) in linksByBook) {
            val fileName = "${sanitizeFileName(bookKey)}_links.json"
            val outFile = linksRoot.resolve(fileName)
            outFile.writeText(json.encodeToString(entries))
        }
        return linksRoot
    }

    private fun addLinkEntry(
        linksByBook: MutableMap<String, MutableList<LinkOutputEntry>>,
        from: RefEntry,
        to: RefEntry,
        connectionType: String
    ) {
        val bookKey = Paths.get(from.path).fileName.toString().substringBeforeLast(".")
        val list = linksByBook.getOrPut(bookKey) { mutableListOf() }
        list += LinkOutputEntry(
            lineIndex1 = from.lineIndex,
            heRef2 = to.heRef,
            path2 = to.path,
            lineIndex2 = to.lineIndex,
            connectionType = connectionType
        )
    }

    private fun parseCsvLine(line: String): List<String> {
        val result = mutableListOf<String>()
        val sb = StringBuilder()
        var inQuotes = false
        var i = 0
        val len = line.length
        while (i < len) {
            val c = line[i]
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < len && line[i + 1] == '"') {
                        sb.append('"')
                        i++
                    } else {
                        inQuotes = false
                    }
                } else {
                    sb.append(c)
                }
            } else {
                when (c) {
                    '"' -> inQuotes = true
                    ',' -> {
                        result += sb.toString()
                        sb.setLength(0)
                    }
                    else -> sb.append(c)
                }
            }
            i++
        }
        result += sb.toString()
        return result
    }

    private fun copyMetadataIfAvailable(dbRoot: Path, targetRoot: Path) {
        val candidates = listOfNotNull(
            dbRoot.parent?.resolve("metadata.json"),
            dbRoot.resolve("metadata.json"),
            Paths.get("sefaria/src/commonMain/python/metadata.json")
        ).filter { it.exists() }

        val target = targetRoot.resolve("metadata.json")
        if (target.exists()) {
            logger.i { "metadata.json already present at ${target.toAbsolutePath()}" }
            return
        }

        val resourceStream = this::class.java.getResourceAsStream("/metadata.json")
        when {
            candidates.isNotEmpty() -> {
                val source = candidates.first()
                Files.createDirectories(target.parent)
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
                logger.i { "Copied metadata.json from $source" }
            }

            resourceStream != null -> {
                Files.createDirectories(target.parent)
                resourceStream.use { Files.copy(it, target, StandardCopyOption.REPLACE_EXISTING) }
                logger.i { "Copied metadata.json from bundled resources" }
            }

            else -> logger.w { "metadata.json not found; skipping copy" }
        }
    }

    private fun loadBlacklist(): Set<String> {
        val resourceNames = listOf("blacklist.txt", "/blacklist.txt")
        val stream = resourceNames.asSequence()
            .mapNotNull { name -> this::class.java.getResourceAsStream(name) }
            .firstOrNull()
        val fromResource = stream?.bufferedReader()?.use { br ->
            br.lineSequence().map { it.trim() }.filter { it.isNotEmpty() }.toSet()
        } ?: emptySet()
        if (fromResource.isNotEmpty()) return fromResource

        val fallback = Paths.get("sefaria/src/commonMain/python/blackList.txt")
        return runCatching {
            fallback.readText().lineSequence().map { it.trim() }.filter { it.isNotEmpty() }.toSet()
        }.getOrElse { emptySet() }
    }

    private fun buildSchemaLookup(schemaDir: Path): Map<String, Path> {
        val lookup = mutableMapOf<String, Path>()
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
                }.onFailure { e -> logger.w(e) { "Failed to index schema $schemaPath" } }
            }
        }
        return lookup
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

    private fun normalizeTitleKey(value: String?): String? {
        if (value.isNullOrBlank()) return null
        return value.lowercase().replace("\\s+".toRegex(), " ").replace('_', ' ').trim()
    }

    private fun findDatabaseExportRoot(base: Path): Path {
        if (isDatabaseExportCandidate(base)) return base
        val direct = base.resolve("database_export")
        if (isDatabaseExportCandidate(direct)) return direct

        val queue = ArrayDeque<Path>()
        Files.newDirectoryStream(base).use { ds ->
            for (entry in ds) {
                if (entry.isDirectory()) queue.add(entry)
            }
        }

        while (queue.isNotEmpty()) {
            val current = queue.removeFirst()
            if (isDatabaseExportCandidate(current)) return current
            val candidate = current.resolve("database_export")
            if (isDatabaseExportCandidate(candidate)) return candidate
            Files.newDirectoryStream(current).use { ds ->
                for (child in ds) {
                    if (child.isDirectory()) queue.add(child)
                }
            }
        }

        throw IllegalStateException("database_export folder not found under $base")
    }

    private fun isDatabaseExportCandidate(path: Path): Boolean {
        if (!path.isDirectory()) return false
        val jsonDir = path.resolve("json")
        val schemaDir = path.resolve("schemas")
        return jsonDir.isDirectory() && schemaDir.isDirectory()
    }

    private fun sanitizeFolder(name: String?): String {
        if (name.isNullOrBlank()) return ""
        return name.replace("\"", "").trim()
    }

    private fun sanitizeFileName(name: String): String {
        val invalid = Regex("[\\\\/:*?\"<>|]")
        return name.replace("\"", "")
            .replace("'", "")
            .replace("״", "")
            .replace(invalid, " ")
            .trim()
            .ifEmpty { "book" }
    }

    private fun headingTagForLevel(level: Int): Pair<String, String> =
        headingTags.getOrElse(level) { headingTags.last() }

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

    private fun trimTrailingSeparators(value: String): String =
        value.trimEnd(':', ' ', ',')

    private fun csvEscape(value: String): String {
        if (value.contains('"') || value.contains(',') || value.contains('\n')) {
            val escaped = value.replace("\"", "\"\"")
            return "\"$escaped\""
        }
        return value
    }

    private fun toGematria(num: Int): String {
        if (num <= 0) return num.toString()
        val thousands = num / 1000
        var remainder = num % 1000
        val builder = StringBuilder()
        if (thousands > 0) {
            builder.append(toGematria(thousands)).append(' ')
        }

        val hundredsMap = listOf(
            400 to "ת",
            300 to "ש",
            200 to "ר",
            100 to "ק"
        )
        for ((value, letter) in hundredsMap) {
            while (remainder >= value) {
                builder.append(letter)
                remainder -= value
            }
        }

        if (remainder == 15) {
            builder.append("טו")
            remainder = 0
        } else if (remainder == 16) {
            builder.append("טז")
            remainder = 0
        }

        val tensMap = listOf(
            90 to "צ",
            80 to "פ",
            70 to "ע",
            60 to "ס",
            50 to "נ",
            40 to "מ",
            30 to "ל",
            20 to "כ",
            10 to "י"
        )
        for ((value, letter) in tensMap) {
            if (remainder >= value) {
                builder.append(letter)
                remainder -= value
            }
        }

        val unitsMap = listOf(
            9 to "ט",
            8 to "ח",
            7 to "ז",
            6 to "ו",
            5 to "ה",
            4 to "ד",
            3 to "ג",
            2 to "ב",
            1 to "א"
        )
        for ((value, letter) in unitsMap) {
            if (remainder >= value) {
                builder.append(letter)
                remainder -= value
            }
        }

        return builder.toString()
    }

    private fun toDaf(index: Int): String {
        val i = index + 1
        return if (i % 2 == 0) "${toGematria(i / 2)}." else "${toGematria(i / 2)}:"
    }

    private fun toEnglishDaf(index: Int): String {
        val i = index + 1
        return if (i % 2 == 0) "${i / 2}a" else "${i / 2}b"
    }

    private fun JsonElement?.isTriviallyEmpty(): Boolean {
        return when (this) {
            null, JsonNull -> true
            is JsonPrimitive -> this.contentOrNull.isNullOrBlank() && this.booleanOrNull == null
            is JsonArray -> this.isEmpty() || this.all { it.isTriviallyEmpty() }
            is JsonObject -> this.isEmpty()
        }
    }

    private fun JsonPrimitive.intOrNull(): Int? = runCatching { this.int }.getOrNull()
    private fun JsonElement.stringOrNull(): String? = (this as? JsonPrimitive)?.contentOrNull

    @Serializable
    private data class ManifestEntry(val hash: String?)
}

private enum class OutputMode { LIBRARY, REFS }

data class ConversionResult(
    val libraryRoot: Path,
    val refsCsv: Path?,
    val manifestPath: Path?,
    val linksRoot: Path?
)

data class RefEntry(
    val ref: String,
    val heRef: String,
    val path: String,
    val lineIndex: Int
)

@Serializable
data class LinkOutputEntry(
    val lineIndex1: Int,
    val heRef2: String,
    val path2: String,
    val lineIndex2: Int,
    val connectionType: String
)
