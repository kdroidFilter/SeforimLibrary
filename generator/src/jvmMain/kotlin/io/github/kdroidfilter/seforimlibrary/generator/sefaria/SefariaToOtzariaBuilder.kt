package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import co.touchlab.kermit.Logger
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.math.max

/**
 * Builds an Otzaria-shaped source tree (אוצריא + links + metadata.json) from
 * Sefaria export folders (json + export_schemas + export_toc + export_links).
 */
class SefariaToOtzariaBuilder(private val logger: Logger) {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    data class TocBook(
        val heTitle: String,
        val enTitle: String,
        val hePath: List<String>
    )

    private data class IndexedText(
        val enTitle: String,
        val textPath: Path,     // .../json/.../<Book>/Hebrew/merged.json
        val schemaPath: Path?   // export_schemas/schemas/<Normalized>.json (if found)
    )

    private data class PendingFile(val relPath: String, val content: String)

    // From-export refs index entry
    private data class RefEntry(
        val ref: String,        // English ref, e.g., "Genesis 1:1"
        val heRef: String,      // Hebrew ref for display, segments joined with &&&
        val lineIndex: Int,     // 1-based line index in output .txt
        val path: String        // Output Hebrew title (sanitized)
    )

    // Parsed link parts (split_link equivalent)
    private data class ParsedLink(
        val firstPart: List<String>,
        val startIndex: List<Int>,
        val endIndex: List<Int>,
    )
    data class BuildResult(
        val fsRoot: Path,
        // Keep a strong reference to the FS provider (e.g., Jimfs) so it doesn't get GC'd
        val keepAlive: Any?,
        val filesCount: Int,
        val metadataCount: Int,
    )

    suspend fun build(sefariaRoot: Path, outRoot: Path): Path = coroutineScope {
        val parallelism = System.getProperty("sefariaParallelism")?.toIntOrNull()
            ?: System.getenv("SEFARIA_PARALLELISM")?.toIntOrNull()
            ?: Runtime.getRuntime().availableProcessors()
        val dispatcher = kotlinx.coroutines.Dispatchers.Default.limitedParallelism(parallelism)
        logger.i { "Using parallelism=$parallelism for Sefaria→Otzaria build" }
        val toc = loadToc(sefariaRoot)
        val indexByTitle = indexTexts(sefariaRoot)
        val enToHe = toc.associate { normalizeTitle(it.enTitle) to (it.heTitle.ifBlank { it.enTitle }) }
        val linksRoot = sefariaRoot.resolve("export_links").resolve("links")

        val otzariaRoot = outRoot
        val libRoot = otzariaRoot.resolve("אוצריא")
        val linksOut = otzariaRoot.resolve("links")
        Files.createDirectories(libRoot)
        Files.createDirectories(linksOut)

        // Reduce to only books with available text
        val toProcess = toc.mapNotNull { book ->
            val idx = indexByTitle[normalizeTitle(book.enTitle)] ?: return@mapNotNull null
            book to idx
        }
        logger.i { "Planning to build ${toProcess.size} books from ToC" }

        // Preload all schemas and texts into RAM
        val allSchemaPaths = toProcess.mapNotNull { it.second.schemaPath }.distinct()
        val schemaCache: Map<Path, JsonObject?> = coroutineScope {
            allSchemaPaths.map { p -> async(dispatcher) { p to (runCatching { json.parseToJsonElement(p.readText()) }.getOrNull() as? JsonObject) } }
                .awaitAll().toMap()
        }
        val allTextPaths = toProcess.map { it.second.textPath }.distinct()
        val textCache: Map<Path, JsonElement> = coroutineScope {
            allTextPaths.map { p -> async(dispatcher) { p to (runCatching { json.parseToJsonElement(p.readText()) }.getOrNull()) } }
                .awaitAll().filter { it.second != null }.associate { it.first to it.second!! }
        }
        logger.i { "Preloaded ${schemaCache.size} schemas and ${textCache.size} texts into RAM" }

        // Pre-index all link CSVs into memory
        val useCsvLinks = true
        val linksByTitle = if (useCsvLinks) preindexLinks(linksRoot) else emptyMap()
        logger.i { "Preindexed links for ${linksByTitle.size} titles" }

        // Thread-safe sink
        val metadataEntries = java.util.Collections.synchronizedList(mutableListOf<Map<String, Any?>>())
        val filesToWrite = java.util.Collections.synchronizedList(mutableListOf<PendingFile>())
        val enToOutTitle = java.util.concurrent.ConcurrentHashMap<String, String>()
        val heToOutTitle = java.util.concurrent.ConcurrentHashMap<String, String>()
        val outTitlesSet: MutableSet<String> = java.util.Collections.newSetFromMap(java.util.concurrent.ConcurrentHashMap<String, Boolean>())
        val footnoteLinksByOutTitle = java.util.concurrent.ConcurrentHashMap<String, String>()
        val allRefs = java.util.Collections.synchronizedList(mutableListOf<RefEntry>())
        
        // Process in parallel, in chunks to avoid oversubscription
        toProcess.chunked(256).forEachIndexed { chunkIdx, chunk ->
            logger.i { "Processing chunk ${chunkIdx + 1}/${(toProcess.size + 255) / 256}" }
            coroutineScope {
                chunk.map { (book, idx) ->
                    async(dispatcher) {
                        val schemaJson = idx.schemaPath?.let { schemaCache[it] }
                        val heTitle = book.heTitle.ifBlank { schemaJson?.get("heTitle")?.jsonPrimitive?.contentOrNull ?: book.enTitle }
                        val outTitle = sanitizeFilename(heTitle)
                        val bookRelDir = buildString {
                            append("אוצריא")
                            for (seg in book.hePath) {
                                append('/')
                                append(sanitizeFilename(seg))
                            }
                        }

                        val root = textCache[idx.textPath] ?: return@async
                        val (content0, refs) = buildContentWithRefsFromCached(root, schemaJson)
                        var content = content0
                        val (contentWithNotes, footnotes, footLinksJson) = extractFootnotesAndLinks(content, outTitle)
                        content = contentWithNotes
                        filesToWrite.add(
                            PendingFile(
                                relPath = "$bookRelDir/${outTitle}.txt",
                                content = content.joinToString(separator = "")
                            )
                        )

                        // Map EN base title -> output title used for file/DB
                        enToOutTitle[normalizeTitle(book.enTitle)] = outTitle
                        heToOutTitle[normalizeHeb(heTitle)] = outTitle
                        outTitlesSet.add(outTitle)

                        val meta = mutableMapOf<String, Any?>()
                        meta["title"] = heTitle
                        meta["author"] = (schemaJson?.get("authors") as? JsonArray)
                            ?.firstOrNull()?.jsonObject?.get("he")?.jsonPrimitive?.contentOrNull
                        meta["heShortDesc"] = asString(schemaJson?.get("heShortDesc"))
                        meta["pubDate"] = asString(schemaJson?.get("pubDate"))
                        meta["pubPlace"] = asString(schemaJson?.get("pubPlace"))
                        metadataEntries.add(meta)

                        if (footnotes.isNotEmpty()) {
                            filesToWrite.add(
                                PendingFile(
                                    relPath = "$bookRelDir/${"הערות על "+outTitle}.txt",
                                    content = footnotes.joinToString("\n")
                                )
                            )
                        }
                        val useCsvLinks = true
                        val csvJson = if (useCsvLinks) exportLinksForBookIndexed(
                            enTitle = book.enTitle,
                            heTitle = outTitle,
                            enToHe = enToHe,
                            linksByTitle = linksByTitle,
                            enToOutTitle = enToOutTitle,
                            heToOutTitle = heToOutTitle,
                            outTitlesSet = outTitlesSet,
                        ) else null
                        val mergedLinks = mergeLinkJsonArrays(footLinksJson, csvJson)
                        if (mergedLinks != null && mergedLinks.isNotBlank()) {
                            filesToWrite.add(PendingFile(relPath = "links/${outTitle}_links.json", content = mergedLinks))
                        }
                    }
                }.awaitAll()
            }
        }

        // Build metadata file (in RAM)
        val metaJson = buildString {
            append("[")
            metadataEntries.forEachIndexed { i, m ->
                if (i > 0) append(",")
                append(mapToJson(m))
            }
            append("]")
        }
        filesToWrite.add(PendingFile(relPath = "metadata.json", content = metaJson))

        // Flush all pending files to disk at the very end (parallelized)
        filesToWrite.chunked(256).forEach { chunk ->
            coroutineScope {
                chunk.map { pf ->
                    async(dispatcher) {
                        val abs = otzariaRoot.resolve(pf.relPath)
                        abs.parent?.let { Files.createDirectories(it) }
                        Files.writeString(abs, pf.content)
                    }
                }.awaitAll()
            }
        }
        logger.i { "Wrote ${filesToWrite.size} files to ${otzariaRoot.toAbsolutePath()} (metadata entries: ${metadataEntries.size})" }

        otzariaRoot
    }

    // Fully in-memory build using Jimfs; returns a Path rooted in the in-memory FS.
    suspend fun buildToMemoryFs(sefariaRoot: Path): BuildResult = coroutineScope {
        val parallelism = System.getProperty("sefariaParallelism")?.toIntOrNull()
            ?: System.getenv("SEFARIA_PARALLELISM")?.toIntOrNull()
            ?: Runtime.getRuntime().availableProcessors()
        val dispatcher = kotlinx.coroutines.Dispatchers.Default.limitedParallelism(parallelism)
        logger.i { "Using parallelism=$parallelism for in‑RAM Sefaria build" }
        val toc = loadToc(sefariaRoot)
        val indexByTitle = indexTexts(sefariaRoot)
        val enToHe = toc.associate { normalizeTitle(it.enTitle) to (it.heTitle.ifBlank { it.enTitle }) }
        val linksRoot = sefariaRoot.resolve("export_links").resolve("links")

        val toProcess = toc.mapNotNull { book ->
            val idx = indexByTitle[normalizeTitle(book.enTitle)] ?: return@mapNotNull null
            book to idx
        }

        // Preload
        val allSchemaPaths = toProcess.mapNotNull { it.second.schemaPath }.distinct()
        val schemaCache: Map<Path, JsonObject?> = coroutineScope {
            allSchemaPaths.map { p -> async(dispatcher) { p to (runCatching { json.parseToJsonElement(p.readText()) }.getOrNull() as? JsonObject) } }
                .awaitAll().toMap()
        }
        val allTextPaths = toProcess.map { it.second.textPath }.distinct()
        val textCache: Map<Path, JsonElement> = coroutineScope {
            allTextPaths.map { p -> async(dispatcher) { p to (runCatching { json.parseToJsonElement(p.readText()) }.getOrNull()) } }
                .awaitAll().filter { it.second != null }.associate { it.first to it.second!! }
        }
        val useCsvLinks = true
        val linksByTitle = if (useCsvLinks) preindexLinks(linksRoot) else emptyMap()

        val metadataEntries = java.util.Collections.synchronizedList(mutableListOf<Map<String, Any?>>())
        val filesToWrite = java.util.Collections.synchronizedList(mutableListOf<PendingFile>())
        val enToOutTitle = java.util.concurrent.ConcurrentHashMap<String, String>()
        val heToOutTitle = java.util.concurrent.ConcurrentHashMap<String, String>()
        val outTitlesSet: MutableSet<String> = java.util.Collections.newSetFromMap(java.util.concurrent.ConcurrentHashMap<String, Boolean>())
        // Accumulators for links (mirror of build())
        val footnoteLinksByOutTitle = java.util.concurrent.ConcurrentHashMap<String, String>()
        val allRefs = java.util.Collections.synchronizedList(mutableListOf<RefEntry>())

        toProcess.chunked(256).forEach { chunk ->
            coroutineScope {
                chunk.map { (book, idx) ->
                    async(dispatcher) {
                        val schemaJson = idx.schemaPath?.let { schemaCache[it] }
                        val heTitle = book.heTitle.ifBlank { schemaJson?.get("heTitle")?.jsonPrimitive?.contentOrNull ?: book.enTitle }
                        val outTitle = sanitizeFilename(heTitle)
                        val bookRelDir = buildString {
                            append("אוצריא")
                            for (seg in book.hePath) { append('/'); append(sanitizeFilename(seg)) }
                        }
                        val root = textCache[idx.textPath] ?: return@async
                        val (content0, refs) = buildContentWithRefsFromCached(root, schemaJson)
                        var content = content0
                        val (contentWithNotes, footnotes, footLinksJson) = extractFootnotesAndLinks(content, outTitle)
                        content = contentWithNotes
                        filesToWrite.add(PendingFile(relPath = "$bookRelDir/${outTitle}.txt", content = content.joinToString("")))

                        enToOutTitle[normalizeTitle(book.enTitle)] = outTitle
                        heToOutTitle[normalizeHeb(heTitle)] = outTitle
                        outTitlesSet.add(outTitle)

                        val meta = mutableMapOf<String, Any?>()
                        meta["title"] = heTitle
                        meta["author"] = (schemaJson?.get("authors") as? JsonArray)
                            ?.firstOrNull()?.jsonObject?.get("he")?.jsonPrimitive?.contentOrNull
                        meta["heShortDesc"] = asString(schemaJson?.get("heShortDesc"))
                        meta["pubDate"] = asString(schemaJson?.get("pubDate"))
                        meta["pubPlace"] = asString(schemaJson?.get("pubPlace"))
                        metadataEntries.add(meta)

                        if (footnotes.isNotEmpty()) {
                            filesToWrite.add(PendingFile(relPath = "$bookRelDir/${"הערות על "+outTitle}.txt", content = footnotes.joinToString("\n")))
                        }
                        if (!footLinksJson.isNullOrBlank()) {
                            footnoteLinksByOutTitle[outTitle] = footLinksJson
                        }
                        if (refs.isNotEmpty()) refs.forEach { allRefs.add(it.copy(path = outTitle)) }
                    }
                }.awaitAll()
            }
        }

        // Build links via from_export matching (CSV) and merge footnote links
        if (useCsvLinks) {
            val linksByBook = buildLinksFromExportPipeline(linksRoot = linksRoot, allRefs = allRefs.toList())
            for (entry in linksByBook.entries) {
                val bookOutTitle = entry.key
                val csvJson = entry.value
                val footJson = footnoteLinksByOutTitle[bookOutTitle]
                val merged = mergeLinkJsonArrays(footJson, csvJson)
                if (!merged.isNullOrBlank()) filesToWrite.add(PendingFile(relPath = "links/${bookOutTitle}_links.json", content = merged))
            }
            // Books that only have footnote links
            for (entry in footnoteLinksByOutTitle.entries) {
                val bookOutTitle = entry.key
                val footJson = entry.value
                if (linksByBook.containsKey(bookOutTitle)) continue
                if (!footJson.isNullOrBlank()) filesToWrite.add(PendingFile(relPath = "links/${bookOutTitle}_links.json", content = footJson))
            }
        } else {
            for (entry in footnoteLinksByOutTitle.entries) {
                val bookOutTitle = entry.key
                val footJson = entry.value
                if (!footJson.isNullOrBlank()) filesToWrite.add(PendingFile(relPath = "links/${bookOutTitle}_links.json", content = footJson))
            }
        }

        // Build metadata file
        val metaJson = buildString {
            append("[")
            metadataEntries.forEachIndexed { i, m ->
                if (i > 0) append(",")
                append(mapToJson(m))
            }
            append("]")
        }
        filesToWrite.add(PendingFile(relPath = "metadata.json", content = metaJson))

        // Create a Jimfs FS and flush all files there
        val fs = com.google.common.jimfs.Jimfs.newFileSystem(com.google.common.jimfs.Configuration.unix())
        val fsRoot = fs.getPath("/")
        for (pf in filesToWrite) {
            val abs = fsRoot.resolve(pf.relPath)
            abs.parent?.let { Files.createDirectories(it) }
            Files.writeString(abs, pf.content)
        }
        BuildResult(fsRoot = fsRoot, keepAlive = fs, filesCount = filesToWrite.size, metadataCount = metadataEntries.size)
    }

    private fun loadToc(sefariaRoot: Path): List<TocBook> {
        val tocPath = sefariaRoot.resolve("export_toc").resolve("table_of_contents.json")
        val root = runCatching { json.parseToJsonElement(tocPath.readText()) }.getOrNull()
        val out = mutableListOf<TocBook>()
        if (root != null) walkToc(root, emptyList(), out)
        logger.i { "Loaded ToC entries: ${out.size}" }
        return out
    }

    private fun walkToc(el: JsonElement, hePath: List<String>, out: MutableList<TocBook>) {
        when (el) {
            is JsonArray -> el.forEach { walkToc(it, hePath, out) }
            is JsonObject -> {
                val contents = el["contents"]
                if (contents != null) {
                    val cat = el["heCategory"]?.jsonPrimitive?.contentOrNull
                    val path = if (cat != null) hePath + cat else hePath
                    walkToc(contents, path, out)
                } else if (el["title"] != null) {
                    val heTitle = el["heTitle"]?.jsonPrimitive?.contentOrNull ?: ""
                    val enTitle = el["title"]?.jsonPrimitive?.contentOrNull ?: ""
                    out.add(TocBook(heTitle = heTitle, enTitle = enTitle, hePath = hePath))
                }
            }
            else -> {}
        }
    }

    private fun indexTexts(sefariaRoot: Path): Map<String, IndexedText> {
        val root = sefariaRoot.resolve("json")
        val files = Files.walk(root)
            .use { s -> s.filter { it.isRegularFile() && it.name == "merged.json" && it.parent?.name == "Hebrew" }.toList() }
        val schemasRoot = sefariaRoot.resolve("export_schemas").resolve("schemas")
        val out = mutableMapOf<String, IndexedText>()
        for (f in files) {
            // The book title is the directory right above Hebrew: .../<Book>/Hebrew/merged.json
            val enTitle = f.parent?.parent?.name ?: continue
            val normalized = normalizeTitle(enTitle)
            val schemaPath = guessSchemaPath(schemasRoot, enTitle)
            out[normalized] = IndexedText(enTitle = enTitle, textPath = f, schemaPath = schemaPath)
        }
        logger.i { "Indexed ${out.size} Hebrew texts" }
        return out
    }

    private fun guessSchemaPath(schemasRoot: Path, enTitle: String): Path? {
        // Heuristic: Sefaria exports schemas as Title with punctuation replaced by underscores.
        fun normName(s: String): String {
            return s.replace(Regex("[^A-Za-z0-9]+"), "_").trim('_')
        }
        val cand1 = schemasRoot.resolve("${normName(enTitle)}.json")
        if (cand1.exists()) return cand1
        // Some titles include comma-separated parts; try compact variant
        val cand2 = schemasRoot.resolve("${normName(enTitle.replace(",", ""))}.json")
        if (cand2.exists()) return cand2
        return null
    }

    private fun buildContent(textPath: Path, schema: JsonObject?): List<String> {
        val root = runCatching { json.parseToJsonElement(textPath.readText()) }.getOrNull()
        val rootObj = (root as? JsonObject) ?: return emptyList()
        // Prefer top-level "text"; fallback to versions[0].text or versions[0] if it's already an array
        val text: JsonElement = when (val t = rootObj["text"]) {
            null -> {
                val versionsEl = rootObj["versions"]
                if (versionsEl is JsonArray) {
                    val v0 = versionsEl.firstOrNull()
                    when (v0) {
                        is JsonObject -> v0["text"] ?: return emptyList()
                        null -> return emptyList()
                        else -> v0 // already an array-of-arrays text
                    }
                } else return emptyList()
            }
            else -> t
        }

        // Determine section names and depth
        val depth = schema?.get("schema")?.jsonObject?.get("depth")?.jsonPrimitive?.content?.toIntOrNull()
        val heSectionNames = schema?.get("schema")?.jsonObject?.get("heSectionNames") as? JsonArray
        val sectionNames = heSectionNames?.mapNotNull { it.jsonPrimitive.contentOrNull }

        val lines = mutableListOf<String>()
        // Add top-level title and a first author line (from schema) like the Python does
        val heTitle = schema?.get("heTitle")?.jsonPrimitive?.contentOrNull
        heTitle?.let { addHeading(lines, 1, it) }
        val authors = ((schema?.get("authors") as? JsonArray)
            ?.mapNotNull { it.jsonObject["he"]?.jsonPrimitive?.contentOrNull }
            ?: emptyList())
        lines += ((authors.joinToString(", ") ) + "\n")

        // Recursively flatten nested arrays with headings
        recursiveSections(
            lines = lines,
            sectionNames = sectionNames,
            node = text,
            depth = depth ?: inferDepth(text),
            level = 2,
        )
        return lines
    }

    private fun buildContentFromCached(root: JsonElement, schema: JsonObject?): List<String> {
        val rootObj = (root as? JsonObject)
        val text: JsonElement = if (rootObj != null) {
            when (val t = rootObj["text"]) {
                null -> {
                    val versionsEl = rootObj["versions"]
                    if (versionsEl is JsonArray) {
                        val v0 = versionsEl.firstOrNull()
                        when (v0) {
                            is JsonObject -> v0["text"] ?: return emptyList()
                            null -> return emptyList()
                            else -> v0
                        }
                    } else return emptyList()
                }
                else -> t
            }
        } else root

        val depth = schema?.get("schema")?.jsonObject?.get("depth")?.jsonPrimitive?.content?.toIntOrNull()
        val heSectionNames = schema?.get("schema")?.jsonObject?.get("heSectionNames") as? JsonArray
        val sectionNames = heSectionNames?.mapNotNull { it.jsonPrimitive.contentOrNull }

        val lines = mutableListOf<String>()
        val heTitle = schema?.get("heTitle")?.jsonPrimitive?.contentOrNull
        heTitle?.let { addHeading(lines, 1, it) }
        val authors = ((schema?.get("authors") as? JsonArray)
            ?.mapNotNull { it.jsonObject["he"]?.jsonPrimitive?.contentOrNull }
            ?: emptyList())
        lines += ((authors.joinToString(", ") ) + "\n")

        recursiveSections(
            lines = lines,
            sectionNames = sectionNames,
            node = text,
            depth = depth ?: inferDepth(text),
            level = 2,
        )
        return lines
    }

    // Build content lines together with refs_all style entries (from_export parity)
    private fun buildContentWithRefsFromCached(root: JsonElement, schema: JsonObject?): Pair<List<String>, List<RefEntry>> {
        val rootObj = (root as? JsonObject)
        val text: JsonElement = if (rootObj != null) {
            when (val t = rootObj["text"]) {
                null -> {
                    val versionsEl = rootObj["versions"]
                    if (versionsEl is JsonArray) {
                        val v0 = versionsEl.firstOrNull()
                        when (v0) {
                            is JsonObject -> v0["text"] ?: return Pair(emptyList(), emptyList())
                            null -> return Pair(emptyList(), emptyList())
                            else -> v0
                        }
                    } else return Pair(emptyList(), emptyList())
                }
                else -> t
            }
        } else root

        val depth = schema?.get("schema")?.jsonObject?.get("depth")?.jsonPrimitive?.content?.toIntOrNull()
        val heSectionNames = schema?.get("schema")?.jsonObject?.get("heSectionNames") as? JsonArray
        val sectionNames = heSectionNames?.mapNotNull { it.jsonPrimitive.contentOrNull }
        val enRootTitle = schema?.get("schema")?.jsonObject?.get("title")?.jsonPrimitive?.contentOrNull
            ?: schema?.get("title")?.jsonPrimitive?.contentOrNull
            ?: ""
        val heRootTitle = schema?.get("heTitle")?.jsonPrimitive?.contentOrNull ?: ""

        val lines = mutableListOf<String>()
        val refs = mutableListOf<RefEntry>()

        // Title + authors as in Python
        val heTitle = schema?.get("heTitle")?.jsonPrimitive?.contentOrNull
        heTitle?.let { addHeading(lines, 1, it) }
        val authors = ((schema?.get("authors") as? JsonArray)
            ?.mapNotNull { it.jsonObject["he"]?.jsonPrimitive?.contentOrNull }
            ?: emptyList())
        lines += ((authors.joinToString(", ") ) + "\n")

        fun rec(
            node: JsonElement,
            d: Int,
            level: Int,
            anchorRef: MutableList<String>,
            hebAnchorRef: MutableList<String>,
        ) {
            val skip = setOf("שורה", "פירוש", "פסקה", "Line", "Comment", "Paragraph")
            if (d == 0 && node !is JsonObject) {
                when (node) {
                    is JsonArray -> node.forEach { rec(it, d, level, anchorRef, hebAnchorRef) }
                    else -> {
                        val s = node.jsonPrimitive.contentOrNull?.trim()?.replace("\n", "<br>")
                        if (!s.isNullOrBlank()) {
                            val line = s + "\n"
                            lines += line
                            val refStr = buildString {
                                append(enRootTitle)
                                if (!anchorRef.isEmpty()) {
                                    append(' ')
                                    append(anchorRef.joinToString(":"))
                                }
                            }.trim()
                            val heRefStr = buildString {
                                append(heRootTitle)
                                if (!hebAnchorRef.isEmpty()) {
                                    append(' ')
                                    append(hebAnchorRef.joinToString("&&&"))
                                }
                            }.trim()
                            refs += RefEntry(ref = refStr, heRef = heRefStr, lineIndex = lines.size, path = "")
                        }
                    }
                }
            } else if (node is JsonArray) {
                if (node.isNotEmpty()) {
                    for ((i, child) in node.withIndex()) {
                        if (!hasValue(child)) continue
                        var labelToEmit: String? = null
                        if (!sectionNames.isNullOrEmpty()) {
                            val name = sectionNames[max(0, sectionNames.size - d)]
                            val dafMode = (name == "דף" || name == "Daf")
                            val letterVal = if (dafMode) toDaf(i) else toGematria(i + 1)
                            if (d > 1 && !skip.contains(name)) {
                                addHeading(lines, level, "$name $letterVal")
                            } else if (!skip.contains(name) && letterVal.isNotBlank()) {
                                labelToEmit = "($letterVal) "
                            }
                            // Update anchors
                            anchorRef.add(if (dafMode) toEngDaf(i) else (i + 1).toString())
                            hebAnchorRef.add(if (dafMode) toDaf(i) else toGematria(i + 1))
                        }
                        if (labelToEmit != null) lines += labelToEmit
                        rec(child, d - 1, level + 1, anchorRef, hebAnchorRef)
                        if (!sectionNames.isNullOrEmpty()) {
                            // pop anchor after recursion
                            if (anchorRef.isNotEmpty()) anchorRef.removeAt(anchorRef.size - 1)
                            if (hebAnchorRef.isNotEmpty()) hebAnchorRef.removeAt(hebAnchorRef.size - 1)
                        }
                    }
                }
            } else if (node is JsonObject) {
                // Handle object-wrapped sections (e.g., {"Introduction": [...]}) by recursing into values.
                for ((k, child) in node) {
                    if (!hasValue(child)) continue
                    val keyTrim = k.trim()
                    val isNumericKey = keyTrim.all { it.isDigit() }
                    if (keyTrim.isNotEmpty() && !isNumericKey) {
                        // Emit a heading for named sections; do not alter anchors (no numeric index)
                        addHeading(lines, level, keyTrim)
                    }
                    rec(child, d, level + 1, anchorRef, hebAnchorRef)
                }
            }
        }

        rec(
            node = text,
            d = depth ?: inferDepth(text),
            level = 2,
            anchorRef = mutableListOf(),
            hebAnchorRef = mutableListOf()
        )

        return Pair(lines, refs)
    }

    private fun inferDepth(el: JsonElement): Int {
        return when (el) {
            is JsonArray -> if (el.isEmpty()) 0 else 1 + inferDepth(el.first())
            is JsonObject -> if (el.isEmpty()) 0 else inferDepth(el.values.first())
            else -> 0
        }
    }

    private fun recursiveSections(
        lines: MutableList<String>,
        sectionNames: List<String>?,
        node: JsonElement,
        depth: Int,
        level: Int,
    ) {
        val skip = setOf("שורה", "פירוש", "פסקה", "Line", "Comment", "Paragraph")
        if (depth == 0 && node !is JsonObject) {
            when (node) {
                is JsonArray -> node.forEach { recursiveSections(lines, sectionNames, it, depth, level) }
                else -> {
                    val s = node.jsonPrimitive.contentOrNull?.trim()?.replace("\n", "<br>") ?: return
                    lines += s + "\n"
                }
            }
        } else if (node is JsonArray) {
            if (node.isNotEmpty()) {
                // Iterate children
                for ((i, child) in node.withIndex()) {
                    if (!hasValue(child)) continue
                    val label = if (!sectionNames.isNullOrEmpty()) {
                        val name = sectionNames[max(0, sectionNames.size - depth)]
                        val letter = if (name == "דף" || name == "Daf") toDaf(i) else toGematria(i + 1)
                        if (depth > 1 && !skip.contains(name)) {
                            addHeading(lines, level, "$name $letter")
                            null
                        } else if (!skip.contains(name) && letter.isNotBlank()) {
                            "($letter) "
                        } else null
                    } else null
                    if (label != null) lines += label
                    recursiveSections(lines, sectionNames, child, depth - 1, level + 1)
                }
            } else {
                // Empty array → nothing
            }
        } else if (node is JsonObject) {
            // Recurse into object-wrapped sections; emit a heading for key if meaningful
            for ((k, v) in node) {
                if (!hasValue(v)) continue
                val keyTrim = k.trim()
                val isNumericKey = keyTrim.all { it.isDigit() }
                if (keyTrim.isNotEmpty() && !isNumericKey && !setOf("שורה", "פירוש", "פסקה", "Line", "Comment", "Paragraph").contains(keyTrim)) {
                    addHeading(lines, level, keyTrim)
                }
                recursiveSections(lines, sectionNames, v, depth, level + 1)
            }
        }
    }

    private fun addHeading(lines: MutableList<String>, level: Int, text: String) {
        if (text.isBlank()) return
        val lv = level.coerceAtMost(6)
        lines += "<h$lv>$text</h$lv>\n"
    }

    private fun hasValue(node: JsonElement): Boolean {
        return when (node) {
            is JsonArray -> node.any { hasValue(it) }
            is JsonObject -> node.values.any { hasValue(it) }
            else -> {
                val prim = try { node.jsonPrimitive } catch (e: Exception) { return false }
                if (prim.isString) {
                    prim.contentOrNull?.isNotBlank() == true
                } else {
                    val c = prim.contentOrNull?.lowercase()
                    c != null && c != "false"
                }
            }
        }
    }

    // Create footnote-like daf label as in Python to_daf
    private fun toDaf(iZeroBased: Int): String {
        val i = iZeroBased + 1
        return if (i % 2 == 0) "${toGematria(i / 2)}." else "${toGematria(i / 2)}:"
    }

    // English daf marker as in Python to_eng_daf
    private fun toEngDaf(iZeroBased: Int): String {
        val j = iZeroBased
        return if (j % 2 == 0) "${(j / 2) + 1}a" else "${(j / 2) + 1}b"
    }

    // Basic Hebrew gematria for 1.. (no geresh/gershayim formatting)
    private fun toGematria(n: Int): String {
        var num = n
        val ones = arrayOf("", "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט")
        val tens = arrayOf("", "י", "כ", "ל", "מ", "נ", "ס", "ע", "פ", "צ")
        val hundreds = arrayOf("", "ק", "ר", "ש", "ת")
        val sb = StringBuilder()
        var x = num
        if (x >= 100) {
            val h = (x / 100).coerceAtMost(4)
            sb.append(hundreds[h])
            x %= 100
        }
        if (x >= 10) {
            val t = x / 10
            sb.append(tens[t])
            x %= 10
        }
        if (x > 0) sb.append(ones[x])
        return sb.toString()
    }

    private fun normalizeTitle(s: String): String {
        return s.lowercase().replace(Regex("[^a-z0-9]+"), " ").trim().replace(Regex("\\s+"), " ")
    }

    private fun sanitizeFilename(s: String): String {
        // Remove Hebrew diacritics; strip forbidden FS chars; normalize quotes/underscores
        return s
            .replace(Regex("[\u0591-\u05C7]"), "")
            .replace(Regex("[\\/:*\"״?<>|]"), "")
            .replace("_", " ")
            .replace("''", "")
            .replace("'", "")
            .trim()
    }

    private fun normalizeHeb(s: String): String {
        // Remove Hebrew diacritics and common quote marks; collapse spaces
        return s
            .replace(Regex("[\u0591-\u05C7]"), "") // niqqud/taamim
            .replace("\u05F4", "") // gershayim ״
            .replace("\u201D", "") // ”
            .replace("\u201C", "") // “
            .replace("\"", "")   // "
            .replace("'", "")
            .replace(Regex("\\s+"), " ")
            .trim()
    }

    private fun exportLinksForBook(
        linksRoot: Path,
        linksOut: Path,
        enTitle: String,
        heTitle: String,
        enToHe: Map<String, String>,
    ) {
        // links*.csv columns include: Citation 1, Citation 2, Conection Type, Text 1, Text 2, Category 1, Category 2
        val csvFiles = if (linksRoot.exists()) Files.list(linksRoot).use { it.filter { p -> p.isRegularFile() && p.name.endsWith(".csv") }.toList() } else emptyList()
        if (csvFiles.isEmpty()) return
        val norm = normalizeTitle(enTitle)
        val entries = mutableListOf<String>()
        for (csv in csvFiles) {
            val lines = runCatching { csv.readText() }.getOrDefault("").lines()
            if (lines.isEmpty()) continue
            val header = lines.first().split(',')
            val idxText1 = header.indexOfFirst { it.trim().equals("Text 1", ignoreCase = true) }
            val idxText2 = header.indexOfFirst { it.trim().equals("Text 2", ignoreCase = true) }
            val idxConn = header.indexOfFirst { it.contains("Conection", ignoreCase = true) }
            val idxCit1 = header.indexOfFirst { it.trim().startsWith("Citation 1") }
            val idxCit2 = header.indexOfFirst { it.trim().startsWith("Citation 2") }
            if (idxText1 < 0 || idxText2 < 0 || idxConn < 0 || idxCit1 < 0 || idxCit2 < 0) continue
            for (row in lines.drop(1)) {
                if (row.isBlank()) continue
                val cols = splitCsvRow(row)
                if (cols.size <= max(idxText1, idxText2)) continue
                val t1 = cols[idxText1].trim('"')
                val t2 = cols[idxText2].trim('"')
                val who = when {
                    normalizeTitle(t1) == norm -> 1
                    normalizeTitle(t2) == norm -> 2
                    else -> 0
                }
                if (who == 0) continue
                val cit1 = cols.getOrNull(idxCit1)?.trim('"') ?: continue
                val cit2 = cols.getOrNull(idxCit2)?.trim('"') ?: continue
                val conn = cols.getOrNull(idxConn)?.trim('"') ?: "other"
                // We don’t have exact line indices here; make best-effort: skip unless simple digit
                val lineIdx1 = extractLineIndex(cit1) ?: continue
                val lineIdx2 = extractLineIndex(cit2) ?: continue
                val path2TitleEn = if (who == 1) t2 else t1
                val heTarget = enToHe[normalizeTitle(path2TitleEn)] ?: path2TitleEn
                val item = mapOf(
                    "line_index_1" to lineIdx1,
                    "heRef_2" to heTarget,
                    // DatabaseGenerator extracts the target title from file name; ensure Hebrew title + .txt
                    "path_2" to "$heTarget.txt",
                    "line_index_2" to lineIdx2,
                    "Conection Type" to conn
                )
                entries += mapToJson(item)
            }
        }
        if (entries.isNotEmpty()) {
            val out = linksOut.resolve("${sanitizeFilename(heTitle)}_links.json")
            Files.writeString(out, "[" + entries.joinToString(",") + "]")
        }
    }

    private fun splitCsvRow(row: String): List<String> {
        val out = mutableListOf<String>()
        val sb = StringBuilder()
        var inQuotes = false
        var i = 0
        while (i < row.length) {
            val c = row[i]
            when (c) {
                '"' -> {
                    inQuotes = !inQuotes
                }
                ',' -> {
                    if (inQuotes) sb.append(c) else { out += sb.toString(); sb.setLength(0) }
                }
                else -> sb.append(c)
            }
            i++
        }
        out += sb.toString()
        return out
    }

    // from_export: split_link
    private fun splitLink(link: String): ParsedLink {
        var parts = link.split(", ")
        var lastPart = parts.lastOrNull() ?: ""
        if (parts.size == 1) {
            lastPart = parts[0]
            parts = emptyList()
        } else {
            parts = parts.dropLast(1)
        }
        val splitLast = lastPart.split(" ")
        val lastToken = splitLast.lastOrNull() ?: ""
        val rangeParts = lastToken.split("-")
        var startIndexTokens: List<String> = emptyList()
        var endIndexTokens: List<String> = emptyList()
        if (rangeParts.size == 2) {
            val (start, end) = parseRange(rangeParts[0], rangeParts[1])
            startIndexTokens = start
            endIndexTokens = end
        } else {
            startIndexTokens = lastToken.split(":")
        }
        val firstPart = buildList {
            if (parts.isNotEmpty()) addAll(parts)
            if (splitLast.size > 1) add(splitLast.dropLast(1).joinToString(" "))
        }
        return try {
            ParsedLink(
                firstPart = firstPart,
                startIndex = convertRefToInt(startIndexTokens),
                endIndex = convertRefToInt(endIndexTokens)
            )
        } catch (e: Exception) {
            ParsedLink(
                firstPart = firstPart + startIndexTokens,
                startIndex = emptyList(),
                endIndex = emptyList()
            )
        }
    }

    private fun parseRange(startIndex: String, endIndex: String): Pair<List<String>, List<String>> {
        val start = startIndex.split(":").toMutableList()
        val end = endIndex.split(":").toMutableList()
        if (start.size != end.size) {
            // Prepend to shorter until sizes match
            while (end.size < start.size) {
                end.add(0, start.first())
            }
            while (start.size < end.size) {
                start.add(0, end.first())
            }
        }
        return start to end
    }

    private fun convertRefToInt(tokens: List<String>): List<Int> {
        return tokens.filter { it.isNotBlank() }.map { t ->
            t.replace("a", "1").replace("b", "2").toInt()
        }
    }

    private fun matchLinks(a: List<Int>, b: List<Int>): Boolean {
        val common = minOf(a.size, b.size)
        for (i in 0 until common) if (a[i] != b[i]) return false
        return true
    }

    private fun matchRange(refStart: List<Int>, rangeStart: List<Int>, rangeEnd: List<Int>): Boolean {
        if (refStart.isEmpty()) return false
        val common = minOf(refStart.size, rangeStart.size, rangeEnd.size)
        for (i in 0 until common) {
            val v = refStart[i]
            val lo = rangeStart[i]
            val hi = rangeEnd[i]
            if (v < lo || v > hi) return false
        }
        return true
    }

    private fun getBestMatch(candidates: List<ParsedLink>): List<ParsedLink> {
        if (candidates.isEmpty()) return emptyList()
        val maxLen = candidates.maxOf { it.startIndex.size }
        val filtered = candidates.filter { it.startIndex.size == maxLen }
        val cmp = Comparator<ParsedLink> { a, b ->
            val n = minOf(a.startIndex.size, b.startIndex.size)
            for (i in 0 until n) {
                val d = a.startIndex[i].compareTo(b.startIndex[i])
                if (d != 0) return@Comparator d
            }
            a.startIndex.size.compareTo(b.startIndex.size)
        }
        return filtered.sortedWith(cmp)
    }

    private fun getBestMatchWithFirstPart(candidates: List<ParsedLink>): List<ParsedLink> {
        if (candidates.isEmpty()) return emptyList()
        val maxFirst = candidates.maxOf { it.firstPart.size }
        val filtered = candidates.filter { it.firstPart.size == maxFirst }
        return getBestMatch(filtered)
    }

    private fun matchFirstPart(a: List<String>, b: List<String>): Boolean {
        val common = minOf(a.size, b.size)
        for (i in 0 until common) if (a[i] != b[i]) return false
        return true
    }

    private fun fixHeRef(ref: String, enRefLen: Int): String {
        val splitRef = ref.split(",").map { it.trim() }.toMutableList()
        var lastPart = splitRef.lastOrNull() ?: ""
        if (splitRef.size == 1) {
            lastPart = splitRef[0]
            splitRef.clear()
        } else {
            splitRef.removeAt(splitRef.lastIndex)
        }
        val splitLastPart = lastPart.split(" ")
        val startIndex = (splitLastPart.lastOrNull() ?: "").split("&&&").map { it.trim() }
        val firstPart = buildList {
            addAll(splitRef)
            if (splitLastPart.size > 1) add(splitLastPart.dropLast(1).joinToString(" "))
        }
        val startIndexNew = startIndex.take(enRefLen)
        val fp = firstPart.joinToString(", ")
        val si = startIndexNew.joinToString(", ")
        return (if (fp.isNotBlank()) "$fp " else "") + si
    }

    private data class MatchedLine(val heRef: String, val path: String, val lineIndex: Int)

    // Full from_export matching: build final per-book JSON link arrays
    private fun buildLinksFromExportPipeline(linksRoot: Path, allRefs: List<RefEntry>): Map<String, String> {
        if (!linksRoot.exists()) return emptyMap()
        // 1) Read CSV links
        val setLinks = mutableSetOf<String>()
        val setRanges = mutableSetOf<String>()
        data class CsvRow(val cit1: String, val cit2: String, val conn: String)
        val csvRows = mutableListOf<CsvRow>()
        Files.list(linksRoot).use { s ->
            s.filter { it.isRegularFile() && it.name.endsWith(".csv") }.forEach { csv ->
                val lines = runCatching { csv.readText() }.getOrDefault("").lines()
                if (lines.isEmpty()) return@forEach
                val header = lines.first().split(',')
                val idxCit1 = header.indexOfFirst { it.trim().startsWith("Citation 1") }
                val idxCit2 = header.indexOfFirst { it.trim().startsWith("Citation 2") }
                val idxConn = header.indexOfFirst { it.contains("Conection", ignoreCase = true) }
                if (idxCit1 < 0 || idxCit2 < 0 || idxConn < 0) return@forEach
                for (row in lines.drop(1)) {
                    if (row.isBlank()) continue
                    val cols = splitCsvRow(row)
                    val c1 = cols.getOrNull(idxCit1)?.trim('"')?.trim() ?: continue
                    val c2 = cols.getOrNull(idxCit2)?.trim('"')?.trim() ?: continue
                    val conn = cols.getOrNull(idxConn)?.trim('"')?.trim() ?: "other"
                    csvRows += CsvRow(c1, c2, conn)
                    fun bucket(c: String) {
                        val lastTok = c.split(", ").lastOrNull()?.split(" ")?.lastOrNull() ?: ""
                        if (lastTok.contains('-')) setRanges.add(c) else setLinks.add(c)
                    }
                    bucket(c1); bucket(c2)
                }
            }
        }

        // 2) Build otzaria_parse and all_otzaria_links from refs
        val allOtzaria = mutableMapOf<String, MutableList<MatchedLine>>()
        val otzariaParse = mutableMapOf<String, MutableList<ParsedLink>>()
        // Also exact matches bucket
        val otzariaLinks = mutableMapOf<String, MutableList<MatchedLine>>()
        for (ref in allRefs) {
            val p = splitLink(ref.ref)
            if (p.firstPart.isEmpty()) continue
            val key = (p.firstPart.joinToString(", ") + " " + p.startIndex.joinToString(":"))
            allOtzaria.getOrPut(key) { mutableListOf() }
                .add(MatchedLine(heRef = ref.heRef.replace("&&&", ", "), path = ref.path, lineIndex = ref.lineIndex))
            otzariaParse.getOrPut(p.firstPart.first()) { mutableListOf() }.add(p)
            if (ref.ref in setLinks) {
                otzariaLinks.getOrPut(ref.ref) { mutableListOf() }
                    .add(MatchedLine(heRef = ref.heRef.replace("&&&", ", "), path = ref.path, lineIndex = ref.lineIndex))
            }
        }

        // Remove already satisfied exact matches
        setLinks.removeAll(otzariaLinks.keys)

        // 3) Approximate match for single refs
        val setLinksCopy = setLinks.toMutableSet()
        for (i in setLinks) {
            val parse = splitLink(i)
            val base = parse.firstPart.firstOrNull() ?: continue
            val candidates = otzariaParse[base] ?: continue
            val result = mutableListOf<ParsedLink>()
            for (j in candidates) {
                if (j.firstPart == parse.firstPart) {
                    if (matchLinks(parse.startIndex, j.startIndex)) result += j
                }
            }
            val best = if (result.isNotEmpty()) getBestMatch(result) else run {
                val res2 = mutableListOf<ParsedLink>()
                for (j in candidates) {
                    if (matchFirstPart(parse.firstPart, j.firstPart)) {
                        if (matchLinks(parse.startIndex, j.startIndex)) res2 += j
                    }
                }
                if (res2.isNotEmpty()) getBestMatchWithFirstPart(res2) else emptyList()
            }
            if (best.isNotEmpty()) {
                val match = best.first()
                val key = (match.firstPart.joinToString(", ") + " " + match.startIndex.joinToString(":"))
                val lines = (allOtzaria[key] ?: emptyList()).map { ml ->
                    val fixed = fixHeRef(ml.heRef, match.startIndex.size)
                    MatchedLine(heRef = fixed, path = ml.path, lineIndex = ml.lineIndex)
                }
                if (lines.isNotEmpty()) {
                    otzariaLinks.getOrPut(i) { mutableListOf() }.addAll(lines)
                    setLinksCopy.remove(i)
                }
            }
        }
        setLinks.clear(); setLinks.addAll(setLinksCopy)

        // 4) Approximate match for ranges
        val setRangesCopy = setRanges.toMutableSet()
        for (i in setRanges) {
            val parse = splitLink(i)
            val base = parse.firstPart.firstOrNull() ?: continue
            val candidates = otzariaParse[base] ?: continue
            val result = mutableListOf<ParsedLink>()
            for (j in candidates) {
                if (j.firstPart == parse.firstPart) {
                    if (matchRange(j.startIndex, parse.startIndex, parse.endIndex)) result += j
                }
            }
            val best = if (result.isNotEmpty()) result else run {
                val res2 = mutableListOf<ParsedLink>()
                for (j in candidates) {
                    if (matchFirstPart(parse.firstPart, j.firstPart)) {
                        if (matchRange(j.startIndex, parse.startIndex, parse.endIndex)) res2 += j
                    }
                }
                if (res2.isNotEmpty()) getBestMatchWithFirstPart(res2) else emptyList()
            }
            if (best.isNotEmpty()) {
                for (m in best) {
                    val key = (m.firstPart.joinToString(", ") + " " + m.startIndex.joinToString(":"))
                    val lines = (allOtzaria[key] ?: emptyList()).map { ml ->
                        val fixed = fixHeRef(ml.heRef, m.startIndex.size)
                        MatchedLine(heRef = fixed, path = ml.path, lineIndex = ml.lineIndex)
                    }
                    if (lines.isNotEmpty()) {
                        otzariaLinks.getOrPut(i) { mutableListOf() }.addAll(lines)
                        setRangesCopy.remove(i)
                    }
                }
            }
        }
        setRanges.clear(); setRanges.addAll(setRangesCopy)

        // 5) Produce final per-book links combining both directions
        val finalByBook = mutableMapOf<String, MutableList<String>>()
        for (row in csvRows) {
            val l1 = otzariaLinks[row.cit1] ?: continue
            val l2 = otzariaLinks[row.cit2] ?: continue
            for (a in l1) {
                val arr = finalByBook.getOrPut(a.path) { mutableListOf() }
                for (b in l2) {
                    val o = mapOf(
                        "line_index_1" to a.lineIndex,
                        "line_index_2" to b.lineIndex,
                        "heRef_2" to b.heRef,
                        "path_2" to "${b.path}.txt",
                        "Conection Type" to row.conn
                    )
                    arr += mapToJson(o)
                }
            }
            for (b in l2) {
                val arr = finalByBook.getOrPut(b.path) { mutableListOf() }
                for (a in l1) {
                    val o = mapOf(
                        "line_index_1" to b.lineIndex,
                        "line_index_2" to a.lineIndex,
                        "heRef_2" to a.heRef,
                        "path_2" to "${a.path}.txt",
                        "Conection Type" to row.conn
                    )
                    arr += mapToJson(o)
                }
            }
        }

        // Serialize
        return finalByBook.mapValues { (_, v) -> "[" + v.joinToString(",") + "]" }
    }

    private fun extractLineIndex(citation: String): Int? {
        // Very rough: pull last number token in citation (e.g. "Book 12:3" -> 3)
        val m = Regex("(\\d+)([^0-9]*)$").find(citation.trim()) ?: return null
        return m.groupValues[1].toIntOrNull()
    }

    private data class LinkRow(
        val citation1: String,
        val citation2: String,
        val connectionType: String,
        val text1: String,
        val text2: String,
    )

    private fun preindexLinks(linksRoot: Path): Map<String, List<LinkRow>> {
        if (!linksRoot.exists()) return emptyMap()
        val csvFiles = Files.list(linksRoot).use { it.filter { p -> p.isRegularFile() && p.name.endsWith(".csv") }.toList() }
        if (csvFiles.isEmpty()) return emptyMap()
        val acc = mutableMapOf<String, MutableList<LinkRow>>()
        for (csv in csvFiles) {
            val lines = runCatching { csv.readText() }.getOrDefault("").lines()
            if (lines.isEmpty()) continue
            val header = lines.first().split(',')
            val idxText1 = header.indexOfFirst { it.trim().equals("Text 1", ignoreCase = true) }
            val idxText2 = header.indexOfFirst { it.trim().equals("Text 2", ignoreCase = true) }
            val idxConn = header.indexOfFirst { it.contains("Conection", ignoreCase = true) }
            val idxCit1 = header.indexOfFirst { it.trim().startsWith("Citation 1") }
            val idxCit2 = header.indexOfFirst { it.trim().startsWith("Citation 2") }
            if (idxText1 < 0 || idxText2 < 0 || idxConn < 0 || idxCit1 < 0 || idxCit2 < 0) continue
            for (row in lines.drop(1)) {
                if (row.isBlank()) continue
                val cols = splitCsvRow(row)
                if (cols.size <= max(idxText1, idxText2)) continue
                val r = LinkRow(
                    citation1 = cols.getOrNull(idxCit1)?.trim('"') ?: continue,
                    citation2 = cols.getOrNull(idxCit2)?.trim('"') ?: continue,
                    connectionType = cols.getOrNull(idxConn)?.trim('"') ?: "other",
                    text1 = cols[idxText1].trim('"'),
                    text2 = cols[idxText2].trim('"'),
                )
                val k1 = normalizeTitle(r.text1)
                val k2 = normalizeTitle(r.text2)
                acc.getOrPut(k1) { mutableListOf() }.add(r)
                acc.getOrPut(k2) { mutableListOf() }.add(r)
            }
        }
        return acc
    }

    private fun exportLinksForBookIndexed(
        enTitle: String,
        heTitle: String,
        enToHe: Map<String, String>,
        linksByTitle: Map<String, List<LinkRow>>,
        enToOutTitle: Map<String, String>,
        heToOutTitle: Map<String, String>,
        outTitlesSet: Set<String>,
    ): String? {
        val norm = normalizeTitle(enTitle)
        val rows = linksByTitle[norm] ?: return null
        val entries = mutableListOf<String>()
        for (r in rows) {
            val who = when {
                normalizeTitle(r.text1) == norm -> 1
                normalizeTitle(r.text2) == norm -> 2
                else -> 0
            }
            if (who == 0) continue
            val lineIdx1 = extractLineIndex(r.citation1) ?: continue
            val lineIdx2 = extractLineIndex(r.citation2) ?: continue
            val path2TitleEnRaw = if (who == 1) r.text2 else r.text1
            val normFullEn = normalizeTitle(path2TitleEnRaw)
            val normFullHe = normalizeHeb(path2TitleEnRaw)
            // Prefer exact full-title mapping (EN then HE)
            var outTitle: String? = enToOutTitle[normFullEn]
            if (outTitle == null) outTitle = heToOutTitle[normFullHe]
            if (outTitle == null) {
                // Try ToC EN->HE then sanitize to outTitle
                val heToc = enToHe[normFullEn]
                if (heToc != null) outTitle = sanitizeFilename(heToc)
            }
            if (outTitle == null) {
                // Fallback: base before comma (for dictionaries like Jastrow)
                val baseEn = englishBaseTitle(path2TitleEnRaw)
                outTitle = outTitle
                    ?: enToOutTitle[normalizeTitle(baseEn)]
                    ?: heToOutTitle[normalizeHeb(baseEn)]
                    ?: enToHe[normalizeTitle(baseEn)]?.let { sanitizeFilename(it) }
                    ?: sanitizeFilename(baseEn)
            }
            // Ensure the mapped target really exists among built books
            if (outTitle == null || !outTitlesSet.contains(outTitle)) continue
            val item = mapOf(
                "line_index_1" to lineIdx1,
                "heRef_2" to outTitle,
                "path_2" to "$outTitle.txt",
                "line_index_2" to lineIdx2,
                "Conection Type" to r.connectionType
            )
            entries += mapToJson(item)
        }
        if (entries.isEmpty()) return null
        return "[" + entries.joinToString(",") + "]"
    }

    private fun englishBaseTitle(t: String): String {
        // If contains comma, take first segment (e.g., "Jastrow, XXX" -> "Jastrow").
        val base = t.substringBefore(',').trim()
        // Remove common section suffixes like "Chapter X", "Section Y" if they define a part of a larger work
        return base
    }

    // Extract footnotes like Python utils. Returns (updatedLines, footnotes, linksJson)
    private fun extractFootnotesAndLinks(lines: List<String>, outTitle: String): Triple<List<String>, List<String>, String?> {
        if (lines.isEmpty()) return Triple(lines, emptyList(), null)
        val updated = ArrayList<String>(lines.size)
        val notes = ArrayList<String>()
        val links = ArrayList<String>()
        for ((idx0, line) in lines.withIndex()) {
            var newLine = line
            var created = false
            try {
                val doc = org.jsoup.Jsoup.parse(line)
                doc.outputSettings().prettyPrint(false)
                val body = doc.body()
                val sups = body.select("sup.footnote-marker")
                if (sups.isNotEmpty()) {
                    for (sup in sups) {
                        // style gray and remove class
                        sup.attr("style", "color: gray;")
                        sup.removeClass("footnote-marker")
                        val next = sup.nextElementSibling()
                        if (next != null && next.tagName().lowercase() == "i" && next.classNames().contains("footnote")) {
                            val txt = next.text()?.trim()
                            next.remove()
                            if (!txt.isNullOrEmpty()) {
                                val note = txt.replace("\n", "<br>")
                                notes.add(note)
                                val linkObj = mapOf(
                                    "line_index_1" to (idx0 + 1),
                                    "heRef_2" to "הערות",
                                    "path_2" to "${"הערות על "+outTitle}.txt",
                                    "line_index_2" to notes.size,
                                    "Conection Type" to "commentary"
                                )
                                links.add(mapToJson(linkObj))
                                created = true
                            } else {
                                sup.remove()
                            }
                        }
                    }
                    newLine = body.html()
                }
            } catch (_: Exception) {
                // keep original line on parser errors
            }
            updated.add(newLine)
        }
        val linksJson = if (links.isNotEmpty()) "[" + links.joinToString(",") + "]" else null
        return Triple(updated, notes, linksJson)
    }

    private fun mergeLinkJsonArrays(a: String?, b: String?): String? {
        if (a.isNullOrBlank() && b.isNullOrBlank()) return null
        if (a.isNullOrBlank()) return b
        if (b.isNullOrBlank()) return a
        return buildString {
            append("[")
            append(a.trim().trimStart('[').trimEnd(']'))
            append(",")
            append(b.trim().trimStart('[').trimEnd(']'))
            append("]")
        }
    }

    private fun mapToJson(map: Map<*, *>): String {
        return buildString {
            append("{")
            var first = true
            for ((k, v) in map) {
                if (!first) append(",") else first = false
                append('"').append(escapeJson(k.toString())).append('"').append(":")
                append(valueToJson(v))
            }
            append("}")
        }
    }

    private fun valueToJson(v: Any?): String = when (v) {
        null -> "null"
        is Number, is Boolean -> v.toString()
        is Iterable<*> -> v.joinToString(prefix = "[", postfix = "]") { valueToJson(it) }
        is Array<*> -> v.joinToString(prefix = "[", postfix = "]") { valueToJson(it) }
        is Map<*, *> -> mapToJson(v)
        else -> '"' + escapeJson(v.toString()) + '"'
    }

    private fun escapeJson(s: String): String = s
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")

    private fun asString(el: JsonElement?): String? = when (el) {
        null -> null
        is JsonPrimitive -> el.contentOrNull
        is JsonArray -> asString(el.firstOrNull())
        is JsonObject -> null
        else -> null
    }
}
