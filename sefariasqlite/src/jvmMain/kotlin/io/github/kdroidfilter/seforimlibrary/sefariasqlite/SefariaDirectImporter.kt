package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.serialization.protobuf.ProtoBuf
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.invariantSeparatorsPathString
import kotlin.io.path.isDirectory
import kotlin.io.path.name
import kotlin.io.path.readText

/**
 * Direct importer: reads Sefaria database_export and writes into SQLite without intermediate Otzaria files.
 * Scope: replicate existing Otzaria-based logic (books/lines/links) with best-effort citation matching.
 */
class SefariaDirectImporter(
    private val exportRoot: Path,
    private val repository: SeforimRepository,
    private val catalogOutput: Path,
    private val logger: Logger = Logger.withTag("SefariaDirectImporter")
    ) {

    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    private data class BookPayload(
        val heTitle: String,
        val enTitle: String,
        val categoriesHe: List<String>,
        val lines: List<String>,
        val refEntries: List<RefEntry>,
        val headings: List<Heading>,
        val authors: List<String>,
        val description: String?,
        val pubDates: List<PubDate>,
        val altStructures: List<AltStructurePayload>
    )

    private data class RefEntry(
        val ref: String,
        val heRef: String,
        val path: String,
        val lineIndex: Int
    )

    private data class Heading(
        val title: String,
        val level: Int,
        val lineIndex: Int
    )

    private data class AltStructurePayload(
        val key: String,
        val title: String?,
        val heTitle: String?,
        val nodes: List<AltNodePayload>
    )

    private data class AltNodePayload(
        val title: String?,
        val heTitle: String?,
        val wholeRef: String?,
        val refs: List<String>,
        val addressTypes: List<String>,
        val childLabel: String?,
        val addresses: List<Int>,
        val skippedAddresses: List<Int>,
        val startingAddress: String?,
        val offset: Int?,
        val children: List<AltNodePayload>
    )

    /**
     * Parse table_of_contents.json to extract category and book orders
     */
    private fun parseTableOfContentsOrders(dbRoot: Path): Pair<Map<String, Int>, Map<String, Int>> {
        val tocFile = dbRoot.resolve("table_of_contents.json")
        if (!Files.exists(tocFile)) {
            logger.w { "table_of_contents.json not found, using default ordering" }
            return Pair(emptyMap(), emptyMap())
        }

        val categoryOrders = mutableMapOf<String, Int>() // category path -> order
        val bookOrders = mutableMapOf<String, Int>()     // book title -> order

        try {
            val tocJson = Files.readString(tocFile)
            val tocEntries = json.parseToJsonElement(tocJson).jsonArray

            fun processTocItem(item: JsonObject, categoryPath: List<String> = emptyList()) {
                // If it's a book (has title but no subcategories with contents)
                val title = item["title"]?.jsonPrimitive?.contentOrNull
                val heTitle = item["heTitle"]?.jsonPrimitive?.contentOrNull
                val order = item["order"]?.jsonPrimitive?.intOrNull
                if (title != null && order != null) {
                    bookOrders[title] = order
                }
                if (heTitle != null && order != null) {
                    bookOrders[heTitle] = order
                    bookOrders[sanitizeFolder(heTitle)] = order
                }

                // If it has a category name, add to category orders
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

                // Process subcategories/books recursively
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

                // Store order with BOTH English and Hebrew names
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

    private fun normalizePriorityEntry(raw: String): String {
        var entry = raw.trim().replace('\\', '/')
        if (entry.startsWith("/")) entry = entry.removePrefix("/")
        return entry.split('/').filter { it.isNotBlank() }.joinToString("/") { sanitizeFolder(it) }
    }

    private fun normalizedBookPath(categories: List<String>, heTitle: String): String =
        (categories.map { sanitizeFolder(it) } + sanitizeFolder(heTitle)).joinToString("/")

    private fun loadPriorityList(): List<String> = try {
        val stream = this::class.java.classLoader.getResourceAsStream("priority.txt") ?: return emptyList()
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

    private fun applyPriorityOrdering(
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

    suspend fun import() {
        val dbRoot = findDatabaseExportRoot(exportRoot)
        val jsonDir = dbRoot.resolve("json")

        // Parse table of contents for ordering
        val (categoryOrders, bookOrders) = parseTableOfContentsOrders(dbRoot)
        val schemaDir = dbRoot.resolve("schemas")
        require(jsonDir.isDirectory() && schemaDir.isDirectory()) { "Missing json/schemas under $dbRoot" }

        val schemaLookup = buildSchemaLookup(schemaDir)
        val bookPayloads = mutableListOf<BookPayload>()

        Files.walk(jsonDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.name.equals("merged.json", ignoreCase = true) }
                .forEach { textPath ->
                    runCatching {
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
                        ) ?: return@runCatching

                        val schemaJson = json.parseToJsonElement(schemaPath.readText()).jsonObject
                        val schemaObj = schemaJson["schema"]?.jsonObject ?: return@runCatching
                        val englishTitle = schemaObj["title"]?.stringOrNull() ?: fileTitle ?: folderName ?: return@runCatching
                        val hebrewTitle = schemaObj["heTitle"]?.stringOrNull() ?: fileHeTitle ?: englishTitle

                        val textElement = textJson["text"] ?: return@runCatching
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
                        bookPayloads += BookPayload(
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
                        logger.i { "Prepared book $hebrewTitle with ${lines.size} lines and ${refs.size} refs" }
                    }.onFailure { e ->
                        logger.w(e) { "Failed to prepare book from $textPath" }
                    }
                }
        }

        val priorityEntries = loadPriorityList()
        val (orderedBookPayloads, missingPriorityEntries) = applyPriorityOrdering(bookPayloads, priorityEntries)
        if (priorityEntries.isNotEmpty()) {
            val matched = priorityEntries.size - missingPriorityEntries.size
            logger.i { "Applied priority ordering for $matched/${priorityEntries.size} entries" }
            if (missingPriorityEntries.isNotEmpty()) {
                logger.w { "Priority entries not found in Sefaria export (first 5): ${missingPriorityEntries.take(5)}" }
            }
        }

        // Build DB entries
        val sourceId = repository.insertSource("Sefaria")
        val categoryIds = mutableMapOf<String, Long>() // key: path string "cat1/cat2"

        suspend fun ensureCategoryPath(pathParts: List<String>): Long {
            var parentId: Long? = null
            val builder = StringBuilder()
            pathParts.forEachIndexed { idx, part ->
                if (builder.isNotEmpty()) builder.append('/')
                builder.append(part)
                val key = builder.toString()
                val existing = categoryIds[key]
                if (existing != null) {
                    parentId = existing
                    return@forEachIndexed
                }
                // Get order from table_of_contents.json, default to 999 if not found
                val categoryOrder = categoryOrders[key] ?: categoryOrders[part] ?: 999
                val cat = Category(
                    id = 0,
                    parentId = parentId,
                    title = part,
                    level = idx,
                    order = categoryOrder
                )
                val id = repository.insertCategory(cat)
                categoryIds[key] = id
                parentId = id
            }
            return parentId ?: throw IllegalStateException("No category created for $pathParts")
        }

        var nextBookId = 1L
        var nextLineId = 1L
        val lineKeyToId = mutableMapOf<Pair<String, Int>, Long>() // (path,lineIndex) -> lineId
        val lineIdToBookId = mutableMapOf<Long, Long>()
        val allRefsWithPath = mutableListOf<RefEntry>()

        for (payload in orderedBookPayloads) {
            val catId = ensureCategoryPath(payload.categoriesHe)
            val bookId = nextBookId++
            val bookPath = buildBookPath(payload.categoriesHe, payload.heTitle)
            // Get order from table_of_contents.json using English title, default to 999 if not found
            val bookOrder = bookOrders[payload.enTitle]?.toFloat() ?: 999f
            val book = Book(
                id = bookId,
                categoryId = catId,
                sourceId = sourceId,
                title = payload.heTitle,
                authors = payload.authors.map { Author(name = it) },
                pubPlaces = emptyList(),
                pubDates = payload.pubDates,
                heShortDesc = payload.description,
                notesContent = null,
                order = bookOrder,
                topics = emptyList(),
                isBaseBook = true,
                totalLines = payload.lines.size,
                hasAltStructures = false
            )
            repository.insertBook(book)

            val refsForBook = payload.refEntries.map { it.copy(path = bookPath) }
            allRefsWithPath += refsForBook

            // Create a mapping from lineIndex to RefEntry for quick lookup
            val refsByLineIndex = payload.refEntries.associateBy { it.lineIndex - 1 }

            payload.lines.forEachIndexed { idx, content ->
                val lineId = nextLineId++
                val refEntry = refsByLineIndex[idx]
                repository.insertLine(
                    Line(
                        id = lineId,
                        bookId = bookId,
                        lineIndex = idx,
                        content = content,
                        ref = refEntry?.ref,
                        heRef = refEntry?.heRef
                    )
                )
                lineKeyToId[bookPath to idx] = lineId
                lineIdToBookId[lineId] = bookId
            }
            // Insert TOC entries hierarchically and build line_toc mappings
            if (payload.headings.isNotEmpty()) {
                val levelStack = ArrayDeque<Pair<Int, Long>>()
                val headingLineToToc = mutableMapOf<Int, Long>()
                val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
                val allTocIds = mutableListOf<Long>()
                val tocParentMap = mutableMapOf<Long, Long?>() // tocId -> parentId

                payload.headings.sortedBy { it.lineIndex }.forEach { h ->
                    while (levelStack.isNotEmpty() && levelStack.last().first >= h.level) levelStack.removeLast()
                    val parent = levelStack.lastOrNull()?.second
                    val lineIdForHeading = lineKeyToId[bookPath to h.lineIndex]
                    val tocId = repository.insertTocEntry(
                        TocEntry(
                            id = 0,
                            bookId = bookId,
                            parentId = parent,
                            textId = null,
                            text = h.title,
                            level = h.level,
                            lineId = lineIdForHeading,
                            isLastChild = false,
                            hasChildren = false
                        )
                    )
                    headingLineToToc[h.lineIndex] = tocId
                    levelStack.addLast(h.level to tocId)
                    allTocIds.add(tocId)
                    tocParentMap[tocId] = parent
                    entriesByParent.getOrPut(parent) { mutableListOf() }.add(tocId)
                }

                // DEUXIÈME PASSE: Mettre à jour hasChildren et isLastChild
                val parentIds = tocParentMap.values.filterNotNull().toSet()
                for (tocId in allTocIds) {
                    if (tocId in parentIds) {
                        repository.updateTocEntryHasChildren(tocId, true)
                    }
                }
                for ((parentId, children) in entriesByParent) {
                    if (children.isNotEmpty()) {
                        val lastChildId = children.last()
                        repository.updateTocEntryIsLastChild(lastChildId, true)
                    }
                }

                // Build line_toc mappings
                val sortedKeys = headingLineToToc.keys.sorted()
                for (lineIdx in payload.lines.indices) {
                    val key = sortedKeys.lastOrNull { it <= lineIdx } ?: continue
                    val tocId = headingLineToToc[key] ?: continue
                    val lineId = lineKeyToId[bookPath to lineIdx] ?: continue
                    repository.executeRawQuery(
                        "INSERT INTO line_toc(lineId, tocEntryId) VALUES ($lineId, $tocId)"
                    )
                }
            }
            // Alternative structures (e.g., Parasha/Aliyah)
            val generatedAltStructures = buildAltTocStructuresForBook(
                payload = payload,
                bookId = bookId,
                bookPath = bookPath,
                lineKeyToId = lineKeyToId,
                totalLines = payload.lines.size
            )
            repository.updateHasAltStructures(bookId, generatedAltStructures)
            if (!generatedAltStructures && payload.altStructures.isNotEmpty()) {
                logger.w { "Alternative structures defined for ${payload.heTitle} but none were generated" }
            }
        }
        // Build citation lookup for links
        val refsByCanonical = allRefsWithPath.groupBy { canonicalCitation(it.ref) }
        val refsByBase = mutableMapOf<String, RefEntry>()
        allRefsWithPath.forEach { entry ->
            val base = canonicalBase(entry.ref)
            val existing = refsByBase[base]
            if (existing == null || entry.lineIndex < existing.lineIndex) {
                refsByBase[base] = entry
            }
        }

        // Insert links directly
        val linksDir = dbRoot.resolve("links")
        if (linksDir.exists()) {
            Files.list(linksDir)
                .filter { it.fileName.toString().endsWith(".csv") }
                .use { stream ->
                    stream.forEach { file ->
                        Files.newBufferedReader(file).use { reader ->
                            val iter = reader.lineSequence().iterator()
                            if (!iter.hasNext()) return@use
                            val headers = parseCsvLine(iter.next()).map { normalizeCitation(it) }
                            val idxC1 = headers.indexOf("Citation 1")
                            val idxC2 = headers.indexOf("Citation 2")
                            val idxConn = headers.indexOf("Conection Type")
                            if (idxC1 < 0 || idxC2 < 0 || idxConn < 0) return@use

                            while (iter.hasNext()) {
                                val row = parseCsvLine(iter.next())
                                if (row.isEmpty()) continue
                                val c1 = normalizeCitation(row.getOrNull(idxC1).orEmpty())
                                val c2 = normalizeCitation(row.getOrNull(idxC2).orEmpty())
                                if (c1.isEmpty() || c2.isEmpty()) continue
                                val conn = row.getOrNull(idxConn)?.trim().orEmpty()

                                val fromRefs = resolveRefs(c1, refsByCanonical, refsByBase)
                                val toRefs = resolveRefs(c2, refsByCanonical, refsByBase)
                                if (fromRefs.isEmpty() || toRefs.isEmpty()) continue

                                for (from in fromRefs) {
                                    for (to in toRefs) {
                                        val srcLine = lineKeyToId[from.path to (from.lineIndex - 1)] ?: continue
                                        val tgtLine = lineKeyToId[to.path to (to.lineIndex - 1)] ?: continue

                                        // Create bidirectional links (from→to and to→from) like SefariaToOtzariaConverter
                                        val linkForward = Link(
                                            sourceBookId = lineBookId(srcLine, lineIdToBookId),
                                            targetBookId = lineBookId(tgtLine, lineIdToBookId),
                                            sourceLineId = srcLine,
                                            targetLineId = tgtLine,
                                            connectionType = ConnectionType.fromString(conn)
                                        )
                                        kotlinx.coroutines.runBlocking { repository.insertLink(linkForward) }

                                        // Insert reverse link (to→from)
                                        val linkReverse = Link(
                                            sourceBookId = lineBookId(tgtLine, lineIdToBookId),
                                            targetBookId = lineBookId(srcLine, lineIdToBookId),
                                            sourceLineId = tgtLine,
                                            targetLineId = srcLine,
                                            connectionType = ConnectionType.fromString(conn)
                                        )
                                        kotlinx.coroutines.runBlocking { repository.insertLink(linkReverse) }
                                    }
                                }
                            }
                        }
                    }
                }
        }

        repository.rebuildCategoryClosure()
        // book_has_links flags & connection flags
        updateBookHasLinks()
        buildAndSaveCatalog()
        logger.i { "Direct Sefaria import completed." }
    }

    private suspend fun buildAltTocStructuresForBook(
        payload: BookPayload,
        bookId: Long,
        bookPath: String,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        totalLines: Int
    ): Boolean {
        if (payload.altStructures.isEmpty()) return false

        var hasGeneratedAltStructures = false

        // Treat Bavli/Yerushalmi tractates specially: only top-level alt nodes,
        // clicking them should jump to the first child (no explicit pages shown).
        val isTalmudTractate = payload.categoriesHe.any { it.contains("תלמוד") }
        // For Shulchan Arukh codes and Tur we also limit to top-level topics (no siman children in the alt tree)
        val isShulchanArukhCode = payload.categoriesHe.any { it.contains("שולחן ערוך") }
        val isTurCode = payload.categoriesHe.any { it.contains("טור") }

        // Local lookup map for this book: canonical ref -> (lineId, lineIndex)
        val refsForBook = payload.refEntries.map { it.copy(path = bookPath) }
        val bookAliasKeys = buildSet {
            val titles = listOf(
                payload.enTitle,
                payload.heTitle,
                sanitizeFolder(payload.enTitle),
                sanitizeFolder(payload.heTitle)
            )
            titles.forEach { title ->
                add(canonicalCitation(title))
                normalizeTitleKey(title)?.let { normalized ->
                    add(canonicalCitation(normalized))
                }
            }
        }.filterNot { it.isBlank() }.toSet()
        val canonicalToLine: Map<String, Pair<Long?, Int?>> = buildMap {
            refsForBook.forEach { entry ->
                val lineIdx = entry.lineIndex - 1
                val lineId = lineKeyToId[bookPath to lineIdx]
                val refsForEntry = listOfNotNull(entry.ref, entry.heRef)
                refsForEntry.forEach { value ->
                    val canonical = canonicalCitation(value)
                    fun addKey(key: String?) {
                        if (key.isNullOrBlank()) return
                        val current = this[key]?.second
                        if (current == null || (lineIdx >= 0 && lineIdx < current)) {
                            put(key, lineId to lineIdx)
                        }
                    }
                    addKey(canonical)
                    addKey(stripBookAlias(canonical, bookAliasKeys))
                    addKey(canonicalTail(value))
                }
            }
        }
        payload.altStructures.forEach { structure ->
            // Psalms 30‑day cycle: only show days, not individual chapters
            val isPsalms30DayCycle = structure.key == "30 Day Cycle"
            val structureId = repository.upsertAltTocStructure(
                AltTocStructure(
                    bookId = bookId,
                    key = structure.key,
                    title = structure.title,
                    heTitle = structure.heTitle
                )
            )

            val headingLineToToc = mutableMapOf<Int, Long>()
            val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
            val entryLineInfo = mutableMapOf<Long, Pair<Long?, Int?>>()
            val usedLineIdsByParent = mutableMapOf<Long?, MutableSet<Long>>()

            fun parseDafIndex(address: String?): Int? {
                if (address.isNullOrBlank()) return null
                val match = Regex("(\\d+)([ab])?", RegexOption.IGNORE_CASE).find(address.trim())
                val (pageStr, amudRaw) = match?.destructured ?: return null
                val page = pageStr.toIntOrNull() ?: return null
                val amud = amudRaw.lowercase()
                val offset = if (amud == "b") 2 else 1
                return ((page - 1) * 2) + offset
            }

            fun computeAddressValue(node: AltNodePayload, idx: Int): Int? {
                node.addresses.getOrNull(idx)?.let { return it }
                val skip = node.skippedAddresses.toSet()
                val base = node.offset
                    ?: parseDafIndex(node.startingAddress)?.minus(1)
                    ?: -1
                if (base < 0) return null
                var current = base
                var steps = idx
                while (steps >= 0) {
                    current += 1
                    if (current in skip) continue
                    steps--
                }
                return current
            }

            fun resolveLineForCitation(
                citation: String?,
                isChapterOrSimanLevel: Boolean,
                allowChapterFallback: Boolean = true,
                allowTailFallback: Boolean = true
            ): Pair<Long?, Int?> {
                if (citation.isNullOrBlank()) return null to null

                fun fallbackWithinChapter(canonical: String): Pair<Long?, Int?>? {
                    if (!canonical.contains(':')) return null
                    val base = canonical.substringBefore(':')
                    val numStr = canonical.substringAfter(':').takeWhile { it.isDigit() }
                    val start = numStr.toIntOrNull() ?: return null
                    for (n in start downTo 1) {
                        val candidate = "$base:$n"
                        val candidates = listOf(candidate, stripBookAlias(candidate, bookAliasKeys))
                        candidates.forEach { key ->
                            if (!key.isNullOrBlank()) {
                                canonicalToLine[key]?.let { return it }
                            }
                        }
                    }
                    return null
                }

                fun lookup(raw: String): Pair<Long?, Int?>? {
                    val canonical = canonicalCitation(raw)
                    val stripped = stripBookAlias(canonical, bookAliasKeys)
                    val tail = canonicalTail(raw)
                    val candidates = buildList {
                        add(canonical)
                        add(stripped)
                        if (allowTailFallback) add(tail)
                    }
                    candidates.forEach { key ->
                        if (!key.isNullOrBlank()) {
                            canonicalToLine[key]?.let { return it }
                        }
                    }
                    val rangeStart = citationRangeStart(canonical)
                    if (rangeStart != null) {
                        val rangeCandidates = buildList {
                            add(rangeStart)
                            add(stripBookAlias(rangeStart, bookAliasKeys))
                            if (allowTailFallback) add(canonicalTail(rangeStart))
                        }
                        rangeCandidates.forEach { key ->
                            if (!key.isNullOrBlank()) {
                                canonicalToLine[key]?.let { return it }
                            }
                        }
                    }

                    if (allowChapterFallback) {
                        val chapterKey = canonical.substringBefore(':').takeIf { it.isNotBlank() }
                        if (chapterKey != null) {
                            val chapterStart = "$chapterKey:1"
                            val chapterCandidates = listOf(
                                chapterStart,
                                stripBookAlias(chapterStart, bookAliasKeys),
                                canonicalTail(chapterStart),
                                chapterKey, // allow bare chapter key
                                stripBookAlias(chapterKey, bookAliasKeys)
                            )
                            chapterCandidates.forEach { key ->
                                if (!key.isNullOrBlank()) {
                                    canonicalToLine[key]?.let { return it }
                                }
                            }
                        }
                    }
                    fallbackWithinChapter(canonical)?.let { return it }
                    return null
                }

                lookup(citation)?.let { return it }

                if (isChapterOrSimanLevel) {
                    // Handle siman/chapter-level refs like "Tur, Orach Chayim 1-7" or "Psalms 10-17"
                    val canonical = canonicalCitation(citation)
                    val base = canonical.substringBefore('-').trim()
                    if (!base.contains(':')) {
                        val withColon = "$base:1"
                        lookup(withColon)?.let { return it }
                    }
                }

                return null to null
            }

            fun mapBaseToHebrew(base: String?): String? {
                if (base.isNullOrBlank()) return null
                val norm = base.lowercase()
                return when {
                    "aliyah" in norm -> "עליה"
                    "daf" in norm -> "דף"
                    "chapter" in norm -> "פרק"
                    "perek" in norm -> "פרק"
                    "siman" in norm -> "סימן"
                    "section" in norm -> "סימן"
                    "psalm" in norm || "psalms" in norm -> "מזמור"
                    "day" in norm -> "יום"
                    else -> base
                }
            }

            fun buildChildLabel(base: String?, idx: Int, addressValue: Int?, addressType: String?): String {
                val numericValue = (addressValue ?: (idx + 1)).coerceAtLeast(1)
                val suffix = if (addressType.equals("Talmud", ignoreCase = true)) {
                    toDaf(numericValue)
                } else {
                    toGematria(numericValue)
                }
                val hebBase = mapBaseToHebrew(base)
                val cleanBase = hebBase?.takeIf { it.isNotBlank() }
                return cleanBase?.let { "$it $suffix" } ?: suffix
            }

            suspend fun updateParentLineIfMissing(tocId: Long) {
                val current = entryLineInfo[tocId]
                if (current?.second != null) return
                val childWithLine = entriesByParent[tocId]
                    ?.firstNotNullOfOrNull { childId ->
                        entryLineInfo[childId]?.second?.let { idx -> childId to (entryLineInfo[childId]!!) }
                    }
                val childLine = childWithLine?.second ?: return
                val lineId = childLine.first ?: return
                val lineIndex = childLine.second ?: return
                repository.updateAltTocEntryLineId(tocId, lineId)
                entryLineInfo[tocId] = lineId to lineIndex
                headingLineToToc[lineIndex] = tocId
            }

            fun nodeLabel(node: AltNodePayload, position: Int?): String {
                if (!node.heTitle.isNullOrBlank()) return node.heTitle
                if (!node.title.isNullOrBlank()) return node.title

                val addressType = node.addressTypes.firstOrNull()
                val addrValue = computeAddressValue(node, 0)
                val base = mapBaseToHebrew(node.childLabel)
                    ?: if (addressType.equals("Talmud", ignoreCase = true)) "דף" else null
                val suffix = when {
                    addrValue != null && addressType.equals("Talmud", ignoreCase = true) -> toDaf(addrValue)
                    addrValue != null -> toGematria(addrValue)
                    position != null -> toGematria(position + 1)
                    else -> toGematria(1)
                }
                return base?.let { "$it $suffix" } ?: "פרק $suffix"
            }

            suspend fun addEntry(node: AltNodePayload, level: Int, parentId: Long?, position: Int?): Long {
                val isChapterOrSimanLevel = node.addressTypes.any {
                    it.equals("Siman", ignoreCase = true) ||
                            it.equals("Perek", ignoreCase = true) ||
                            it.equals("Chapter", ignoreCase = true)
                }
                val isDafNode = node.addressTypes.any { it.equals("Talmud", ignoreCase = true) }

                // Try wholeRef first, then fall back to first child ref that resolves
                val primaryCandidates = buildList {
                    node.wholeRef?.let { add(it) }
                    addAll(node.refs)
                }
                var lineId: Long? = null
                var lineIndex: Int? = null
                for (candidate in primaryCandidates) {
                    val (lid, lidx) = resolveLineForCitation(
                        candidate,
                        isChapterOrSimanLevel,
                        allowChapterFallback = !isDafNode,
                        allowTailFallback = !isDafNode
                    )
                    if (lid != null && lidx != null) {
                        lineId = lid
                        lineIndex = lidx
                        break
                    }
                }
                // If we can't anchor the parent node at all, skip it
                if (lineId == null || lineIndex == null) return 0L
                val text = nodeLabel(node, position)

                if (lineId != null) {
                    val used = usedLineIdsByParent.getOrPut(parentId) { mutableSetOf() }
                    if (lineId in used) return 0L
                    used += lineId
                }

                val tocId = repository.insertAltTocEntry(
                    AltTocEntry(
                        structureId = structureId,
                        parentId = parentId,
                        textId = null,
                        text = text,
                        level = level,
                        lineId = lineId,
                        isLastChild = false,
                        hasChildren = false
                    )
                )
                hasGeneratedAltStructures = true
                entryLineInfo[tocId] = lineId to lineIndex
                entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocId)
                if (lineIndex != null) {
                    headingLineToToc[lineIndex] = tocId
                }

                // One extra level: direct children from node.refs (except for Talmud tractates
                // and Shulchan Arukh codes, where we only want the top-level structure and
                // clicking it should go to the first child section).
                var hasChild = false
                if (!isTalmudTractate && !isShulchanArukhCode && !isTurCode && !isPsalms30DayCycle && node.refs.isNotEmpty()) {
                    for ((idx, ref) in node.refs.withIndex()) {
                        val (childLineId, childLineIndex) = resolveLineForCitation(
                            ref,
                            isChapterOrSimanLevel,
                            allowChapterFallback = !isDafNode,
                            allowTailFallback = !isDafNode
                        )
                        // Only create entries that are anchored to a real line
                        if (childLineId == null || childLineIndex == null) continue

                        val used = usedLineIdsByParent.getOrPut(tocId) { mutableSetOf() }
                        if (childLineId in used) continue
                        used += childLineId

                        val addressValue = computeAddressValue(node, idx)
                        val label = buildChildLabel(node.childLabel, idx, addressValue, node.addressTypes.firstOrNull())
                        val childTocId = repository.insertAltTocEntry(
                            AltTocEntry(
                                structureId = structureId,
                                parentId = tocId,
                                textId = null,
                                text = label,
                                level = level + 1,
                                lineId = childLineId,
                                isLastChild = false,
                                hasChildren = false
                            )
                        )
                        hasGeneratedAltStructures = true
                        hasChild = true
                        entryLineInfo[childTocId] = childLineId to childLineIndex
                        entriesByParent.getOrPut(tocId) { mutableListOf() }.add(childTocId)
                        headingLineToToc[childLineIndex] = childTocId
                    }
                }

                if (hasChild) {
                    repository.updateAltTocEntryHasChildren(tocId, true)
                }

                return tocId
            }

            suspend fun createContainerEntry(node: AltNodePayload, level: Int, parentId: Long?, position: Int?): Long {
                val text = when {
                    !node.heTitle.isNullOrBlank() -> node.heTitle
                    position != null -> "פרק ${toGematria(position + 1)}"
                    !node.title.isNullOrBlank() -> node.title
                    !structure.heTitle.isNullOrBlank() -> structure.heTitle!!
                    !structure.title.isNullOrBlank() -> structure.title!!
                    else -> structure.key
                }
                val tocId = repository.insertAltTocEntry(
                    AltTocEntry(
                        structureId = structureId,
                        parentId = parentId,
                        textId = null,
                        text = text,
                        level = level,
                        lineId = null,
                        isLastChild = false,
                        hasChildren = false
                    )
                )
                entryLineInfo[tocId] = null to null
                entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocId)
                return tocId
            }

            // Traverse the alt tree, keeping grouping nodes (with titles) as containers when they have children.
            suspend fun traverseAltNode(node: AltNodePayload, level: Int, parentId: Long?, position: Int?): Boolean {
                val hasOwnRefs = node.wholeRef != null || node.refs.isNotEmpty()
                val hasTitle = !node.heTitle.isNullOrBlank() || !node.title.isNullOrBlank()
                val isDafNode = node.addressTypes.any { it.equals("Talmud", ignoreCase = true) }
                val inlineChildrenOnly = isDafNode && node.refs.isNotEmpty() && !hasTitle
                var currentParent = parentId
                var containerId: Long? = null
                var inserted = false

                if (!hasOwnRefs && node.children.isNotEmpty() && hasTitle) {
                    containerId = createContainerEntry(node, level, parentId, position)
                    currentParent = containerId
                }

                if (inlineChildrenOnly) {
                    node.refs.forEachIndexed { idx, ref ->
                        val (childLineId, childLineIndex) = resolveLineForCitation(
                            ref,
                            isChapterOrSimanLevel = false,
                        allowChapterFallback = false,
                        allowTailFallback = false
                    )
                        if (childLineId == null || childLineIndex == null) return@forEachIndexed
                        val addressValue = computeAddressValue(node, idx)
                        val label = buildChildLabel(node.childLabel, idx, addressValue, node.addressTypes.firstOrNull())
                        if (childLineId in usedLineIdsByParent.getOrPut(currentParent) { mutableSetOf() }) return@forEachIndexed
                        usedLineIdsByParent.getOrPut(currentParent) { mutableSetOf() } += childLineId

                        val childId = repository.insertAltTocEntry(
                            AltTocEntry(
                                structureId = structureId,
                                parentId = currentParent,
                                textId = null,
                                text = label,
                                level = level,
                                lineId = childLineId,
                                isLastChild = false,
                                hasChildren = false
                            )
                        )
                        hasGeneratedAltStructures = true
                        inserted = true
                        entryLineInfo[childId] = childLineId to childLineIndex
                        entriesByParent.getOrPut(currentParent) { mutableListOf() }.add(childId)
                        headingLineToToc[childLineIndex] = childId
                    }
                } else if (hasOwnRefs) {
                    val tocId = addEntry(node, level, parentId, position)
                    if (tocId != 0L) {
                        entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocId)
                        inserted = true
                        if (node.children.isNotEmpty()) {
                            currentParent = tocId
                        }
                    }
                }

                var childInserted = false
                if (node.children.isNotEmpty()) {
                    val childLevel = level + if (currentParent != null && currentParent != parentId) 1 else 0
                    node.children.forEachIndexed { idx, child ->
                        if (traverseAltNode(child, childLevel, currentParent, idx)) {
                            childInserted = true
                        }
                    }
                    if (currentParent == containerId && childInserted) {
                        repository.updateAltTocEntryHasChildren(containerId!!, true)
                    } else if (currentParent != null && currentParent != parentId && childInserted) {
                        repository.updateAltTocEntryHasChildren(currentParent, true)
                    }
                }

                if (containerId != null) {
                    val hasChildren = entriesByParent[containerId].orEmpty().isNotEmpty()
                    if (hasChildren) {
                        repository.updateAltTocEntryHasChildren(containerId, true)
                        updateParentLineIfMissing(containerId)
                        if (entryLineInfo[containerId]?.second != null) {
                            hasGeneratedAltStructures = true
                            inserted = true
                        }
                    } else {
                        // No anchored children, drop the container to avoid empty duplicates
                        repository.executeRawQuery("DELETE FROM alt_toc_entry WHERE id=$containerId")
                        entriesByParent[parentId]?.remove(containerId)
                        entryLineInfo.remove(containerId)
                    }
                }

                return inserted || childInserted
            }

            structure.nodes.forEachIndexed { idx, node ->
                traverseAltNode(node, level = 0, parentId = null, position = idx)
            }

            // Mark last child flags (single-level tree)
            for ((_, children) in entriesByParent) {
                if (children.isNotEmpty()) {
                    val lastChildId = children.last()
                    repository.updateAltTocEntryIsLastChild(lastChildId, true)
                }
            }

            // Build line_alt_toc mappings based on nearest heading at or before each line
            val sortedKeys = headingLineToToc.keys.sorted()
            for (lineIdx in 0 until totalLines) {
                val key = sortedKeys.lastOrNull { it <= lineIdx } ?: continue
                val tocId = headingLineToToc[key] ?: continue
                val lineId = lineKeyToId[bookPath to lineIdx] ?: continue
                repository.upsertLineAltToc(lineId, structureId, tocId)
            }
        }
        return hasGeneratedAltStructures
    }

    private fun buildBookPath(categories: List<String>, title: String): String {
        val parts = mutableListOf<String>()
        parts += categories
        parts += title
        return parts.joinToString(separator = "/")
    }

    private fun lineBookId(lineId: Long, lineIdToBookId: Map<Long, Long>): Long =
        lineIdToBookId[lineId] ?: 0

    private fun buildBookContent(
        schemaObj: JsonObject,
        textElement: JsonElement,
        bookHeTitle: String,
        bookEnTitle: String,
        authors: List<String>
    ): Triple<List<String>, List<RefEntry>, List<Heading>> {
        val output = mutableListOf<String>()
        val refs = mutableListOf<RefEntry>()
        val headings = mutableListOf<Heading>()

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
            val heTitle = node["heTitle"]?.stringOrNull().orEmpty()
            val key = node["key"]?.stringOrNull()
            val isDefault = key.equals("default", ignoreCase = true)
            val hasTitle = heTitle.isNotBlank()

            // Only add heading if we have a title to display
            if (hasTitle) {
                val tagNode = headingTagForLevel(level)
                output += "${tagNode.first}$heTitle${tagNode.second}"
                headings += Heading(title = heTitle, level = level, lineIndex = output.size - 1)
            }

            if (node.containsKey("nodes")) {
                val children = node["nodes"]?.jsonArray ?: JsonArray(emptyList())
                val textObject = text as? JsonObject
                for (childElement in children) {
                    val child = childElement.jsonObject
                    val childKey = child["key"]?.stringOrNull()
                    val childTitle = child["title"]?.stringOrNull().orEmpty()
                    val childHeTitle = child["heTitle"]?.stringOrNull().orEmpty()
                    val childText = if (textObject != null) {
                        if (!childKey.equals("default", ignoreCase = true) && childTitle.isNotBlank()) {
                            textObject[childTitle]
                        } else {
                            textObject[""] ?: textObject[childTitle]
                        }
                    } else null
                    val newRefPrefix = buildString {
                        append(refPrefix)
                        if (!childKey.equals("default", ignoreCase = true) && childTitle.isNotBlank()) append(childTitle).append(", ")
                    }
                    val newHeRefPrefix = buildString {
                        append(heRefPrefix)
                        if (!childKey.equals("default", ignoreCase = true) && childHeTitle.isNotBlank()) append(childHeTitle).append(", ")
                    }
                    // All direct children of a node should be at the same level
                    processNode(child, childText, level + 1, newRefPrefix, newHeRefPrefix)
                }
            } else {
                val sectionNames = node["heSectionNames"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
                val depth = node["depth"]?.jsonPrimitive?.intOrNullSafe() ?: sectionNames.size
                val addressTypes = node["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
                val referenceableSections = node["referenceableSections"]?.jsonArray?.mapNotNull { it.jsonPrimitive.booleanOrNull } ?: emptyList()
                // Don't increment level for default nodes without title - they should be siblings, not children
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
                        path = "", // filled later when mapping lines
                        lineIndex = output.size
                    )
                )
            }
            return
        }
        if (text !is JsonArray) return
        val index = (sectionNames.size - depth).coerceAtLeast(0)
        val sectionName = sectionNames.getOrNull(index) ?: ""

        // Count non-empty items at this depth level to determine if numbering is needed
        val nonEmptyCount = if (depth == 1) {
            text.count { !it.isTriviallyEmpty() }
        } else {
            0
        }

        text.forEachIndexed { idx, item ->
            if (item.isTriviallyEmpty()) return@forEachIndexed

            // Use addressTypes from schema
            val currentAddressType = addressTypes.getOrNull(addressTypes.size - depth)
            val letter = when (currentAddressType) {
                "Talmud" -> toDaf(idx + 1)            // Talmud pages use Daf notation
                "Integer" -> toGematria(idx + 1)      // Prefer gematria even when schema marks Integer
                else -> toGematria(idx + 1)           // Default to gematria for Hebrew numbering
            }

            // Check if this level should show inline prefixes using schema's referenceableSections
            // Only add prefix if there's more than one line in the section
            val sectionIndex = sectionNames.size - depth
            val isReferenceable = referenceableSections.getOrNull(sectionIndex) ?: true
            val nextLinePrefix = if (depth == 1 && isReferenceable && currentAddressType != "Integer" && nonEmptyCount > 1) {
                "($letter) "
            } else {
                ""
            }

            // Add intermediate section headings using schema's referenceableSections
            // Only generate headings if:
            // 1. depth > 1 (not the leaf level)
            // 2. sectionName is not empty (schema defines a name for this level)
            // 3. isReferenceable is true (schema marks this level as referenceable)
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
                // Use addressTypes for English reference format
                val refNumber = when (currentAddressType) {
                    "Talmud" -> toEnglishDaf(idx + 1)  // e.g., "2a", "2b"
                    else -> (idx + 1).toString()  // Default to integer for English references
                }
                append(refNumber)
                append(":")
            }
            val newHeRefPrefix = buildString {
                append(heRefPrefix)
                append(letter)  // Use the same letter variable we computed above
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
                addressTypes = addressTypes,  // Pass addressTypes through recursion
                referenceableSections = referenceableSections  // Pass referenceableSections through recursion
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

    private fun findDatabaseExportRoot(base: Path): Path {
        if (isDatabaseExportCandidate(base)) return base
        val direct = base.resolve("database_export")
        if (isDatabaseExportCandidate(direct)) return direct

        Files.newDirectoryStream(base).use { ds ->
            for (entry in ds) {
                if (entry.isDirectory() && isDatabaseExportCandidate(entry.resolve("database_export"))) {
                    return entry.resolve("database_export")
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
                }
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

    private fun sanitizeFolder(name: String?): String {
        if (name.isNullOrBlank()) return ""
        // Convert ASCII double quotes to Hebrew guersayim (״) instead of removing them
        // Sefaria uses " instead of ״ in their JSON
        return name.replace("\"", "״").trim()
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

    private fun trimTrailingSeparators(value: String): String =
        value.trimEnd(':', ' ', ',')

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

    private fun JsonElement?.stringOrNull(): String? = (this as? JsonPrimitive)?.contentOrNull
    private fun JsonPrimitive.intOrNullSafe(): Int? = runCatching { this.int }.getOrNull()

    private fun JsonElement?.isTriviallyEmpty(): Boolean {
        return when (this) {
            null, JsonNull -> true
            is JsonPrimitive -> this.contentOrNull.isNullOrBlank() && this.booleanOrNull == null
            is JsonArray -> this.isEmpty() || this.all { it.isTriviallyEmpty() }
            is JsonObject -> this.isEmpty()
        }
    }

    // --- Links helpers ---
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

    private fun normalizeCitation(raw: String): String =
        raw.trim().trim('"', '\'').replace("\\s+".toRegex(), " ")

    private fun canonicalCitation(raw: String): String =
        normalizeCitation(raw).replace(",", "").lowercase()

    private fun canonicalTail(raw: String): String {
        val canonical = canonicalCitation(raw)
        val tokens = canonical.split(' ').filter { it.isNotBlank() }
        val startIdx = tokens.indexOfFirst { token ->
            token.any { it.isDigit() } || token.contains(':') || token.contains('-')
        }
        return if (startIdx >= 0) tokens.drop(startIdx).joinToString(" ") else canonical
    }

    private fun stripBookAlias(canonical: String, aliases: Set<String>): String {
        var result = canonical
        for (alias in aliases) {
            if (alias.isBlank()) continue
            if (result == alias) {
                result = ""
                break
            }
            if (result.startsWith("$alias ")) {
                result = result.removePrefix(alias).trimStart()
                break
            }
        }
        return result.ifBlank { canonical }
    }

    private fun canonicalBase(citation: String): String {
        val normalized = canonicalCitation(citation)
        // For Talmud, keep daf+amud (e.g., "2a") as base; strip only the line/segment after colon
        val stripAfterColon = normalized.replace(Regex(":\\d+[ab]?(?:-\\d+[ab]?)?$"), "")
        // If nothing left (e.g., no colon), fall back to removing trailing line numbers after space
        return stripAfterColon
            .replace(Regex(" +(\\d+[ab]?)$"), " $1") // keep daf marker
            .trim()
    }

    private fun citationRangeStart(citation: String): String? {
        val dashParts = citation.split('-', limit = 2)
        val start = dashParts.firstOrNull()?.trim().orEmpty()
        if (start.isBlank()) return null
        return canonicalCitation(start)
    }

    private fun resolveRefs(
        citation: String,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>
    ): List<RefEntry> {
        val canonical = canonicalCitation(citation)
        refsByCanonical[canonical]?.let { if (it.isNotEmpty()) return it }

        val rangeStart = citationRangeStart(canonical)
        if (rangeStart != null) {
            refsByCanonical[rangeStart]?.let { if (it.isNotEmpty()) return it }
            refsByBase[canonicalBase(rangeStart)]?.let { return listOf(it) }
            if (!rangeStart.contains(":")) {
                val baseWithOne = canonicalBase("$rangeStart 1")
                refsByBase[baseWithOne]?.let { return listOf(it) }
            }
        }

        // If the citation stops one level early (e.g., "96:4" vs stored "96:4:1"),
        // try to anchor to the first child by appending :1.
        if (canonical.count { it == ':' } == 1) {
            val canonicalWithOne = "$canonical:1"
            refsByCanonical[canonicalWithOne]?.let { if (it.isNotEmpty()) return it }
            refsByBase[canonicalBase(canonicalWithOne)]?.let { return listOf(it) }
        }

        refsByBase[canonicalBase(canonical)]?.let { return listOf(it) }
        if (!canonical.contains(":")) {
            val baseWithOne = canonicalBase("$canonical 1")
            refsByBase[baseWithOne]?.let { return listOf(it) }
        }
        return emptyList()
    }

    private suspend fun updateBookHasLinks() {
        repository.executeRawQuery(
            "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
                    "SELECT id, 0, 0 FROM book"
        )
        repository.executeRawQuery("UPDATE book_has_links SET hasSourceLinks=0, hasTargetLinks=0")
        repository.executeRawQuery(
            "UPDATE book_has_links SET hasSourceLinks=1 " +
                    "WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
        )
        repository.executeRawQuery(
            "UPDATE book_has_links SET hasTargetLinks=1 " +
                    "WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
        )

        repository.executeRawQuery(
            "UPDATE book SET hasTargumConnection=0, hasReferenceConnection=0, hasCommentaryConnection=0, hasOtherConnection=0"
        )

        suspend fun setConnFlag(typeName: String, column: String) {
            val sql = "UPDATE book SET $column=1 WHERE id IN (" +
                    "SELECT DISTINCT bId FROM (" +
                    "SELECT sourceBookId AS bId FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId WHERE ct.name='$typeName' " +
                    "UNION " +
                    "SELECT targetBookId AS bId FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId WHERE ct.name='$typeName'" +
                    ")" +
                    ")"
            repository.executeRawQuery(sql)
        }

        setConnFlag("TARGUM", "hasTargumConnection")
        setConnFlag("REFERENCE", "hasReferenceConnection")
        setConnFlag("COMMENTARY", "hasCommentaryConnection")
        setConnFlag("OTHER", "hasOtherConnection")
    }

    private suspend fun buildAndSaveCatalog() {
        logger.i { "Building precomputed catalog..." }
        val catalog = buildCatalogTree()
        saveCatalog(catalog, catalogOutput)
        logger.i { "Saved catalog to $catalogOutput (size=${catalogOutput.toFile().length() / 1024} KB)" }
    }

    private suspend fun buildCatalogTree(): PrecomputedCatalog {
        val allBooks = repository.getAllBooks()
        val booksByCategory = allBooks.groupBy { it.categoryId }
        val rootCategories = repository.getRootCategories().sortedBy { it.order }
        var totalCategories = 0

        logger.i { "Building catalog from ${allBooks.size} books" }

        val catalogRoots = rootCategories.map { root ->
            buildCatalogCategoryRecursive(root, booksByCategory).also {
                totalCategories += countCategories(it)
            }
        }

        logger.i { "Built catalog with $totalCategories categories and ${allBooks.size} books" }

        return PrecomputedCatalog(
            rootCategories = catalogRoots,
            version = 1,
            totalBooks = allBooks.size,
            totalCategories = totalCategories
        )
    }

    private suspend fun buildCatalogCategoryRecursive(
        category: Category,
        booksByCategory: Map<Long, List<Book>>
    ): CatalogCategory {
        // Get books in this category and sort by order
        val catBooks = booksByCategory[category.id]?.map { book ->
            CatalogBook(
                id = book.id,
                categoryId = book.categoryId,
                title = book.title,
                order = book.order,
                authors = book.authors.map { it.name },
                totalLines = book.totalLines,
                isBaseBook = book.isBaseBook,
                hasTargumConnection = book.hasTargumConnection,
                hasReferenceConnection = book.hasReferenceConnection,
                hasCommentaryConnection = book.hasCommentaryConnection,
                hasOtherConnection = book.hasOtherConnection,
                hasAltStructures = book.hasAltStructures
            )
        }?.sortedBy { it.order } ?: emptyList()

        // Get subcategories and build them recursively, sorted by order
        val subCategories = repository.getCategoryChildren(category.id)
            .sortedBy { it.order }
            .map {
                buildCatalogCategoryRecursive(it, booksByCategory)
            }

        return CatalogCategory(
            id = category.id,
            title = category.title,
            level = category.level,      // Use DB value, not calculated!
            parentId = category.parentId, // Use DB value, not calculated!
            books = catBooks,
            subcategories = subCategories
        )
    }

    private fun countCategories(root: CatalogCategory): Int {
        var total = 1
        for (child in root.subcategories) total += countCategories(child)
        return total
    }

    private fun saveCatalog(catalog: PrecomputedCatalog, outputPath: Path) {
        val bytes = ProtoBuf.encodeToByteArray(PrecomputedCatalog.serializer(), catalog)
        outputPath.toFile().parentFile?.mkdirs()
        Files.write(outputPath, bytes)
    }
}
