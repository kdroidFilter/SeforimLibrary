package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import kotlinx.serialization.protobuf.ProtoBuf
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.name
import kotlin.io.path.readText

/**
 * Direct importer: reads Sefaria database_export and writes into SQLite without intermediate Otzaria files.
 * Scope: replicate existing Otzaria-based logic (books/lines/links) with best-effort citation matching.
 *
 * OPTIMIZED VERSION: Uses batch processing, parallel file reading, and transaction grouping.
 */
class SefariaDirectImporter(
    private val exportRoot: Path,
    private val repository: SeforimRepository,
    private val catalogOutput: Path,
    private val logger: Logger = Logger.withTag("SefariaDirectImporter")
) {

    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    // Batch sizes for optimal performance
    companion object {
        private const val LINE_BATCH_SIZE = 5000
        private const val LINK_BATCH_SIZE = 2000
        private const val TOC_BATCH_SIZE = 1000
        private const val FILE_PARALLELISM = 8
    }

    private data class BookMeta(
        val isBaseBook: Boolean,
        val categoryLevel: Int
    )

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

    @Serializable
    private data class DefaultCommentatorsEntry(
        val book: String,
        val commentators: List<String>
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

        val categoryOrders = ConcurrentHashMap<String, Int>()
        val bookOrders = ConcurrentHashMap<String, Int>()

        try {
            val tocJson = Files.readString(tocFile)
            val tocEntries = json.parseToJsonElement(tocJson).jsonArray

            fun processTocItem(item: JsonObject, categoryPath: List<String> = emptyList()) {
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

    /**
     * Loads default commentators configuration from bundled JSON.
     * Returns a map keyed by normalized base-book title → ordered list of normalized commentator titles.
     */
    private fun loadDefaultCommentatorsConfig(): Map<String, List<String>> = try {
        val stream = this::class.java.classLoader.getResourceAsStream("default_commentators.json") ?: return emptyMap()
        val jsonText = stream.bufferedReader(Charsets.UTF_8).use { it.readText() }
        val entries = json.decodeFromString<List<DefaultCommentatorsEntry>>(jsonText)
        entries.mapNotNull { entry ->
            val bookKey = normalizeTitleKey(entry.book)
            if (bookKey.isNullOrBlank()) return@mapNotNull null
            val commentatorKeys = entry.commentators
                .mapNotNull { normalizeTitleKey(it) }
                .filter { it.isNotBlank() }
            if (commentatorKeys.isEmpty()) return@mapNotNull null
            bookKey to commentatorKeys
        }.toMap()
    } catch (e: Exception) {
        logger.w(e) { "Unable to read default_commentators.json, continuing without default commentators" }
        emptyMap()
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

    suspend fun import() = coroutineScope {
        val dbRoot = findDatabaseExportRoot(exportRoot)
        val jsonDir = dbRoot.resolve("json")

        // Parse table of contents for ordering
        val (categoryOrders, bookOrders) = parseTableOfContentsOrders(dbRoot)
        val schemaDir = dbRoot.resolve("schemas")
        require(jsonDir.isDirectory() && schemaDir.isDirectory()) { "Missing json/schemas under $dbRoot" }

        val schemaLookup = buildSchemaLookup(schemaDir)

        // OPTIMIZATION: Read and parse files in parallel
        logger.i { "Starting parallel file processing..." }
        val bookPayloads = readBooksInParallel(jsonDir, schemaDir, schemaLookup)
        logger.i { "Parsed ${bookPayloads.size} books" }

        val priorityEntries = loadPriorityList()
        val (orderedBookPayloads, missingPriorityEntries) = applyPriorityOrdering(bookPayloads, priorityEntries)
        val baseBookKeys = priorityEntries.toSet()

        // Load default commentators configuration (per base-book title) from resources
        val defaultCommentatorsConfig = loadDefaultCommentatorsConfig()

        if (priorityEntries.isNotEmpty()) {
            val matched = priorityEntries.size - missingPriorityEntries.size
            logger.i { "Applied priority ordering for $matched/${priorityEntries.size} entries" }
            if (missingPriorityEntries.isNotEmpty()) {
                logger.w { "Priority entries not found in Sefaria export (first 5): ${missingPriorityEntries.take(5)}" }
            }
        }

        // OPTIMIZATION: Disable synchronous writes during bulk import
        repository.executeRawQuery("PRAGMA synchronous = OFF")
        repository.executeRawQuery("PRAGMA journal_mode = MEMORY")
        repository.executeRawQuery("PRAGMA cache_size = -64000") // 64MB cache

        // Build DB entries
        val sourceId = repository.insertSource("Sefaria")
        val categoryIds = ConcurrentHashMap<String, Long>()
        val categoryLevelsById = ConcurrentHashMap<Long, Int>()

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
                categoryLevelsById[id] = idx
                parentId = id
            }
            return parentId ?: throw IllegalStateException("No category created for $pathParts")
        }

        val nextBookId = AtomicLong(1L)
        val nextLineId = AtomicLong(1L)
        val lineKeyToId = ConcurrentHashMap<Pair<String, Int>, Long>()
        val lineIdToBookId = ConcurrentHashMap<Long, Long>()
        val allRefsWithPath = mutableListOf<RefEntry>()
        val bookMetaById = ConcurrentHashMap<Long, BookMeta>()
        val normalizedTitleToBookId = ConcurrentHashMap<String, Long>()

        // OPTIMIZATION: Batch line insertions
        val lineBatch = mutableListOf<Line>()
        val lineTocBatch = mutableListOf<Pair<Long, Long>>() // lineId, tocId

        logger.i { "Inserting books and lines..." }
        var processedBooks = 0

        for (payload in orderedBookPayloads) {
            val catId = ensureCategoryPath(payload.categoriesHe)
            val bookId = nextBookId.getAndIncrement()
            val bookPath = buildBookPath(payload.categoriesHe, payload.heTitle)
            val bookOrder = bookOrders[payload.enTitle]?.toFloat() ?: 999f
            val normalizedPath = normalizedBookPath(payload.categoriesHe, payload.heTitle)
            val isBaseBook = normalizedPath in baseBookKeys

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
                isBaseBook = isBaseBook,
                totalLines = payload.lines.size,
                hasAltStructures = false
            )
            repository.insertBook(book)

            // Track normalized titles (Hebrew/English) for later default-commentator mapping
            listOf(payload.heTitle, payload.enTitle).forEach { title ->
                val normalized = normalizeTitleKey(title)
                if (normalized != null) {
                    normalizedTitleToBookId.putIfAbsent(normalized, bookId)
                }
            }

            val catLevel = categoryLevelsById[catId] ?: payload.categoriesHe.lastIndex.coerceAtLeast(0)
            bookMetaById[bookId] = BookMeta(isBaseBook = book.isBaseBook, categoryLevel = catLevel)

            val refsForBook = payload.refEntries.map { it.copy(path = bookPath) }
            synchronized(allRefsWithPath) {
                allRefsWithPath += refsForBook
            }

            // Create a mapping from lineIndex to RefEntry for quick lookup
            val refsByLineIndex = payload.refEntries.associateBy { it.lineIndex - 1 }

            // OPTIMIZATION: Batch lines
            payload.lines.forEachIndexed { idx, content ->
                val lineId = nextLineId.getAndIncrement()
                val refEntry = refsByLineIndex[idx]
                lineBatch += Line(
                    id = lineId,
                    bookId = bookId,
                    lineIndex = idx,
                    content = content,
                    ref = refEntry?.ref,
                    heRef = refEntry?.heRef
                )
                lineKeyToId[bookPath to idx] = lineId
                lineIdToBookId[lineId] = bookId

                // Flush batch when full
                if (lineBatch.size >= LINE_BATCH_SIZE) {
                    repository.insertLinesBatch(lineBatch)
                    lineBatch.clear()
                }
            }

            // Insert TOC entries hierarchically and build line_toc mappings
            if (payload.headings.isNotEmpty()) {
                insertTocEntriesOptimized(
                    payload = payload,
                    bookId = bookId,
                    bookPath = bookPath,
                    lineKeyToId = lineKeyToId,
                    lineTocBatch = lineTocBatch
                )
            }

            // Alternative structures
            val generatedAltStructures = buildAltTocStructuresForBook(
                payload = payload,
                bookId = bookId,
                bookPath = bookPath,
                lineKeyToId = lineKeyToId,
                totalLines = payload.lines.size
            )
            repository.updateHasAltStructures(bookId, generatedAltStructures)

            processedBooks++
            if (processedBooks % 100 == 0) {
                logger.i { "Processed $processedBooks/${orderedBookPayloads.size} books" }
            }
        }

        // Flush remaining lines
        if (lineBatch.isNotEmpty()) {
            repository.insertLinesBatch(lineBatch)
            lineBatch.clear()
        }

        // Flush line_toc mappings
        if (lineTocBatch.isNotEmpty()) {
            repository.insertLineTocBatch(lineTocBatch)
            lineTocBatch.clear()
        }

        logger.i { "Inserted all books and lines" }

        // Apply default commentators mapping based on configuration and inserted books
        if (defaultCommentatorsConfig.isNotEmpty()) {
            applyDefaultCommentators(defaultCommentatorsConfig, normalizedTitleToBookId)
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

        // OPTIMIZATION: Process links in parallel and batch insert
        val linksDir = dbRoot.resolve("links")
        if (linksDir.exists()) {
            logger.i { "Processing links..." }
            processLinksInParallel(
                linksDir = linksDir,
                refsByCanonical = refsByCanonical,
                refsByBase = refsByBase,
                lineKeyToId = lineKeyToId,
                lineIdToBookId = lineIdToBookId,
                bookMetaById = bookMetaById
            )
            logger.i { "Links processed" }
        }

        // Re-enable normal SQLite settings
        repository.executeRawQuery("PRAGMA synchronous = NORMAL")
        repository.executeRawQuery("PRAGMA journal_mode = DELETE")

        repository.rebuildCategoryClosure()
        updateBookHasLinks()
        buildAndSaveCatalog()
        logger.i { "Direct Sefaria import completed." }
    }

    /**
     * OPTIMIZATION: Read and parse book files in parallel using coroutines
     */
    private suspend fun readBooksInParallel(
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
        val semaphore = kotlinx.coroutines.sync.Semaphore(FILE_PARALLELISM)

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

    /**
     * OPTIMIZATION: Insert TOC entries with batch operations
     */
    private suspend fun insertTocEntriesOptimized(
        payload: BookPayload,
        bookId: Long,
        bookPath: String,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineTocBatch: MutableList<Pair<Long, Long>>
    ) {
        val levelStack = ArrayDeque<Pair<Int, Long>>()
        val headingLineToToc = mutableMapOf<Int, Long>()
        val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
        val allTocIds = mutableListOf<Long>()
        val tocParentMap = mutableMapOf<Long, Long?>()

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

        // Update hasChildren and isLastChild
        val parentIds = tocParentMap.values.filterNotNull().toSet()
        for (tocId in allTocIds) {
            if (tocId in parentIds) {
                repository.updateTocEntryHasChildren(tocId, true)
            }
        }
        for ((_, children) in entriesByParent) {
            if (children.isNotEmpty()) {
                val lastChildId = children.last()
                repository.updateTocEntryIsLastChild(lastChildId, true)
            }
        }

        // Build line_toc mappings - add to batch instead of individual inserts
        val sortedKeys = headingLineToToc.keys.sorted()
        for (lineIdx in payload.lines.indices) {
            val key = sortedKeys.lastOrNull { it <= lineIdx } ?: continue
            val tocId = headingLineToToc[key] ?: continue
            val lineId = lineKeyToId[bookPath to lineIdx] ?: continue
            synchronized(lineTocBatch) {
                lineTocBatch.add(lineId to tocId)
            }
        }
    }

    /**
     * OPTIMIZATION: Process links in parallel with batch insertions
     */
    private suspend fun processLinksInParallel(
        linksDir: Path,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>
    ) = coroutineScope {
        val csvFiles = Files.list(linksDir)
            .filter { it.fileName.toString().endsWith(".csv") }
            .toList()

        logger.i { "Processing ${csvFiles.size} link files..." }

        // Channel for collecting links from parallel processors
        val linkChannel = Channel<Link>(Channel.BUFFERED)

        // Launch parallel file processors
        val processors = csvFiles.map { file ->
            launch(Dispatchers.IO) {
                processLinkFile(
                    file = file,
                    refsByCanonical = refsByCanonical,
                    refsByBase = refsByBase,
                    lineKeyToId = lineKeyToId,
                    lineIdToBookId = lineIdToBookId,
                    bookMetaById = bookMetaById,
                    linkChannel = linkChannel
                )
            }
        }

        // Launch batch inserter
        val inserter = launch {
            val batch = mutableListOf<Link>()
            for (link in linkChannel) {
                batch += link
                if (batch.size >= LINK_BATCH_SIZE) {
                    repository.insertLinksBatch(batch)
                    batch.clear()
                }
            }
            // Flush remaining
            if (batch.isNotEmpty()) {
                repository.insertLinksBatch(batch)
            }
        }

        // Wait for all processors to finish
        processors.joinAll()
        linkChannel.close()

        // Wait for inserter to finish
        inserter.join()
    }

    private suspend fun processLinkFile(
        file: Path,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>,
        linkChannel: Channel<Link>
    ) {
        Files.newBufferedReader(file).use { reader ->
            val iter = reader.lineSequence().iterator()
            if (!iter.hasNext()) return
            val headers = parseCsvLine(iter.next()).map { normalizeCitation(it) }
            val idxC1 = headers.indexOf("Citation 1")
            val idxC2 = headers.indexOf("Citation 2")
            val idxConn = headers.indexOf("Conection Type")
            if (idxC1 < 0 || idxC2 < 0 || idxConn < 0) return

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
                        val baseConnectionType = ConnectionType.fromString(conn)
                        val (forwardType, reverseType) = resolveDirectionalConnectionTypes(
                            baseType = baseConnectionType,
                            sourceBookId = lineBookId(srcLine, lineIdToBookId),
                            targetBookId = lineBookId(tgtLine, lineIdToBookId),
                            bookMetaById = bookMetaById
                        )

                        // Send links to channel
                        linkChannel.send(Link(
                            sourceBookId = lineBookId(srcLine, lineIdToBookId),
                            targetBookId = lineBookId(tgtLine, lineIdToBookId),
                            sourceLineId = srcLine,
                            targetLineId = tgtLine,
                            connectionType = forwardType
                        ))

                        linkChannel.send(Link(
                            sourceBookId = lineBookId(tgtLine, lineIdToBookId),
                            targetBookId = lineBookId(srcLine, lineIdToBookId),
                            sourceLineId = tgtLine,
                            targetLineId = srcLine,
                            connectionType = reverseType
                        ))
                    }
                }
            }
        }
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

        val isTalmudTractate = payload.categoriesHe.any { it.contains("תלמוד") }
        val isShulchanArukhCode = payload.categoriesHe.any { it.contains("שולחן ערוך") }
        val isTurCode = payload.categoriesHe.any { it.contains("טור") }

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
                        if (current == null || (lineIdx in 0..<current)) {
                            put(key, lineId to lineIdx)
                        }
                    }
                    addKey(canonical)
                    addKey(stripBookAlias(canonical, bookAliasKeys))
                    addKey(canonicalTail(value))
                }
            }
        }
        val maxColonDepth = canonicalToLine.keys.maxOfOrNull { key -> key.count { it == ':' } } ?: 0

        payload.altStructures.forEach { structure ->
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

                fun expandedCandidates(base: String): List<String> {
                    if (base.isBlank() || maxColonDepth <= 0) return emptyList()
                    val colonCount = base.count { it == ':' }
                    if (colonCount >= maxColonDepth) return emptyList()
                    val expansions = mutableListOf<String>()
                    var current = base
                    repeat(maxColonDepth - colonCount) {
                        current += ":1"
                        expansions += current
                    }
                    return expansions
                }

                fun matchKey(key: String): Pair<Long?, Int?>? {
                    val variants = linkedSetOf(key).apply {
                        if (key.contains('.')) {
                            add(key.replace('.', ' '))
                            add(key.replace(Regex("\\.(\\d+)")) { match -> ":${match.groupValues[1]}" })
                            add(key.replace(Regex("\\.(\\d+)")) { match -> " ${match.groupValues[1]}" })
                            add(key.replace(".", ""))
                        }
                    }.filter { it.isNotBlank() }
                    variants.forEach { variant ->
                        canonicalToLine[variant]?.let { return it }
                        for (expanded in expandedCandidates(variant)) {
                            canonicalToLine[expanded]?.let { return it }
                        }
                    }
                    return null
                }

                fun fallbackWithinChapter(canonical: String): Pair<Long?, Int?>? {
                    if (!canonical.contains(':')) return null
                    val base = canonical.substringBefore(':')
                    val numStr = canonical.substringAfter(':').takeWhile { it.isDigit() }
                    val start = numStr.toIntOrNull() ?: return null
                    for (n in start downTo 1) {
                        val candidate = "$base:$n"
                        val candidates = listOf(candidate, stripBookAlias(candidate, bookAliasKeys))
                        candidates.forEach { key ->
                            if (key.isNotBlank()) {
                                matchKey(key)?.let { return it }
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
                        if (key.isNotBlank()) {
                            matchKey(key)?.let { return it }
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
                            if (key.isNotBlank()) {
                                matchKey(key)?.let { return it }
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
                                chapterKey,
                                stripBookAlias(chapterKey, bookAliasKeys)
                            )
                            chapterCandidates.forEach { key ->
                                if (key.isNotBlank()) {
                                    matchKey(key)?.let { return it }
                                }
                            }
                        }
                    }
                    fallbackWithinChapter(canonical)?.let { return it }
                    return null
                }

                lookup(citation)?.let { return it }

                if (isChapterOrSimanLevel) {
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
                    "klal" in norm -> "כלל"
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
                        entryLineInfo[childId]?.second?.let { _ -> childId to (entryLineInfo[childId]!!) }
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
                            it.equals("Chapter", ignoreCase = true) ||
                            it.equals("Integer", ignoreCase = true)
                }
                val isDafNode = node.addressTypes.any { it.equals("Talmud", ignoreCase = true) }

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
                if (lineId == null || lineIndex == null) return 0L
                val text = nodeLabel(node, position)

                val used = usedLineIdsByParent.getOrPut(parentId) { mutableSetOf() }
                if (lineId in used) return 0L
                used += lineId

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
                headingLineToToc[lineIndex] = tocId

                var hasChild = false
                if (!isTalmudTractate && !isShulchanArukhCode && !isTurCode && !isPsalms30DayCycle && node.refs.isNotEmpty()) {
                    for ((idx, ref) in node.refs.withIndex()) {
                        val (childLineId, childLineIndex) = resolveLineForCitation(
                            ref,
                            isChapterOrSimanLevel,
                            allowChapterFallback = !isDafNode,
                            allowTailFallback = !isDafNode
                        )
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
                    !structure.heTitle.isNullOrBlank() -> structure.heTitle
                    !structure.title.isNullOrBlank() -> structure.title
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

            for ((_, children) in entriesByParent) {
                if (children.isNotEmpty()) {
                    val lastChildId = children.last()
                    repository.updateAltTocEntryIsLastChild(lastChildId, true)
                }
            }

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
            val heTitle = node["heTitle"]?.stringOrNull().orEmpty()
            val key = node["key"]?.stringOrNull()
            val isDefault = key.equals("default", ignoreCase = true)
            val hasTitle = heTitle.isNotBlank()

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
                    processNode(child, childText, level + 1, newRefPrefix, newHeRefPrefix)
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

        // Normalize various quote styles (ASCII and Hebrew) so titles that differ
        // only by גרש/גרשיים or straight quotes map to the same key.
        val withoutQuotes = value
            .replace("\"", "")
            .replace("'", "")
            .replace("\u05F3", "") // Hebrew geresh
            .replace("\u05F4", "") // Hebrew gershayim

        return withoutQuotes
            .lowercase()
            .replace("\\s+".toRegex(), " ")
            .replace('_', ' ')
            .trim()
    }

    private fun sanitizeFolder(name: String?): String {
        if (name.isNullOrBlank()) return ""
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

    // OPTIMIZATION: Pre-computed gematria lookup table for common values
    private val gematriaCache = ConcurrentHashMap<Int, String>()

    private fun toGematria(num: Int): String {
        if (num <= 0) return num.toString()

        // Check cache first
        gematriaCache[num]?.let { return it }

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

        val result = builder.toString()
        // Cache only small values to avoid memory bloat
        if (num < 10000) {
            gematriaCache[num] = result
        }
        return result
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
        val stripAfterColon = normalized.replace(Regex(":\\d+[ab]?(?:-\\d+[ab]?)?$"), "")
        return stripAfterColon
            .replace(Regex(" +(\\d+[ab]?)$"), " $1")
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

    private suspend fun applyDefaultCommentators(
        defaultsByBookKey: Map<String, List<String>>,
        normalizedTitleToBookId: Map<String, Long>
    ) {
        if (defaultsByBookKey.isEmpty()) return

        logger.i { "Applying default commentators for ${defaultsByBookKey.size} base books" }

        // Clear previous mappings for a clean regeneration
        repository.clearAllDefaultCommentators()

        var totalRows = 0

        defaultsByBookKey.forEach { (bookKey, commentatorKeys) ->
            val baseBookId = normalizedTitleToBookId[bookKey] ?: return@forEach

            val uniqueCommentatorIds = LinkedHashSet<Long>()
            commentatorKeys.forEach { commentatorKey ->
                val commentatorBookId = normalizedTitleToBookId[commentatorKey]
                if (commentatorBookId != null && commentatorBookId != baseBookId) {
                    uniqueCommentatorIds += commentatorBookId
                }
            }

            if (uniqueCommentatorIds.isNotEmpty()) {
                repository.setDefaultCommentatorsForBook(baseBookId, uniqueCommentatorIds.toList())
                totalRows += uniqueCommentatorIds.size
            }
        }

        logger.i { "Inserted $totalRows default commentator rows" }
    }

    private fun resolveDirectionalConnectionTypes(
        baseType: ConnectionType,
        sourceBookId: Long,
        targetBookId: Long,
        bookMetaById: Map<Long, BookMeta>
    ): Pair<ConnectionType, ConnectionType> {
        if (baseType != ConnectionType.COMMENTARY && baseType != ConnectionType.TARGUM) {
            return baseType to baseType
        }

        val sourceMeta = bookMetaById[sourceBookId] ?: return baseType to baseType
        val targetMeta = bookMetaById[targetBookId] ?: return baseType to baseType

        fun typesFor(sourceIsSecondary: Boolean): Pair<ConnectionType, ConnectionType> {
            return when (baseType) {
                ConnectionType.COMMENTARY ->
                    if (sourceIsSecondary) {
                        ConnectionType.SOURCE to ConnectionType.COMMENTARY
                    } else {
                        ConnectionType.COMMENTARY to ConnectionType.SOURCE
                    }

                ConnectionType.TARGUM ->
                    if (sourceIsSecondary) {
                        ConnectionType.SOURCE to ConnectionType.TARGUM
                    } else {
                        ConnectionType.TARGUM to ConnectionType.SOURCE
                    }

                else -> baseType to baseType
            }
        }

        if (sourceMeta.isBaseBook && !targetMeta.isBaseBook) {
            return typesFor(sourceIsSecondary = false)
        }
        if (!sourceMeta.isBaseBook && targetMeta.isBaseBook) {
            return typesFor(sourceIsSecondary = true)
        }

        val sourceLevel = sourceMeta.categoryLevel
        val targetLevel = targetMeta.categoryLevel
        if (sourceLevel < targetLevel) {
            return typesFor(sourceIsSecondary = false)
        }
        if (targetLevel < sourceLevel) {
            return typesFor(sourceIsSecondary = true)
        }

        return if (sourceBookId > targetBookId) {
            typesFor(sourceIsSecondary = true)
        } else {
            typesFor(sourceIsSecondary = false)
        }
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
            "UPDATE book SET hasTargumConnection=0, hasReferenceConnection=0, hasSourceConnection=0, hasCommentaryConnection=0, hasOtherConnection=0"
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
        setConnFlag("SOURCE", "hasSourceConnection")
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
                hasSourceConnection = book.hasSourceConnection,
                hasCommentaryConnection = book.hasCommentaryConnection,
                hasOtherConnection = book.hasOtherConnection,
                hasAltStructures = book.hasAltStructures
            )
        }?.sortedBy { it.order } ?: emptyList()

        val subCategories = repository.getCategoryChildren(category.id)
            .sortedBy { it.order }
            .map {
                buildCatalogCategoryRecursive(it, booksByCategory)
            }

        return CatalogCategory(
            id = category.id,
            title = category.title,
            level = category.level,
            parentId = category.parentId,
            books = catBooks,
            subcategories = subCategories
        )
    }

    private fun countCategories(root: CatalogCategory): Int {
        var total = 1
        for (child in root.subcategories) total += countCategories(child)
        return total
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun saveCatalog(catalog: PrecomputedCatalog, outputPath: Path) {
        val bytes = ProtoBuf.encodeToByteArray(PrecomputedCatalog.serializer(), catalog)
        outputPath.toFile().parentFile?.mkdirs()
        Files.write(outputPath, bytes)
    }
}
