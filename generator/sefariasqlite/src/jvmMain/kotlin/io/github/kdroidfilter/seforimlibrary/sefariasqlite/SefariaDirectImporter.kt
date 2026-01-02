package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Author
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.json.Json
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.io.path.exists
import kotlin.io.path.isDirectory

/**
 * Direct importer: reads Sefaria `database_export` and writes into SQLite without intermediate Otzaria files.
 * Scope: replicate existing Otzaria-based logic (books/lines/links) with best-effort citation matching.
 *
 * OPTIMIZED VERSION: Uses batch processing, parallel file reading, and transaction grouping.
 */
class SefariaDirectImporter(
    private val exportRoot: Path,
    private val repository: SeforimRepository,
    private val logger: Logger = Logger.withTag("SefariaDirectImporter")
) {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    suspend fun import() = coroutineScope {
        val dbRoot = findDatabaseExportRoot(exportRoot)
        val jsonDir = dbRoot.resolve("json")
        val schemaDir = dbRoot.resolve("schemas")

        // Parse table of contents for ordering
        val (categoryOrders, bookOrders) = parseTableOfContentsOrders(dbRoot, json, logger)
        require(jsonDir.isDirectory() && schemaDir.isDirectory()) { "Missing json/schemas under $dbRoot" }

        val bookPayloadReader = SefariaBookPayloadReader(json, logger)
        val schemaLookup = bookPayloadReader.buildSchemaLookup(schemaDir)

        // Read and parse files in parallel
        logger.i { "Starting parallel file processing..." }
        val bookPayloads = bookPayloadReader.readBooksInParallel(jsonDir, schemaDir, schemaLookup)
        logger.i { "Parsed ${bookPayloads.size} books" }

        val classLoader = javaClass.classLoader
        val priorityEntries = loadPriorityList(classLoader, logger)
        val (orderedBookPayloads, missingPriorityEntries) = applyPriorityOrdering(bookPayloads, priorityEntries)
        val baseBookKeys = priorityEntries.toSet()

        // Load default configuration (per base-book title) from resources
        val defaultCommentatorsConfig = loadDefaultCommentatorsConfig(classLoader, json, logger)
        val defaultTargumConfig = loadDefaultTargumConfig(classLoader, json, logger)

        if (priorityEntries.isNotEmpty()) {
            val matched = priorityEntries.size - missingPriorityEntries.size
            logger.i { "Applied priority ordering for $matched/${priorityEntries.size} entries" }
            if (missingPriorityEntries.isNotEmpty()) {
                logger.w { "Priority entries not found in Sefaria export (first 5): ${missingPriorityEntries.take(5)}" }
            }
        }

        // Disable synchronous writes during bulk import
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

        // Batch insertions
        val lineBatch = mutableListOf<Line>()
        val lineTocBatch = mutableListOf<Pair<Long, Long>>() // lineId, tocId

        val tocInserter = SefariaTocInserter(repository)
        val altTocBuilder = SefariaAltTocBuilder(repository)
        val linksImporter = SefariaLinksImporter(repository, logger)

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
                if (lineBatch.size >= SefariaImportTuning.LINE_BATCH_SIZE) {
                    repository.insertLinesBatch(lineBatch)
                    lineBatch.clear()
                }
            }

            // Insert TOC entries hierarchically and build line_toc mappings
            if (payload.headings.isNotEmpty()) {
                tocInserter.insertTocEntriesOptimized(
                    payload = payload,
                    bookId = bookId,
                    bookPath = bookPath,
                    categoryId = catId,
                    bookTitle = payload.heTitle,
                    lineKeyToId = lineKeyToId,
                    lineTocBatch = lineTocBatch
                )
            }

            // Alternative structures
            val generatedAltStructures = altTocBuilder.buildAltTocStructuresForBook(
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

        // Apply default mappings
        if (defaultCommentatorsConfig.isNotEmpty()) {
            applyDefaultCommentators(repository, logger, defaultCommentatorsConfig, normalizedTitleToBookId)
        }
        if (defaultTargumConfig.isNotEmpty()) {
            applyDefaultTargumim(repository, logger, defaultTargumConfig, normalizedTitleToBookId)
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

        // Process links in parallel and batch insert
        val linksDir = dbRoot.resolve("links")
        if (linksDir.exists()) {
            logger.i { "Processing links..." }
            linksImporter.processLinksInParallel(
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
        linksImporter.updateBookHasLinks()

        logger.i { "Direct Sefaria import completed." }
    }
}
