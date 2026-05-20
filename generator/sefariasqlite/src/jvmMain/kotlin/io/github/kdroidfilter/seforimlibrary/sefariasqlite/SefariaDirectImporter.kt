package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.changes.SefariaSourceHashComputer
import io.github.kdroidfilter.seforimlibrary.common.changes.TouchedBookDetector
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Author
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.text.HebrewTextUtils
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.json.Json
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
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
    private val allocator: IdAllocator = InMemoryIdAllocator.load(path = null),
    private val buildVersion: Int = 0,
    private val logger: Logger = Logger.withTag("SefariaDirectImporter")
) {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
    private val bindings = IdAllocatorBindings(allocator, repository)
    private val sourceName = "Sefaria"

    suspend fun import() = coroutineScope {
        val dbRoot = findDatabaseExportRoot(exportRoot)
        val jsonDir = dbRoot.resolve("json")
        val schemaDir = dbRoot.resolve("schemas")

        // ─── Phase 2: touched-book detection ───────────────────────────────────
        // Computes a per-book sha256 of the source artefact and classifies books
        // against the previous build's hashes. The classification is logged for
        // observability; the fast-path that skips unchanged books is Phase 2.5.
        // Source hashes are recorded on the allocator at the END of import() so
        // the snapshot persists them for the next build.
        val currentSourceHashes = SefariaSourceHashComputer(sourceName).compute(dbRoot, buildVersion)
        run {
            val previousHashes = currentSourceHashes.keys
                .mapNotNull { key -> allocator.previousSourceHash(key)?.let { key to it } }
                .toMap()
            val classification = TouchedBookDetector.classify(previousHashes, currentSourceHashes)
            logger.i { "Source-hash classification: ${classification.summary()}" }
        }

        // Parse table of contents for ordering
        val (categoryOrders, bookOrders) = parseTableOfContentsOrders(dbRoot, json, logger)
        require(jsonDir.isDirectory() && schemaDir.isDirectory()) { "Missing json/schemas under $dbRoot" }

        val bookPayloadReader = SefariaBookPayloadReader(json, logger)
        val schemaLookup = bookPayloadReader.buildSchemaLookup(schemaDir)

        // Pre-download every `textimages.sefaria.org` asset and cache as base64
        // so that merged.json content can be inlined as `data:image/...` URIs.
        // Without this, books like Tikkunei Zohar render broken ❌ placeholders
        // (issue 392). This scan reads all merged.json once; the embedder uses
        // a disk cache under build/sefaria/image-cache so re-runs skip network.
        val mergedFiles = java.nio.file.Files.walk(jsonDir).use { stream ->
            stream.filter {
                java.nio.file.Files.isRegularFile(it) &&
                    it.fileName.toString().equals("merged.json", ignoreCase = true)
            }.toList()
        }
        SefariaImageEmbedder.prefetch(mergedFiles, logger = logger)

        // Read and parse files in parallel
        logger.i { "Starting parallel file processing..." }
        val bookPayloads = bookPayloadReader.readBooksInParallel(jsonDir, schemaDir, schemaLookup)
        logger.i { "Parsed ${bookPayloads.size} books" }

        val classLoader = javaClass.classLoader
        val blacklists = loadSefariaBlacklists(classLoader, logger)
        if (!blacklists.isEmpty()) {
            logger.i {
                "Loaded blacklists: authors=${blacklists.authorKeys.size}, " +
                    "bookTitles=${blacklists.bookTitleKeys.size}, bookPaths=${blacklists.bookPathKeys.size}"
            }
        }
        val blacklistResult = filterBlacklistedPayloads(bookPayloads, blacklists)
        if (blacklistResult.skippedTotal > 0) {
            logger.i {
                "Skipped ${blacklistResult.skippedTotal} books by blacklist " +
                    "(books=${blacklistResult.skippedByBook}, authors=${blacklistResult.skippedByAuthor})"
            }
            if (blacklistResult.skippedBookExamples.isNotEmpty()) {
                logger.i { "Book blacklist examples: ${blacklistResult.skippedBookExamples}" }
            }
            if (blacklistResult.skippedAuthorExamples.isNotEmpty()) {
                logger.i { "Author blacklist examples: ${blacklistResult.skippedAuthorExamples}" }
            }
        }

        val priorityEntries = loadPriorityList(classLoader, logger)
        val (orderedBookPayloads, missingPriorityEntriesRaw) =
            applyPriorityOrdering(blacklistResult.payloads, priorityEntries)
        val (blacklistedPriorityEntries, missingPriorityEntries) = missingPriorityEntriesRaw.partition {
            normalizePriorityEntry(it) in blacklistResult.skippedNormalizedPaths
        }
        val baseBookKeys = priorityEntries.toSet()
        val priorityIndexByPath = buildMap {
            priorityEntries.forEachIndexed { index, entry ->
                val normalized = normalizePriorityEntry(entry)
                if (normalized.isNotBlank() && !containsKey(normalized)) {
                    put(normalized, index)
                }
            }
        }

        // Load default configuration (per base-book title) from resources
        val defaultCommentatorsConfig = loadDefaultCommentatorsConfig(classLoader, json, logger)
        val defaultTargumConfig = loadDefaultTargumConfig(classLoader, json, logger)

        if (priorityEntries.isNotEmpty()) {
            val matched = priorityEntries.size - missingPriorityEntriesRaw.size
            logger.i { "Applied priority ordering for $matched/${priorityEntries.size} entries" }
            if (blacklistedPriorityEntries.isNotEmpty()) {
                logger.i { "Priority entries skipped by blacklist (first 5): ${blacklistedPriorityEntries.take(5)}" }
            }
            if (missingPriorityEntries.isNotEmpty()) {
                logger.w {
                    "Priority entries not found in Sefaria export (first 5): ${missingPriorityEntries.take(5)}"
                }
            }
        }

        // Disable synchronous writes during bulk import
        repository.executeRawQuery("PRAGMA synchronous = OFF")
        repository.executeRawQuery("PRAGMA journal_mode = MEMORY")
        repository.executeRawQuery("PRAGMA cache_size = -64000") // 64MB cache

        // Build DB entries (ids driven by IdAllocator for cross-build stability)
        val sourceId = bindings.upsertSource(sourceName)
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
                val id = bindings.upsertCategory(
                    canonicalPath = key,
                    parentId = parentId,
                    title = part,
                    level = idx,
                    orderIndex = categoryOrder,
                )
                categoryIds[key] = id
                categoryLevelsById[id] = idx
                parentId = id
            }
            return parentId ?: throw IllegalStateException("No category created for $pathParts")
        }

        // Per-(bookId, contentHash) occurrence counter, so identical lines within
        // the same book still receive distinct stable ids.
        val lineOccurrenceByBook = ConcurrentHashMap<Long, ConcurrentHashMap<Long, Int>>()
        fun nextLineOccurrence(bookId: Long, contentHash: ByteArray): Int {
            // contentHash key: lossy 64-bit hash is enough since collisions inside a
            // single book are vanishingly rare; we only need a per-book counter.
            val hashKey = contentHash.fold(0L) { acc, b -> (acc shl 5) - acc + b.toLong() }
            val map = lineOccurrenceByBook.computeIfAbsent(bookId) { ConcurrentHashMap() }
            return map.compute(hashKey) { _, v -> (v ?: -1) + 1 }!!
        }

        val lineKeyToId = ConcurrentHashMap<Pair<String, Int>, Long>()
        val lineIdToBookId = ConcurrentHashMap<Long, Long>()
        val allRefsWithPath = mutableListOf<RefEntry>()
        val bookMetaById = ConcurrentHashMap<Long, BookMeta>()
        val normalizedTitleToBookId = ConcurrentHashMap<String, Long>()
        val headingLineIds = ConcurrentHashMap.newKeySet<Long>()
        // Deferred base_text_titles → bookId resolution. We can't resolve at
        // book-insert time because a commentary's base text may not have been
        // inserted yet. Resolved in a second pass after the main loop.
        val pendingBaseTextKeysByBookId = ConcurrentHashMap<Long, List<String>>()

        // Batch insertions
        val lineBatch = mutableListOf<Line>()
        val lineTocBatch = mutableListOf<Pair<Long, Long>>() // lineId, tocId

        val tocInserter = SefariaTocInserter(repository, bindings)
        val altTocBuilder = SefariaAltTocBuilder(repository, bindings)
        val linksImporter = SefariaLinksImporter(repository, bindings, logger)

        logger.i { "Inserting books and lines..." }
        var processedBooks = 0

        for (payload in orderedBookPayloads) {
            val catId = ensureCategoryPath(payload.categoriesHe)
            val bookId = allocator.bookId(sourceName, canonicalHeTitle(payload))
            val bookPath = buildBookPath(payload.categoriesHe, payload.heTitle)
            val bookOrder = (bookOrders[payload.enTitle]
                ?: bookOrders[payload.heTitle]
                ?: bookOrders[sanitizeFolder(payload.heTitle)])?.toFloat() ?: 999f
            val normalizedPath = normalizedBookPath(payload.categoriesHe, payload.heTitle)
            val isBaseBook = normalizedPath in baseBookKeys

            // Detect teamim and nekudot in book lines
            val (hasTeamim, hasNekudot) = detectTeamimAndNekudot(payload.lines)

            // Pre-resolve author + pubDate IDs through the IdAllocator so they
            // stay stable across builds (without this, INSERT OR IGNORE INTO author
            // would assign a fresh auto-increment id on every build and break the
            // delta producer's secondary-UNIQUE collision pre-check).
            val resolvedAuthors = payload.authors.map { name ->
                Author(id = bindings.upsertAuthor(name), name = name)
            }
            val resolvedPubDates = payload.pubDates.map { pd ->
                io.github.kdroidfilter.seforimlibrary.core.models.PubDate(
                    id = bindings.upsertPubDate(pd.date),
                    date = pd.date,
                )
            }
            val book = Book(
                id = bookId,
                categoryId = catId,
                sourceId = sourceId,
                title = payload.heTitle,
                heRef = payload.heTitle,
                authors = resolvedAuthors,
                pubPlaces = emptyList(),
                pubDates = resolvedPubDates,
                heShortDesc = payload.description,
                notesContent = null,
                order = bookOrder,
                topics = emptyList(),
                isBaseBook = isBaseBook,
                totalLines = payload.lines.size,
                hasAltStructures = false,
                hasTeamim = hasTeamim,
                hasNekudot = hasNekudot
            )
            repository.insertBook(book)

            // Track normalized titles (Hebrew/English) for later default-commentator mapping.
            // Indexes the primary titles plus all Sefaria-known aliases (titleVariants /
            // heTitleVariants) so title-pattern base parsing ("X on Avot" → Pirkei Avot)
            // can resolve abbreviated names. `putIfAbsent` keeps priority-ordered primaries
            // canonical when an alias also matches another book.
            listOf(payload.heTitle, payload.enTitle).forEach { title ->
                val normalized = normalizeTitleKey(title)
                if (normalized != null) {
                    normalizedTitleToBookId.putIfAbsent(normalized, bookId)
                }
            }
            payload.titleAliasKeys.forEach { alias ->
                normalizedTitleToBookId.putIfAbsent(alias, bookId)
            }

            val catLevel = categoryLevelsById[catId] ?: payload.categoriesHe.lastIndex.coerceAtLeast(0)
            val priorityRank = priorityIndexByPath[normalizedPath]
            bookMetaById[bookId] = BookMeta(
                isBaseBook = book.isBaseBook,
                categoryLevel = catLevel,
                priorityRank = priorityRank,
                dependence = payload.dependence,
                // Resolved in a second pass; left empty for now.
                baseTextBookIds = emptySet(),
                collectiveTitleEn = payload.collectiveTitleEn,
            )
            if (payload.baseTextTitleKeys.isNotEmpty()) {
                pendingBaseTextKeysByBookId[bookId] = payload.baseTextTitleKeys
            }

            val refsForBook = payload.refEntries.map { it.copy(path = bookPath) }
            synchronized(allRefsWithPath) {
                allRefsWithPath += refsForBook
            }

            // Create a mapping from lineIndex to RefEntry for quick lookup
            val refsByLineIndex = payload.refEntries.associateBy { it.lineIndex - 1 }

            payload.lines.forEachIndexed { idx, content ->
                val refEntry = refsByLineIndex[idx]
                // Prefer Sefaria's stable citation address (heRef) as natural key
                // when available — survives Sefaria's verse-prefix renumbering
                // (DELTA_UPDATE_PLAN.md §2.1). Fallback to content hash for
                // headings / structural lines that have no heRef.
                val contentHash = IdAllocatorBindings.lineNaturalKeyHash(content, refEntry?.heRef)
                val occurrence = nextLineOccurrence(bookId, contentHash)
                val lineId = allocator.lineId(bookId, contentHash, occurrence)
                val lineCharCount = countVisibleChars(content)
                lineBatch += Line(
                    id = lineId,
                    bookId = bookId,
                    lineIndex = idx,
                    content = content,
                    heRef = refEntry?.heRef,
                    charCount = lineCharCount,
                )
                lineKeyToId[bookPath to idx] = lineId
                lineIdToBookId[lineId] = bookId
                // Track heading lines (contain <h1>, <h2>, etc. tags)
                if (content.contains("<h1>") || content.contains("<h2>") ||
                    content.contains("<h3>") || content.contains("<h4>")) {
                    headingLineIds.add(lineId)
                }

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

        // Resolve deferred base_text_titles → bookIds now that every book has been
        // inserted and normalizedTitleToBookId is fully populated. This gives the
        // link orientation resolver explicit base→dependant edges instead of the
        // priorityRank heuristic.
        if (pendingBaseTextKeysByBookId.isNotEmpty()) {
            var resolved = 0
            var unresolved = 0
            pendingBaseTextKeysByBookId.forEach { (bookId, keys) ->
                val ids = keys.mapNotNullTo(HashSet()) { normalizedTitleToBookId[it] }
                resolved += ids.size
                unresolved += keys.size - ids.size
                if (ids.isNotEmpty()) {
                    val meta = bookMetaById[bookId] ?: return@forEach
                    // Set BOTH baseTextBookIds (for resolver) and the strict
                    // Sefaria-declared subset (for SOURCE view boost). Subsequent
                    // inference/density passes will only mutate `baseTextBookIds`.
                    bookMetaById[bookId] = meta.copy(
                        baseTextBookIds = ids,
                        sefariaDeclaredBaseTextBookIds = ids,
                    )
                }
            }
            logger.i {
                "Resolved base_text_titles for ${pendingBaseTextKeysByBookId.size} books " +
                    "($resolved title→bookId hits, $unresolved misses)"
            }
        }

        // Augment baseTextBookIds via link-density chaining. Sefaria's schemas
        // chain Talmud-side super-commentaries (Rif/Ran/HaMaor/...) but flatten
        // most Tanakh super-commentaries (Mizrachi, Gur Aryeh, Levush HaOrah)
        // to point directly at the Torah instead of at Rashi. The aggregated
        // `links/links_by_book.csv` exposes this implicitly via link density;
        // see SefariaLinkDensityChaining.kt for the bimodal-distribution rationale.
        run {
            val linksByBookCsv = dbRoot.resolve("links").resolve("links_by_book.csv")
            if (linksByBookCsv.exists()) {
                val countsByBookPair = parseLinksByBookCsv(linksByBookCsv, normalizedTitleToBookId)
                if (countsByBookPair.isNotEmpty()) {
                    // Step 1: seed primary bases for dependants whose schema ships
                    // empty base_text_titles (Bartenura on Torah, Ralbag on Torah,
                    // …). These books would otherwise be invisible to the sibling
                    // chaining since it requires at least one declared base.
                    inferPrimaryBasesForEmptyDeclaredBookmeta(bookMetaById, countsByBookPair, logger)
                    // Step 2: walk the asymmetric density rule from declared bases
                    // to chain intermediate dependants (super-commentaries).
                    applyLinkDensitySiblingChaining(bookMetaById, countsByBookPair, logger)
                }
            }
        }

        // Process links in parallel and batch insert
        val linksDir = dbRoot.resolve("links")
        if (linksDir.exists()) {
            logger.i { "Processing links (${headingLineIds.size} heading lines will be excluded from link targets)..." }
            linksImporter.processLinksInParallel(
                linksDir = linksDir,
                refsByCanonical = refsByCanonical,
                refsByBase = refsByBase,
                lineKeyToId = lineKeyToId,
                lineIdToBookId = lineIdToBookId,
                bookMetaById = bookMetaById,
                headingLineIds = headingLineIds
            )
            logger.i { "Links processed" }
        }

        // Re-enable normal SQLite settings
        repository.executeRawQuery("PRAGMA synchronous = NORMAL")
        repository.executeRawQuery("PRAGMA journal_mode = DELETE")

        repository.rebuildCategoryClosure()
        linksImporter.updateBookHasLinks()

        // Persist current source hashes for next build's touched-book detection.
        // We only record hashes for books that actually went through the importer
        // (i.e. whose natural key now exists in the allocator), so books that were
        // filtered out (blacklists, dedup vs Sefaria) don't get spurious hashes.
        var recorded = 0
        for ((key, hash) in currentSourceHashes) {
            if (allocator.peekBookId(key.sourceName, key.canonicalHeTitle) != null) {
                allocator.recordSourceHash(key, hash)
                recorded++
            }
        }
        logger.i { "Recorded source hashes for $recorded / ${currentSourceHashes.size} Sefaria books" }

        logger.i { "Direct Sefaria import completed." }
    }
}

/**
 * Canonical hebrew-title key used as part of a book's natural key. Picking
 * `heTitle` (the raw payload key) keeps the natural key stable as long as
 * Sefaria doesn't rename the title; a renamed book is handled by the alias
 * mechanism (§4.5) in a later phase.
 */
private fun canonicalHeTitle(payload: BookPayload): String = payload.heTitle

/**
 * Detects whether any line in the list contains teamim (cantillation marks) or nekudot (vowel points).
 * Uses early exit optimization - stops scanning once both are found.
 *
 * @param lines The list of line contents (HTML strings) to scan
 * @return A pair of (hasTeamim, hasNekudot) booleans
 */
private fun detectTeamimAndNekudot(lines: List<String>): Pair<Boolean, Boolean> {
    var hasTeamim = false
    var hasNekudot = false
    for (line in lines) {
        // detectNikudAndTeamim does both scans in a single char-by-char
        // pass and bails as soon as both are seen — roughly halves the
        // work vs the previous two-regex approach on a fresh line.
        val (n, t) = HebrewTextUtils.detectNikudAndTeamim(line)
        if (n) hasNekudot = true
        if (t) hasTeamim = true
        if (hasTeamim && hasNekudot) break
    }
    return hasTeamim to hasNekudot
}
