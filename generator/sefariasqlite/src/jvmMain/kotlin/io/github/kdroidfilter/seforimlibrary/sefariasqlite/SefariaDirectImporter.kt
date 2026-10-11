package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.changes.SefariaSourceHashComputer
import io.github.kdroidfilter.seforimlibrary.common.changes.TouchedBookDetector
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.ids.LegacyLineKey
import io.github.kdroidfilter.seforimlibrary.common.ids.LineOccurrenceCounter
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import io.github.kdroidfilter.seforimlibrary.core.models.Author
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
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

    // Lazy, so constructing the importer (tests included) never reaches for the
    // ForDB archive; the first book that has an author pays for it, once.
    private val authorNames: SefariaAuthorCanonicalNames by lazy {
        SefariaAuthorCanonicalNames.load(logger)
    }

    // Insert-time (PRE-demotion) per-type link-import metrics, set once the links
    // phase ran (null otherwise). See [SefariaLinksImporter.metricsSnapshot].
    internal var linkImportMetrics: LinkImportMetrics? = null
        private set

    // Authoritative POST-demotion per-type link counts (name → COUNT(*)), read
    // from the DB after demoteCrossCorpusDependantLinks(). Null unless links ran.
    internal var persistedLinkCountsByType: Map<String, Long>? = null
        private set

    suspend fun import() = coroutineScope {
        SefariaDashlessDibburim.resetSummary()
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

        // Parse table of contents once for ordering and category descriptions.
        val toc = parseTableOfContentsMetadata(dbRoot, json, logger)
        val categoryOrders = toc.categoryOrders
        val bookOrders = toc.bookOrders
        val categoryDescriptions = toc.categoryDescriptions
        require(jsonDir.isDirectory() && schemaDir.isDirectory()) { "Missing json/schemas under $dbRoot" }

        val authorTitles = SefariaAuthorTitles.load(dbRoot, json, logger)
        val preferredVersions = loadPreferredVersions(javaClass.classLoader, logger)
        val bookPayloadReader = SefariaBookPayloadReader(json, logger, authorTitles, preferredVersions)
        val schemaLookup = bookPayloadReader.buildSchemaLookup(schemaDir)

        // Pre-download every `textimages.sefaria.org` asset and cache as base64
        // so that merged.json content can be inlined as `data:image/...` URIs.
        // Without this, books like Tikkunei Zohar render broken ❌ placeholders
        // (issue 392). This scan reads all merged.json once; the embedder uses
        // a disk cache under build/sefaria/image-cache so re-runs skip network.
        // Scans every book json (merged + per-version siblings) so version
        // content also gets its textimages inlined from the warm cache.
        val bookJsonFiles = java.nio.file.Files.walk(jsonDir).use { stream ->
            stream.filter {
                java.nio.file.Files.isRegularFile(it) &&
                    it.fileName.toString().endsWith(".json", ignoreCase = true)
            }.toList()
        }
        SefariaImageEmbedder.prefetch(bookJsonFiles, logger = logger)

        // Read and parse files in parallel
        logger.i { "Starting parallel file processing..." }
        val bookPayloads = bookPayloadReader.readBooksInParallel(jsonDir, schemaDir, schemaLookup)
        logger.i { "Parsed ${bookPayloads.size} books" }

        val classLoader = javaClass.classLoader
        // Parsed up front so a malformed file fails before the long insert phase.
        val versionHebrewTitles = loadVersionHebrewTitles(classLoader)
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
        val usedCategoryDescriptionKeys = mutableSetOf<String>()

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
                val descriptions = categoryDescriptions[key]
                val id = bindings.upsertCategory(
                    canonicalPath = key,
                    parentId = parentId,
                    title = part,
                    level = idx,
                    orderIndex = categoryOrder,
                    heShortDesc = descriptions?.heShortDesc,
                    heDesc = descriptions?.heDesc,
                )
                if (descriptions != null) usedCategoryDescriptionKeys += key
                categoryIds[key] = id
                categoryLevelsById[id] = idx
                parentId = id
            }
            return parentId ?: throw IllegalStateException("No category created for $pathParts")
        }

        // Per-(bookId, contentHash) occurrence counter, so identical lines within
        // the same book still receive distinct stable ids.
        val lineOccurrences = LineOccurrenceCounter()
        // Transition shim (see LegacyLineKey): the legacy scheme counted
        // occurrences of content+heRef, so it needs its own counter.
        val legacyLineOccurrences = LineOccurrenceCounter()

        val lineKeyToId = ConcurrentHashMap<Pair<String, Int>, Long>()
        val lineIdToBookId = ConcurrentHashMap<Long, Long>()
        val allRefsWithPath = mutableListOf<RefEntry>()
        val canonicalTitleByBookPath = ConcurrentHashMap<String, String>()
        val bookMetaById = ConcurrentHashMap<Long, BookMeta>()
        val normalizedTitleToBookId = ConcurrentHashMap<String, Long>()
        // Collected in priority order during the book loop; the title→bookId
        // index is built from it in two global phases AFTER the loop so a
        // primary title always beats any alias (see buildNormalizedTitleToBookId).
        val titleIndexEntries = mutableListOf<BookTitleIndexEntry>()
        val headingLineIds = ConcurrentHashMap.newKeySet<Long>()
        var rangeMarkerLines = 0
        // Deferred base-text-key → bookId resolution. We can't resolve at
        // book-insert time because a commentary's base text may not have been
        // inserted yet. Resolved in a second pass after the main loop. Declared
        // (base_text_titles) and inferred ("X on Y") keys stay in separate maps
        // so their provenance can be distinguished.
        val pendingDeclaredKeysByBookId = ConcurrentHashMap<Long, List<String>>()
        val pendingInferredKeysByBookId = ConcurrentHashMap<Long, List<String>>()

        // Batch insertions
        val lineBatch = mutableListOf<Line>()
        val lineTocBatch = mutableListOf<Pair<Long, Long>>() // lineId, tocId

        val tocInserter = SefariaTocInserter(repository, bindings)
        val altTocBuilder = SefariaAltTocBuilder(repository, bindings)
        val linksImporter = SefariaLinksImporter(repository, bindings, logger)
        // Inputs for the inline-anchor pass (itags → link_anchor), collected in
        // the book loop so the pass can run after links exist.
        val anchorBookInputs = mutableListOf<SefariaInlineAnchors.BookInput>()
        // Inputs for the versions pass (book_version / version_line), which
        // runs after all lines are inserted so ref→lineId joins always land.
        val versionBookInputs = mutableListOf<SefariaVersionsImporter.BookInput>()

        logger.i { "Inserting books and lines..." }
        var processedBooks = 0

        for (payload in orderedBookPayloads) {
            // The loop only allocates ids and inserts; every text-only derivation
            // was done on the parse workers (BookPayload.precomputeLineData).
            val precomputed = payload.precomputed
                ?: error("Payload '${payload.heTitle}' reached the insert loop without precomputed line data")
            check(precomputed.lineCount == payload.lines.size) {
                "Payload '${payload.heTitle}' precompute size mismatch: " +
                    "${precomputed.lineCount} entries for ${payload.lines.size} lines"
            }
            val lineKeyHashes = precomputed.lineKeyHashes
                ?: error("Payload '${payload.heTitle}' precompute was already released")
            val legacyLineKeyHashes = precomputed.legacyLineKeyHashes
                ?: error("Payload '${payload.heTitle}' precompute was already released")
            val lineCharCounts = precomputed.lineCharCounts
                ?: error("Payload '${payload.heTitle}' precompute was already released")
            val lineIsHeading = precomputed.lineIsHeading
                ?: error("Payload '${payload.heTitle}' precompute was already released")
            val catId = ensureCategoryPath(payload.categoriesHe)
            val bookId = allocator.bookId(sourceName, canonicalHeTitle(payload))
            val bookPath = buildBookPath(payload.categoriesHe, payload.heTitle)
            val canonicalTitle = canonicalHeTitle(payload)
            val previousCanonicalTitle = canonicalTitleByBookPath.putIfAbsent(bookPath, canonicalTitle)
            check(previousCanonicalTitle == null || previousCanonicalTitle == canonicalTitle) {
                "Conflicting Sefaria book identities for linker sidecar path $bookPath: " +
                    "$previousCanonicalTitle vs $canonicalTitle"
            }
            val bookOrder = (bookOrders[payload.enTitle]
                ?: bookOrders[payload.heTitle]
                ?: bookOrders[sanitizeFolder(payload.heTitle)])?.toFloat() ?: 999f
            val normalizedPath = normalizedBookPath(payload.categoriesHe, payload.heTitle)
            val isBaseBook = normalizedPath in baseBookKeys

            // Teamim/nekudot were detected on the parse worker.
            val hasTeamim = precomputed.hasTeamim
            val hasNekudot = precomputed.hasNekudot

            // Pre-resolve author + pubDate IDs through the IdAllocator so they
            // stay stable across builds (without this, INSERT OR IGNORE INTO author
            // would assign a fresh auto-increment id on every build and break the
            // delta producer's secondary-UNIQUE collision pre-check).
            // Canonicalize before the id allocator sees the name: an author row
            // per spelling splits one man in two, and a search by author then
            // returns only the corpus that happened to match.
            val resolvedAuthors = payload.authors
                .map(authorNames::canonical)
                .distinct()
                .map { name -> Author(id = bindings.upsertAuthor(name), name = name) }
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
                title = sefariaDisplayTitle(payload.heTitle, payload.collectiveTitleEn),
                heRef = payload.heTitle,
                authors = resolvedAuthors,
                pubPlaces = emptyList(),
                pubDates = resolvedPubDates,
                heShortDesc = payload.heShortDesc,
                heDesc = payload.description,
                dependenceType = payload.rawDependence,
                collectiveTitleHe = payload.collectiveTitleHe,
                collectiveTitleEn = payload.collectiveTitleEn,
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

            // Track normalized titles (Hebrew/English) + Sefaria-known aliases
            // (titleVariants / heTitleVariants) for later default-commentator mapping
            // and title-pattern base parsing ("X on Avot" → Pirkei Avot). The actual
            // index is built after the loop so a primary title always beats any alias.
            titleIndexEntries += BookTitleIndexEntry(
                bookId = bookId,
                primaryTitles = listOf(payload.heTitle, payload.enTitle),
                aliasKeys = payload.titleAliasKeys,
            )

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
                topCategoryEn = payload.categoriesEn.firstOrNull(),
                authorKeys = resolvedAuthors.mapTo(HashSet()) { it.name },
                isReferenceWork = payload.isReferenceWork,
            )
            if (payload.declaredBaseTextTitleKeys.isNotEmpty()) {
                pendingDeclaredKeysByBookId[bookId] = payload.declaredBaseTextTitleKeys
            }
            if (payload.inferredBaseTextTitleKeys.isNotEmpty()) {
                pendingInferredKeysByBookId[bookId] = payload.inferredBaseTextTitleKeys
            }

            val refsForBook = payload.refEntries.map { it.copy(path = bookPath) }
            synchronized(allRefsWithPath) {
                allRefsWithPath += refsForBook
            }

            // Mapping from lineIndex to RefEntry, built on the parse worker.
            val refsByLineIndex = precomputed.refsByLineIndex

            anchorBookInputs += SefariaInlineAnchors.BookInput(
                bookId = bookId,
                enTitle = payload.enTitle,
                bookPath = bookPath,
                lines = payload.lines,
                refsByLineIndex = refsByLineIndex,
                singleVersionTitle = payload.singleVersionTitle,
                cleanShiftByLineIndex = payload.cleanShiftByLineIndex,
            )

            versionBookInputs += SefariaVersionsImporter.BookInput(
                payload = payload,
                bookId = bookId,
                bookPath = bookPath,
            )

            payload.lines.forEachIndexed { idx, content ->
                val refEntry = refsByLineIndex[idx]
                // Keyed on the raw segment only — neither heRef nor the generated
                // prefix. Both hashes come from the parse worker, used as-is.
                val contentHash = lineKeyHashes[idx]
                val occurrence = lineOccurrences.next(bookId, contentHash)
                val legacyHash = legacyLineKeyHashes[idx]
                val legacy = LegacyLineKey(legacyHash, legacyLineOccurrences.next(bookId, legacyHash))
                val lineId = allocator.lineId(bookId, contentHash, occurrence, legacy)
                val lineCharCount = lineCharCounts[idx]
                lineBatch += Line(
                    id = lineId,
                    bookId = bookId,
                    lineIndex = idx,
                    content = if (payload.boldDashDibburim) SefariaTalmudDibburBold.bold(content) else content,
                    heRef = refEntry?.heRef,
                    charCount = lineCharCount,
                )
                lineKeyToId[bookPath to idx] = lineId
                lineIdToBookId[lineId] = bookId
                // Track heading lines (contain <h1>, <h2>, etc. tags) — flagged
                // on the parse worker. Range-marker lines join them: they carry no
                // text to link to (see [SefariaRangeMarkerLines]).
                if (lineIsHeading[idx]) {
                    headingLineIds.add(lineId)
                } else if (SefariaRangeMarkerLines.isRangeMarker(content)) {
                    headingLineIds.add(lineId)
                    rangeMarkerLines++
                }

                // Flush batch when full
                if (lineBatch.size >= SefariaImportTuning.LINE_BATCH_SIZE) {
                    repository.insertLinesBatch(lineBatch)
                    lineBatch.clear()
                }
            }

            // Every precomputed per-line array has been consumed. Drop them now:
            // the allocator owns the hashes from here on, and the loop's peak
            // heap — the build's high-water mark — lands at the END of this loop,
            // so anything still referenced here costs us against -Xmx.
            precomputed.release()

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

        logger.i { "Inserted all books and lines ($rangeMarkerLines range-marker lines excluded from links)" }
        SefariaDashlessDibburim.logSummary(logger)

        // Build the title→bookId index in two global phases (all primaries, then
        // all aliases) so a primary title always beats any alias regardless of
        // priority-order interleaving. Consumed by default mappings, base-text
        // resolution, and link-density chaining below.
        normalizedTitleToBookId.putAll(buildNormalizedTitleToBookId(titleIndexEntries))

        // Versions pass — after all lines exist so ref→lineId joins always land.
        logger.i { "Importing book versions..." }
        val versionsBlacklist = loadVersionsBlacklist(classLoader, logger)
        if (!versionsBlacklist.isEmpty()) {
            logger.i {
                "Loaded versions blacklist: global=${versionsBlacklist.globalKeys.size}, " +
                    "perBook=${versionsBlacklist.perBookKeys.values.sumOf { it.size }}"
            }
        }
        SefariaVersionsImporter(
            repository, allocator, json, bookPayloadReader, logger, versionsBlacklist, versionHebrewTitles,
        ).import(versionBookInputs, lineKeyToId)

        // Apply default mappings
        if (defaultCommentatorsConfig.isNotEmpty()) {
            applyDefaultCommentators(repository, logger, defaultCommentatorsConfig, normalizedTitleToBookId)
        }
        if (defaultTargumConfig.isNotEmpty()) {
            applyDefaultTargumim(repository, logger, defaultTargumConfig, normalizedTitleToBookId)
        }

        // Build citation lookup for links
        val refsByCanonical = allRefsWithPath.groupBy { canonicalCitation(it.ref) }
        val refsByPath = allRefsWithPath.groupBy { it.path }
        val refsByBase = mutableMapOf<String, RefEntry>()
        allRefsWithPath.forEach { entry ->
            val base = canonicalBase(entry.ref)
            val existing = refsByBase[base]
            if (existing == null || entry.lineIndex < existing.lineIndex) {
                refsByBase[base] = entry
            }
        }

        // Linker sidecar (stage 5): when -DlinkerSidecarPath is set, dump every RefEntry with
        // its resolved lineId so the LINKER Phase-2 importer can rebuild refsByCanonical/
        // refsByBase and map a resolved ref → lineId. Gated → zero effect on a normal build.
        System.getProperty("linkerSidecarPath")?.let { sidecarPath ->
            writeLinkerSidecar(
                sidecarPath,
                allRefsWithPath,
                lineKeyToId,
                canonicalTitleByBookPath,
                sourceName,
            )
        }

        // Resolve deferred base-text keys → bookIds now that every book has been
        // inserted and normalizedTitleToBookId is fully populated. Declared and
        // inferred keys are resolved into separate sets: baseTextBookIds (for the
        // orientation resolver) is their union — preserving existing direction
        // behavior — while the provenance sets stay disjoint. book_base_text is
        // written from the DECLARED set only (metadata fidelity, not UI ranking).
        run {
            val bookIds = HashSet<Long>().apply {
                addAll(pendingDeclaredKeysByBookId.keys)
                addAll(pendingInferredKeysByBookId.keys)
            }
            var declaredHits = 0
            var inferredHits = 0
            var baseTextRows = 0
            for (bookId in bookIds) {
                val declaredIds = pendingDeclaredKeysByBookId[bookId]
                    ?.mapNotNullTo(HashSet()) { normalizedTitleToBookId[it] } ?: HashSet()
                val inferredIds = pendingInferredKeysByBookId[bookId]
                    ?.mapNotNullTo(HashSet()) { normalizedTitleToBookId[it] } ?: HashSet()
                declaredHits += declaredIds.size
                inferredHits += inferredIds.size
                val meta = bookMetaById[bookId] ?: continue
                bookMetaById[bookId] = meta.copy(
                    baseTextBookIds = declaredIds + inferredIds,
                    sefariaDeclaredBaseTextBookIds = declaredIds,
                    inferredBaseTextBookIds = inferredIds,
                )
                for (baseBookId in declaredIds) {
                    repository.insertBookBaseText(bookId, baseBookId)
                    baseTextRows++
                }
            }
            logger.i {
                "Resolved base-text keys for ${bookIds.size} books " +
                    "($declaredHits declared, $inferredHits inferred hits; $baseTextRows book_base_text rows)"
            }
        }

        // Augment baseTextBookIds via link-density chaining. Sefaria's schemas
        // chain Talmud-side super-commentaries (Rif/Ran/HaMaor/...) but flatten
        // most Tanakh super-commentaries (Mizrachi, Gur Aryeh, Levush HaOrah)
        // to point directly at the Torah instead of at Rashi. The aggregated
        // `links/links_by_book.csv` exposes this implicitly via link density;
        // see SefariaLinkDensityChaining.kt for the bimodal-distribution rationale.
        // (base, dependant) edges from step 3 below; exempted from cross-corpus demotion.
        var densityPrimaryBaseEdges: Set<Pair<Long, Long>> = emptySet()
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
                    // Step 1b: a declared super-commentary inherits the volume Sefaria left
                    // out of its list (מחוקקי יהודה ↔ אבן עזרא על שמות, both commentaries),
                    // before step 2 can chain it the wrong way round.
                    completeDeclaredBaseFamilies(bookMetaById, countsByBookPair, logger)
                    // Step 2: walk the asymmetric density rule from declared bases
                    // to chain intermediate dependants (super-commentaries).
                    applyLinkDensitySiblingChaining(bookMetaById, countsByBookPair, logger)
                    // Step 3: undeclared primary bases (Sifra for Malbim on Leviticus) — demotion exemption only.
                    val withoutCommentaryCsv = dbRoot.resolve("links").resolve("links_by_book_without_commentary.csv")
                    require(withoutCommentaryCsv.exists()) { "Missing $withoutCommentaryCsv next to $linksByBookCsv" }
                    densityPrimaryBaseEdges = findPrimaryBaseDensityEdges(
                        bookMetaById,
                        dependantTypedLinkCounts(
                            countsByBookPair,
                            parseLinksByBookCsv(withoutCommentaryCsv, normalizedTitleToBookId),
                        ),
                        logger,
                    )
                }
            }
        }

        // Process links in parallel and batch insert
        val linksDir = dbRoot.resolve("links")
        if (linksDir.exists()) {
            logger.i { "Processing links (${headingLineIds.size} heading lines will be excluded from link targets)..." }
            val charLevelPending = java.util.concurrent.ConcurrentLinkedQueue<PendingCharLevelAnchor>()
            // Commentary links between the books of sparsely linked commentaries (see
            // SefariaSparseCommentaryLinks.kt). Filled by the single inserter coroutine.
            val sparseCommentaries = collectSparseCommentaries(
                allRefsWithPath,
                lineKeyToId,
                lineIdToBookId,
                anchorBookInputs.associate { it.bookPath to it.lines },
            )
            val sparseLinks = sparseCommentaries.associateWith { SparseCommentaryLinks() }
            linksImporter.processLinksInParallel(
                linksDir = linksDir,
                refsByCanonical = refsByCanonical,
                refsByBase = refsByBase,
                lineKeyToId = lineKeyToId,
                lineIdToBookId = lineIdToBookId,
                bookMetaById = bookMetaById,
                headingLineIds = headingLineIds,
                charLevelPending = charLevelPending,
                refsByPath = refsByPath,
                requireExportedVisibility = true,
                onLinkStored = { link ->
                    if (link.connectionType == ConnectionType.COMMENTARY) {
                        sparseLinks.forEach { (commentary, links) -> links.record(commentary, link) }
                    }
                },
            )
            sparseLinks.forEach { (commentary, links) ->
                val anchors = anchorUnlinkedCommentaryLines(commentary, links)
                linksImporter.insertCommentaryAnchors(commentary, anchors, bookMetaById)
                logger.i { "Anchored ${anchors.size} lines of book ${commentary.commentaryBookId} to its base text" }
            }
            // After the anchors: they are counted as written links too.
            linkImportMetrics = linksImporter.metricsSnapshot()
            logger.i { "Links processed" }

            // Word-level anchors: resolve the itags embedded in base texts to
            // the freshly inserted link rows (see SefariaInlineAnchors).
            logger.i { "Resolving inline commentary anchors (itags)..." }
            SefariaInlineAnchors(repository, logger).generate(
                books = anchorBookInputs,
                bookMetaById = bookMetaById,
                refsByCanonical = refsByCanonical,
                lineKeyToId = lineKeyToId,
            )

            // Char-level anchors from the quotation finder's charLevelData —
            // exact-only (single-version + unmodified-line gates).
            logger.i { "Resolving char-level anchors (charLevelData, ${charLevelPending.size} cells)..." }
            SefariaCharLevelAnchors(repository, logger).generate(
                pending = charLevelPending,
                books = anchorBookInputs,
            )
        }

        // Re-enable normal SQLite settings
        repository.executeRawQuery("PRAGMA synchronous = NORMAL")
        repository.executeRawQuery("PRAGMA journal_mode = DELETE")

        repository.rebuildCategoryClosure()

        // Demote dependant-typed links (COMMENTARY/TARGUM/MIDRASH/…) whose
        // source and target books are anchored on incompatible top-level
        // corpora per Sefaria's own categorisation — e.g. Berakhot (תלמוד)
        // ↔ Tora Temima (תנ״ך), Bereshit (תנ״ך) ↔ Iggerot Tzafon
        // (מחשבת ישראל). Otzaria-sourced links (Chevruta etc.) and
        // cross-cutting corpora (חסידות, קבלה) are exempt.
        // See [SefariaLinksImporter.demoteCrossCorpusDependantLinks].
        logger.i { "Demoting cross-corpus dependant links per Sefaria categorisation..." }
        linksImporter.demoteCrossCorpusDependantLinks(densityPrimaryBaseEdges)
        linksImporter.updateBookHasLinks()

        // Capture the authoritative post-demotion per-type split. Demotion only
        // retypes rows (never adds/removes), so Σ persisted must equal Σ insert-
        // time written — fail loudly if the invariant breaks.
        linkImportMetrics?.let { inserted ->
            val persisted = linksImporter.persistedCountsByType()
            val insertedWrittenSum = inserted.insertedByType.values.sumOf { it.written }
            val persistedSum = persisted.values.sum()
            check(insertedWrittenSum == persistedSum) {
                "Link metrics invariant violated: Σ insertedByType.written=$insertedWrittenSum " +
                    "!= Σ persistedByType=$persistedSum (demotion only retypes rows, never adds/removes)"
            }
            persistedLinkCountsByType = persisted
        }

        // Persist current source hashes for next build's touched-book detection.
        // We only record hashes for books that actually went through the importer
        // (i.e. whose natural key now exists in the allocator), so books that were
        // filtered out (blacklists, dedup vs Sefaria) don't get spurious hashes.
        var recorded = 0
        // A computed hash whose natural key never reached the allocator has no
        // book to attach to, so the next build sees that book as `added` and
        // reprocesses it in full — every cycle, forever. 391 of 6,216 in the
        // audited build, reported only as a bare `5825 / 6216`. Classify them.
        val unrecordedByReason = LinkedHashMap<String, MutableList<String>>()
        for ((key, hash) in currentSourceHashes) {
            if (allocator.peekBookId(key.sourceName, key.canonicalHeTitle) != null) {
                allocator.recordSourceHash(key, hash)
                recorded++
            } else {
                unrecordedByReason
                    .getOrPut(classifyUnimportedSefariaBook(key, blacklistResult)) { mutableListOf() }
                    .add(key.canonicalHeTitle)
            }
        }
        logger.i { "Recorded source hashes for $recorded / ${currentSourceHashes.size} Sefaria books" }
        reportUnrecordedSefariaSourceHashes(currentSourceHashes.size, unrecordedByReason, logger)

        logger.i {
            "Category descriptions: parsed=${categoryDescriptions.size}, " +
                "used=${usedCategoryDescriptionKeys.size}, " +
                "unused=${categoryDescriptions.keys.count { it !in usedCategoryDescriptionKeys }}"
        }

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

/** How many book titles each source-hash class names before deferring to the report file. */
private const val MAX_REPORTED_UNRECORDED_HASHES = 20

/**
 * Why a Sefaria book whose source hash was computed never reached the allocator.
 *
 * The hash computer walks `json/**/merged.json` and keys every book it finds by
 * its `heTitle` — exactly [canonicalHeTitle] — but it runs BEFORE the blacklist
 * filter, so every blacklisted book contributes a hash that no book id will ever
 * claim. In the audited build that is essentially the whole gap: 395 payloads
 * skipped by blacklist against 391 unrecorded hashes (the difference being books
 * blacklisted only recently, whose ids the allocator still carries from an
 * earlier cycle, so `peekBookId` still answers).
 *
 * Anything the blacklist does not explain is a genuinely unaccounted book and
 * says so rather than being folded into a plausible-looking bucket.
 */
internal fun classifyUnimportedSefariaBook(key: BookKey, blacklist: BlacklistFilterResult): String =
    if (key.canonicalHeTitle in blacklist.skippedHeTitles) {
        "skipped by the book/author blacklist"
    } else {
        "not imported (reason not tracked)"
    }

/**
 * One WARN per class with counts and a bounded, sorted name list; the complete
 * list goes to the report file. Recording itself is unchanged — which books get
 * a hash decides which books the NEXT build treats as touched, so a "fix" here
 * would change that build's content, not this one's logging.
 */
internal fun reportUnrecordedSefariaSourceHashes(
    computed: Int,
    byReason: Map<String, List<String>>,
    logger: Logger,
) {
    if (byReason.isEmpty()) return
    val total = byReason.values.sumOf { it.size }
    logger.w {
        "source hashes: $total of $computed Sefaria books have no source hash — " +
            "they are fully reprocessed every cycle"
    }
    val ordered = byReason.entries.sortedWith(
        compareByDescending<Map.Entry<String, List<String>>> { it.value.size }.thenBy { it.key },
    )
    for ((reason, titles) in ordered) {
        val names = titles.sorted()
        logger.w {
            "source hashes: ${titles.size} not recorded — $reason " +
                "(${names.take(MAX_REPORTED_UNRECORDED_HASHES).joinToString()}" +
                if (names.size > MAX_REPORTED_UNRECORDED_HASHES) {
                    ", … and ${names.size - MAX_REPORTED_UNRECORDED_HASHES} more)"
                } else {
                    ")"
                }
        }
    }
    GeneratorReport.write("sefaria-source-hashes-not-recorded", logger) {
        put("computed", computed.toLong())
        put("notRecorded", total.toLong())
        putRows(
            "byReason",
            ordered.map { (reason, titles) -> mapOf<String, Any?>("reason" to reason, "books" to titles.size) },
        )
        putRows(
            "books",
            ordered.flatMap { (reason, titles) ->
                titles.sorted().map { mapOf<String, Any?>("title" to it, "reason" to reason) }
            },
        )
    }
}

/**
 * One book's contribution to the title→bookId index, in priority order.
 * [primaryTitles] are raw titles (heTitle/enTitle, normalized on build);
 * [aliasKeys] are already-normalized alias variants (titleVariants/heTitleVariants).
 */
internal data class BookTitleIndexEntry(
    val bookId: Long,
    val primaryTitles: List<String?>,
    val aliasKeys: List<String>,
)

/**
 * Builds the normalized-title → bookId index in TWO GLOBAL PHASES over the
 * priority-ordered [entries]: first every book's PRIMARY titles, then every
 * book's alias variants. `putIfAbsent` within each phase keeps the earliest
 * (highest-priority) claimant; the phase split guarantees a primary title
 * always wins over any alias, even one from an earlier book. This fixes the
 * Meilah bug where Mishnah Meilah's alias "meilah" shadowed Talmud Meilah's
 * primary title, inflating book_base_text rows.
 */
internal fun buildNormalizedTitleToBookId(entries: List<BookTitleIndexEntry>): Map<String, Long> {
    val map = LinkedHashMap<String, Long>()
    for (entry in entries) {
        for (title in entry.primaryTitles) {
            val normalized = normalizeTitleKey(title) ?: continue
            map.putIfAbsent(normalized, entry.bookId)
        }
    }
    for (entry in entries) {
        for (alias in entry.aliasKeys) {
            map.putIfAbsent(alias, entry.bookId)
        }
    }
    return map
}

/**
 * Detects whether any line in the list contains teamim (cantillation marks) or nekudot (vowel points).
 * Uses early exit optimization - stops scanning once both are found.
 *
 * Runs on the parse worker via [precomputeLineData], not in the insert loop.
 *
 * @param lines The list of line contents (HTML strings) to scan
 * @return A pair of (hasTeamim, hasNekudot) booleans
 */
internal fun detectTeamimAndNekudot(lines: List<String>): Pair<Boolean, Boolean> {
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

// Perek families that actually yield ranged refs, and so must never come back
// empty. Mishnah/Tosefta perakim are section-level and excluded on purpose.
private val SEFARIA_RANGED_PEREK_FAMILIES = listOf("Talmud/Bavli", "Talmud/Yerushalmi")
