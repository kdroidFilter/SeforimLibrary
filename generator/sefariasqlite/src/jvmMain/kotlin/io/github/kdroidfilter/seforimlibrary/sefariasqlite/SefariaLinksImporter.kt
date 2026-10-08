package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.LinkCoverage
import io.github.kdroidfilter.seforimlibrary.core.models.LinkRange
import io.github.kdroidfilter.seforimlibrary.core.models.SuppressionReason
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.util.Queue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.LongAdder

internal class SefariaLinksImporter(
    private val repository: SeforimRepository,
    private val bindings: IdAllocatorBindings,
    private val logger: Logger
) {
    // Lazy per-book-path prefix indexes for range-end resolution; built only
    // for paths actually hit by ranged citations.
    private val pathIndexCache = ConcurrentHashMap<String, PathRefPrefixIndex>()
    private val pendingRanges = ConcurrentLinkedQueue<LinkRange>()
    private val pendingCoverage = ConcurrentLinkedQueue<LinkCoverage>()

    // Range accounting — surfaced in the end-of-phase summary log.
    private val rangedRowsSeen = LongAdder()
    private val rangesResolved = LongAdder()
    private val rangeEndUnresolved = LongAdder()
    private val rangeReversed = LongAdder()
    private val rangedRowsDropped = LongAdder()

    // Whole perek/parasha range sides: range kept, coverage skipped. Counted per
    // resolved ref pair, like the other range counters.
    private val wholeUnitCoverageSuppressed = LongAdder()

    // Per-connection-type importer counters (QA plan §10.5). Thread-safe — link
    // files are processed in parallel. Semantics in [LinkImportTypeMetrics].
    private val rowsReadByType = ConcurrentHashMap<ConnectionType, LongAdder>()
    private val rowsDroppedByType = ConcurrentHashMap<ConnectionType, LongAdder>()
    private val resolvedPairsByType = ConcurrentHashMap<ConnectionType, LongAdder>()
    private val linksWrittenByType = ConcurrentHashMap<ConnectionType, LongAdder>()

    private fun ConcurrentHashMap<ConnectionType, LongAdder>.bump(type: ConnectionType) {
        computeIfAbsent(type) { LongAdder() }.increment()
    }

    suspend fun processLinksInParallel(
        linksDir: Path,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>,
        headingLineIds: Set<Long> = emptySet(),
        // Collects the CSV `Char Level Data` cells for the post-links anchor
        // pass (see SefariaCharLevelAnchors). Must be thread-safe — link files
        // are processed in parallel. null = don't collect.
        charLevelPending: Queue<PendingCharLevelAnchor>? = null,
        // All RefEntries grouped by book path — range-end resolution input.
        refsByPath: Map<String, List<RefEntry>> = emptyMap(),
        // Canonical whole perek/parasha refs ([SefariaWholeUnitRefs]): such a
        // citation keeps its link_range but gets no link_coverage.
        wholeUnitCitations: Set<String> = emptySet(),
        // Schema-3 production builds require SefariaExport's complete,
        // authoritative per-side verdict. Tests and explicit schema-2 tooling
        // may leave this false to exercise the legacy whole-unit fallback.
        requireExportedVisibility: Boolean = false,
        onLinkStored: (Link) -> Unit = {},
    ) = coroutineScope {
        // Pre-register all connection types we'll use so their ids are stable
        // (so `link.connectionTypeId` is reproducible across builds).
        ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }

        val csvFiles = Files.list(linksDir)
            .filter { it.fileName.toString().endsWith(".csv") }
            .toList()
            .filter { file ->
                val skip = file.fileName.toString() in SEFARIA_AGGREGATE_LINK_FILES
                if (skip) logger.i { "Skipping Sefaria aggregate summary file: ${file.fileName}" }
                !skip
            }
            .sorted()

        val expectedVisibility = if (requireExportedVisibility) {
            validateLinkVisibilityMetadata(linksDir.parent.resolve("metadata/link-visibility-v1.json")).also {
                csvFiles.forEach(::requireVisibilityHeaders)
            }
        } else null
        val observedVisibility = expectedVisibility?.let { LinkVisibilityObservedCounts() }

        logger.i { "Processing ${csvFiles.size} link files..." }

        // Pre-scan: which (dependant, base) pairs have daf-aligned addressing.
        val dafAlignedPairs = computeDafAlignedBookPairs(
            csvFiles = csvFiles,
            refsByCanonical = refsByCanonical,
            refsByBase = refsByBase,
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMetaById,
        )

        // Channels keep producer memory bounded. Visibility verdicts are sent
        // in small lists to a single disk-backed accumulator.
        val linkChannel = Channel<Link>(Channel.BUFFERED)
        val suppressionChannel = Channel<List<SuppressionContribution>>(Channel.BUFFERED)
        val suppressionAccumulator = LinkSuppressionAccumulator.create()

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
                    headingLineIds = headingLineIds,
                    linkChannel = linkChannel,
                    charLevelPending = charLevelPending,
                    refsByPath = refsByPath,
                    dafAlignedPairs = dafAlignedPairs,
                    wholeUnitCitations = wholeUnitCitations,
                    suppressionChannel = suppressionChannel,
                    observedVisibility = observedVisibility,
                )
            }
        }

        // Launch batch inserter
        val inserter = launch {
            val batch = mutableListOf<Link>()
            // written counts ACTUAL insertions: INSERT OR IGNORE dedups on link id,
            // so a duplicate send must not bump the counter.
            suspend fun flush() {
                if (batch.isEmpty()) return
                val inserted = repository.insertLinksBatchReportingInserted(batch)
                batch.forEachIndexed { i, link ->
                    if (inserted[i]) linksWrittenByType.bump(link.connectionType)
                }
                batch.clear()
            }
            for (link in linkChannel) {
                onLinkStored(link)
                batch += link
                if (batch.size >= SefariaImportTuning.LINK_BATCH_SIZE) flush()
            }
            flush()
        }

        val suppressionInserter = launch(Dispatchers.IO) {
            for (batch in suppressionChannel) suppressionAccumulator.addBatch(batch)
        }

        try {
            // Wait for all processors to finish
            processors.joinAll()
            linkChannel.close()
            suppressionChannel.close()

            // Wait for both inserters to finish
            inserter.join()
            suppressionInserter.join()
            if (expectedVisibility != null && observedVisibility != null) {
                expectedVisibility.requireMatches(observedVisibility.snapshot())
            }

            // Ranges/coverage/suppression reference link ids, so they are
            // inserted only after every link row exists.
            insertPendingRangesAndCoverage()
            insertSuppressedSides(suppressionAccumulator)
        } finally {
            linkChannel.close()
            suppressionChannel.close()
            suppressionAccumulator.close()
        }
    }

    /**
     * Pre-scan of the explicitly-typed `commentary` rows: a (dependant, base) book pair
     * is "daf-aligned" when the majority of its typed daf-vs-daf rows carry the SAME
     * top-level daf on both sides. Only aligned pairs are eligible for the blank-row
     * daf gate in [inferBlankConnectionType] — books with their own pagination (Rif,
     * Baal HaMaor, Milchamot) never reach a same-daf majority and stay exempt.
     */
    private suspend fun computeDafAlignedBookPairs(
        csvFiles: List<Path>,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>,
    ): Set<Pair<Long, Long>> = coroutineScope {
        val equalByPair = ConcurrentHashMap<Pair<Long, Long>, LongAdder>()
        val differByPair = ConcurrentHashMap<Pair<Long, Long>, LongAdder>()
        csvFiles.map { file ->
            launch(Dispatchers.IO) {
                Files.newBufferedReader(file).use { reader ->
                    val iter = reader.lineSequence().iterator()
                    if (!iter.hasNext()) return@use
                    val headers = parseCsvLine(iter.next()).map { normalizeCitation(it) }
                    val idxC1 = headers.indexOf("Citation 1")
                    val idxC2 = headers.indexOf("Citation 2")
                    val idxConn = headers.indexOf("Conection Type")
                    // Malformed headers fail loudly in the main pass; nothing to scan here.
                    if (idxC1 < 0 || idxC2 < 0 || idxConn < 0) return@use
                    while (iter.hasNext()) {
                        val row = parseCsvLine(iter.next())
                        if (row.isEmpty()) continue
                        val conn = row.getOrNull(idxConn)?.trim().orEmpty()
                        if (!conn.equals("commentary", ignoreCase = true)) continue
                        val c1 = normalizeCitation(row.getOrNull(idxC1).orEmpty())
                        val c2 = normalizeCitation(row.getOrNull(idxC2).orEmpty())
                        if (c1.isEmpty() || c2.isEmpty()) continue
                        val tok1 = topLevelAddressToken(c1)
                        val tok2 = topLevelAddressToken(c2)
                        if (!tok1.matches(DAF_TOKEN_REGEX) || !tok2.matches(DAF_TOKEN_REGEX)) continue
                        for (from in resolveRefs(c1, refsByCanonical, refsByBase)) {
                            for (to in resolveRefs(c2, refsByCanonical, refsByBase)) {
                                val srcLine = lineKeyToId[from.path to (from.lineIndex - 1)] ?: continue
                                val tgtLine = lineKeyToId[to.path to (to.lineIndex - 1)] ?: continue
                                val srcBookId = lineIdToBookId[srcLine] ?: continue
                                val tgtBookId = lineIdToBookId[tgtLine] ?: continue
                                val srcMeta = bookMetaById[srcBookId]
                                val tgtMeta = bookMetaById[tgtBookId]
                                val tgtDependsOnSrc = tgtMeta != null && srcBookId in tgtMeta.baseTextBookIds
                                val srcDependsOnTgt = srcMeta != null && tgtBookId in srcMeta.baseTextBookIds
                                // Same orientation rule as the gate: exactly one dependant side.
                                if (tgtDependsOnSrc == srcDependsOnTgt) continue
                                val (pair, depTok, baseTok) = if (tgtDependsOnSrc) {
                                    Triple(tgtBookId to srcBookId, tok2, tok1)
                                } else {
                                    Triple(srcBookId to tgtBookId, tok1, tok2)
                                }
                                (if (depTok == baseTok) equalByPair else differByPair)
                                    .computeIfAbsent(pair) { LongAdder() }.increment()
                            }
                        }
                    }
                }
            }
        }.joinAll()
        val aligned = buildSet {
            for ((pair, eq) in equalByPair) {
                if (eq.sum() > (differByPair[pair]?.sum() ?: 0L)) add(pair)
            }
        }
        logger.i {
            val scanned = (equalByPair.keys + differByPair.keys).size
            "Daf-alignment pre-scan: $scanned pairs with typed daf-daf rows, " +
                "${aligned.size} aligned (blank-row daf gate ON), ${scanned - aligned.size} exempt"
        }
        aligned
    }

    private suspend fun insertSuppressedSides(accumulator: LinkSuppressionAccumulator) {
        val result = accumulator.drainSuppressed(SefariaImportTuning.LINK_BATCH_SIZE) {
            repository.insertLinkSuppressedSidesBatch(it)
        }
        logger.i {
            "Suppressed link sides: ${result.suppressedSides} hidden of ${result.contributedSides} sides carrying a verdict " +
                "(${result.contributedSides - result.suppressedSides} cleared by a visible contribution)"
        }
    }

    private fun requireVisibilityHeaders(file: Path) {
        val header = Files.newBufferedReader(file).use { it.readLine() }
            ?: error("Empty link file '${file.fileName}' — missing visibility headers")
        val headers = parseCsvLine(header).map(::normalizeCitation)
        require(headers.count { it == "Suppression Mask 1" } == 1 && headers.count { it == "Suppression Mask 2" } == 1) {
            "Schema-3 link import requires exactly one of each Suppression Mask column in ${file.fileName}"
        }
    }

    private suspend fun insertPendingRangesAndCoverage() {
        val ranges = generateSequence { pendingRanges.poll() }.toList()
        val coverage = generateSequence { pendingCoverage.poll() }.toList()
        ranges.chunked(SefariaImportTuning.LINK_BATCH_SIZE).forEach {
            repository.insertLinkRangesBatch(it)
        }
        coverage.chunked(SefariaImportTuning.LINK_BATCH_SIZE).forEach {
            repository.insertLinkCoverageBatch(it)
        }
        logger.i {
            "Ranged links: ${rangedRowsSeen.sum()} CSV rows with a ranged citation, " +
                "${rangesResolved.sum()} range sides resolved (${ranges.size} range rows, " +
                "${coverage.size} coverage rows), " +
                "end-unresolved=${rangeEndUnresolved.sum()}, reversed=${rangeReversed.sum()}, " +
                "rows dropped with unresolved citation=${rangedRowsDropped.sum()}, " +
                "whole perek/parasha range sides kept coverage-free=${wholeUnitCoverageSuppressed.sum()}"
        }

        // Per-connection-type importer summary (QA plan §10.5); semantics in
        // [LinkImportTypeMetrics].
        //
        // The three read-side counters and the write-side one are keyed by
        // DIFFERENT types — see [LinkImportTypeMetrics] — and printing them
        // unqualified side by side made the line read as a contradiction:
        // `type=REFERENCE rowsRead=381152 dropped=376731 resolvedPairs=4421
        // written=106912` looks like 24 rows written per resolved pair. It is
        // not: 4,421 is how many pairs came from rows the CSV *typed* REFERENCE,
        // 106,912 is how many links were *stored* as REFERENCE — nearly all of
        // them from blank-typed rows that [inferBlankConnectionType] resolved to
        // REFERENCE. The write path is correct and is left untouched; the field
        // names now say which keying each number uses, and the totals line gives
        // the one comparison that is meaningful across the two keyings.
        val metrics = metricsSnapshot()
        logger.i { formatPerTypeCounters(metrics) }
    }

    /**
     * Structured snapshot of the INSERT-TIME per-type counters, sorted by type
     * name (deterministic key order). Valid after [processLinksInParallel]
     * returns. These carry PRE-demotion semantics: `written` is keyed by the
     * type at insertion, before [demoteCrossCorpusDependantLinks] retypes any
     * cross-corpus dependant link to RELATED. For the authoritative final split
     * query the DB via [persistedCountsByType] after demotion.
     */
    fun metricsSnapshot(): LinkImportMetrics {
        val names = (rowsReadByType.keys + rowsDroppedByType.keys +
            resolvedPairsByType.keys + linksWrittenByType.keys)
            .map { it.name }.toSortedSet()
        return LinkImportMetrics(
            names.associateWith { name ->
                val t = ConnectionType.valueOf(name)
                LinkImportTypeMetrics(
                    rowsRead = rowsReadByType[t]?.sum() ?: 0,
                    dropped = rowsDroppedByType[t]?.sum() ?: 0,
                    resolvedPairs = resolvedPairsByType[t]?.sum() ?: 0,
                    written = linksWrittenByType[t]?.sum() ?: 0,
                )
            }
        )
    }

    /**
     * Authoritative final per-type link counts, read from the DB and sorted by
     * type name (deterministic key order). Call AFTER
     * [demoteCrossCorpusDependantLinks] to capture the persisted split (which
     * differs from insert-time [metricsSnapshot] wherever a cross-corpus
     * dependant link was retyped to RELATED). Demotion only retypes rows, so
     * the total must equal Σ insert-time `written`.
     */
    suspend fun persistedCountsByType(): Map<String, Long> =
        repository.countLinksGroupedByType().toSortedMap()

    private suspend fun processLinkFile(
        file: Path,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>,
        headingLineIds: Set<Long>,
        linkChannel: Channel<Link>,
        charLevelPending: Queue<PendingCharLevelAnchor>? = null,
        refsByPath: Map<String, List<RefEntry>> = emptyMap(),
        dafAlignedPairs: Set<Pair<Long, Long>> = emptySet(),
        wholeUnitCitations: Set<String> = emptySet(),
        suppressionChannel: Channel<List<SuppressionContribution>>,
        observedVisibility: LinkVisibilityObservedCounts?,
    ) {
        Files.newBufferedReader(file).use { reader ->
            val iter = reader.lineSequence().iterator()
            val fileName = file.fileName.toString()
            if (!iter.hasNext()) {
                error("Empty link file '$fileName' — missing required header row (Citation 1, Citation 2, Conection Type)")
            }
            val headers = parseCsvLine(iter.next()).map { normalizeCitation(it) }
            val idxC1 = headers.indexOf("Citation 1")
            val idxC2 = headers.indexOf("Citation 2")
            val idxConn = headers.indexOf("Conection Type")
            // Optional word-level columns (SefariaExport extension; absent in
            // older exports).
            val idxCld1 = headers.indexOf("Char Level Data 1")
            val idxCld2 = headers.indexOf("Char Level Data 2")
            // Sefaria's own per-side visibility verdict. Absent in exports older
            // than the link-visibility change; then we fall back to deriving
            // whole-unit suppression locally (see [SefariaWholeUnitRefs]).
            val idxMask1 = headers.indexOf("Suppression Mask 1")
            val idxMask2 = headers.indexOf("Suppression Mask 2")
            require((idxMask1 >= 0) == (idxMask2 >= 0)) {
                "Link file '$fileName' must contain both Suppression Mask columns or neither"
            }
            val hasExportedMasks = idxMask1 >= 0
            if (idxC1 < 0 || idxC2 < 0 || idxConn < 0) {
                val missing = buildList {
                    if (idxC1 < 0) add("Citation 1")
                    if (idxC2 < 0) add("Citation 2")
                    if (idxConn < 0) add("Conection Type")
                }.joinToString(", ")
                error("Link file '$fileName' is missing required header(s): $missing")
            }

            val suppressionBatch = ArrayList<SuppressionContribution>(SUPPRESSION_BATCH_SIZE)
            suspend fun queueSuppression(linkId: Long, side: Int, mask: Int) {
                suppressionBatch += SuppressionContribution(linkId, side, mask)
                if (suppressionBatch.size >= SUPPRESSION_BATCH_SIZE) {
                    suppressionChannel.send(suppressionBatch.toList())
                    suppressionBatch.clear()
                }
            }

            // 1-based CSV line number (header consumed above = line 1).
            var lineNumber = 1
            while (iter.hasNext()) {
                lineNumber++
                val row = parseCsvLine(iter.next())
                if (row.isEmpty()) continue
                // New-format files require explicit verdicts on every data row,
                // including malformed/unresolved rows that are dropped later.
                val exportedMask1 = if (hasExportedMasks) {
                    parseSuppressionMask(row.getOrNull(idxMask1), "$fileName:$lineNumber side 1")
                } else 0
                val exportedMask2 = if (hasExportedMasks) {
                    parseSuppressionMask(row.getOrNull(idxMask2), "$fileName:$lineNumber side 2")
                } else 0
                observedVisibility?.record(exportedMask1, exportedMask2)
                val conn = row.getOrNull(idxConn)?.trim().orEmpty()
                // Validate the type BEFORE the empty-citation skip: an unmapped type
                // must fail the build even on rows whose citations are blank/unresolved.
                val csvConnectionType = mapCsvConnectionType(conn, "$fileName:$lineNumber")
                // rowsRead counts EVERY parsed data row, before any skip —
                // empty-citation rows are read AND dropped.
                rowsReadByType.bump(csvConnectionType)
                val c1 = normalizeCitation(row.getOrNull(idxC1).orEmpty())
                val c2 = normalizeCitation(row.getOrNull(idxC2).orEmpty())
                if (c1.isEmpty() || c2.isEmpty()) {
                    rowsDroppedByType.bump(csvConnectionType)
                    continue
                }
                var rowWroteLink = false
                // `Conection Type` is blank for ~36% of CSV rows. We try to
                // recover those via schema metadata inside the inner loop —
                // the inference is per-pair because the bookId depends on the
                // resolved line.
                val connIsBlank = conn.isBlank() ||
                    conn.equals("none", ignoreCase = true)
                val cld1 = if (charLevelPending != null && idxCld1 >= 0) {
                    parseCharLevelCell(row.getOrNull(idxCld1))
                } else null
                val cld2 = if (charLevelPending != null && idxCld2 >= 0) {
                    parseCharLevelCell(row.getOrNull(idxCld2))
                } else null

                // Ranged citations ("Exodus 1:1-6:1") resolve to their FIRST
                // line via resolveRefs; the range's remaining lines are added
                // below as link_range + link_coverage rows.
                val canonical1 = canonicalCitation(c1)
                val canonical2 = canonicalCitation(c2)
                val range1 = parseCitationRange(canonical1)
                val range2 = parseCitationRange(canonical2)
                if (range1 != null || range2 != null) rangedRowsSeen.increment()
                // Whole perek/parasha side: range kept, coverage skipped, so the
                // link surfaces once at the unit's head. [SefariaWholeUnitRefs]
                // Exported verdict wins when present; otherwise derive locally.
                val coverage1 = if (hasExportedMasks) {
                    (exportedMask1 and WHOLE_UNIT_REASONS) == 0
                } else {
                    canonical1 !in wholeUnitCitations
                }
                val coverage2 = if (hasExportedMasks) {
                    (exportedMask2 and WHOLE_UNIT_REASONS) == 0
                } else {
                    canonical2 !in wholeUnitCitations
                }

                val fromRefs = resolveRefs(c1, refsByCanonical, refsByBase)
                val toRefs = resolveRefs(c2, refsByCanonical, refsByBase)
                if (fromRefs.isEmpty() || toRefs.isEmpty()) {
                    if (range1 != null || range2 != null) rangedRowsDropped.increment()
                    rowsDroppedByType.bump(csvConnectionType)
                    continue
                }

                for (from in fromRefs) {
                    for (to in toRefs) {
                        val srcLineIndex = from.lineIndex - 1
                        val tgtLineIndex = to.lineIndex - 1
                        val srcLine = lineKeyToId[from.path to srcLineIndex] ?: continue
                        val tgtLine = lineKeyToId[to.path to tgtLineIndex] ?: continue
                        // resolvedPairs: both sides resolved to line ids — counted
                        // BEFORE heading/self-link filters, keyed by the raw csv type.
                        resolvedPairsByType.bump(csvConnectionType)
                        // Skip links where source or target is a heading line
                        if (srcLine in headingLineIds || tgtLine in headingLineIds) continue
                        val srcBookId = lineBookId(srcLine, lineIdToBookId)
                        val tgtBookId = lineBookId(tgtLine, lineIdToBookId)
                        // Upgrade blank/none Conection Type to a schema-derived
                        // type when one side explicitly declares the other as
                        // its base text. Without this, ~1.5M legitimate
                        // commentary/targum links (e.g. Abarbanel → Tanakh
                        // verse it expounds) silently land in OTHER and are
                        // excluded from the SOURCE view. The promotion is gated
                        // by a structural-home check (see [inferBlankConnectionType])
                        // so blank-typed cross-references don't masquerade as
                        // commentary in the reader's מפרשים panel.
                        val baseConnectionType = if (connIsBlank) {
                            inferBlankConnectionType(
                                srcBookId = srcBookId,
                                tgtBookId = tgtBookId,
                                srcMeta = bookMetaById[srcBookId],
                                tgtMeta = bookMetaById[tgtBookId],
                                srcRef = c1,
                                tgtRef = c2,
                                dafAlignedPairs = dafAlignedPairs,
                            ) ?: csvConnectionType
                        } else {
                            reclassifyProvenanceConnectionType(
                                type = csvConnectionType,
                                srcMeta = bookMetaById[srcBookId],
                                tgtMeta = bookMetaById[tgtBookId],
                            )
                        }
                        // Drop self-commentary / self-targum links. Sefaria ships a handful
                        // of links that point back to the same book (e.g. Genesis → Genesis
                        // tagged as COMMENTARY), which makes the book appear as a
                        // commentator on itself in the reader's "מפרשים" panel
                        // (Zayit issue #300). Cross-references (OTHER / REFERENCE) are
                        // legitimate inside a single book and are kept.
                        if (srcBookId == tgtBookId &&
                            (baseConnectionType == ConnectionType.COMMENTARY ||
                                baseConnectionType == ConnectionType.TARGUM)
                        ) {
                            continue
                        }
                        // Normalize direction: one row per CSV link, stored in the
                        // canonical base→dependant direction when applicable. SOURCE
                        // is never persisted — it is synthesized at read time from
                        // links where the line appears as `targetLineId`.
                        val (forwardType, _) = resolveDirectionalConnectionTypes(
                            baseType = baseConnectionType,
                            sourceBookId = srcBookId,
                            targetBookId = tgtBookId,
                            bookMetaById = bookMetaById
                        )

                        val (storedSrcBook, storedTgtBook, storedSrcLine, storedTgtLine,
                            storedTgtLineIndex, storedType, wasSwapped) =
                            if (forwardType == ConnectionType.SOURCE) {
                                // CSV had the dependant book as Citation 1; swap so
                                // the stored row goes base→dependant with the
                                // semantic type (COMMENTARY / TARGUM / …).
                                StoredLink(
                                    srcBookId = tgtBookId,
                                    tgtBookId = srcBookId,
                                    srcLineId = tgtLine,
                                    tgtLineId = srcLine,
                                    tgtLineIndex = srcLineIndex,
                                    connectionType = baseConnectionType,
                                    wasSwapped = true,
                                )
                            } else {
                                StoredLink(
                                    srcBookId = srcBookId,
                                    tgtBookId = tgtBookId,
                                    srcLineId = srcLine,
                                    tgtLineId = tgtLine,
                                    tgtLineIndex = tgtLineIndex,
                                    connectionType = forwardType,
                                    wasSwapped = false,
                                )
                            }

                        val typeId = bindings.upsertConnectionType(storedType.name)
                        val linkId = bindings.allocator.linkId(storedSrcLine, storedTgtLine, typeId)

                        val baseProvenance =
                            computeBaseProvenance(storedSrcBook, bookMetaById[storedTgtBook])

                        linkChannel.send(
                            Link(
                                id = linkId,
                                sourceBookId = storedSrcBook,
                                targetBookId = storedTgtBook,
                                sourceLineId = storedSrcLine,
                                targetLineId = storedTgtLine,
                                targetLineIndex = storedTgtLineIndex,
                                connectionType = storedType,
                                baseProvenance = baseProvenance,
                            )
                        )
                        rowWroteLink = true

                        // Per-side visibility, mapped from CSV order onto the
                        // stored direction. The disk-backed accumulator keeps a
                        // side hidden iff every contributor is hidden; its
                        // diagnostic reason mask is the OR of their reasons.
                        if (hasExportedMasks) {
                            val srcSideMask = if (wasSwapped) exportedMask2 else exportedMask1
                            val tgtSideMask = if (wasSwapped) exportedMask1 else exportedMask2
                            queueSuppression(linkId, side = 0, mask = srcSideMask)
                            queueSuppression(linkId, side = 1, mask = tgtSideMask)
                        }

                        // Ranged sides: record the range end + per-line coverage.
                        // The side is relative to the STORED direction (0 = source).
                        if (range1 != null) {
                            queueRangeSide(
                                range = range1, entry = from,
                                side = if (wasSwapped) 1 else 0, linkId = linkId,
                                lineKeyToId = lineKeyToId, headingLineIds = headingLineIds,
                                refsByPath = refsByPath, emitCoverage = coverage1,
                            )
                        }
                        if (range2 != null) {
                            queueRangeSide(
                                range = range2, entry = to,
                                side = if (wasSwapped) 0 else 1, linkId = linkId,
                                lineKeyToId = lineKeyToId, headingLineIds = headingLineIds,
                                refsByPath = refsByPath, emitCoverage = coverage2,
                            )
                        }

                        // Queue char-level cells for the post-links anchor pass.
                        // Citation 1 data refers to `from`'s line, Citation 2 to
                        // `to`'s — the anchor side follows the stored direction.
                        if (charLevelPending != null) {
                            if (cld1 != null) {
                                charLevelPending.add(
                                    cld1.toPending(
                                        entry = from,
                                        storedSrcLine = storedSrcLine,
                                        storedTgtLine = storedTgtLine,
                                        side = if (wasSwapped) 1 else 0,
                                    )
                                )
                            }
                            if (cld2 != null) {
                                charLevelPending.add(
                                    cld2.toPending(
                                        entry = to,
                                        storedSrcLine = storedSrcLine,
                                        storedTgtLine = storedTgtLine,
                                        side = if (wasSwapped) 0 else 1,
                                    )
                                )
                            }
                        }
                    }
                }
                // Row read but produced no stored link (all pairs unresolved /
                // missing line / heading / self-link) — count once under raw type.
                if (!rowWroteLink) rowsDroppedByType.bump(csvConnectionType)
            }
            if (suppressionBatch.isNotEmpty()) suppressionChannel.send(suppressionBatch.toList())
        }
    }

    /**
     * Resolves a ranged citation side to its end line and queues a
     * [LinkRange] row plus [LinkCoverage] rows for every covered line after
     * the first (heading lines excluded). [entry] is the already-resolved
     * range START; the end resolves via the path's prefix index, so a range
     * cited at any depth ("13:11-13") covers through the LAST leaf line under
     * its end address. Unresolvable/reversed ends are counted, never guessed.
     *
     * [emitCoverage] false → the range row is still written (the panel shows the
     * cited extent) but the per-line coverage is skipped, so the link surfaces
     * only at the range's first line. Set for whole-perek/parasha citations.
     */
    private fun queueRangeSide(
        range: CitationRange,
        entry: RefEntry,
        side: Int,
        linkId: Long,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        headingLineIds: Set<Long>,
        refsByPath: Map<String, List<RefEntry>>,
        emitCoverage: Boolean = true,
    ) {
        val pathIndex = pathIndexCache.computeIfAbsent(entry.path) {
            PathRefPrefixIndex.build(refsByPath[it].orEmpty())
        }
        val endEntry = pathIndex.lastUnder(range.endCanonical)
        if (endEntry == null) {
            rangeEndUnresolved.increment()
            return
        }
        if (endEntry.lineIndex < entry.lineIndex) {
            rangeReversed.increment()
            return
        }
        // Degenerate range ("1:1-1"): single line, nothing to record.
        if (endEntry.lineIndex == entry.lineIndex) return
        val endLineId = lineKeyToId[entry.path to (endEntry.lineIndex - 1)]
        if (endLineId == null) {
            rangeEndUnresolved.increment()
            return
        }
        pendingRanges += LinkRange(
            linkId = linkId,
            side = side,
            endLineId = endLineId,
            endLineIndex = endEntry.lineIndex - 1,
        )
        if (emitCoverage) {
            for (lineIndex1 in (entry.lineIndex + 1)..endEntry.lineIndex) {
                val coveredId = lineKeyToId[entry.path to (lineIndex1 - 1)] ?: continue
                if (coveredId in headingLineIds) continue
                pendingCoverage += LinkCoverage(lineId = coveredId, linkId = linkId, side = side)
            }
        } else {
            wholeUnitCoverageSuppressed.increment()
        }
        rangesResolved.increment()
    }

    /**
     * Stores the [anchors] of a sparsely linked commentary as COMMENTARY links from its base
     * text. See [anchorUnlinkedCommentaryLines].
     */
    suspend fun insertCommentaryAnchors(
        commentary: SparseCommentary,
        anchors: List<CommentaryAnchor>,
        bookMetaById: Map<Long, BookMeta>,
    ) {
        val typeId = bindings.upsertConnectionType(ConnectionType.COMMENTARY.name)
        val baseProvenance = computeBaseProvenance(commentary.baseBookId, bookMetaById[commentary.commentaryBookId])
        anchors
            .map { anchor ->
                Link(
                    id = bindings.allocator.linkId(anchor.baseLineId, anchor.commentaryLine.lineId, typeId),
                    sourceBookId = commentary.baseBookId,
                    targetBookId = commentary.commentaryBookId,
                    sourceLineId = anchor.baseLineId,
                    targetLineId = anchor.commentaryLine.lineId,
                    targetLineIndex = anchor.commentaryLine.lineIndex,
                    connectionType = ConnectionType.COMMENTARY,
                    baseProvenance = baseProvenance,
                )
            }
            .chunked(SefariaImportTuning.LINK_BATCH_SIZE)
            .forEach { batch ->
                val inserted = repository.insertLinksBatchReportingInserted(batch)
                batch.forEachIndexed { i, link ->
                    if (inserted[i]) linksWrittenByType.bump(link.connectionType)
                }
            }
    }

    private data class StoredLink(
        val srcBookId: Long,
        val tgtBookId: Long,
        val srcLineId: Long,
        val tgtLineId: Long,
        val tgtLineIndex: Int,
        val connectionType: ConnectionType,
        val wasSwapped: Boolean,
    )

    /** A parsed `Char Level Data` CSV cell, before line/side resolution. */
    internal data class CharLevelCell(
        val start: Int,
        val end: Int,
        val versionTitle: String,
        val language: String,
        val isWordBased: Boolean,
    ) {
        fun toPending(
            entry: RefEntry,
            storedSrcLine: Long,
            storedTgtLine: Long,
            side: Int,
        ) = PendingCharLevelAnchor(
            path = entry.path,
            lineIndex0 = entry.lineIndex - 1,
            srcLineId = storedSrcLine,
            tgtLineId = storedTgtLine,
            side = side,
            startChar = start,
            endChar = end,
            versionTitle = versionTitle,
            language = language,
            isWordBased = isWordBased,
        )
    }

    private fun lineBookId(lineId: Long, lineIdToBookId: Map<Long, Long>): Long =
        lineIdToBookId[lineId] ?: 0

    private fun resolveDirectionalConnectionTypes(
        baseType: ConnectionType,
        sourceBookId: Long,
        targetBookId: Long,
        bookMetaById: Map<Long, BookMeta>
    ): Pair<ConnectionType, ConnectionType> {
        return resolveDirectionalConnectionTypesForMeta(
            baseType = baseType,
            sourceBookId = sourceBookId,
            targetBookId = targetBookId,
            sourceMeta = bookMetaById[sourceBookId],
            targetMeta = bookMetaById[targetBookId]
        )
    }

    /**
     * Demote dependant-typed links whose source and target books live in
     * incompatible corpora.
     *
     * Sefaria categorises every book under a top-level corpus
     * (`תנ״ך` / `תלמוד` / `משנה` / `הלכה` / `חסידות` / `קבלה` / `מוסר` /
     * `מחשבת ישראל` / …). That categorisation IS Sefaria's authoritative
     * statement of what a book is "anchored on". A link tagged COMMENTARY
     * between two books whose anchored corpora don't overlap is structurally
     * inconsistent — the CSV row is treating a cross-corpus citation as a
     * dependant relationship.
     *
     * Concrete examples this catches:
     *  - תורה תמימה (anchored on תנ״ך) ↔ ברכות (anchored on תלמוד):
     *    425 CSV COMMENTARY rows because Tora Temima's footnotes cite the
     *    Berakhot sugya. Tora Temima is a Torah commentary, not a Talmud
     *    commentary, so on the Berakhot reader page it's editorial noise.
     *  - אגרות צפון (anchored on מחשבת ישראל) ↔ בראשית (תנ״ך):
     *    1 stray COMMENTARY row out of 13 — Sefaria CSV typo. Igrot Tzafon
     *    is an independent treatise on Jewish thought, not a Tanakh
     *    commentary.
     *  - בית יוסף (הלכה) ↔ ברכות (תלמוד): Beit Yosef commentates on Tur, not
     *    on Talmud directly.
     *
     * Cross-corpus dependant signals that ARE legitimate:
     *  - חסידות / קבלה / מילונים: cross-cutting corpora that legitimately
     *    commentate across Tanakh / Talmud / Halakha. Mishna ↔ Talmud is
     *    also a single editorial cluster.
     *
     * Otzaria-sourced books (sourceId != 1) bypass this rule entirely —
     * they're imported through a separate pipeline whose links are curated
     * per-book (e.g. Chevruta-Talmud via [generateHavroutaLinks]).
     *
     * Demoted links land in RELATED so the connection panel still shows
     * them as cross-references but the commentator panel
     * (`ct.name IN COMMENTARY/SUPER_COMMENTARY/…`) excludes them.
     */
    // keptBaseEdges: (base, dependant) pairs from findPrimaryBaseDensityEdges — never demoted.
    suspend fun demoteCrossCorpusDependantLinks(keptBaseEdges: Set<Pair<Long, Long>> = emptySet()) {
        val dependantTypes = listOf(
            "COMMENTARY", "SUPER_COMMENTARY", "TARGUM", "MIDRASH", "PARSHANUT", "ELUCIDATION",
        ).joinToString(",") { "'$it'" }
        // Build a temp table (bookId, corpusKey) for every book — corpusKey
        // is the title of the book's category root (absent if not a known corpus).
        // NB: a regular table, not TEMP. executeRawQuery runs each statement
        // through the SQLDelight JdbcSqliteDriver, which on a file-backed DB may
        // serve a different pooled connection per call. TEMP tables are
        // connection-scoped, so the CREATE and the later UPDATE…JOIN _book_corpus
        // could land on different connections — yielding "no such table:
        // _book_corpus". A plain table is visible across connections; the
        // DROP IF EXISTS guards on both ends keep it out of the shipped DB.
        repository.executeRawQuery("DROP TABLE IF EXISTS _book_corpus")
        repository.executeRawQuery(
            "CREATE TABLE _book_corpus (bookId INTEGER PRIMARY KEY NOT NULL, corpus TEXT) WITHOUT ROWID"
        )
        // Tag by the true root (parentId IS NULL): by title, מדרש>הלכה books were tagged הלכה.
        // flattenTalmudCategories renamed "תלמוד" → "תלמוד בבלי"/"תלמוד ירושלמי";
        // map both back to "תלמוד" or every Talmud book gets a NULL corpus.
        repository.executeRawQuery(
            """
            INSERT INTO _book_corpus (bookId, corpus)
            SELECT b.id,
                   CASE WHEN c.title IN ('תלמוד בבלי','תלמוד ירושלמי') THEN 'תלמוד'
                        ELSE c.title END AS corpus
            FROM book b
            JOIN category_closure cc ON cc.descendantId = b.categoryId
            JOIN category c ON c.id = cc.ancestorId AND c.parentId IS NULL
            WHERE c.title IN ('תנ״ך','תלמוד בבלי','תלמוד ירושלמי','משנה','משניות','הלכה','חסידות','קבלה','מדרש','מוסר','ספרי מוסר','מחשבת ישראל')
            """.trimIndent()
        )
        repository.executeRawQuery("DROP TABLE IF EXISTS _kept_base_edge")
        repository.executeRawQuery(
            "CREATE TABLE _kept_base_edge (baseBookId INTEGER NOT NULL, depBookId INTEGER NOT NULL, " +
                "PRIMARY KEY (baseBookId, depBookId)) WITHOUT ROWID"
        )
        for (chunk in keptBaseEdges.chunked(500)) {
            repository.executeRawQuery(
                "INSERT INTO _kept_base_edge (baseBookId, depBookId) VALUES " +
                    chunk.joinToString(",") { (base, dep) -> "($base, $dep)" }
            )
        }
        // Cross-cutting target corpora — commentators in these corpora
        // legitimately span Tanakh/Talmud/Halakha and must NOT be demoted.
        // 'מדרש' is anchored on Tanakh; everything else strict is on its
        // own corpus.
        val crossCutting = listOf("חסידות", "קבלה").joinToString(",") { "'$it'" }
        // Allowed pairs of (source corpus, target corpus) when corpora differ:
        //   - same corpus (handled by NOT IN)
        //   - target in cross-cutting set
        //   - {משנה ↔ תלמוד} cluster
        //   - {משניות ↔ תלמוד} cluster
        //   - {מדרש → תנ״ך}: a Midrash commentates on a Tanakh book, so
        //     Tanakh-source → Midrash-target is the canonical direction.
        repository.executeRawQuery(
            """
            UPDATE link SET connectionTypeId = (SELECT id FROM connection_type WHERE name='RELATED' LIMIT 1)
            WHERE baseProvenance = 0
              AND connectionTypeId IN (SELECT id FROM connection_type WHERE name IN ($dependantTypes))
              AND NOT EXISTS (
                SELECT 1 FROM _kept_base_edge k
                WHERE k.baseBookId = link.sourceBookId AND k.depBookId = link.targetBookId
              )
              AND EXISTS (
                SELECT 1
                FROM book sb JOIN book tb
                  ON sb.id = link.sourceBookId AND tb.id = link.targetBookId
                JOIN _book_corpus sc ON sc.bookId = sb.id
                JOIN _book_corpus tc ON tc.bookId = tb.id
                WHERE sb.sourceId = 1 AND tb.sourceId = 1
                  AND sc.corpus IS NOT NULL AND tc.corpus IS NOT NULL
                  AND sc.corpus != tc.corpus
                  AND tc.corpus NOT IN ($crossCutting)
                  AND NOT (sc.corpus IN ('תלמוד') AND tc.corpus IN ('משנה','משניות'))
                  AND NOT (sc.corpus IN ('משנה','משניות') AND tc.corpus IN ('תלמוד'))
                  AND NOT (sc.corpus = 'תנ״ך' AND tc.corpus = 'מדרש')
              )
            """.trimIndent()
        )
        repository.executeRawQuery("DROP TABLE IF EXISTS _book_corpus")
        repository.executeRawQuery("DROP TABLE IF EXISTS _kept_base_edge")
    }

    suspend fun updateBookHasLinks() {
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

        suspend fun setConnFlag(
            typeName: String,
            column: String,
            includeTargets: Boolean = true,
            excludeSelfLinks: Boolean = false
        ) {
            val selfFilter = if (excludeSelfLinks) " AND l.sourceBookId != l.targetBookId" else ""
            val sourceSelect =
                "SELECT sourceBookId AS bId FROM link l " +
                    "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                    "WHERE ct.name='$typeName'$selfFilter"
            val targetSelect = if (includeTargets) {
                " UNION SELECT targetBookId AS bId FROM link l " +
                    "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                    "WHERE ct.name='$typeName'$selfFilter"
            } else {
                ""
            }
            val sql = "UPDATE book SET $column=1 WHERE id IN (" +
                "SELECT DISTINCT bId FROM (" +
                sourceSelect +
                targetSelect +
                ")" +
                ")"
            repository.executeRawQuery(sql)
        }

        setConnFlag("TARGUM", "hasTargumConnection")
        setConnFlag("REFERENCE", "hasReferenceConnection")
        setConnFlag("COMMENTARY", "hasCommentaryConnection")
        setConnFlag("OTHER", "hasOtherConnection")

        // hasSourceConnection: target side of a SOURCE_VIEW_TYPES link.
        repository.recomputeHasSourceConnection()
    }
}

/**
 * Per-connection-type importer counters (QA plan §10.5).
 *
 * - [rowsRead]: every parsed non-empty CSV data row, counted BEFORE any skip
 *   (empty-citation rows included), keyed by the raw csv-mapped type.
 * - [dropped]: rows that produced no stored link (empty citation, unresolved
 *   refs, or all pairs filtered), keyed by the raw csv-mapped type.
 * - [resolvedPairs]: (from,to) pairs whose BOTH citations resolved to line ids,
 *   counted before heading/self-link filters, keyed by the raw csv-mapped type.
 * - [written]: links ACTUALLY inserted (INSERT OR IGNORE duplicates excluded),
 *   keyed by the FINAL stored type (after blank→schema upgrade / direction swap).
 */
@kotlinx.serialization.Serializable
internal data class LinkImportTypeMetrics(
    val rowsRead: Long,
    val dropped: Long,
    val resolvedPairs: Long,
    val written: Long,
)

/**
 * Insert-time per-type metrics keyed by [ConnectionType] name, sorted for
 * determinism. Carries PRE-demotion semantics — see [metricsSnapshot]. The
 * final persisted split is obtained separately via [persistedCountsByType] and
 * surfaced alongside this in [LinkImportMetricsReport].
 */
@kotlinx.serialization.Serializable
internal data class LinkImportMetrics(val insertedByType: Map<String, LinkImportTypeMetrics>)

/**
 * Machine-checkable link-import metrics JSON, written next to seforim.db for QA
 * checks (QA plan §10.5). Shape is deterministic (declared key order; maps are
 * sorted by type name).
 *
 * - [dbSchemaVersion]/[dbVersion]: read from the persisted DB's `schema_meta`
 *   (null until the pipeline's later stampSchemaVersion stage stamps them).
 * - [dbSizeBytes]: on-disk size of the persisted DB the report describes.
 * - [insertedByType]: PRE-demotion insert-time counters (see [LinkImportTypeMetrics]).
 * - [persistedByType]: authoritative POST-demotion `name → COUNT(*)` from `link`.
 *   Σ persistedByType == Σ insertedByType.written (demotion only retypes rows).
 */
@kotlinx.serialization.Serializable
internal data class LinkImportMetricsReport(
    @kotlinx.serialization.SerialName("db_schema_version") val dbSchemaVersion: String?,
    @kotlinx.serialization.SerialName("db_version") val dbVersion: String?,
    @kotlinx.serialization.SerialName("db_size_bytes") val dbSizeBytes: Long,
    val insertedByType: Map<String, LinkImportTypeMetrics>,
    val persistedByType: Map<String, Long>,
)

private val metricsReportJson = Json { prettyPrint = true }

/** JSON form written next to seforim.db for machine QA checks. */
internal fun LinkImportMetricsReport.toJsonReport(): String =
    metricsReportJson.encodeToString(LinkImportMetricsReport.serializer(), this)

// Sefaria's aggregate summary exports (header `Text 1,Text 2,Link Count`), not
// per-link data — skipped by exact filename per the no-fallbacks policy.
internal val SEFARIA_AGGREGATE_LINK_FILES = setOf(
    "links_by_book.csv",
    "links_by_book_without_commentary.csv",
)

private const val SUPPRESSION_BATCH_SIZE = 10_000

private val charLevelJson = Json { ignoreUnknownKeys = true }

/**
 * Parses one `Char Level Data` CSV cell — a JSON dict with either
 * startChar/endChar (char offsets into the version's segment text) or
 * startWord/endWord (word indices, used by Sefaria for Tanakh verse sides),
 * plus the versionTitle+language the offsets were computed against.
 * Returns null for empty/unparseable cells (unparseable = malformed source
 * data; there is nothing exact to import from it).
 */
internal fun parseCharLevelCell(cell: String?): SefariaLinksImporter.CharLevelCell? {
    val raw = cell?.trim().orEmpty()
    if (raw.isEmpty()) return null
    return runCatching {
        val obj = charLevelJson.parseToJsonElement(raw).jsonObject
        val startChar = obj["startChar"]?.jsonPrimitive?.intOrNull
        val endChar = obj["endChar"]?.jsonPrimitive?.intOrNull
        val startWord = obj["startWord"]?.jsonPrimitive?.intOrNull
        val endWord = obj["endWord"]?.jsonPrimitive?.intOrNull
        val versionTitle = obj["versionTitle"]?.jsonPrimitive?.content?.trim().orEmpty()
        val language = obj["language"]?.jsonPrimitive?.content?.trim().orEmpty()
        val (start, end, wordBased) = when {
            startChar != null && endChar != null -> Triple(startChar, endChar, false)
            startWord != null && endWord != null -> Triple(startWord, endWord, true)
            else -> return@runCatching null
        }
        if (versionTitle.isEmpty()) return@runCatching null
        SefariaLinksImporter.CharLevelCell(
            start = start,
            end = end,
            versionTitle = versionTitle,
            language = language,
            isWordBased = wordBased,
        )
    }.getOrNull()
}

/**
 * Types whose direction has a semantic base→dependant orientation. For these,
 * we normalize the stored direction so that the base book is always on the
 * `source` side and the dependant book on the `target` side.
 *
 * Symmetric / lateral types (REFERENCE, OTHER, RELATED, QUOTATION,
 * MESORAT_HASHAS, MISHNAH_IN_TALMUD) keep Sefaria's `Citation 1 → Citation 2`
 * direction verbatim — they don't have a clear "base/commentary" semantics.
 */
private fun Dependence.toConnectionType(): ConnectionType = when (this) {
    Dependence.COMMENTARY -> ConnectionType.COMMENTARY
    Dependence.TARGUM -> ConnectionType.TARGUM
    Dependence.MIDRASH -> ConnectionType.MIDRASH
    // Sub-Commentary / Guides / etc. — collapsed to COMMENTARY for the
    // purposes of the SOURCE view (they're all oriented dependants).
    Dependence.OTHER_DEPENDANT -> ConnectionType.COMMENTARY
}

/**
 * Strict mapping of a raw CSV `Conection Type` cell to a [ConnectionType].
 * Empty/`none`/`other` map to OTHER; any other unrecognized value is a hard
 * build failure (no silent OTHER fallback — a new Sefaria type must surface).
 */
/**
 * Provenance of the base→dependant orientation, keyed on the STORED target's
 * schema: 2 when the target **explicitly declares** the source as a base text
 * (`base_text_titles`), 1 when it was recovered from the "X on Y" title pattern,
 * 0 otherwise (density chaining, primary-base inference, priorityRank fallback,
 * unoriented, or no target metadata). Declared wins over inferred. Boosts
 * declared bases above lateral citations in the SOURCE view.
 */
internal fun computeBaseProvenance(storedSrcBook: Long, storedTgtMeta: BookMeta?): Int = when {
    storedTgtMeta == null -> 0
    storedSrcBook in storedTgtMeta.sefariaDeclaredBaseTextBookIds -> 2
    storedSrcBook in storedTgtMeta.inferredBaseTextBookIds -> 1
    else -> 0
}

/** The reasons that make a ranged side stop covering its unit's lines. */
internal const val WHOLE_UNIT_REASONS = SuppressionReason.WHOLE_PEREK or SuppressionReason.WHOLE_PARASHA

/**
 * A required `Suppression Mask 1/2` cell from a new-format export. Zero must be
 * explicit: a blank/missing/unparsable/unknown value fails closed.
 */
internal fun parseSuppressionMask(cell: String?, source: String): Int {
    val raw = cell?.trim().orEmpty()
    require(raw.isNotEmpty()) { "Missing suppression mask at $source" }
    val mask = raw.toIntOrNull()
        ?: error("Unparsable suppression mask '$raw' at $source")
    require(mask >= 0 && (mask and SuppressionReason.ALL.inv()) == 0) {
        "Suppression mask $mask contains unknown reasons at $source"
    }
    return mask
}

/**
 * Renders the per-type importer summary.
 *
 * The prefix `Sefaria links importer per-type counters` is load-bearing:
 * pipeline-monitor's `generate_phases.sh` anchors its `links_post` phase marker
 * on it. Everything after it is free-form.
 */
internal fun formatPerTypeCounters(metrics: LinkImportMetrics): String = buildString {
    append(
        "Sefaria links importer per-type counters " +
            "(csv* keyed by the CSV's `Conection Type`; storedWritten keyed by the type " +
            "the link was STORED under, after blank-type inference and base→dependant " +
            "direction normalisation — the two keyings describe different row sets, so " +
            "only the totals are comparable):"
    )
    var rowsRead = 0L
    var dropped = 0L
    var resolvedPairs = 0L
    var written = 0L
    for ((name, t) in metrics.insertedByType) {
        append("\ntype=$name csvRowsRead=${t.rowsRead}")
        append(" csvDropped=${t.dropped}")
        append(" csvResolvedPairs=${t.resolvedPairs}")
        append(" storedWritten=${t.written}")
        rowsRead += t.rowsRead
        dropped += t.dropped
        resolvedPairs += t.resolvedPairs
        written += t.written
    }
    append("\ntotals csvRowsRead=$rowsRead csvDropped=$dropped")
    append(" csvResolvedPairs=$resolvedPairs storedWritten=$written")
    append(" (resolvedPairs-written=${resolvedPairs - written}: pairs dropped by the")
    append(" heading/self-link filters or collapsed by INSERT OR IGNORE)")
}

/**
 * Sefaria's `Dibur Hamatchil` and `Parshanut` name the tool that generated a link
 * (one book each: Yalkut Shimoni, Tzror HaMor, Meshech Hochmah), not what the linked
 * book is. The reader groups commentators by type, so take the type from the book's
 * own schema instead: a Midrash anthology lands with the Midrashim (Otzaria#1508).
 */
internal fun reclassifyProvenanceConnectionType(
    type: ConnectionType,
    srcMeta: BookMeta?,
    tgtMeta: BookMeta?,
): ConnectionType {
    if (type != ConnectionType.DIBUR_HAMATCHIL && type != ConnectionType.PARSHANUT) return type
    val metas = listOfNotNull(srcMeta, tgtMeta)
    metas.firstNotNullOfOrNull { it.dependence }?.let { return it.toConnectionType() }
    if (metas.any { it.topCategoryEn == "Midrash" }) return ConnectionType.MIDRASH
    return ConnectionType.COMMENTARY
}

internal fun mapCsvConnectionType(raw: String, source: String): ConnectionType {
    val type = ConnectionType.fromKnownStringOrNull(raw)
        ?: error("Unmapped Sefaria connection type '$raw' in $source")
    // SOURCE is a virtual/derived type synthesized at read time — it must never
    // be persisted from CSV input.
    if (type == ConnectionType.SOURCE) {
        error("Connection type '$raw' in $source resolves to SOURCE, which is a virtual type and cannot appear in CSV")
    }
    return type
}

/**
 * When the CSV's `Conection Type` is empty, decide the link's type from
 * schema metadata. Returns the dependant side's connection type if exactly
 * one side declares the other as its base text; null otherwise (link stays
 * OTHER, the caller's fallback).
 */
internal fun inferConnectionTypeFromSchema(
    srcBookId: Long,
    tgtBookId: Long,
    srcMeta: BookMeta?,
    tgtMeta: BookMeta?,
): ConnectionType? {
    val targetDependsOnSource = tgtMeta != null && srcBookId in tgtMeta.baseTextBookIds
    val sourceDependsOnTarget = srcMeta != null && tgtBookId in srcMeta.baseTextBookIds
    return when {
        targetDependsOnSource && !sourceDependsOnTarget ->
            tgtMeta.dependence?.toConnectionType() ?: ConnectionType.COMMENTARY
        sourceDependsOnTarget && !targetDependsOnSource ->
            srcMeta.dependence?.toConnectionType() ?: ConnectionType.COMMENTARY
        else -> null
    }
}

/**
 * Blank `Conection Type` recovery, gated by a structural-home check.
 *
 * [inferConnectionTypeFromSchema] promotes any blank-typed link to the dependant
 * side's oriented type (e.g. COMMENTARY) whenever one book declares the other as a
 * base text. That is right for a commentary segment that actually expounds the base
 * segment it points at (Abarbanel → the verse it comments on), but wrong for a lateral
 * cross-reference: `Magen Avraham 302:6` links to `Shulchan Arukh, Orach Chayim 323:6`
 * only because it cites siman 323, even though it lives in siman 302. Promoting those
 * to COMMENTARY makes the commentator panel surface comments from unrelated simanim.
 *
 * Genuine commentary links are explicitly typed `commentary` in Sefaria and never reach
 * this path — only blank-typed links do. So we keep the oriented promotion only when the
 * dependant segment's top-level structural address (siman / perek) matches the base
 * segment it points at; otherwise the link is a [ConnectionType.REFERENCE].
 *
 * Daf-style tokens (`4b` vs `11a`) are compared only when [dafAlignedPairs] contains the
 * (dependant, base) book pair — i.e. the pair's explicitly-typed commentary rows proved
 * same-daf addressing. Books with their own pagination (Rif, Baal HaMaor, Milchamot —
 * whose dapim never match the Bavli's) never pass that pre-scan and stay exempt, as do
 * refs without a comparable top level (whole-book citations, mixed schemes).
 */
internal fun inferBlankConnectionType(
    srcBookId: Long,
    tgtBookId: Long,
    srcMeta: BookMeta?,
    tgtMeta: BookMeta?,
    srcRef: String,
    tgtRef: String,
    dafAlignedPairs: Set<Pair<Long, Long>> = emptySet(),
): ConnectionType? {
    val inferred = inferConnectionTypeFromSchema(srcBookId, tgtBookId, srcMeta, tgtMeta)
        ?: return null
    if (inferred !in ORIENTED_DEPENDANT_TYPES) return inferred

    // Which side is the dependant (commentary) and which is the base it expounds?
    // `srcRef`/`tgtRef` are Citation 1/2, matching the src/tgt book ids respectively.
    val targetDependsOnSource = tgtMeta != null && srcBookId in tgtMeta.baseTextBookIds
    val dependantRef = if (targetDependsOnSource) tgtRef else srcRef
    val baseRef = if (targetDependsOnSource) srcRef else tgtRef
    val dependantBookId = if (targetDependsOnSource) tgtBookId else srcBookId
    val baseBookId = if (targetDependsOnSource) srcBookId else tgtBookId

    val dependantTok = topLevelAddressToken(dependantRef)
    val baseTok = topLevelAddressToken(baseRef)

    val dependantNum = dependantTok.toIntOrNull()
    val baseNum = baseTok.toIntOrNull()
    if (dependantNum != null && baseNum != null) {
        return if (dependantNum != baseNum) ConnectionType.REFERENCE else inferred
    }

    // Daf-vs-daf: comparable only for pairs whose typed rows proved daf alignment.
    if (dependantTok.matches(DAF_TOKEN_REGEX) && baseTok.matches(DAF_TOKEN_REGEX) &&
        (dependantBookId to baseBookId) in dafAlignedPairs
    ) {
        return if (dependantTok != baseTok) ConnectionType.REFERENCE else inferred
    }
    return inferred
}

/**
 * Top component of a Sefaria reference's trailing address run, parsed from the end so
 * textual title parts are skipped. `Magen Avraham 302:6` → "302"; `Shabbat 2a:5` → "2a";
 * `Genesis` → "Genesis" (callers detect non-address tokens via numeric/daf checks).
 */
internal fun topLevelAddressToken(ref: String): String =
    ref.trim()
        .substringAfterLast(' ') // address portion, e.g. "302:6" / "2a:5"
        .substringBefore(':') // top-level component, e.g. "302" / "2a"

/** Numeric form of [topLevelAddressToken]; null for whole-book / daf-style refs. */
internal fun topLevelStructuralIndex(ref: String): Int? =
    topLevelAddressToken(ref).toIntOrNull()

// Talmud daf-amud address token ("2a", "104b"). Dashed ranges ("2a-2b") don't match.
internal val DAF_TOKEN_REGEX = Regex("^[0-9]+[ab]$")

// EIN_MISHPAT included: priorityRank flips it to Talmud→code.
private val ORIENTED_DEPENDANT_TYPES = ConnectionType.SOURCE_VIEW_TYPES

/**
 * Resolves which side of an oriented-dependant link is the base text and which
 * is the dependant. The output pair is (sourceConnectionType, targetConnectionType)
 * where `SOURCE` marks the base-text side — i.e. the side we'd later flip onto
 * the `target` column when storing the canonical base→dependant row.
 *
 * Signals, by descending strength:
 *   1. **Schema `base_text_titles`** — explicit declaration that book A depends
 *      on book B. This is Sefaria's own metadata and is right by construction.
 *   2. **Schema `dependence`** — one side is flagged as dependant and the other
 *      isn't, so the non-dependant side is the base.
 *   3. **`isBaseBook` curated flag** — kept for books that lack schema info
 *      but appear in our priority list.
 *   4. **`priorityRank`** — last-resort heuristic when both sides are equivalent
 *      under all signals above.
 *
 * When no signal fires, the CSV direction is kept as-is.
 */
internal fun resolveDirectionalConnectionTypesForMeta(
    baseType: ConnectionType,
    sourceBookId: Long,
    targetBookId: Long,
    sourceMeta: BookMeta?,
    targetMeta: BookMeta?
): Pair<ConnectionType, ConnectionType> {
    if (baseType !in ORIENTED_DEPENDANT_TYPES) {
        return baseType to baseType
    }

    if (sourceMeta == null || targetMeta == null) {
        return baseType to baseType
    }

    // A reference work (e.g. Otzar La'azei Rashi, a dictionary of Rashi's Old French
    // glosses) is neither the base nor a commentary of the texts it explains. Sefaria
    // still types those rows `commentary`, and since a dictionary has no `dependence`
    // the asymmetry below would make it Rashi's base (Zayit issue #503). Links inside
    // the reference category (Hafla'ah she-ba'Arakhin on Sefer HeArukh) stay oriented.
    if (sourceMeta.isReferenceWork != targetMeta.isReferenceWork) {
        return ConnectionType.REFERENCE to ConnectionType.REFERENCE
    }

    // (1) Strongest signal: explicit base_text_titles declaration.
    val targetDependsOnSource = sourceBookId in targetMeta.baseTextBookIds
    val sourceDependsOnTarget = targetBookId in sourceMeta.baseTextBookIds
    if (targetDependsOnSource && !sourceDependsOnTarget) {
        return baseType to ConnectionType.SOURCE
    }
    if (sourceDependsOnTarget && !targetDependsOnSource) {
        return ConnectionType.SOURCE to baseType
    }

    // (2) Schema `dependence` asymmetry — one side is dependant, the other isn't.
    val sourceIsDependant = sourceMeta.dependence != null
    val targetIsDependant = targetMeta.dependence != null
    if (!sourceIsDependant && targetIsDependant) {
        return baseType to ConnectionType.SOURCE
    }
    if (sourceIsDependant && !targetIsDependant) {
        return ConnectionType.SOURCE to baseType
    }

    // (3) Curated isBaseBook flag.
    if (sourceMeta.isBaseBook && !targetMeta.isBaseBook) {
        return baseType to ConnectionType.SOURCE
    }
    if (!sourceMeta.isBaseBook && targetMeta.isBaseBook) {
        return ConnectionType.SOURCE to baseType
    }

    // (4) priorityRank (lower = more primary).
    val sourceRank = sourceMeta.priorityRank
    val targetRank = targetMeta.priorityRank
    if (sourceRank != null && targetRank != null) {
        if (sourceRank < targetRank) return baseType to ConnectionType.SOURCE
        if (targetRank < sourceRank) return ConnectionType.SOURCE to baseType
    }

    // (5) No directional signal succeeded — neither schema declares the
    // other, dependence flags agree, isBaseBook flags agree, priorityRank
    // is inconclusive or unavailable on both sides. The CSV's oriented
    // type was provided without any structural backing, so it likely
    // reflects a lateral citation, not a base→dep relation.
    //
    // Concrete examples this catches:
    //   • Both-dependant: Bartenura on Torah ↔ Rashi on Genesis. Sefaria
    //     ships both with `dependence: Commentary`. Neither lists the
    //     other in `base_text_titles`. Density chaining excluded the
    //     pair (ratio 0.69 < 0.8). Bartenura is a direct Torah commentary
    //     that cites Rashi, not a super-commentary on Rashi.
    //   • Both-primary, neither in priority list: Sod Yesharim ↔ Zohar.
    //     Both have `dependence: null` and are not in the curated priority
    //     list. The CSV labels the cross-citation `commentary`, but
    //     neither work depends on the other in any structural sense —
    //     Sod Yesharim is a Chassidic work by R. Gershon Leiner that
    //     cites Zohar.
    //
    // Downgrading the type to OTHER keeps the link visible as a
    // cross-reference (it still shows in the connections panel) without
    // polluting either side's SOURCE virtual view.
    return ConnectionType.OTHER to ConnectionType.OTHER
}
