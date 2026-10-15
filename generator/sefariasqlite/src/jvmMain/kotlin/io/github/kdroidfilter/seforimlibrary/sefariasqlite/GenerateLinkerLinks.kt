package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.ids.DiskBackedLinkIdAllocator
import io.github.kdroidfilter.seforimlibrary.common.linker.LinkerInputView
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.LinkAnchor
import io.github.kdroidfilter.seforimlibrary.core.models.LinkRange
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

/**
 * Phase-2 LINKER importer (stage 5): turns LinkerToOtzaria's ref-based artifacts into
 * clickable, delta-stable in-app links.
 *
 * For each artifact record `(book_key, line_index, start, end, target_ref)`:
 *  • target ← `resolveRefs(target_ref)` (the SAME resolver Sefaria links use) → a RefEntry;
 *    its (path, lineIndex) maps to a lineId via the sidecar → `targetLineId`.
 *  • source ← `BookKey(source_name, canonical_he_title)` → `bookId` (built from the DB:
 *    `source.name` + `COALESCE(book.heRef, book.title)`), then `(bookId, line_index)` → lineId.
 *  • write `link(source→target, LINKER)` (stable id via the allocator) + a `link_anchor`
 *    (side=0) whose visible-char range is the whole citation phrase (raw start/end mapped
 *    through `countVisibleChars`). No `<a href>` touches the text — the app renders the
 *    anchor as an internal link.
 *
 * Delta-robustness comes for free: target refs are resolved fresh each build against
 * heRef-keyed line ids, and link ids are allocated stably, so a Sefaria content update
 * costs zero churn. The SOURCE side (whose byte offsets can't be re-resolved) is guarded by
 * `source_hash`: if a source line changed since the snapshot the offsets were computed on,
 * the record is safe-dropped and recovered on the next re-link. LINKER is excluded from the
 * SOURCE virtual view by the repository's allow-list, so no reverse-view wiring is needed.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:generateLinkerLinks \
 *     -PseforimDb=/path/seforim.db -PlinkerArtifacts=/unpacked/artifacts -PlinkerSidecar=/sidecar.tsv
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateLinkerLinks")

    val dbPath = prop("seforimDb", args.getOrNull(0)) ?: Paths.get("build", "seforim.db").toString()
    val artifactsDir = prop("linkerArtifacts", null)
        ?: error("linkerArtifacts (unpacked artifacts/ dir) is required")
    val sidecarPath = prop("linkerSidecar", null)
        ?: error("linkerSidecar (TSV from the Sefaria import) is required")
    // Phase-2 REPLACES LINKER by type, so the inputs are validated before any destructive
    // work: a missing/empty artifacts dir must fail loudly, not rebuild the type to zero.
    val artifactsRoot = File(artifactsDir)
    check(artifactsRoot.isDirectory) { "linkerArtifacts is not a directory: $artifactsDir" }
    val artifactFiles = artifactsRoot.walkTopDown().filter { it.isFile && it.extension == "jsonl" }.toList()
    check(artifactFiles.isNotEmpty()) { "linkerArtifacts contains no .jsonl artifact files: $artifactsDir" }
    // The lineage gate below is opt-in, so its flag is parsed before any destructive work:
    // an unrecognized value must fail the build, never disable the gate in silence.
    val linkerStrict = parseStrictFlag("linkerStrict", prop("linkerStrict", null))

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath, driver)
    // The repository init downgrades the GLOBAL kermit severity to Assert;
    // restore Info so this CLI's logs stay visible.
    Logger.setMinSeverity(Severity.Info)
    val buildStatePath = Paths.get(prop("buildStatePath", null) ?: "$dbPath.buildstate")
    check(Files.isRegularFile(buildStatePath)) { "Phase-2 requires build_state.db at $buildStatePath" }
    var ctLinker: Long? = null
    driver.executeQuery(
        null,
        "SELECT id FROM connection_type WHERE name = ?",
        { cursor ->
            if (cursor.next().value) ctLinker = cursor.getLong(0)
            QueryResult.Value(Unit)
        },
        1,
    ) { bindString(0, ConnectionType.LINKER.name) }
    val linkerTypeId = checkNotNull(ctLinker) { "Database has no LINKER connection type" }

    // Havrouta links sit at implicit rowids ABOVE the persisted link counter (they are
    // deleted+recreated each build outside the allocator), so allocating fresh LINKER
    // ids straight from the counter would collide with them: the link INSERT OR IGNOREs
    // away and its anchors attach to the Havrouta row. Raise the counter past the DB.
    var maxLinkId = 0L
    driver.executeQuery(null, "SELECT COALESCE(MAX(id), 0) FROM link",
        { c -> if (c.next().value) maxLinkId = c.getLong(0) ?: 0L; QueryResult.Value(Unit) }, 0)
    val allocator = DiskBackedLinkIdAllocator.open(
        path = buildStatePath,
        expectedConnectionTypeName = ConnectionType.LINKER.name,
        expectedConnectionTypeId = linkerTypeId,
        minimumNextId = maxLinkId + 1L,
    )
    var sourceLineConnection: Connection? = null

    try {
        repository.executeRawQuery("PRAGMA foreign_keys = OFF")
        repository.executeRawQuery("PRAGMA synchronous = OFF")
        repository.executeRawQuery("PRAGMA journal_mode = OFF")

        // ── sidecar → refsByCanonical / refsByBase + exact target identity ──
        logger.i { "Loading sidecar…" }
        val allRefs = ArrayList<RefEntry>()
        val lineIdByRefKey = HashMap<Pair<String, Int>, Long>()
        // Stable book identity is invariant for every row of one sidecar path. Store it once
        // per path instead of once per line: the full corpus has ≈2.8M ref rows but only
        // thousands of paths.
        val stableBookKeyByPath = HashMap<String, Pair<String, String>>()
        for (row in readLinkerSidecar(sidecarPath)) {
            val key = row.path to row.lineIndex
            val previousLineId = lineIdByRefKey.putIfAbsent(key, row.lineId)
            check(previousLineId == null || previousLineId == row.lineId) {
                "Conflicting line IDs in linker sidecar for $key"
            }
            val stableBookKey = row.sourceName to row.canonicalHeTitle
            val previousBookKey = stableBookKeyByPath.putIfAbsent(row.path, stableBookKey)
            check(previousBookKey == null || previousBookKey == stableBookKey) {
                "Conflicting stable book identities in linker sidecar for ${row.path}"
            }
            allRefs.add(RefEntry(row.ref, row.heRef, row.path, row.lineIndex))
        }
        val refsByCanonical = allRefs.groupBy { canonicalCitation(it.ref) }
        val refsByBase = HashMap<String, RefEntry>()
        // lastByBase — the base's LAST segment: closes the scope of section-level
        // citations (a whole amud/chapter) so the app can show/load the full range.
        val lastByBase = HashMap<String, RefEntry>()
        for (e in allRefs) {
            val base = canonicalBase(e.ref)
            val existing = refsByBase[base]
            if (existing == null || e.lineIndex < existing.lineIndex) refsByBase[base] = e
            val existingLast = lastByBase[base]
            if (existingLast == null || e.lineIndex > existingLast.lineIndex) lastByBase[base] = e
        }
        logger.i { "Sidecar: ${allRefs.size} refs, ${refsByCanonical.size} canonical keys" }

        // LINKER is a generated, type-owned projection. Replace it by type so
        // links that the engine intentionally stopped emitting (especially old
        // heading links) cannot survive forever through upserts.
        var replacedLinks = 0L
        driver.executeQuery(
            null,
            "SELECT COUNT(*) FROM link WHERE connectionTypeId = ?",
            { cursor ->
                if (cursor.next().value) replacedLinks = cursor.getLong(0) ?: 0L
                QueryResult.Value(Unit)
            },
            1,
        ) { bindLong(0, linkerTypeId) }
        if (replacedLinks > 0) {
            val linkerIds = "SELECT id FROM link WHERE connectionTypeId = $linkerTypeId"
            repository.executeRawQuery("DELETE FROM link_anchor WHERE linkId IN ($linkerIds)")
            repository.executeRawQuery("DELETE FROM link_range WHERE linkId IN ($linkerIds)")
            repository.executeRawQuery("DELETE FROM link_coverage WHERE linkId IN ($linkerIds)")
            repository.executeRawQuery("DELETE FROM link WHERE connectionTypeId = $linkerTypeId")
            logger.i { "Removed $replacedLinks existing LINKER links before deterministic rebuild" }
        }

        // ── DB identity maps: BookKey→bookId and (bookId,lineIndex)→lineId ──
        val bookIdByKey = HashMap<Pair<String, String>, Long>()
        driver.executeQuery(null,
            "SELECT b.id, s.name, COALESCE(b.heRef, b.title) FROM book b JOIN source s ON b.sourceId = s.id",
            { c ->
                while (c.next().value) {
                    bookIdByKey[(c.getString(1) ?: "") to (c.getString(2) ?: "")] = c.getLong(0)!!
                }
                QueryResult.Value(Unit)
            }, 0)
        // Target identity metadata stays resident because arbitrary target refs are resolved
        // throughout the artifact stream. Source lines, however, are ordered by book/line in the
        // artifacts and are fetched through the one-entry cache below; a second 5.9M-entry
        // (bookId,lineIndex)→lineId HashMap duplicated the database index and consumed >1 GiB.
        data class TargetLineMeta(val bookId: Long, val lineIndex: Int, val heRef: String?)
        val lineMeta = HashMap<Long, TargetLineMeta>()
        driver.executeQuery(null, "SELECT id, bookId, lineIndex, heRef FROM line",
            { c ->
                while (c.next().value) {
                    val id = c.getLong(0)!!; val bookId = c.getLong(1)!!; val idx = c.getLong(2)!!.toInt()
                    lineMeta[id] = TargetLineMeta(bookId, idx, c.getString(3))
                }
                QueryResult.Value(Unit)
            }, 0)
        logger.i { "DB maps: ${bookIdByKey.size} books, ${lineMeta.size} lines" }

        // Resolve and cache exactly ONE source line. Artifact records are written per book in line
        // order, so consecutive citations hit this cache. This replaces both the corpus-sized
        // source map and the old second lookup by line id while preserving exact row identity.
        // Inline, not LineContentShape: the recovery path overlays this one file onto an
        // older payload whose generator-common may predate that helper.
        var lineContentSplit = false
        driver.executeQuery(null,
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='line_content') " +
                "AND NOT EXISTS(SELECT 1 FROM pragma_table_info('line') WHERE name='content')",
            { c -> if (c.next().value) lineContentSplit = c.getLong(0) == 1L; QueryResult.Value(Unit) }, 0)
        // Anchors hash the plain text; a compressed DB would mismatch every record.
        var lineContentCompressed = false
        driver.executeQuery(null,
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='zstd_dict')",
            { c -> if (c.next().value) lineContentCompressed = c.getLong(0) == 1L; QueryResult.Value(Unit) }, 0)
        check(!lineContentCompressed) { "Phase-2 needs plain line text; run it before compressLineContent" }
        val sourceLineSql = if (lineContentSplit) {
            """
            SELECT l.id, lc.content,
                   COALESCE(NULLIF(TRIM(l.heRef), ''), COALESCE(b.heRef, b.title))
            FROM line l JOIN book b ON l.bookId = b.id JOIN line_content lc ON lc.id = l.id
            WHERE l.bookId = ? AND l.lineIndex = ?
            """.trimIndent()
        } else {
            """
            SELECT l.id, l.content,
                   COALESCE(NULLIF(TRIM(l.heRef), ''), COALESCE(b.heRef, b.title))
            FROM line l JOIN book b ON l.bookId = b.id
            WHERE l.bookId = ? AND l.lineIndex = ?
            """.trimIndent()
        }
        data class SourceLine(val id: Long, val content: String, val contextRef: String)
        // One persistent read-only connection: outside a transaction the driver opens a new
        // connection per statement, re-loading the schema and sqlite_stat1/stat4 every lookup.
        val sourceLineDb = DriverManager.getConnection("jdbc:sqlite:$dbPath").also { sourceLineConnection = it }
        sourceLineDb.createStatement().use { it.execute("PRAGMA query_only = ON") }
        val sourceLineStatement = sourceLineDb.prepareStatement(sourceLineSql)
        var cachedSourceBookId = Long.MIN_VALUE
        var cachedSourceLineIndex = Int.MIN_VALUE
        var cachedSourceLine: SourceLine? = null
        fun sourceLineFor(bookId: Long, lineIndex: Int): SourceLine? {
            if (bookId != cachedSourceBookId || lineIndex != cachedSourceLineIndex) {
                var found: SourceLine? = null
                sourceLineStatement.setLong(1, bookId)
                sourceLineStatement.setLong(2, lineIndex.toLong())
                sourceLineStatement.executeQuery().use { rs ->
                    if (rs.next()) {
                        found = SourceLine(
                            id = rs.getLong(1),
                            content = rs.getString(2) ?: "",
                            contextRef = rs.getString(3) ?: "",
                        )
                    }
                }
                cachedSourceBookId = bookId
                cachedSourceLineIndex = lineIndex
                cachedSourceLine = found
            }
            return cachedSourceLine
        }

        // ── walk artifacts → links + anchors ──
        val json = Json { ignoreUnknownKeys = true }
        val linkBatch = ArrayList<Link>()
        val anchorBatch = ArrayList<LinkAnchor>()
        // Do not retain corpus-sized duplicate indexes here. The allocator already maps each
        // (source,target,type) to its stable link id, and SQLite's INSERT OR IGNORE keys make both
        // link and anchor writes idempotent. Keeping duplicate HashMap/HashSet projections of the
        // full 1.86M-record payload was the remaining Phase-2 heap leak.
        // linkId → (endLineId, endLineIndex): target-side scope end for multi-line
        // citations (a whole amud/section or a dashed range). Written as link_range
        // side=1 so the app shows the range in the title and loads the full content
        // in the preview. No link_coverage rows — target-page surfacing stays
        // first-line-only.
        //
        // KNOWN TRADE-OFF — widest end wins: when one source line cites the same
        // target at two scopes ("27:7" AND "27:7-8"), both resolve to one link
        // (stable id = src,tgt,type) and the wider range is kept, so the narrow
        // citation's preview shows a longer passage that still STARTS at its exact
        // target. Splitting them would either churn every stable link id (scope in
        // the key) or need per-anchor scope columns (schema+app change) — not worth
        // it for the ~0.07% of ranges affected.
        val rangeEndByLink = HashMap<Long, Pair<Long, Int>>()
        var links = 0L; var anchors = 0L; var unresolvedTarget = 0; var ambiguousTarget = 0
        var targetIdentityMismatch = 0; var unmappedSource = 0; var staleSource = 0; var staleContext = 0
        var headingSource = 0
        var missingSourceHash = 0
        var recordsParsed = 0L

        suspend fun flush() {
            if (linkBatch.isNotEmpty()) { repository.insertLinksBatch(linkBatch); linkBatch.clear() }
            if (anchorBatch.isNotEmpty()) { repository.insertLinkAnchorsBatch(anchorBatch); anchorBatch.clear() }
        }

        logger.i { "Processing ${artifactFiles.size} artifact files…" }
        for (file in artifactFiles) {
            file.bufferedReader(Charsets.UTF_8).useLines { lines ->
                for ((rec, nextCitationStart) in boundedArtifactRecords(lines, json)) {
                    recordsParsed++
                    check((rec.context_ref == null) == (rec.relative_direction == null)) {
                        "Malformed contextual LINKER record in ${file.path}: context_ref and " +
                            "relative_direction must be present together"
                    }
                    check(rec.relative_direction == null || rec.relative_direction in setOf("above", "below")) {
                        "Malformed contextual LINKER record in ${file.path}: invalid relative_direction"
                    }
                    // Anchor offsets are a producer contract (artifact.schema.json): 0 <= start < end.
                    check(rec.start >= 0 && rec.end > rec.start) {
                        "Malformed LINKER record in ${file.path}: offsets [${rec.start}, ${rec.end}) " +
                            "must satisfy 0 <= start < end"
                    }
                    // line_index is 0-based by contract (artifact.schema.json: line_index_base const 0).
                    check(rec.line_index_base == 0) {
                        "Malformed LINKER record in ${file.path}: line_index_base=${rec.line_index_base} " +
                            "is unsupported; the contract requires 0"
                    }
                    // Contract check FIRST, before any skip path (unresolved target,
                    // self-link) can hide a hash-less record from -PlinkerStrict.
                    if (rec.source_hash == null) missingSourceHash++
                    // target: resolveRefs → RefEntry → lineId (sidecar) → (bookId, 0-based idx) via lineMeta
                    val targetCandidates = resolveRefs(rec.target_ref, refsByCanonical, refsByBase)
                        .distinctBy { it.path to it.lineIndex }
                    if (targetCandidates.size > 1) { ambiguousTarget++; continue }
                    val tgtEntry = targetCandidates.singleOrNull()
                    val tgtLineId = tgtEntry?.let { lineIdByRefKey[it.path to it.lineIndex] }
                    val tgtMeta = tgtLineId?.let { lineMeta[it] }
                    if (tgtLineId == null || tgtMeta == null) { unresolvedTarget++; continue }
                    val expectedTargetKey = stableBookKeyByPath[tgtEntry.path]
                    val expectedTargetBookId = expectedTargetKey?.let { bookIdByKey[it] }
                    if (!targetIdentityMatches(tgtEntry, tgtMeta.bookId, tgtMeta.lineIndex, tgtMeta.heRef, expectedTargetBookId)) {
                        targetIdentityMismatch++
                        continue
                    }
                    // source: BookKey → bookId → (bookId, line_index) → lineId
                    val srcBookId = bookIdByKey[rec.book_key.source_name to rec.book_key.canonical_he_title]
                    val srcLine = srcBookId?.let { sourceLineFor(it, rec.line_index) }
                    val srcLineId = srcLine?.id
                    if (srcBookId == null || srcLine == null || srcLineId == null) { unmappedSource++; continue }
                    if (srcLineId == tgtLineId) continue

                    // Source-drift guard: the offsets index the snapshot the linker ran on. If this
                    // source line changed since (cross-cycle drift), the offsets are untrustworthy —
                    // safe-drop the whole record (link + anchor); it recovers on the next re-link.
                    // A record with NO hash bypasses the guard — already counted at decode,
                    // fatal under -PlinkerStrict (every engine-produced record carries one).
                    val content = srcLine.content
                    if (isHeadingContent(content)) { headingSource++; continue }
                    // The offsets index the text the linker was handed (LinkerInputView), not
                    // necessarily the stored line; the hash says which of the two it was.
                    val shift = linkerOffsetShift(rec.book_key.source_name, content, rec.source_hash)
                    if (shift == null) { staleSource++; continue }
                    if (rec.context_ref != null && srcLine.contextRef != rec.context_ref) { staleContext++; continue }
                    val start = rec.start + shift
                    val end = rec.end + shift
                    // Content is now hash-verified, so the offsets must index it exactly.
                    check(end <= content.length) {
                        "Out-of-range LINKER offsets in ${file.path}: [${rec.start}, ${rec.end}) " +
                            "+ $shift exceeds source line length ${content.length} for " +
                            "${rec.book_key.canonical_he_title} line ${rec.line_index}"
                    }

                    // The allocator returns the same stable id for repeated (source,target,type)
                    // citations; INSERT OR IGNORE performs the dedup without an unbounded map.
                    val linkId = allocator.linkId(srcLineId, tgtLineId, linkerTypeId)
                    linkBatch.add(Link(
                        id = linkId, sourceBookId = srcBookId, targetBookId = tgtMeta.bookId,
                        sourceLineId = srcLineId, targetLineId = tgtLineId,
                        targetLineIndex = tgtMeta.lineIndex, connectionType = ConnectionType.LINKER,
                    ))
                    // A separate clickable anchor for EACH citation occurrence (same line may cite
                    // the same ref twice at different offsets → one link, two anchors).
                    val cs = countVisibleChars(content, start)
                    val anchorLimit = maxOf(end, minOf(content.length, nextCitationStart?.let { it + shift } ?: content.length))
                    val ce = countVisibleChars(content, closeDanglingBrackets(content, start, end, anchorLimit))
                    if (ce > cs) {
                        anchorBatch.add(LinkAnchor(linkId = linkId, side = 0, charStart = cs, charEnd = ce))
                    }

                    // Multi-line citation scope (whole amud/section or dashed range) →
                    // remember its end for a link_range(side=1) row.
                    val endEntry = resolveRefEnd(rec.target_ref, refsByCanonical, refsByBase, lastByBase)
                    if (endEntry != null &&
                        endEntry.path == tgtEntry.path &&
                        endEntry.lineIndex > tgtEntry.lineIndex
                    ) {
                        val endLineId = lineIdByRefKey[endEntry.path to endEntry.lineIndex]
                        val endMeta = endLineId?.let { lineMeta[it] }
                        val expectedEndKey = stableBookKeyByPath[endEntry.path]
                        val expectedEndBookId = expectedEndKey?.let { bookIdByKey[it] }
                        if (endLineId != null && endMeta != null &&
                            targetIdentityMatches(endEntry, endMeta.bookId, endMeta.lineIndex, endMeta.heRef, expectedEndBookId)
                        ) {
                            val prev = rangeEndByLink[linkId]
                            if (prev == null || endMeta.lineIndex > prev.second) {
                                rangeEndByLink[linkId] = endLineId to endMeta.lineIndex
                            }
                        } else if (endLineId != null || endMeta != null) {
                            targetIdentityMismatch++
                        }
                    }
                    if (linkBatch.size >= 5000) flush()
                }
            }
        }
        flush()
        sourceLineDb.close()
        driver.executeQuery(
            null,
            "SELECT COUNT(*) FROM link WHERE connectionTypeId = ?",
            { c -> if (c.next().value) links = c.getLong(0) ?: 0L; QueryResult.Value(Unit) },
            1,
        ) { bindLong(0, linkerTypeId) }
        driver.executeQuery(
            null,
            "SELECT COUNT(*) FROM link_anchor WHERE linkId IN (SELECT id FROM link WHERE connectionTypeId = ?)",
            { c -> if (c.next().value) anchors = c.getLong(0) ?: 0L; QueryResult.Value(Unit) },
            1,
        ) { bindLong(0, linkerTypeId) }
        if (rangeEndByLink.isNotEmpty()) {
            val ranges = rangeEndByLink.map { (linkId, end) ->
                LinkRange(linkId = linkId, side = 1, endLineId = end.first, endLineIndex = end.second)
            }
            ranges.chunked(5000).forEach { repository.insertLinkRangesBatch(it) }
        }
        logger.i {
            "LINKER: $links links, $anchors anchors, ${rangeEndByLink.size} target ranges " +
                "(heading source: $headingSource, unresolved target: $unresolvedTarget, " +
                "ambiguous target: $ambiguousTarget, target identity mismatch: $targetIdentityMismatch, " +
                "unmapped source: $unmappedSource, stale source: $staleSource, stale context: $staleContext)"
        }

        // Serial-pipeline invariant (-PlinkerStrict): the linker just ran on THIS build's
        // snapshot, so every record's source line must exist, carry a hash, and match it.
        // A violation means the artifacts came from some other snapshot — fail the build.
        // (unresolvedTarget stays advisory: refs to books outside the corpus are expected.)
        if (linkerStrict) {
            check(unmappedSource == 0 && staleSource == 0 && staleContext == 0 &&
                targetIdentityMismatch == 0 && ambiguousTarget == 0 && missingSourceHash == 0
            ) {
                "linkerStrict: unmapped source=$unmappedSource, stale source=$staleSource, " +
                    "stale context=$staleContext, ambiguous target=$ambiguousTarget, " +
                    "target identity mismatch=$targetIdentityMismatch, missing source_hash=$missingSourceHash — " +
                    "artifacts/sidecar do not match this build's exact identity lineage"
            }
            // An empty payload passes every counter above; a strict release must ship LINKER links.
            check(links > 0) {
                "linkerStrict: 0 LINKER links written from $recordsParsed records parsed in " +
                    "${artifactFiles.size} artifact files"
            }
        }

        // LINKER was type-replaced above, so refresh these materialized flags exactly.
        repository.executeRawQuery(
            "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) SELECT id, 0, 0 FROM book")
        repository.executeRawQuery("UPDATE book_has_links SET hasSourceLinks=0, hasTargetLinks=0")
        repository.executeRawQuery(
            "UPDATE book_has_links SET hasSourceLinks=1 WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)")
        repository.executeRawQuery(
            "UPDATE book_has_links SET hasTargetLinks=1 WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)")

        repository.executeRawQuery("PRAGMA foreign_keys = ON")
        repository.executeRawQuery("PRAGMA synchronous = NORMAL")
        repository.executeRawQuery("PRAGMA journal_mode = WAL")
        allocator.commit(extraMeta = mapOf("generator" to "linkerlinks"))
        logger.i {
            "Disk-backed stable link IDs: reused=${allocator.reusedCount}, fresh=${allocator.freshCount}"
        }
        Unit
    } catch (e: Exception) {
        logger.e(e) { "Error generating LINKER links" }
        throw e
    } finally {
        sourceLineConnection?.let { runCatching { it.close() } }
        allocator.close()
        repository.close()
    }
}

/**
 * How far a record's offsets sit from the stored line [content], or null when its
 * [sourceHash] matches neither the line nor a view of it (the line changed since
 * the snapshot).
 *
 * The linker digests and indexes [LinkerInputView.linkerContent], which may leave
 * a line's opening "(א) " (or "<b>(א)</b> ") out; the hash proves which text it
 * was, so the book's view needs no second decision here. A record made before any
 * view (a whole-line hash) still indexes the stored line and is taken as is. A
 * record with no hash cannot be checked: it is read as a Sefaria line's plain view
 * and as the whole line elsewhere; -PlinkerStrict counts it.
 */
internal fun linkerOffsetShift(sourceName: String, content: String, sourceHash: String?): Int? {
    if (sourceHash == null) {
        val mode = if (sourceName == "Sefaria") LinkerInputView.Mode.PLAIN else LinkerInputView.Mode.NONE
        return LinkerInputView.strippedPrefixLength(mode, content)
    }
    return LinkerInputView.offsetShift(content) { linkerContentHash(it) == sourceHash }
}

/**
 * Extend a raw UTF-16 citation range over immediately following closing brackets.
 * Only brackets opened inside the citation may be closed, in nesting order. Whitespace,
 * whitespace entities and inline formatting are transparent; notes, links and other HTML
 * elements are barriers. The source text and the original start are never changed;
 * ambiguous or non-matching suffixes are left outside the anchor.
 *
 * [limit] is an exclusive boundary, e.g. the next citation's start or a rehydrated note's
 * start. Keep this step BEFORE #73's linkerStoredRange mapping to the stored line.
 * The walk is linear in the citation and its adjacent suffix, with O(bracket depth) memory.
 */
internal fun closeDanglingBrackets(content: String, start: Int, end: Int, limit: Int = content.length): Int {
    require(start in 0..end && end <= limit && limit <= content.length)
    var pending: StringBuilder? = null
    var i = start
    while (i < end) {
        val c: Char
        val next: Int
        when (content[i]) {
            '<' -> {
                i = bracketFormattingTagEnd(content, i, end) ?: return end
                continue
            }
            '&' -> {
                val entity = bracketEntity(content, i, end)
                if (entity == null) { i++; continue }
                c = entity.first
                next = entity.second
            }
            else -> { c = content[i]; next = i + 1 }
        }
        when (c) {
            '(', '[' -> {
                val stack = pending ?: StringBuilder().also { pending = it }
                stack.append(if (c == '(') ')' else ']')
            }
            ')', ']' -> {
                val stack = pending
                if (stack != null && stack.isNotEmpty()) {
                    if (stack.last() != c) return end // crossed/malformed brackets: do not guess
                    stack.setLength(stack.length - 1)
                }
                // An unmatched closer at the start is outside our scope; never move start.
            }
        }
        i = next
    }
    val stack = pending ?: return end
    var extendedEnd = end
    while (stack.isNotEmpty() && i < limit) {
        val c: Char
        val next: Int
        when (content[i]) {
            '<' -> {
                i = bracketFormattingTagEnd(content, i, limit) ?: break
                continue
            }
            '&' -> {
                val entity = bracketEntity(content, i, limit) ?: break
                c = entity.first
                next = entity.second
            }
            else -> { c = content[i]; next = i + 1 }
        }
        when {
            c.isWhitespace() -> Unit
            c == stack.last() -> {
                stack.setLength(stack.length - 1)
                extendedEnd = next
            }
            else -> break
        }
        i = next
    }
    return extendedEnd
}

private val bracketFormattingTags = setOf("b", "strong", "i", "em", "span", "u", "s", "strike", "small", "big", "font", "sub")

// Inspect attributes rather than searching the tag text: a title containing class="footnote"
// is not a note, whereas a multi-valued, single-quoted or unquoted class may be one.
private fun bracketFormattingTagEnd(content: String, start: Int, limit: Int): Int? {
    var close = start + 1
    while (close < limit && content[close] != '>') close++
    if (close == limit) return null
    var i = start + 1
    if (content[i] == '/') i++
    val nameStart = i
    while (i < close && content[i].isLetterOrDigit()) i++
    if (content.substring(nameStart, i).lowercase() !in bracketFormattingTags) return null
    if (i < close && !content[i].isWhitespace() && content[i] != '/') return null
    while (i < close) {
        while (i < close && (content[i].isWhitespace() || content[i] == '/')) i++
        if (i == close) break
        val attrStart = i
        while (i < close && !content[i].isWhitespace() && content[i] != '=') i++
        val name = content.substring(attrStart, i)
        if (name.equals("data-commentator", ignoreCase = true)) return null
        while (i < close && content[i].isWhitespace()) i++
        if (i == close || content[i] != '=') continue
        i++
        while (i < close && content[i].isWhitespace()) i++
        if (i == close) return null
        val quote = content[i].takeIf { it == '\'' || it == '"' }
        if (quote != null) i++
        val valueStart = i
        while (i < close && if (quote != null) content[i] != quote else !content[i].isWhitespace()) i++
        if (quote != null && i == close) return null
        if (name.equals("class", ignoreCase = true)) {
            val classes = content.substring(valueStart, i).splitToSequence(' ', '\t', '\n', '\r', '\u000c')
            if (classes.any { it.equals("footnote", true) || it.equals("footnote-marker", true) }) return null
        }
        if (quote != null) i++
    }
    return close + 1
}

// Only decode characters relevant to balancing and whitespace. Match the existing visible-char
// counter's entity length bound so that raw and stored anchor coordinates stay consistent.
private fun bracketEntity(content: String, start: Int, limit: Int): Pair<Char, Int>? {
    val bound = minOf(limit, start + 10)
    var end = start + 1
    while (end < bound && content[end] != ';') end++
    if (end == bound) return null
    val entity = content.substring(start + 1, end)
    val decoded = when (entity) {
        "nbsp" -> '\u00a0'
        "ensp" -> '\u2002'
        "emsp" -> '\u2003'
        "thinsp" -> '\u2009'
        "lpar" -> '('
        "rpar" -> ')'
        "lbrack", "lsqb" -> '['
        "rbrack", "rsqb" -> ']'
        else -> {
            val hex = entity.startsWith("#x", true)
            val digits = when {
                hex -> entity.substring(2)
                entity.startsWith('#') -> entity.substring(1)
                else -> return null
            }
            val number = digits.takeIf { it.isNotEmpty() && it[0] != '+' && it[0] != '-' }
                ?.toIntOrNull(if (hex) 16 else 10)
            number?.takeIf { it in 0..0xffff }?.toChar()
        }
    } ?: return null
    return decoded to end + 1
}

/** SHA-1(UTF-8(content)) truncated to 16 hex chars. MUST equal linker_artifact.content_hash. */
internal fun linkerContentHash(content: String): String {
    val digest = java.security.MessageDigest.getInstance("SHA-1").digest(content.toByteArray(Charsets.UTF_8))
    val sb = StringBuilder(digest.size * 2)
    for (b in digest) { val v = b.toInt() and 0xff; sb.append("0123456789abcdef"[v ushr 4]); sb.append("0123456789abcdef"[v and 0xf]) }
    return sb.substring(0, 16)
}

private val headingContentRegex = Regex(
    pattern = "^[\\s\\uFEFF]*<h[1-6](?:\\s|>)",
    option = RegexOption.IGNORE_CASE,
)

internal fun isHeadingContent(content: String): Boolean =
    headingContentRegex.containsMatchIn(content)

internal fun targetIdentityMatches(
    entry: RefEntry,
    actualBookId: Long,
    actualLineIndex: Int,
    actualHeRef: String?,
    expectedBookId: Long?,
): Boolean = expectedBookId != null &&
    actualBookId == expectedBookId &&
    actualLineIndex == entry.lineIndex - 1 &&
    actualHeRef == entry.heRef

/**
 * The producer groups each book's records by source line. Buffer only that line so a
 * closing bracket cannot be taken from the next citation, even with duplicate targets or
 * unsorted spans within the line. Preserve record order (and hence stable ID allocation).
 */
private fun boundedArtifactRecords(lines: Sequence<String>, json: Json): Sequence<Pair<ArtifactRecord, Int?>> = sequence {
    val group = ArrayList<ArtifactRecord>()
    suspend fun SequenceScope<Pair<ArtifactRecord, Int?>>.emitGroup() {
        if (group.size == 1) {
            yield(group.single() to null)
        } else {
            val starts = group.map { it.start }.distinct().sorted()
            for (record in group) {
                yield(record to starts.getOrNull(starts.binarySearch(record.start) + 1))
            }
        }
        group.clear()
    }
    for (line in lines) {
        if (line.isBlank()) continue
        val record = json.decodeFromString<ArtifactRecord>(line)
        val previous = group.lastOrNull()
        if (previous != null && (previous.book_key != record.book_key || previous.line_index != record.line_index)) {
            emitGroup()
        }
        group.add(record)
    }
    if (group.isNotEmpty()) emitGroup()
}

@Serializable
private data class ArtifactBookKey(val source_name: String, val canonical_he_title: String)

@Serializable
private data class ArtifactRecord(
    val book_key: ArtifactBookKey,
    val line_index: Int,
    val start: Int,
    val end: Int,
    val target_ref: String,
    val line_index_base: Int = 0,
    val source_path: String? = null,
    val source_hash: String? = null,
    val context_ref: String? = null,
    val relative_direction: String? = null,
)

private fun prop(name: String, fallback: String?): String? =
    System.getProperty(name) ?: System.getenv(name.uppercase()) ?: fallback

// Build-gate boolean: only "true"/"false" (any case, surrounding blanks ignored) mean anything.
// "1", "yes", a typo or a bare -Pflag (empty string) fail loudly instead of disabling the gate.
internal fun parseStrictFlag(name: String, raw: String?): Boolean = when (raw?.trim()?.lowercase()) {
    null -> false
    "true" -> true
    "false" -> false
    else -> error(
        "$name: unrecognized value \"$raw\" — accepted values are \"true\" or \"false\" " +
            "(case-insensitive); pass -P$name=true / -P$name=false, or set ${name.uppercase()} in the environment"
    )
}
