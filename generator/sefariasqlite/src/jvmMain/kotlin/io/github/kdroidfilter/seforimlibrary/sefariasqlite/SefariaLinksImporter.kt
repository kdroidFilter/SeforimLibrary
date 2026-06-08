package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import java.nio.file.Files
import java.nio.file.Path

internal class SefariaLinksImporter(
    private val repository: SeforimRepository,
    private val bindings: IdAllocatorBindings,
    private val logger: Logger
) {
    suspend fun processLinksInParallel(
        linksDir: Path,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>,
        headingLineIds: Set<Long> = emptySet()
    ) = coroutineScope {
        // Pre-register all connection types we'll use so their ids are stable
        // (so `link.connectionTypeId` is reproducible across builds).
        ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }

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
                    headingLineIds = headingLineIds,
                    linkChannel = linkChannel
                )
            }
        }

        // Launch batch inserter
        val inserter = launch {
            val batch = mutableListOf<Link>()
            for (link in linkChannel) {
                batch += link
                if (batch.size >= SefariaImportTuning.LINK_BATCH_SIZE) {
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
        headingLineIds: Set<Long>,
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

                // Hoisted: `conn` is constant across the inner pair loop, no
                // reason to re-parse it for every (from, to) combination.
                val csvConnectionType = ConnectionType.fromString(conn)
                // `Conection Type` is blank for ~36% of CSV rows. We try to
                // recover those via schema metadata inside the inner loop —
                // the inference is per-pair because the bookId depends on the
                // resolved line.
                val connIsBlank = conn.isBlank() ||
                    conn.equals("none", ignoreCase = true)

                for (from in fromRefs) {
                    for (to in toRefs) {
                        val srcLineIndex = from.lineIndex - 1
                        val tgtLineIndex = to.lineIndex - 1
                        val srcLine = lineKeyToId[from.path to srcLineIndex] ?: continue
                        val tgtLine = lineKeyToId[to.path to tgtLineIndex] ?: continue
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
                            ) ?: csvConnectionType
                        } else {
                            csvConnectionType
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
                            storedTgtLineIndex, storedType) =
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
                                    connectionType = baseConnectionType
                                )
                            } else {
                                StoredLink(
                                    srcBookId = srcBookId,
                                    tgtBookId = tgtBookId,
                                    srcLineId = srcLine,
                                    tgtLineId = tgtLine,
                                    tgtLineIndex = tgtLineIndex,
                                    connectionType = forwardType
                                )
                            }

                        val typeId = bindings.upsertConnectionType(storedType.name)

                        // Flag: was this orientation chosen because the target's
                        // schema **explicitly declares** the source as a base text?
                        // Only true for Sefaria-declared `base_text_titles` matches
                        // — NOT for density chaining, primary-base inference, or
                        // priorityRank fallback. Used by the SOURCE virtual view
                        // to boost Sefaria-confirmed bases above lateral citations
                        // (e.g. Mishnah Avot at #1 for Nachalat Avot, even though
                        // Tehillim has 4× more citations).
                        val storedTgtMeta = bookMetaById[storedTgtBook]
                        val isDeclaredBase = storedTgtMeta != null &&
                            storedSrcBook in storedTgtMeta.sefariaDeclaredBaseTextBookIds

                        linkChannel.send(
                            Link(
                                id = bindings.allocator.linkId(storedSrcLine, storedTgtLine, typeId),
                                sourceBookId = storedSrcBook,
                                targetBookId = storedTgtBook,
                                sourceLineId = storedSrcLine,
                                targetLineId = storedTgtLine,
                                targetLineIndex = storedTgtLineIndex,
                                connectionType = storedType,
                                isDeclaredBase = isDeclaredBase,
                            )
                        )
                    }
                }
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
    )

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
    suspend fun demoteCrossCorpusDependantLinks() {
        val dependantTypes = listOf(
            "COMMENTARY", "SUPER_COMMENTARY", "TARGUM", "MIDRASH", "PARSHANUT",
        ).joinToString(",") { "'$it'" }
        // Build a temp table (bookId, corpusKey) for every book — corpusKey
        // is the top-level Sefaria-category title the book transitively
        // descends from (NULL for books whose chain doesn't reach a known
        // corpus root).
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
        // category_closure(ancestorId, descendantId) lets us tag each book
        // by checking whether any of its category ancestors matches a known
        // corpus root. CASE picks the most-specific matching corpus name.
        repository.executeRawQuery(
            """
            INSERT INTO _book_corpus (bookId, corpus)
            SELECT b.id, MIN(c.title) AS corpus
            FROM book b
            JOIN category_closure cc ON cc.descendantId = b.categoryId
            JOIN category c ON c.id = cc.ancestorId
            WHERE c.title IN ('תנ״ך','תלמוד','משנה','משניות','הלכה','חסידות','קבלה','מדרש','מוסר','ספרי מוסר','מחשבת ישראל')
            GROUP BY b.id
            """.trimIndent()
        )
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
            WHERE isDeclaredBase = 0
              AND connectionTypeId IN (SELECT id FROM connection_type WHERE name IN ($dependantTypes))
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

        // hasSourceConnection: virtual flag — set when this book is the *target*
        // (dependant side) of any stored *oriented* dependant link. Only types
        // whose direction has clear base→dep semantics are considered; lateral
        // types (QUOTATION, MISHNAH_IN_TALMUD, MESORAT_HASHAS, RELATED) are NOT
        // sources — Talmud quoting Mishna does not make Talmud a "source" of
        // Mishna. EIN_MISHPAT is included: it is the canonical halakhic-index
        // pointer from a Talmud sugya to the matching halakhah in Mishneh
        // Torah / Shulchan Arukh / Tur (the code derives FROM the Talmud).
        // Keep this list in sync with the mirror SOURCE queries in LinkQueries.sq.
        val dependantTypes = listOf(
            "COMMENTARY", "SUPER_COMMENTARY", "TARGUM", "MIDRASH",
            "PARSHANUT", "DIBUR_HAMATCHIL", "EIN_MISHPAT",
        ).joinToString(",") { "'$it'" }
        repository.executeRawQuery(
            "UPDATE book SET hasSourceConnection=1 WHERE id IN (" +
                "SELECT DISTINCT l.targetBookId FROM link l " +
                "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
                "WHERE ct.name IN ($dependantTypes) AND l.sourceBookId != l.targetBookId" +
                ")"
        )
    }
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
 * segment it points at; otherwise the link is a [ConnectionType.REFERENCE]. Refs without
 * a parseable numeric top level (whole-book citations, daf-style `2a`) are left as
 * inferred — the gate only fires on a confident mismatch.
 */
internal fun inferBlankConnectionType(
    srcBookId: Long,
    tgtBookId: Long,
    srcMeta: BookMeta?,
    tgtMeta: BookMeta?,
    srcRef: String,
    tgtRef: String,
): ConnectionType? {
    val inferred = inferConnectionTypeFromSchema(srcBookId, tgtBookId, srcMeta, tgtMeta)
        ?: return null
    if (inferred !in ORIENTED_DEPENDANT_TYPES) return inferred

    // Which side is the dependant (commentary) and which is the base it expounds?
    // `srcRef`/`tgtRef` are Citation 1/2, matching the src/tgt book ids respectively.
    val targetDependsOnSource = tgtMeta != null && srcBookId in tgtMeta.baseTextBookIds
    val dependantRef = if (targetDependsOnSource) tgtRef else srcRef
    val baseRef = if (targetDependsOnSource) srcRef else tgtRef

    val dependantTop = topLevelStructuralIndex(dependantRef)
    val baseTop = topLevelStructuralIndex(baseRef)
    return if (dependantTop != null && baseTop != null && dependantTop != baseTop) {
        ConnectionType.REFERENCE
    } else {
        inferred
    }
}

/**
 * Leading (top-level) numeric index of a Sefaria reference — the top component of its
 * trailing address run, parsed from the end so textual title parts are skipped.
 * `Magen Avraham 302:6` → 302; `Shulchan Arukh, Orach Chayim 323:6` → 323;
 * `Rashi on Genesis 1:1:1` → 1. Returns null when the top component is not purely
 * numeric — whole-book refs (`Genesis`) and daf-style refs (`Shabbat 2a`) — so such
 * links are never demoted by the structural gate.
 */
internal fun topLevelStructuralIndex(ref: String): Int? =
    ref.trim()
        .substringAfterLast(' ') // address portion, e.g. "302:6" / "2a:5"
        .substringBefore(':') // top-level component, e.g. "302" / "2a"
        .toIntOrNull()

private val ORIENTED_DEPENDANT_TYPES = setOf(
    ConnectionType.COMMENTARY,
    ConnectionType.SUPER_COMMENTARY,
    ConnectionType.TARGUM,
    ConnectionType.MIDRASH,
    ConnectionType.PARSHANUT,
    ConnectionType.DIBUR_HAMATCHIL,
    // Ein Mishpat / Ner Mitzvah is the standard halakhic-index layer on the
    // Talmud folio that anchors each sugya to the matching halakhah in
    // Mishneh Torah / Tur / Shulchan Arukh / Sefer Mitzvot Gadol. Sefaria
    // ships these as `ein mishpat / ner mitsvah` Conection Type. The CSV
    // typically lists the halakhic code first (e.g. `Mishneh Torah, Sabbath
    // 1:1 → Shabbat 2a`). We treat them as oriented so the priorityRank
    // fallback swaps the row into Talmud→code direction (Talmud sits much
    // earlier in the priority list than MT/SA/Tur), which makes the Talmud
    // tractate appear in the code's SOURCE virtual view.
    ConnectionType.EIN_MISHPAT,
)

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
