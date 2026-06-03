package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.exists

/**
 * Density-based sibling-chaining for super-commentaries that Sefaria didn't
 * encode in [BookMeta.baseTextBookIds].
 *
 * Sefaria's individual schemas only chain a fraction of super-commentaries
 * (≈ 292 / 5505 dependant books, mostly Talmud-side Rif/Ran/HaMaor families).
 * Tanakh super-commentaries like Mizrachi, Gur Aryeh and Levush HaOrah are
 * declared as direct commentaries on the Torah — even though they are in
 * fact super-commentaries on Rashi.
 *
 * Sefaria itself exports an aggregated link-density file at
 * `links/links_by_book.csv` (`Text 1, Text 2, Link Count`). The ratio
 *
 *     r(D, S) = linkCount(D, S) / linkCount(D, P)
 *
 * where `D` is a dependant book, `P` is a base it declares, and `S` is
 * another dependant sharing `P` as base — separates two qualitatively
 * different populations:
 *
 *   - `r ≈ 1.0`: D *pivots through* S (Mizrachi 0.97, Gur Aryeh 0.94,
 *     Levush HaOrah 0.85). These are real super-commentaries.
 *   - `r ≈ 0.5–0.7`: D treats S as a *secondary citation* (Bartenura on
 *     Torah cites Rashi at 0.65 but its primary text is Genesis directly).
 *     These are NOT super-commentaries.
 *
 * The original Phase-3 histogram showed a clean valley below 0.5 separating
 * noise from signal, but within the signal band the citation-vs-pivot
 * distinction sits around 0.7–0.8. Using `0.8` retains the four canonical
 * Tanakh super-commentaries (Mizrachi, Gur Aryeh, Levush, Siftei Chakhamim)
 * while excluding Bartenura-style "directly-on-Torah, cites Rashi" books.
 */
internal const val LINK_DENSITY_CHAIN_THRESHOLD: Double = 0.8

/**
 * Parses `links_by_book.csv` and indexes link counts by (bookId, bookId).
 * The CSV stores English titles which we resolve to bookIds via the same
 * normalized title map that the rest of the importer uses. Pairs that
 * don't fully resolve are dropped — they would never be queried anyway.
 *
 * Returned map is keyed by `(min, max)` so callers don't need to know
 * the original direction.
 */
internal fun parseLinksByBookCsv(
    file: Path,
    normalizedTitleToBookId: Map<String, Long>,
): Map<Pair<Long, Long>, Int> {
    if (!file.exists()) return emptyMap()
    val out = HashMap<Pair<Long, Long>, Int>(256_000)
    Files.newBufferedReader(file).use { reader ->
        val iter = reader.lineSequence().iterator()
        if (!iter.hasNext()) return emptyMap()
        // Header: Text 1, Text 2, Link Count
        iter.next()
        while (iter.hasNext()) {
            val row = parseCsvLine(iter.next())
            if (row.size < 3) continue
            val a = normalizedTitleToBookId[normalizeTitleKey(row[0]) ?: continue] ?: continue
            val b = normalizedTitleToBookId[normalizeTitleKey(row[1]) ?: continue] ?: continue
            if (a == b) continue
            val count = row[2].trim().toIntOrNull() ?: continue
            val key = if (a < b) a to b else b to a
            // CSV has no duplicates per the data audit; keep max defensively
            // in case Sefaria changes that later.
            val existing = out[key]
            if (existing == null || count > existing) out[key] = count
        }
    }
    return out
}

/**
 * Minimum absolute link count to a candidate base before considering the edge.
 * Sefaria's `links_by_book.csv` is noisy below this threshold (sporadic
 * cross-references) and the asymmetry ratio loses meaning.
 */
internal const val LINK_DENSITY_BASE_FLOOR: Int = 50

/**
 * For dependants whose schema declares `dependence` but ships an empty
 * `base_text_titles` list (74 books in the current Sefaria export — e.g.
 * Bartenura on Torah, Tzafnat Pa'neach on Torah, Ralbag on Torah, Ri Migash
 * on Bava Batra, …), pick the **primary** books they are most densely
 * linked to and assign them as their bases.
 *
 * Strictly data-driven: a candidate base is kept iff
 *   1. `linkCount(D, B) >= LINK_DENSITY_BASE_FLOOR`, **and**
 *   2. `bookMetaById[B].dependence == null` (B is itself a primary text in
 *      Sefaria's model — Tanakh, Talmud tractate, Mishnah, etc.).
 *
 * The downstream [applyLinkDensitySiblingChaining] then walks the asymmetric
 * density rule from these primary bases and adds the natural intermediate
 * commentators (e.g. Rashi-on-Torah for Bartenura) on its own.
 */
internal fun inferPrimaryBasesForEmptyDeclaredBookmeta(
    bookMetaById: MutableMap<Long, BookMeta>,
    linkCountByBookPair: Map<Pair<Long, Long>, Int>,
    logger: Logger,
): Pair<Int, Int> {
    if (linkCountByBookPair.isEmpty()) return 0 to 0
    // Pre-compute per-book incident link counts so we can scan candidates per book
    // without iterating the full pair map each time.
    val incidentByBook = HashMap<Long, MutableList<Pair<Long, Int>>>()
    for ((pair, count) in linkCountByBookPair) {
        if (count < LINK_DENSITY_BASE_FLOOR) continue
        val (a, b) = pair
        incidentByBook.getOrPut(a) { ArrayList() }.add(b to count)
        incidentByBook.getOrPut(b) { ArrayList() }.add(a to count)
    }

    var booksTouched = 0
    var basesAssigned = 0
    for ((d, meta) in bookMetaById.toMap()) {
        if (meta.dependence == null) continue
        if (meta.baseTextBookIds.isNotEmpty()) continue
        val incident = incidentByBook[d] ?: continue
        val primaryCandidates = incident
            .asSequence()
            .filter { (b, _) -> bookMetaById[b]?.dependence == null }
            .sortedByDescending { it.second }
            .map { it.first }
            .toHashSet()
        if (primaryCandidates.isEmpty()) continue
        bookMetaById[d] = meta.copy(baseTextBookIds = primaryCandidates)
        booksTouched++
        basesAssigned += primaryCandidates.size
    }
    logger.i {
        "Primary-base inference for empty-base dependants: " +
            "assigned $basesAssigned base edges across $booksTouched books"
    }
    return booksTouched to basesAssigned
}

/**
 * For every dependant book `D` with declared base `P`, finds siblings `S`
 * (other dependants that also declare `P` as base) whose link-density
 * ratio to `D` clears [LINK_DENSITY_CHAIN_THRESHOLD], and adds `S` to
 * `D.baseTextBookIds`.
 *
 * This *augments* Sefaria's metadata; it never removes a base text that
 * Sefaria already declared. Returns `(booksTouched, edgesAdded)` for logging.
 */
internal fun applyLinkDensitySiblingChaining(
    bookMetaById: MutableMap<Long, BookMeta>,
    linkCountByBookPair: Map<Pair<Long, Long>, Int>,
    logger: Logger,
): Pair<Int, Int> {
    if (linkCountByBookPair.isEmpty()) return 0 to 0

    // Index: for each declared base P, which dependant books declared it?
    // We use baseTextBookIds (post-resolution) — that's the source of truth
    // after schema parsing.
    val basesToDependants = HashMap<Long, MutableSet<Long>>()
    for ((bookId, meta) in bookMetaById) {
        if (meta.dependence == null) continue
        for (baseId in meta.baseTextBookIds) {
            basesToDependants.getOrPut(baseId) { HashSet() }.add(bookId)
        }
    }

    fun linkCount(a: Long, b: Long): Int {
        if (a == b) return 0
        val key = if (a < b) a to b else b to a
        return linkCountByBookPair[key] ?: 0
    }

    var booksTouched = 0
    var edgesAdded = 0
    for ((d, meta) in bookMetaById.toMap()) {
        if (meta.dependence == null) continue
        val declaredBases = meta.baseTextBookIds
        if (declaredBases.isEmpty()) continue

        // Aggregate candidate siblings by their schema `collective_title.en`
        // (e.g. "Rashi" groups the 5 Rashi-on-Torah volumes). The per-volume
        // ratio is too noisy to distinguish a super-commentary (Mizrachi:
        // all 5 volumes ≥ 0.91) from a citation pattern (Bartenura on Torah:
        // volumes spread 0.57–0.82). The per-collective aggregate
        // `Σ lc(D, S_i) / Σ lc(D, p_i_shared)` collapses that volume noise:
        // Mizrachi→Rashi = 0.94, Levush HaOrah→Rashi = 0.83, Bartenura→Rashi
        // = 0.69. Threshold 0.8 then cleanly separates them.
        val membersByCollective = HashMap<String, MutableSet<Long>>()
        val sharedBasesByCollective = HashMap<String, MutableSet<Long>>()
        for (p in declaredBases) {
            val nDP = linkCount(d, p)
            if (nDP < LINK_DENSITY_BASE_FLOOR) continue
            val siblings = basesToDependants[p] ?: continue
            for (s in siblings) {
                if (s == d || s in declaredBases) continue
                val nDS = linkCount(d, s)
                if (nDS == 0) continue
                // Asymmetry guard: only chain D→S when S is *more* directly
                // linked to the shared base P than D is. Picks the canonical
                // commentary (S) as base of the deeper-chained super-
                // commentary (D), and stops mutual declaration that would
                // cancel out in the resolver.
                val nSP = linkCount(s, p)
                if (nSP <= nDP) continue
                // Singletons (no collective_title) get a per-book bucket so
                // they're still considered, just on a per-book ratio.
                val key = bookMetaById[s]?.collectiveTitleEn ?: "book#$s"
                membersByCollective.getOrPut(key) { HashSet() }.add(s)
                sharedBasesByCollective.getOrPut(key) { HashSet() }.add(p)
            }
        }

        val additions = HashSet<Long>()
        for ((key, members) in membersByCollective) {
            val sharedBases = sharedBasesByCollective[key] ?: continue
            val sumDS = members.sumOf { linkCount(d, it) }
            val sumDP = sharedBases.sumOf { linkCount(d, it) }
            if (sumDP == 0) continue
            val ratio = sumDS.toDouble() / sumDP.toDouble()
            if (ratio >= LINK_DENSITY_CHAIN_THRESHOLD) {
                additions.addAll(members)
            }
        }
        if (additions.isNotEmpty()) {
            bookMetaById[d] = meta.copy(baseTextBookIds = declaredBases + additions)
            booksTouched++
            edgesAdded += additions.size
        }
    }
    logger.i {
        "Link-density chaining: added $edgesAdded sibling base edges across $booksTouched books " +
            "(threshold $LINK_DENSITY_CHAIN_THRESHOLD)"
    }
    return booksTouched to edgesAdded
}
