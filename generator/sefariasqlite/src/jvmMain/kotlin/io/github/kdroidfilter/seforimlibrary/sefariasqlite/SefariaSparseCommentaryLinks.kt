package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.Link

/**
 * A commentary that Sefaria links to its base text only partly, the rest of it being linked
 * to an intermediate commentary on that base. English titles.
 */
private data class SparseCommentaryTitles(val commentary: String, val intermediate: String, val base: String)

/**
 * The Darkhei Moshe glosses the Beit Yosef and follows its seif numbering. Sefaria links a
 * Darkhei Moshe seif to the Tur only when the Tur carries its own marker for it (about 2,000
 * of 6,000 seifim); the others are linked to the Beit Yosef seif they gloss, so they never
 * showed next to the Tur (Zayit issue #462).
 */
private val sparselyLinkedCommentaries = listOf(
    SparseCommentaryTitles(commentary = "Darkhei Moshe", intermediate = "Beit Yosef", base = "Tur"),
)

/**
 * A content line of a book with its top-level section (e.g. "orach chayim 5") and its
 * segment address (e.g. "orach chayim 5:2" for "5:2:1").
 */
internal data class SectionLine(val lineId: Long, val lineIndex: Int, val section: String, val segment: String)

/** A sparsely linked commentary, the commentary it is linked through, and their base text. */
internal data class SparseCommentary(
    val commentaryBookId: Long,
    val intermediateBookId: Long,
    val baseBookId: Long,
    val commentaryLines: List<SectionLine>,
    val baseLines: List<SectionLine>,
)

/** A missing link from a base line to a commentary line. */
internal data class CommentaryAnchor(val baseLineId: Long, val commentaryLine: SectionLine)

/** Collects the books of every [sparselyLinkedCommentaries] entry present in the export. */
internal fun collectSparseCommentaries(
    refs: List<RefEntry>,
    lineKeyToId: Map<Pair<String, Int>, Long>,
    lineIdToBookId: Map<Long, Long>,
): List<SparseCommentary> {
    fun linesOf(title: String): Pair<Long, List<SectionLine>>? {
        val lines = refs
            .filter { it.ref.startsWith("$title, ") }
            .mapNotNull { entry ->
                val lineId = lineKeyToId[entry.path to entry.lineIndex - 1] ?: return@mapNotNull null
                val address = canonicalCitation(entry.ref.removePrefix(title))
                SectionLine(
                    lineId = lineId,
                    lineIndex = entry.lineIndex - 1,
                    section = address.substringBefore(':').trim(),
                    segment = address.substringBeforeLast(':').trim(),
                )
            }
            .sortedBy { it.lineIndex }
        val bookId = lines.firstOrNull()?.let { lineIdToBookId[it.lineId] } ?: return null
        return bookId to lines
    }

    return sparselyLinkedCommentaries.mapNotNull { titles ->
        val (commentaryBookId, commentaryLines) = linesOf(titles.commentary) ?: return@mapNotNull null
        val (intermediateBookId, _) = linesOf(titles.intermediate) ?: return@mapNotNull null
        val (baseBookId, baseLines) = linesOf(titles.base) ?: return@mapNotNull null
        SparseCommentary(commentaryBookId, intermediateBookId, baseBookId, commentaryLines, baseLines)
    }
}

/**
 * Commentary links stored between the books of a [SparseCommentary], each keyed by the
 * dependant line id with the ids of the lines it depends on.
 */
internal class SparseCommentaryLinks {
    val baseByCommentaryLine = HashMap<Long, MutableSet<Long>>()
    val intermediateByCommentaryLine = HashMap<Long, MutableSet<Long>>()
    val baseByIntermediateLine = HashMap<Long, MutableSet<Long>>()

    /** Records a stored COMMENTARY [link] (base → dependant) if it joins two books of [commentary]. */
    fun record(commentary: SparseCommentary, link: Link) {
        val map = when (link.sourceBookId to link.targetBookId) {
            commentary.baseBookId to commentary.commentaryBookId -> baseByCommentaryLine
            commentary.intermediateBookId to commentary.commentaryBookId -> intermediateByCommentaryLine
            commentary.baseBookId to commentary.intermediateBookId -> baseByIntermediateLine
            else -> return
        }
        map.getOrPut(link.targetLineId) { HashSet() } += link.sourceLineId
    }
}

/**
 * Anchors the commentary lines without a link to their base text through the intermediate
 * commentary: a line linked to an intermediate segment gets that segment's base lines. Lines
 * of a segment share its anchors, so a segment's later paragraphs follow its first one, and a
 * segment linked to the base keeps those links. Only base lines of the commentary line's own
 * section are kept, so lateral citations of other sections are not followed. Lines linked to
 * neither book stay unlinked.
 */
internal fun anchorUnlinkedCommentaryLines(
    commentary: SparseCommentary,
    links: SparseCommentaryLinks,
): List<CommentaryAnchor> {
    val baseSectionByLineId = commentary.baseLines.associate { it.lineId to it.section }

    fun SectionLine.inOwnSection(baseLineIds: Iterable<Long>): Set<Long> =
        baseLineIds.filterTo(LinkedHashSet()) { baseSectionByLineId[it] == section }

    fun SectionLine.directBase(): Set<Long> = inOwnSection(links.baseByCommentaryLine[lineId].orEmpty())

    fun SectionLine.baseThroughIntermediate(): Set<Long> = inOwnSection(
        links.intermediateByCommentaryLine[lineId].orEmpty()
            .flatMap { links.baseByIntermediateLine[it].orEmpty() }
    )

    return commentary.commentaryLines
        .groupBy { it.segment }
        .values
        .flatMap { segment ->
            val baseLineIds = segment.flatMapTo(LinkedHashSet()) { it.directBase() }
                .ifEmpty { segment.flatMapTo(LinkedHashSet()) { it.baseThroughIntermediate() } }
            segment
                .filter { it.directBase().isEmpty() }
                .flatMap { line -> baseLineIds.map { CommentaryAnchor(it, line) } }
        }
}
