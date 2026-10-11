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

// Otzaria: commentary sections left out of the anchoring. In Choshen Mishpat, Sefaria's
// Tur–Beit Yosef links disagree with the Tur's Beit Yosef markers in 2,568 of 3,554 seifim,
// and of the 7 Darkhei Moshe seifim it links both ways, 6 reach another Tur paragraph through
// the markers (v32). Those seifim keep only the links Sefaria gives them.
private val unanchoredSections = mapOf("Darkhei Moshe" to "choshen mishpat ")

// Otzaria: the base prints the intermediate's marker for each of its seifim in the paragraph
// the seif belongs to, e.g. `<i data-commentator="Beit Yosef" data-order="18.1">`.
private fun intermediateMarker(intermediate: String) =
    Regex("""<i data-commentator="${Regex.escape(intermediate)}" data-order="(\d+)(?:\.\d+)?">""")

/**
 * A content line of a book with its top-level section (e.g. "orach chayim 5") and its
 * segment address (e.g. "orach chayim 5:2" for "5:2:1").
 */
internal data class SectionLine(val lineId: Long, val lineIndex: Int, val section: String, val segment: String)

/**
 * A sparsely linked commentary, the commentary it is linked through, and their base text.
 * [markedBaseLines] maps an intermediate line id to the base lines that print its seif's marker.
 */
internal data class SparseCommentary(
    val commentaryBookId: Long,
    val intermediateBookId: Long,
    val baseBookId: Long,
    val commentaryLines: List<SectionLine>,
    val baseLines: List<SectionLine>,
    val markedBaseLines: Map<Long, Set<Long>>,
)

/** A missing link from a base line to a commentary line. */
internal data class CommentaryAnchor(val baseLineId: Long, val commentaryLine: SectionLine)

/**
 * Collects the books of every [sparselyLinkedCommentaries] entry present in the export.
 * [linesByPath] holds each book's line contents by its path.
 */
internal fun collectSparseCommentaries(
    refs: List<RefEntry>,
    lineKeyToId: Map<Pair<String, Int>, Long>,
    lineIdToBookId: Map<Long, Long>,
    linesByPath: Map<String, List<String>>,
): List<SparseCommentary> {
    fun linesOf(title: String): Triple<Long, String, List<SectionLine>>? {
        val bookRefs = refs.filter { it.ref.startsWith("$title, ") }
        val lines = bookRefs
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
        return Triple(bookId, bookRefs.map { it.path }.distinct().single(), lines)
    }

    return sparselyLinkedCommentaries.mapNotNull { titles ->
        // Otzaria: a renamed title must not silently drop the anchoring. All three books or
        // none (test exports carry none).
        val found = listOf(titles.commentary, titles.intermediate, titles.base).associateWith { linesOf(it) }
        if (found.values.all { it == null }) return@mapNotNull null
        val missing = found.filterValues { it == null }.keys
        require(missing.isEmpty()) { "Sparse commentary ${titles.commentary}: no lines for $missing in the export" }
        val (commentaryBookId, _, allCommentaryLines) = found.getValue(titles.commentary)!!
        val (intermediateBookId, _, intermediateLines) = found.getValue(titles.intermediate)!!
        val (baseBookId, basePath, baseLines) = found.getValue(titles.base)!!
        // Otzaria: base lines by the intermediate segment whose marker they print ("even haezer 66:18").
        val marker = intermediateMarker(titles.intermediate)
        val baseContents = linesByPath.getValue(basePath)
        val baseLinesBySegment = HashMap<String, MutableSet<Long>>()
        baseLines.forEach { line ->
            marker.findAll(baseContents[line.lineIndex]).forEach {
                baseLinesBySegment.getOrPut("${line.section}:${it.groupValues[1]}") { LinkedHashSet() } += line.lineId
            }
        }
        require(baseLinesBySegment.isNotEmpty()) {
            "Sparse commentary ${titles.commentary}: no ${titles.intermediate} marker in ${titles.base}"
        }
        val markedBaseLines = intermediateLines
            .mapNotNull { line -> baseLinesBySegment[line.segment]?.let { line.lineId to it } }
            .toMap()
        val commentaryLines = unanchoredSections[titles.commentary]?.let { prefix ->
            val kept = allCommentaryLines.filterNot { it.section.startsWith(prefix) }
            require(kept.size < allCommentaryLines.size) {
                "Sparse commentary ${titles.commentary}: no line in section '${prefix.trim()}' to leave out"
            }
            kept
        } ?: allCommentaryLines
        SparseCommentary(commentaryBookId, intermediateBookId, baseBookId, commentaryLines, baseLines, markedBaseLines)
    }
}

/**
 * Commentary links stored between the books of a [SparseCommentary], each keyed by the
 * dependant line id with the ids of the lines it depends on.
 */
internal class SparseCommentaryLinks {
    val baseByCommentaryLine = HashMap<Long, MutableSet<Long>>()
    val intermediateByCommentaryLine = HashMap<Long, MutableSet<Long>>()

    /** Records a stored COMMENTARY [link] (base → dependant) if it joins two books of [commentary]. */
    fun record(commentary: SparseCommentary, link: Link) {
        val map = when (link.sourceBookId to link.targetBookId) {
            commentary.baseBookId to commentary.commentaryBookId -> baseByCommentaryLine
            commentary.intermediateBookId to commentary.commentaryBookId -> intermediateByCommentaryLine
            else -> return
        }
        map.getOrPut(link.targetLineId) { HashSet() } += link.sourceLineId
    }
}

/**
 * Anchors the commentary lines without a link to their base text through the intermediate
 * commentary: a line linked to an intermediate segment gets the base lines that print that
 * segment's marker (Otzaria: not Sefaria's base–intermediate links, which follow paragraph
 * position, e.g. Even HaEzer 66). An intermediate segment the base does not mark gives none. Lines
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
            .flatMap { commentary.markedBaseLines[it].orEmpty() }
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
