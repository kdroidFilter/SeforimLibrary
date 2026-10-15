package io.github.kdroidfilter.seforimlibrary.search

/**
 * Builds the HTML snippet of a hit: its text from just before where the query's words gather best, with the words
 * that stand for them in `<b>` (see [HighlightPlan]). Results show the first lines of a snippet only, so the matches
 * lead it, as in a search engine's results page.
 */
internal object SnippetBuilder {
    // Visible characters (diacritics aside) of a snippet, longer for vocalized text
    private const val LENGTH = 440
    private const val LENGTH_WITH_DIACRITICS = 720

    // Span of a group of matches: what the first two lines of a result show past the lead-in
    private const val CLUSTER_REACH = 140

    // Context shown before the first match, at most: from the start of its sentence when that is closer
    private const val LEAD = 30
    private const val SENTENCE_ENDS = ".:;?!׃"

    // Bounds the window search on texts with a great many matches
    private const val MAX_CANDIDATES = 400

    /**
     * @param raw the snippet source, HTML-escaped text (the hit line, possibly with its neighbors)
     * @param lineStart start of the hit line in [raw]; the window favors the matches inside it
     * @param lineEnd end of the hit line in [raw]
     */
    fun build(
        raw: String,
        plan: HighlightPlan,
        lineStart: Int = 0,
        lineEnd: Int = raw.length,
    ): String = build(raw, plan.highlights(raw), lineStart, lineEnd)

    /** The snippet of [raw] with [occurrences] (sorted by start) as its matches. */
    fun build(
        raw: String,
        occurrences: List<Occurrence>,
        lineStart: Int = 0,
        lineEnd: Int = raw.length,
    ): String {
        if (raw.isEmpty()) return ""
        val visible = VisibleIndex(raw)
        val window = if (visible.hasDiacritics) LENGTH_WITH_DIACRITICS else LENGTH

        val cluster = bestCluster(occurrences, visible, lineStart, lineEnd)

        // From a short lead-in before the cluster, or from the hit line's start without any
        val (focusStart, focusEnd) = cluster ?: (lineStart to lineStart)
        // (from the hit line's start at the earliest when the cluster is in it)
        var start =
            when {
                cluster == null -> lineStart
                focusStart >= lineStart && focusStart < lineEnd -> maxOf(lineStart, leadIn(raw, visible, focusStart))
                else -> leadIn(raw, visible, focusStart)
            }
        val endVisible = (visible.at(start) + window).coerceAtMost(visible.total)
        var end = if (endVisible >= visible.total) raw.length else visible.offsetOf(endVisible)

        // Cut between words only, never inside the matched ones
        if (start > 0) start = snapForward(raw, start, focusStart)
        if (end < raw.length) end = snapBackward(raw, end, maxOf(focusEnd, start))

        val shown = occurrences.filter { it.start >= start && it.end <= end }
        val body = withBold(raw, start, end, mergedRanges(raw, shown))
        val prefix = if (start > 0) "..." else ""
        val suffix = if (end < raw.length) "..." else ""
        return prefix + body.trim() + suffix
    }

    // Where the snippet starts before [matchStart]: right after the closest sentence end within the lead-in, else
    // the lead-in's length before it (the text's start when that is near)
    private fun leadIn(raw: String, visible: VisibleIndex, matchStart: Int): Int {
        val leadStart = visible.at(matchStart) - LEAD
        if (leadStart <= 0) return 0
        val from = visible.offsetOf(leadStart)
        for (i in matchStart - 1 downTo from) {
            if (raw[i] in SENTENCE_ENDS && i + 1 < raw.length && raw[i + 1].isWhitespace()) return i + 1
        }
        return from
    }

    /**
     * The span (start to end) of the best group of matches that fits the first lines of a result: one starting in
     * the hit line when it holds any, covering the most distinct query units, then the surest forms, then the most
     * matches; the first one on ties.
     */
    private fun bestCluster(
        occurrences: List<Occurrence>,
        visible: VisibleIndex,
        lineStart: Int,
        lineEnd: Int,
    ): Pair<Int, Int>? {
        if (occurrences.isEmpty()) return null
        val candidates = occurrences.take(MAX_CANDIDATES)
        val lineHasMatch = candidates.any { it.start >= lineStart && it.end <= lineEnd }
        val units = candidates.maxOf { it.unit } + 1
        val bestLevel = IntArray(units)
        val reach = CLUSTER_REACH

        var best: Pair<Int, Int>? = null
        var bestScore = longArrayOf(-1, -1, -1, Long.MIN_VALUE)
        for (i in candidates.indices) {
            bestLevel.fill(EvidenceLevel.NONE)
            var inLine = false
            var j = i
            while (j < candidates.size && visible.at(candidates[j].end) - visible.at(candidates[i].start) <= reach) {
                val o = candidates[j]
                if (o.level < bestLevel[o.unit]) bestLevel[o.unit] = o.level
                if (o.start >= lineStart && o.end <= lineEnd) inLine = true
                j++
            }
            if (j == i) j = i + 1 // a single match longer than the window
            // The snippet starts at its group: in the hit line, so the result shows its own line first
            if (lineHasMatch && !candidates[i].let { it.start >= lineStart && it.end <= lineEnd }) continue
            var distinct = 0L
            var sureness = 0L
            for (level in bestLevel) {
                if (level != EvidenceLevel.NONE) {
                    distinct++
                    sureness += EvidenceLevel.NONE - level
                }
            }
            val end = candidates.subList(i, j).maxOf { it.end }
            val score = longArrayOf(if (inLine) 1 else 0, distinct, sureness, (j - i).toLong())
            if (compare(score, bestScore) > 0) {
                bestScore = score
                best = candidates[i].start to end
            }
        }
        return best
    }

    private fun compare(a: LongArray, b: LongArray): Int {
        for (k in a.indices) if (a[k] != b[k]) return a[k].compareTo(b[k])
        return 0
    }

    private fun withBold(raw: String, start: Int, end: Int, spans: List<IntRange>): String {
        val sb = StringBuilder(end - start + spans.size * 7)
        var at = start
        for (span in spans) {
            sb.append(raw, at, span.first).append("<b>").append(raw, span.first, span.last + 1).append("</b>")
            at = span.last + 1
        }
        return sb.append(raw, at, end).toString()
    }

    // The start of the first whole word from [from] (no later than [limit])
    private fun snapForward(raw: String, from: Int, limit: Int): Int {
        if (raw[from - 1].isWhitespace()) return from
        var i = from
        while (i < limit && !raw[i].isWhitespace()) i++
        return if (i < limit) i + 1 else from
    }

    // The end of the last whole word before [from] (no earlier than [limit])
    private fun snapBackward(raw: String, from: Int, limit: Int): Int {
        if (raw[from].isWhitespace()) return from
        var i = from
        while (i > limit && !raw[i - 1].isWhitespace()) i--
        return if (i > limit) i - 1 else from
    }

    /** Positions counted in visible characters: the diacritics take no room on screen. */
    private class VisibleIndex(raw: String) {
        // Visible characters before each offset
        private val before = IntArray(raw.length + 1)

        // Offset of each visible character
        private val offsets: IntArray
        val total: Int
        val hasDiacritics: Boolean

        init {
            var count = 0
            for (i in raw.indices) {
                before[i] = count
                if (!HebrewTextUtils.isNikudOrTeamim(raw[i])) count++
            }
            before[raw.length] = count
            total = count
            hasDiacritics = count != raw.length
            offsets = IntArray(count)
            for (i in raw.indices) if (!HebrewTextUtils.isNikudOrTeamim(raw[i])) offsets[before[i]] = i
        }

        fun at(offset: Int): Int = before[offset.coerceIn(0, before.size - 1)]

        fun offsetOf(visibleIndex: Int): Int = if (visibleIndex >= total) before.size - 1 else offsets[visibleIndex]
    }
}
