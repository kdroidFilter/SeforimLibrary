package io.github.kdroidfilter.seforimlibrary.search

/**
 * The terms of one search, prepared once to build the snippets of all its hits: dictionary expansions make them
 * hundreds, so matching them one by one against every text was the main cost of a results page.
 *
 * @param anchorTerms the terms whose clustering picks the snippet's window (matched anywhere, case-sensitive)
 * @param highlightTerms the terms put in bold (whole words only, case-insensitive)
 */
internal class SnippetTerms(
    val anchorTerms: List<String>,
    highlightTerms: List<String>,
) {
    val anchors = AhoCorasick(anchorTerms)

    private val pool = (highlightTerms + highlightTerms.map { it.trimEnd('$') })
        .distinct()
        .filter { it.isNotBlank() }
        .map { it.lowercase() }

    // A term made of letters and digits only matches whole words exactly: looked up per word of the text
    val wordTerms: Set<String> = pool.filterTo(HashSet()) { term -> term.all { it.isLetterOrDigit() } }

    // Any other term (punctuation, spaces) is searched in the text
    val otherTerms: List<String> = pool.filter { it !in wordTerms }
}

/**
 * Builds the HTML snippet of [raw] for a search: a window around where most anchor terms cluster, with the
 * highlight terms in `<b>`.
 */
internal object SnippetBuilder {
    // Occurrences of a term considered when choosing the window
    private const val MAX_OCCURRENCES_PER_TERM = 5

    fun build(raw: String, terms: SnippetTerms, context: Int = 220): String {
        if (raw.isEmpty()) return ""
        val (plain, mapToOrig) = HebrewTextUtils.stripDiacriticsWithMap(raw)
        val hasDiacritics = plain.length != raw.length
        val effContext = if (hasDiacritics) maxOf(context, 360) else context
        val plainSearch = HebrewTextUtils.replaceFinalsWithBase(plain)

        val (plainIdx, plainLen) = bestAnchor(plainSearch, terms, effContext)
        val plainStart = (plainIdx - effContext).coerceAtLeast(0)
        val plainEnd = (plainIdx + plainLen + effContext).coerceAtMost(plain.length)
        val origStart = HebrewTextUtils.mapToOrigIndex(mapToOrig, plainStart)
        val origEnd = HebrewTextUtils.mapToOrigIndex(mapToOrig, plainEnd).coerceAtMost(raw.length)

        val base = raw.substring(origStart, origEnd)
        val basePlain = plain.substring(plainStart, plainEnd)
        val basePlainSearch = HebrewTextUtils.replaceFinalsWithBase(basePlain)
        val baseMap = IntArray(plainEnd - plainStart) { idx ->
            (mapToOrig[plainStart + idx] - origStart).coerceIn(0, base.length.coerceAtLeast(1) - 1)
        }
        val text = basePlainSearch.lowercase()

        val intervals = mutableListOf<IntRange>()
        fun addMatch(idx: Int, length: Int) {
            val startOrig = HebrewTextUtils.mapToOrigIndex(baseMap, idx)
            val endOrig = HebrewTextUtils.mapToOrigIndex(baseMap, idx + length - 1) + 1
            if (startOrig in 0 until endOrig && endOrig <= base.length) intervals += (startOrig until endOrig)
        }

        // Whole words: each word of the text looked up among the terms
        if (terms.wordTerms.isNotEmpty()) {
            var i = 0
            while (i < text.length) {
                if (!text[i].isLetterOrDigit()) {
                    i++
                    continue
                }
                val start = i
                while (i < text.length && text[i].isLetterOrDigit()) i++
                if (text.substring(start, i) in terms.wordTerms) addMatch(start, i - start)
            }
        }
        for (term in terms.otherTerms) {
            var from = 0
            while (from <= text.length - term.length) {
                val idx = text.indexOf(term, startIndex = from)
                if (idx == -1) break
                if (isWordBoundary(text, idx - 1) && isWordBoundary(text, idx + term.length)) addMatch(idx, term.length)
                from = idx + 1
            }
        }

        val merged = mergeIntervals(intervals.sortedBy { it.first })
        val highlighted = insertBoldTags(base, merged)
        val prefix = if (origStart > 0) "..." else ""
        val suffix = if (origEnd < raw.length) "..." else ""
        return prefix + highlighted + suffix
    }

    /**
     * The window's anchor (plain index and length): the first occurrence, in term order, around which the most
     * distinct anchor terms cluster, a longer term breaking ties; the text start when no term occurs.
     */
    private fun bestAnchor(plainSearch: String, terms: SnippetTerms, effContext: Int): Pair<Int, Int> {
        val anchorTerms = terms.anchorTerms
        var plainIdx = 0
        var plainLen = anchorTerms.firstOrNull()?.length ?: 0
        if (anchorTerms.isEmpty()) return plainIdx to plainLen

        // The first occurrences of each term, overlapping ones included
        val occurrences = terms.anchors.firstOccurrences(plainSearch, MAX_OCCURRENCES_PER_TERM)
        val count = occurrences.sumOf { it.size }
        if (count == 0) return plainIdx to plainLen
        // Candidates in term order, then by position
        val candidatePos = IntArray(count)
        val candidateTerm = IntArray(count)
        var n = 0
        occurrences.forEachIndexed { term, positions ->
            for (p in positions) {
                candidatePos[n] = p
                candidateTerm[n++] = term
            }
        }
        // The same occurrences sorted by position, to count the terms around each one
        val byPosition = (0 until count).sortedBy { candidatePos[it] }
        val sortedPos = IntArray(count) { candidatePos[byPosition[it]] }
        val sortedTerm = IntArray(count) { candidateTerm[byPosition[it]] }
        val seen = IntArray(anchorTerms.size)
        var stamp = 0

        val maxPossibleScore = anchorTerms.size
        var bestScore = 0
        for (c in 0 until count) {
            val pos = candidatePos[c]
            val length = anchorTerms[candidateTerm[c]].length
            val windowStart = pos - effContext
            val windowEnd = pos + length + effContext
            stamp++
            var uniqueTerms = 0
            var i = lowerBound(sortedPos, windowStart)
            while (i < count && sortedPos[i] <= windowEnd) {
                val term = sortedTerm[i++]
                if (seen[term] != stamp) {
                    seen[term] = stamp
                    uniqueTerms++
                }
            }
            val score = uniqueTerms * 100 + length
            if (score > bestScore) {
                bestScore = score
                plainIdx = pos
                plainLen = length
                if (uniqueTerms >= maxPossibleScore) break
            }
        }
        return plainIdx to plainLen
    }

    private fun lowerBound(sorted: IntArray, value: Int): Int {
        var lo = 0
        var hi = sorted.size
        while (lo < hi) {
            val mid = (lo + hi) ushr 1
            if (sorted[mid] < value) lo = mid + 1 else hi = mid
        }
        return lo
    }

    private fun isWordBoundary(text: String, index: Int): Boolean {
        if (index < 0 || index >= text.length) return true
        val ch = text[index]
        return ch.isWhitespace() || !ch.isLetterOrDigit()
    }

    private fun mergeIntervals(ranges: List<IntRange>): List<IntRange> {
        if (ranges.isEmpty()) return ranges
        val out = mutableListOf<IntRange>()
        var cur = ranges[0]
        for (i in 1 until ranges.size) {
            val r = ranges[i]
            if (r.first <= cur.last + 1) {
                cur = cur.first..maxOf(cur.last, r.last)
            } else {
                out += cur
                cur = r
            }
        }
        out += cur
        return out
    }

    private fun insertBoldTags(text: String, intervals: List<IntRange>): String {
        if (intervals.isEmpty()) return text
        val sb = StringBuilder(text)
        for (r in intervals.asReversed()) {
            val start = r.first.coerceIn(0, sb.length)
            val end = (r.last + 1).coerceIn(0, sb.length)
            if (end > start) {
                sb.insert(end, "</b>")
                sb.insert(start, "<b>")
            }
        }
        return sb.toString()
    }
}

/** Finds the occurrences of many terms in one pass over a text (Aho-Corasick automaton). */
internal class AhoCorasick(terms: List<String>) {
    private val termLengths = IntArray(terms.size) { terms[it].length }
    private val next = ArrayList<HashMap<Char, Int>>()
    private val fail = ArrayList<Int>()

    // Terms ending at each node, its own then those of its suffix links
    private val outputs = ArrayList<IntArray>()

    init {
        next += HashMap()
        fail += 0
        val own = ArrayList<MutableList<Int>>().apply { add(mutableListOf()) }
        terms.forEachIndexed { index, term ->
            if (term.isEmpty()) return@forEachIndexed
            var node = 0
            for (ch in term) {
                node = next[node].getOrPut(ch) {
                    next += HashMap()
                    fail += 0
                    own.add(mutableListOf())
                    next.size - 1
                }
            }
            own[node] += index
        }
        // Breadth-first: suffix links, and outputs merged along them
        repeat(next.size) { outputs += IntArray(0) }
        outputs[0] = own[0].toIntArray()
        val queue = ArrayDeque<Int>()
        for (child in next[0].values) {
            fail[child] = 0
            queue += child
        }
        while (queue.isNotEmpty()) {
            val node = queue.removeFirst()
            outputs[node] = (own[node] + outputs[fail[node]].toList()).toIntArray()
            for ((ch, child) in next[node]) {
                var f = fail[node]
                while (f != 0 && ch !in next[f]) f = fail[f]
                fail[child] = next[f][ch]?.takeIf { it != child } ?: 0
                queue += child
            }
        }
    }

    /** Per term, the start positions of its first [max] occurrences in [text], overlapping ones included. */
    fun firstOccurrences(text: String, max: Int): Array<IntArray> {
        val found = Array(termLengths.size) { IntArray(max) }
        val counts = IntArray(termLengths.size)
        var node = 0
        for (i in text.indices) {
            val ch = text[i]
            while (node != 0 && ch !in next[node]) node = fail[node]
            node = next[node][ch] ?: 0
            for (term in outputs[node]) {
                // Matches of a term come by end, so by start: the first ones found are the first ones
                if (counts[term] < max) found[term][counts[term]++] = i - termLengths[term] + 1
            }
        }
        return Array(termLengths.size) { found[it].copyOf(counts[it]) }
    }
}
