package io.github.kdroidfilter.seforimlibrary.search

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

class SnippetBuilderTest {
    @Test
    fun `matches the reference snippet builder on random texts`() {
        val random = Random(42)
        repeat(4000) { case ->
            val text = randomText(random)
            val words = text.split(' ').filter { it.isNotEmpty() }
            val plainWords = words.map { HebrewTextUtils.replaceFinalsWithBase(HebrewTextUtils.stripDiacritics(it)) }
            val highlight = List(random.nextInt(0, 40)) { randomTerm(random, plainWords) }
            // Like buildAnchorTerms: distinct, non-blank, longest first
            val anchors = (highlight + List(random.nextInt(0, 5)) { randomTerm(random, plainWords) })
                .map { it.trim() }
                .filter { it.length >= 2 }
                .distinct()
                .sortedByDescending { it.length }
            assertEquals(
                referenceSnippet(text, anchors, highlight),
                SnippetBuilder.build(text, SnippetTerms(anchors, highlight)),
                "case $case: text=$text anchors=$anchors highlight=$highlight",
            )
        }
    }

    @Test
    fun `aho corasick finds overlapping occurrences in order`() {
        val matcher = AhoCorasick(listOf("אבא", "בא", "א", ""))
        val found = matcher.firstOccurrences("אבאבא", max = 5)
        assertEquals(listOf(0, 2), found[0].toList())
        assertEquals(listOf(1, 3), found[1].toList())
        assertEquals(listOf(0, 2, 4), found[2].toList())
        assertEquals(emptyList(), found[3].toList())
        assertEquals(listOf(0, 2), matcher.firstOccurrences("אבאבא", max = 2)[2].toList())
    }

    private val letters = "אבגדהוזחטיכלמנסעפצקרשתךםןףץ"
    private val marks = listOf('\u05B0', '\u05B4', '\u05B7', '\u05BC', '\u0591', '\u05C1')
    private val punctuation = listOf(",", ".", ":", "׳", "\"", "'", "(", ")", "-", "־")

    private fun randomWord(random: Random): String = buildString {
        repeat(random.nextInt(1, 7)) {
            append(letters[random.nextInt(letters.length)])
            if (random.nextInt(4) == 0) append(marks[random.nextInt(marks.size)])
        }
        if (random.nextInt(25) == 0) append("AbC")
        if (random.nextInt(8) == 0) append(punctuation[random.nextInt(punctuation.size)])
    }

    private fun randomText(random: Random): String {
        // Short lines and long ones, so the window is sometimes the whole text, sometimes a slice
        val words = List(random.nextInt(0, if (random.nextBoolean()) 30 else 400)) { randomWord(random) }
        return words.joinToString(" ")
    }

    private fun randomTerm(random: Random, plainWords: List<String>): String {
        val word = if (plainWords.isNotEmpty() && random.nextInt(4) != 0) plainWords[random.nextInt(plainWords.size)] else {
            HebrewTextUtils.replaceFinalsWithBase(HebrewTextUtils.stripDiacritics(randomWord(random)))
        }
        return when (random.nextInt(8)) {
            0 -> word.take(4)
            1 -> word + "$"
            2 -> word + " " + (plainWords.randomOrNull(random) ?: "")
            3 -> word.lowercase().uppercase()
            4 -> word.drop(1)
            else -> word
        }
    }
}

// The snippet builder before SnippetBuilder, kept verbatim as the reference
private fun referenceSnippet(raw: String, anchorTerms: List<String>, highlightTerms: List<String>, context: Int = 220): String {
    if (raw.isEmpty()) return ""
    val (plain, mapToOrig) = HebrewTextUtils.stripDiacriticsWithMap(raw)
    val hasDiacritics = plain.length != raw.length
    val effContext = if (hasDiacritics) maxOf(context, 360) else context
    val plainSearch = HebrewTextUtils.replaceFinalsWithBase(plain)

    // Find best anchor position: where most terms cluster together
    // Optimized: limit occurrences per term, early exit on perfect score
    var plainIdx = 0
    var plainLen = anchorTerms.firstOrNull()?.length ?: 0

    if (anchorTerms.isNotEmpty()) {
        val maxOccPerTerm = 5 // Limit occurrences per term for perf
        val positions = mutableListOf<Pair<Int, String>>() // (position, term)

        for (term in anchorTerms) {
            if (term.isEmpty()) continue
            var from = 0
            var count = 0
            while (from <= plainSearch.length - term.length && count < maxOccPerTerm) {
                val idx = plainSearch.indexOf(term, startIndex = from)
                if (idx == -1) break
                positions.add(idx to term)
                from = idx + 1
                count++
            }
        }

        if (positions.isNotEmpty()) {
            val maxPossibleScore = anchorTerms.size
            var bestScore = 0

            for ((pos, term) in positions) {
                // Count unique terms in window around this position
                val windowStart = pos - effContext
                val windowEnd = pos + term.length + effContext
                var uniqueTerms = 0
                val seen = mutableSetOf<String>()
                for ((p, t) in positions) {
                    if (p in windowStart..windowEnd && seen.add(t)) uniqueTerms++
                }
                val score = uniqueTerms * 100 + term.length

                if (score > bestScore) {
                    bestScore = score
                    plainIdx = pos
                    plainLen = term.length
                    // Early exit if we found all terms clustered
                    if (uniqueTerms >= maxPossibleScore) break
                }
            }
        }
    }
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

    val pool = (highlightTerms + highlightTerms.map { it.trimEnd('$') }).distinct().filter { it.isNotBlank() }
    val intervals = mutableListOf<IntRange>()
    val basePlainLower = basePlainSearch.lowercase()

    fun isWordBoundary(text: String, index: Int): Boolean {
        if (index < 0 || index >= text.length) return true
        val ch = text[index]
        return ch.isWhitespace() || !ch.isLetterOrDigit()
    }

    for (term in pool) {
        if (term.isEmpty()) continue
        val t = term.lowercase()
        var from = 0
        while (from <= basePlainLower.length - t.length && t.isNotEmpty()) {
            val idx = basePlainLower.indexOf(t, startIndex = from)
            if (idx == -1) break

            val isAtWordStart = isWordBoundary(basePlainLower, idx - 1)
            val isAtWordEnd = isWordBoundary(basePlainLower, idx + t.length)
            val isWholeWord = isAtWordStart && isAtWordEnd
            val shouldHighlight = isWholeWord

            if (shouldHighlight) {
                val startOrig = HebrewTextUtils.mapToOrigIndex(baseMap, idx)
                val endOrig = HebrewTextUtils.mapToOrigIndex(baseMap, (idx + t.length - 1)) + 1
                if (startOrig in 0 until endOrig && endOrig <= base.length) {
                    intervals += (startOrig until endOrig)
                }
            }
            from = idx + 1
        }
    }

    val merged = mergeIntervals(intervals.sortedBy { it.first })
    val highlighted = insertBoldTags(base, merged)
    val prefix = if (origStart > 0) "..." else ""
    val suffix = if (origEnd < raw.length) "..." else ""
    return prefix + highlighted + suffix
}

private fun mergeIntervals(ranges: List<IntRange>): List<IntRange> {
    if (ranges.isEmpty()) return ranges
    val out = mutableListOf<IntRange>()
    var cur = ranges[0]
    for (i in 1 until ranges.size) {
        val r = ranges[i]
        if (r.first <= cur.last + 1) {
            cur = cur.first .. maxOf(cur.last, r.last)
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

