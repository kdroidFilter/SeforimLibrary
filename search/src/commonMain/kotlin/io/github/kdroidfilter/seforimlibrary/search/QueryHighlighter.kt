package io.github.kdroidfilter.seforimlibrary.search

/** How surely a word stands for a query word: its own form, or an inflection of it. */
internal object EvidenceLevel {
    const val LITERAL = 0
    const val INFLECTION = 1
    const val NONE = 2
}

/** A word of the query and the forms that count as it (normalized like [HebrewTextUtils.normalizeHebrew]). */
internal class TokenForms(
    val literal: String,
    val inflections: Collection<String> = emptyList(),
    // The divine name: ה׳, יהוה... (a lone ה stands for it only with its geresh)
    val extraLiterals: Collection<String> = emptyList(),
)

/** A word of a text: its range and its comparison key (no diacritics, quotes or final letters, lowercase). */
internal class TextWord(
    val start: Int,
    val end: Int,
    val key: String,
    // Had a geresh or gershayim (an abbreviation or a number: ה׳, רש״י)
    val quoted: Boolean,
)

/** Where a query unit (a word of the query, or a quoted phrase) shows in a text, and how surely. */
internal class Occurrence(
    val start: Int,
    val end: Int,
    val unit: Int,
    val level: Int,
)

/**
 * What a search highlights in a text: whole words that make a line match the query. A query word counts as itself
 * (also behind Hebrew prefixes, or inside a longer word for 4 letters and more, as the n-gram matching allows) or as
 * an inflection from the dictionary; never as a dictionary variant, which may be a loose association (עם for כל) the
 * search uses to find more but that isn't the word. Quoted phrases count as their exact words in a row.
 */
internal class HighlightPlan private constructor(
    private val single: Map<String, List<FormRef>>,
    private val multi: Map<String, List<MultiForm>>,
    private val substrings: List<Pair<String, Int>>,
    private val phrases: List<Pair<List<String>, Int>>,
    val unitCount: Int,
) {
    private class FormRef(val unit: Int, val level: Int, val needsQuote: Boolean)

    private class MultiForm(val rest: List<String>, val unit: Int, val level: Int)

    val isEmpty: Boolean get() = unitCount == 0

    /** The occurrences of the query units among [words] (from [words] of [text]), sorted by position. */
    fun occurrences(words: List<TextWord>): List<Occurrence> {
        if (isEmpty || words.isEmpty()) return emptyList()
        val out = ArrayList<Occurrence>()
        val best = IntArray(unitCount) { EvidenceLevel.NONE }
        val touched = ArrayList<Int>(4)
        for ((i, word) in words.withIndex()) {
            val key = word.key
            fun offer(unit: Int, level: Int) {
                if (best[unit] == EvidenceLevel.NONE) touched += unit
                if (level < best[unit]) best[unit] = level
            }
            // The word itself, then without up to three prefix letters
            for (p in 0..minOf(MAX_PREFIX, key.length - 1)) {
                if (p > 0 && key[p - 1] !in PREFIX_LETTERS) break
                val stem = if (p == 0) key else key.substring(p)
                if (p > 0 && !prefixAllowed(key, p, stem.length)) continue
                single[stem]?.forEach { ref ->
                    if (!ref.needsQuote || (p == 0 && word.quoted)) offer(ref.unit, ref.level)
                }
            }
            for ((literal, unit) in substrings) {
                if (key.length > literal.length && key.contains(literal)) offer(unit, EvidenceLevel.LITERAL)
            }
            for (unit in touched) {
                out += Occurrence(word.start, word.end, unit, best[unit])
                best[unit] = EvidenceLevel.NONE
            }
            touched.clear()
            // Dictionary forms of several words
            multi[key]?.forEach { form ->
                if (matchesRun(words, i + 1, form.rest)) {
                    out += Occurrence(word.start, words[i + form.rest.size].end, form.unit, form.level)
                }
            }
        }
        for ((phrase, unit) in phrases) {
            for (i in 0..words.size - phrase.size) {
                if (words[i].key == phrase[0] && matchesRun(words, i + 1, phrase.subList(1, phrase.size))) {
                    out += Occurrence(words[i].start, words[i + phrase.size - 1].end, unit, EvidenceLevel.LITERAL)
                }
            }
        }
        out.sortWith(compareBy<Occurrence> { it.start }.thenBy { it.end })
        return out
    }

    /** The occurrences of the query units in [text], sorted by position. */
    fun highlights(text: String): List<Occurrence> = occurrences(splitWords(text))

    private fun matchesRun(words: List<TextWord>, from: Int, keys: List<String>): Boolean {
        if (from + keys.size > words.size) return false
        for (k in keys.indices) if (words[from + k].key != keys[k]) return false
        return true
    }

    companion object {
        private const val MAX_PREFIX = 3

        // ו ה ב כ ל מ ש, and the Aramaic ד
        private const val PREFIX_LETTERS = "והבכלמשד"

        // Keys a query word must exceed to be matched inside a longer word, as the 4-gram matching does
        private const val MIN_SUBSTRING_LENGTH = 4

        /**
         * A prefix may only be split off a stem of 3 letters or more; a 2-letter stem only takes ו or ה, so that
         * כל isn't found in שכל nor בן in לבן.
         */
        private fun prefixAllowed(key: String, prefixLength: Int, stemLength: Int): Boolean =
            stemLength >= 3 || (stemLength == 2 && prefixLength == 1 && (key[0] == 'ו' || key[0] == 'ה'))

        val EMPTY = HighlightPlan(emptyMap(), emptyMap(), emptyList(), emptyList(), 0)

        fun build(tokens: List<TokenForms>, phrases: List<String>): HighlightPlan {
            val single = HashMap<String, MutableList<FormRef>>()
            val multi = HashMap<String, MutableList<MultiForm>>()
            val substrings = ArrayList<Pair<String, Int>>()
            var unit = 0

            fun add(form: String, unitIndex: Int, level: Int) {
                val keys = form.split(' ').map { highlightKey(it) }.filter { it.isNotEmpty() }
                when {
                    keys.isEmpty() -> return
                    keys.size == 1 -> {
                        val key = keys[0]
                        val refs = single.getOrPut(key) { mutableListOf() }
                        if (refs.none { it.unit == unitIndex && it.level <= level }) {
                            refs += FormRef(unitIndex, level, needsQuote = key.length < 2)
                        }
                    }
                    else -> multi.getOrPut(keys[0]) { mutableListOf() } += MultiForm(keys.drop(1), unitIndex, level)
                }
            }

            for (token in tokens) {
                val index = unit++
                add(token.literal, index, EvidenceLevel.LITERAL)
                token.extraLiterals.forEach { add(it, index, EvidenceLevel.LITERAL) }
                token.inflections.forEach { add(it, index, EvidenceLevel.INFLECTION) }
                val key = highlightKey(token.literal)
                if (key.length >= MIN_SUBSTRING_LENGTH && ' ' !in token.literal) substrings += key to index
            }
            val phraseUnits = phrases.mapNotNull { phrase ->
                val keys = phrase.split(' ').map { highlightKey(it) }.filter { it.isNotEmpty() }
                if (keys.isEmpty()) null else keys to unit++
            }
            if (unit == 0) return EMPTY
            return HighlightPlan(single, multi, substrings, phraseUnits, unit)
        }
    }
}

private fun isQuote(c: Char): Boolean = c == '"' || c == '\'' || c == '״' || c == '׳'

private fun keyLetter(c: Char): Char = HebrewTextUtils.baseLetter(c).lowercaseChar()

/** The comparison key of a word or form: no diacritics or quotes, final letters as their base, lowercase. */
internal fun highlightKey(word: String): String {
    val sb = StringBuilder(word.length)
    for (c in word) if (!HebrewTextUtils.isNikudOrTeamim(c) && !isQuote(c) && !c.isWhitespace()) sb.append(keyLetter(c))
    return sb.toString()
}

/**
 * Splits [text] into words: letters and digits with their diacritics, gershayim between letters (רש״י) and a
 * closing geresh (ה׳, וכו׳) included. HTML entities (the snippet sources are escaped) are skipped, as is anything
 * else.
 */
internal fun splitWords(text: String): List<TextWord> {
    val out = ArrayList<TextWord>()
    var i = 0
    val n = text.length
    while (i < n) {
        val c = text[i]
        if (c == '&') {
            val end = entityEnd(text, i)
            if (end > i) {
                i = end
                continue
            }
        }
        if (!c.isLetterOrDigit()) {
            i++
            continue
        }
        val start = i
        var quoted = false
        val key = StringBuilder()
        while (i < n) {
            val ch = text[i]
            when {
                ch.isLetterOrDigit() -> key.append(keyLetter(ch))
                HebrewTextUtils.isNikudOrTeamim(ch) -> Unit
                // Gershayim or geresh inside the word: רש״י, ע"ש
                isQuote(ch) && i + 1 < n && text[i + 1].isLetter() -> quoted = true
                else -> break
            }
            i++
        }
        // A closing geresh: ה׳, וכו'
        if (i < n && (text[i] == '׳' || text[i] == '\'')) {
            quoted = true
            i++
        }
        out += TextWord(start, i, key.toString(), quoted)
    }
    return out
}

// The end of the HTML entity at [start] (&amp; &#39; &nbsp;), or [start] when there is none
private fun entityEnd(text: String, start: Int): Int {
    var i = start + 1
    while (i < text.length && i - start <= 10) {
        val c = text[i]
        if (c == ';') return if (i > start + 1) i + 1 else start
        if (!c.isLetterOrDigit() && c != '#') return start
        i++
    }
    return start
}

/**
 * The ranges of [occurrences] (sorted by start) in [text]: overlapping ones, and ones apart only by spaces or a maqaf,
 * as one, so a run of matched words reads as one span rather than a word-by-word patchwork.
 */
internal fun mergedRanges(text: String, occurrences: List<Occurrence>): List<IntRange> {
    val out = ArrayList<IntRange>()
    for (o in occurrences) {
        val last = out.lastOrNull()
        if (last != null && joins(text, last.last + 1, o.start)) {
            out[out.lastIndex] = last.first..maxOf(last.last, o.end - 1)
        } else {
            out += o.start until o.end
        }
    }
    return out
}

// Whether nothing but spaces and maqafs lies from [from] to [until] (also when they overlap)
private fun joins(text: String, from: Int, until: Int): Boolean {
    for (i in from until until) if (!text[i].isWhitespace() && text[i] != '־') return false
    return true
}

/** [ranges] merged where they overlap or touch, sorted. */
internal fun unionRanges(ranges: List<IntRange>): List<IntRange> {
    val out = ArrayList<IntRange>()
    for (r in ranges.sortedBy { it.first }) {
        val last = out.lastOrNull()
        if (last != null && r.first <= last.last + 1) out[out.lastIndex] = last.first..maxOf(last.last, r.last) else out += r
    }
    return out
}
