package io.github.kdroidfilter.seforimlibrary.common.divine

import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars

internal fun isHebrewLetter(c: Char): Boolean = c in 'א'..'ת'

/**
 * A Hebrew presentation form (שׁ U+FB2A, בּ U+FB31, וּ U+FB35…) → its base letter and marks,
 * or null. Such a word is still one word (so a fragment never looks like a name), but it is
 * never itself marked: the app compares plain letters.
 */
internal fun decomposePresentation(c: Char): Pair<Char, String>? {
    if (c !in 'יִ'..'ﭏ') return null
    val d = java.text.Normalizer.normalize(c.toString(), java.text.Normalizer.Form.NFKD)
    val base = d.firstOrNull()?.takeIf(::isHebrewLetter) ?: return null
    return base to d.substring(1)
}

internal fun isMark(c: Char): Boolean = Character.getType(c) == Character.NON_SPACING_MARK.toInt()

/** Vowel points (incl. qamats qatan); teamim, meteg, dagesh and shin/sin dots are not vowels. */
internal fun isVowel(c: Char): Boolean = c in '\u05B0'..'\u05BB' || c == '\u05C7'

/** Geresh-like characters that continue an abbreviation or a numeral (ה', שו"ע). */
internal fun isWordQuote(c: Char): Boolean = c == '\'' || c == '"' || c == '׳' || c == '״' || c == '’'

internal const val SHEVA = '\u05B0'
internal const val HIRIQ = '\u05B4'
internal const val TSERE = '\u05B5'
internal const val PATAH = '\u05B7'
internal const val HATAF_SEGOL = '\u05B1'
internal const val HOLAM = '\u05B9'
internal const val QAMATS = '\u05B8'
internal const val DAGESH = '\u05BC'
internal const val SHIN_DOT = '\u05C1'
internal const val SIN_DOT = '\u05C2'

/** A base character of a word (a Hebrew letter or a quote) with the marks that follow it. */
internal data class WordLetter(
    val ch: Char,
    val start: Int,
    val end: Int,
    val marks: String,
    val presentation: Boolean = false,
) {
    val vowels: String get() = marks.filter(::isVowel)
}

/**
 * A maximal run of Hebrew letters, marks and word-internal quotes in the raw HTML,
 * never inside a tag or an entity. [glued] means a tag cut the run out of a longer
 * word (`<big>בְּ</big>רֵאשִׁית`) — such a fragment is never a name.
 */
internal class Word(
    val start: Int,
    val end: Int,
    val letters: List<WordLetter>,
    val glued: Boolean,
    /** True when only spaces, maqaf, paseq, spacing entities or inline tags separate it from the previous word. */
    val softBefore: Boolean,
    /** Only tags/marks separate it from the previous word: both are fragments of one word (`אח<big>ר</big>`). */
    val gluedToPrevious: Boolean = false,
) {
    /** Consonants and quotes, marks stripped (presentation forms by their base letter). */
    val skeleton: String = letters.joinToString("") { it.ch.toString() }

    /** Contains a presentation-form letter: usable as context, never marked. */
    val hasPresentation: Boolean get() = letters.any { it.presentation }
    val hasVowels: Boolean get() = letters.any { l -> l.marks.any(::isVowel) }
}

/**
 * One line of stored HTML, scanned once: the visible-char coordinate of every raw
 * index (identical to [countVisibleChars] over the whole line) and its words.
 */
internal class ScannedLine(val raw: String) {
    /** visibleBefore[i] = visible chars in raw[0, i). Size raw.length + 1. */
    private val visibleBefore = IntArray(raw.length + 1)

    /** True when raw[i] is a visible character of its own (not tag content, not part of an entity). */
    private val solo = BooleanArray(raw.length)

    /** True when raw[i] lies inside a tag (`<` … `>`). */
    private val inTagAt = BooleanArray(raw.length)

    val words: List<Word>

    init {
        var count = 0
        var inTag = false
        var i = 0
        val len = raw.length
        while (i < len) {
            val c = raw[i]
            when {
                inTag -> {
                    visibleBefore[i] = count
                    inTagAt[i] = true
                    if (c == '>') inTag = false
                    i++
                }
                c == '<' -> {
                    visibleBefore[i] = count
                    inTagAt[i] = true
                    inTag = true
                    i++
                }
                c == '&' -> {
                    // Same rule as countVisibleChars: `&…;` within 10 chars is one visible char.
                    val end = minOf(len, i + 10)
                    var j = i + 1
                    var terminated = false
                    while (j < end) {
                        if (raw[j] == ';') { terminated = true; break }
                        j++
                    }
                    val last = if (terminated) j else i
                    for (k in i..last) visibleBefore[k] = count
                    count++
                    i = last + 1
                }
                else -> {
                    visibleBefore[i] = count
                    solo[i] = true
                    count++
                    i++
                }
            }
        }
        visibleBefore[len] = count
        words = scanWords()
    }

    /**
     * Visible coordinates of raw[rawStart, rawEnd), or null when any char of it is not
     * a visible char of its own, or the two counters disagree (fail-closed).
     */
    fun visibleRange(rawStart: Int, rawEnd: Int): IntRange? {
        if (rawStart < 0 || rawEnd > raw.length || rawStart >= rawEnd) return null
        for (k in rawStart until rawEnd) if (!solo[k]) return null
        val start = visibleBefore[rawStart]
        val end = visibleBefore[rawEnd - 1] + 1
        if (end - start != rawEnd - rawStart) return null
        if (countVisibleChars(raw, rawStart) != start || countVisibleChars(raw, rawEnd) != end) return null
        return start until end
    }

    private fun wordChar(i: Int): Boolean {
        if (!solo[i]) return false
        val c = raw[i]
        return isHebrewLetter(c) || isMark(c) || isWordQuote(c) || decomposePresentation(c) != null
    }

    private fun scanWords(): List<Word> {
        val out = ArrayList<Word>()
        var i = 0
        var prevEnd = -1
        while (i < raw.length) {
            if (!solo[i] || !(isHebrewLetter(raw[i]) || decomposePresentation(raw[i]) != null)) { i++; continue }
            val start = i
            val letters = ArrayList<WordLetter>()
            while (i < raw.length && wordChar(i)) {
                val c = raw[i]
                if (isMark(c)) {
                    if (letters.isEmpty()) break
                    val last = letters.removeAt(letters.lastIndex)
                    letters += last.copy(end = i + 1, marks = last.marks + c)
                } else {
                    val presentation = decomposePresentation(c)
                    letters += if (presentation != null) {
                        WordLetter(presentation.first, i, i + 1, presentation.second, presentation = true)
                    } else {
                        WordLetter(c, i, i + 1, "")
                    }
                }
                i++
            }
            val end = i
            val gapBefore = if (prevEnd < 0) null else prevEnd until start
            val gluedBefore = gapBefore?.let(::isGlueGap) ?: leadingGlue(start)
            val softBefore = gapBefore?.let(::isSoftGap) ?: false
            if (gluedBefore && out.isNotEmpty() && gapBefore != null) {
                val previous = out.removeAt(out.lastIndex)
                out += Word(previous.start, previous.end, previous.letters, glued = true, softBefore = previous.softBefore, gluedToPrevious = previous.gluedToPrevious)
            }
            out += Word(
                start, end, letters,
                glued = gluedBefore || trailingGlue(end),
                softBefore = softBefore,
                gluedToPrevious = gapBefore != null && gluedBefore,
            )
            prevEnd = end
        }
        return out
    }

    /** A gap made only of tags, stray marks and non-spacing entities: the two words were one word. */
    private fun isGlueGap(gap: IntRange): Boolean = !gap.isEmpty() && gap.all(::isGlueChar)

    private fun isGlueChar(k: Int): Boolean {
        if (inTagAt[k]) return true
        if (solo[k]) return isMark(raw[k])
        val amp = raw.lastIndexOf('&', k)
        if (amp < 0) return false
        val semi = raw.indexOf(';', amp)
        val entity = if (semi < 0) "&" else raw.substring(amp, semi + 1).lowercase()
        return entity !in SOFT_ENTITIES && entity !in PUNCTUATION_ENTITIES
    }

    private fun leadingGlue(start: Int): Boolean {
        var k = start - 1
        var walked = false
        while (k >= 0 && isGlueChar(k)) {
            if (solo[k]) return true // a mark the word cannot own
            walked = true
            k--
        }
        return walked && k >= 0 && solo[k] && (isHebrewLetter(raw[k]) || isMark(raw[k]) || decomposePresentation(raw[k]) != null)
    }

    private fun trailingGlue(end: Int): Boolean {
        var k = end
        var walked = false
        while (k < raw.length && isGlueChar(k)) { walked = true; k++ }
        return walked && k < raw.length && solo[k] && (isHebrewLetter(raw[k]) || isMark(raw[k]) || decomposePresentation(raw[k]) != null)
    }

    private fun isSoftGap(gap: IntRange): Boolean {
        if (gap.isEmpty()) return false
        var k = gap.first
        while (k <= gap.last) {
            val c = raw[k]
            when {
                c == '<' -> {
                    val close = raw.indexOf('>', k)
                    if (close < 0 || close > gap.last) return false
                    if (!isSoftTag(raw.substring(k + 1, close))) return false
                    k = close + 1
                }
                c == '&' -> {
                    val semi = raw.indexOf(';', k)
                    if (semi < 0 || semi > gap.last) return false
                    if (raw.substring(k, semi + 1).lowercase() !in SOFT_ENTITIES) return false
                    k = semi + 1
                }
                c.isWhitespace() || c in SOFT_SEPARATOR_CHARS -> k++
                else -> return false
            }
        }
        return true
    }

    private fun isSoftTag(inner: String): Boolean {
        val name = inner.trim().removePrefix("/").trim().takeWhile { it.isLetterOrDigit() }.lowercase()
        return name in SOFT_TAGS
    }

    companion object {
        private val SOFT_TAGS = setOf("b", "strong", "i", "em", "u", "small", "big", "span")
        private val SOFT_ENTITIES = setOf("&nbsp;", "&thinsp;", "&ensp;", "&emsp;", "&#160;", "&#8201;", "&#x2009;", "&#xa0;")
        private val PUNCTUATION_ENTITIES = setOf("&quot;", "&#34;", "&#39;", "&apos;", "&lt;", "&gt;", "&amp;")

        /** NBSP, thin space, maqaf, paseq, ASCII pipe used as paseq, LRM/RLM. */
        private val SOFT_SEPARATOR_CHARS = setOf(' ', ' ', ' ', '\u05BE', '\u05C0', '|', '‎', '‏')
    }
}

/**
 * Context view of a line's words: fragments a tag split are joined back into one word, so a
 * neighbour check sees `אחר` in `אח<big>ר</big>`. Distances count whole words.
 */
internal class WordContext(words: List<Word>) {
    private val groupOf = IntArray(words.size)
    private val skeletons = ArrayList<String>()

    init {
        val current = StringBuilder()
        for ((i, w) in words.withIndex()) {
            if (i > 0 && !w.gluedToPrevious) {
                skeletons += current.toString()
                current.setLength(0)
            }
            current.append(w.skeleton)
            groupOf[i] = skeletons.size
        }
        if (words.isNotEmpty()) skeletons += current.toString()
    }

    /** Skeleton of the whole word [delta] words away from word [i] (0 = its own), or null. */
    fun skeleton(i: Int, delta: Int): String? = skeletons.getOrNull(groupOf[i] + delta)
}
