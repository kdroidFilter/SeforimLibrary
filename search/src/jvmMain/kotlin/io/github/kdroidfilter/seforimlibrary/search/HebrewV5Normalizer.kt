package io.github.kdroidfilter.seforimlibrary.search

import org.jsoup.parser.Parser

/**
 * Exact JVM port of `normalize_he_v5.py`: strip HTML/nikud/teamim and non-Hebrew
 * characters, then fold final (sofit) letters (ך→כ ם→מ ן→נ ף→פ ץ→צ). It MUST be
 * applied to any text before embedding it with a v5 model (both indexed lines and
 * query), so vectors stay comparable. Pair it with a v5 model + a v5-built index.
 */
object HebrewV5Normalizer {
    private val TAG = Regex("<[^>]+>")
    private val MARKS = Regex("[֑-ׇ]")
    private val DROP = Regex("[^א-ת0-9\\s.,:;!?()\\[\\]\"'\\-/׳״]")
    private val WS = Regex("\\s+")
    // Final (sofit) -> base letter folding.
    private val FINALS = mapOf('ך' to 'כ', 'ם' to 'מ', 'ן' to 'נ', 'ף' to 'פ', 'ץ' to 'צ')

    fun clean(text: String): String {
        var s = TAG.replace(text, " ")
        s = Parser.unescapeEntities(s, false)
        s = MARKS.replace(s, "")
        s = DROP.replace(s, " ")
        s = WS.replace(s, " ").trim()
        return buildString(s.length) { for (c in s) append(FINALS[c] ?: c) }
    }
}
