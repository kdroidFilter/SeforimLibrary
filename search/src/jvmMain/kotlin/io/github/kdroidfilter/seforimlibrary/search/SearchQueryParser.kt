package io.github.kdroidfilter.seforimlibrary.search

/**
 * Parses a raw search query into "exact" phrases (delimited by double quotes) and the
 * remaining free text.
 *
 * Google-style behaviour: text wrapped in double quotes is searched verbatim, as an exact
 * ordered phrase, WITHOUT the magic-dictionary / synonym expansion, fuzzy matching or n-gram
 * substring matching.
 *
 * Two delimiter characters are recognised, so it works whatever the keyboard layout produces:
 *  - the ASCII straight quote `"` (U+0022)
 *  - the Hebrew gershayim `״` (U+05F4)
 * They are interchangeable (a phrase may be opened with one and closed with the other).
 *
 * Hebrew acronyms (rashei tevot) are very commonly written with a double quote between the last
 * two letters (e.g. רש״י / רש"י, רמב״ם, שו״ע, תנ״ך, תשפ״א). Such a quote is NOT a phrase
 * delimiter: a double quote acts as a delimiter only when it sits at a word boundary, i.e. it is
 * NOT flanked by Hebrew letters on both sides. This keeps acronym searches working as before.
 */
object SearchQueryParser {

    private const val ASCII_QUOTE = '"'
    private const val GERSHAYIM = '״'
    private val WHITESPACE = "\\s+".toRegex()

    data class ParsedQuery(
        /** Substrings that were enclosed in double quotes, each to be matched as an exact phrase. */
        val exactPhrases: List<String>,
        /** The remaining (unquoted) text, processed by the normal dictionary-aware pipeline. */
        val freeText: String,
    ) {
        /** True when the query contains at least one real phrase delimiter. */
        val hasExactPhrases: Boolean get() = exactPhrases.isNotEmpty()
    }

    private fun isQuoteDelimiter(c: Char): Boolean = c == ASCII_QUOTE || c == GERSHAYIM

    /** Hebrew letters block (alef..tav), including final forms which are all within this range. */
    private fun isHebrewLetter(c: Char): Boolean = c.code in 0x05D0..0x05EA

    /**
     * A double quote at [index] is an acronym marker (NOT a phrase delimiter) when it is
     * immediately surrounded by Hebrew letters on both sides, e.g. the ״ in רש״י.
     */
    private fun isAcronymQuote(raw: String, index: Int): Boolean {
        if (index <= 0 || index >= raw.length - 1) return false
        return isHebrewLetter(raw[index - 1]) && isHebrewLetter(raw[index + 1])
    }

    fun parse(rawQuery: String): ParsedQuery {
        if (rawQuery.none { isQuoteDelimiter(it) }) {
            return ParsedQuery(emptyList(), rawQuery)
        }

        val exactPhrases = mutableListOf<String>()
        val free = StringBuilder()
        val phrase = StringBuilder()
        var inQuote = false

        for (i in rawQuery.indices) {
            val c = rawQuery[i]
            if (isQuoteDelimiter(c) && !isAcronymQuote(rawQuery, i)) {
                if (inQuote) {
                    addPhrase(exactPhrases, phrase)
                    inQuote = false
                } else {
                    inQuote = true
                }
            } else {
                (if (inQuote) phrase else free).append(c)
            }
        }
        // Unclosed quote: treat the trailing buffered content as an exact phrase.
        if (inQuote) addPhrase(exactPhrases, phrase)

        return ParsedQuery(
            exactPhrases = exactPhrases,
            freeText = free.toString().trim().replace(WHITESPACE, " "),
        )
    }

    private fun addPhrase(into: MutableList<String>, buffer: StringBuilder) {
        val phrase = buffer.toString().trim().replace(WHITESPACE, " ")
        if (phrase.isNotEmpty()) into.add(phrase)
        buffer.setLength(0)
    }
}
