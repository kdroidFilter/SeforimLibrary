package io.github.kdroidfilter.seforimlibrary.reflinks

/** Hebrew numerals, quote normalization and spelling-insensitive keys used by the citation parser. */
object HebrewText {

    private val letterValues: Map<Char, Int> =
        "אבגדהוזחטיכלמנסעפצקרשת".toList().zip(
            listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400),
        ).toMap()

    private val finals = mapOf('ך' to 'כ', 'ם' to 'מ', 'ן' to 'נ', 'ף' to 'פ', 'ץ' to 'צ')

    private val quoteMap = mapOf(
        '׳' to '\'', '״' to '"', '`' to '\'', '’' to '\'', '”' to '"', '“' to '"',
    )

    private val nikud = Regex("[֑-ׇ]")
    private val tags = Regex("<[^>]+>")
    private val spaces = Regex("\\s+")

    fun normalizeQuotes(s: String): String = buildString(s.length) { s.forEach { append(quoteMap[it] ?: it) } }

    fun stripFinals(s: String): String = buildString(s.length) { s.forEach { append(finals[it] ?: it) } }

    fun stripNikud(s: String): String = nikud.replace(s, "")

    /** Visible text of an HTML line: tags removed, maqaf turned into a space, nikud removed, spaces collapsed. */
    fun plain(html: String): String =
        spaces.replace(stripNikud(tags.replace(html, " ").replace('־', ' ')), " ").trim()

    fun collapseSpaces(s: String): String = spaces.replace(s, " ").trim()

    /**
     * Parses a Hebrew numeral ("קצג", "ט\"ו", "רנ\"א"), or returns null when the token is not a well-formed numeral:
     * letters must be in non-increasing order (טו/טז excepted) so that ordinary words are rejected.
     */
    fun gematria(token: String): Int? {
        val t = stripFinals(normalizeQuotes(token).replace("'", "").replace("\"", ""))
        if (t.isEmpty() || t.length > 5 || t == "יה" || t == "יו") return null
        val values = t.map { letterValues[it] ?: return null }
        for (i in 0 until values.size - 1) {
            if (values[i] < values[i + 1] && t.substring(i, i + 2) !in setOf("טו", "טז")) return null
        }
        return values.sum()
    }

    /**
     * Number fused with a one-letter marker: פ"ג / פי"ג (chapter 3 / 13), מי"ג (mishna 13), ה"ב (halacha 2), ס"ד (seif 4).
     * Returns null unless [token] starts with one of [markers].
     */
    fun markerNumber(token: String, markers: String): Int? {
        val t = normalizeQuotes(token).trimEnd('.', ',')
        if (t.length < 3 || t[0] !in markers || '"' !in t) return null
        val rest = t.substring(1)
        if (rest.startsWith('"')) {
            val digits = rest.substring(1)
            return if (digits.length <= 2) gematria(digits) else null
        }
        return if (rest.count { it == '"' } == 1 && rest.indexOf('"') == rest.length - 2) gematria(rest) else null
    }

    /** Spelling-insensitive key for alias lookup: no nikud, normalized quotes and final letters, single spaces. */
    fun nameKey(s: String): String = collapseSpaces(stripFinals(normalizeQuotes(stripNikud(s)).replace('-', ' ')))
}
