package io.github.kdroidfilter.seforimlibrary.common.notes

/**
 * The line as it read before buildLineNoteIndex: each `line_note` body put back at its
 * `charPos` as `<i class="footnote">BODY</i>`. The linker reads this text, so citations
 * inside notes keep producing links from their line. Its visible chars are the original's
 * (buildLineNoteIndex keeps a line inline otherwise), the coordinates of `link_anchor`.
 */
object LineNoteRehydration {

    /** Stored note position and body; [hasMarker] includes an empty, non-null SQL marker. */
    data class NoteAt(val charPos: Int, val body: String, val hasMarker: Boolean = false)

    /** [notes] in noteIndex order (charPos never decreases along it). */
    fun rehydrate(stored: String, notes: List<NoteAt>): String {
        if (notes.isEmpty()) return stored
        val out = StringBuilder(stored.length + notes.sumOf { it.body.length + OPEN.length + CLOSE.length })
        val sups = SUP.findAll(stored).map { it.range }.toList()
        val markers = if (notes.any { it.hasMarker }) InlineNotes.markerRanges(stored) else emptyList()
        var anchoredSeen = 0
        var cursor = 0
        for (note in notes) {
            var at = insertionPoint(stored, note.charPos, sups)
            if (note.hasMarker) {
                val marker = markers.getOrNull(anchoredSeen++)
                if (marker != null && at <= marker.first && onlyTagsBetween(stored, at, marker.first)) {
                    at = afterInlineClosings(stored, marker.last + 1)
                }
            }
            at = maxOf(at, cursor)
            out.append(stored, cursor, at)
            out.append(OPEN).append(note.body).append(CLOSE)
            cursor = at
        }
        return out.append(stored, cursor, stored.length).toString()
    }

    private const val OPEN = "<i class=\"footnote\">"
    private const val CLOSE = "</i>"
    private val SUP = Regex("""<sup\b[^>]*>.*?</sup>""", setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL))

    private val INLINE_CLOSE = Regex(
        """</(?:b|i|em|strong|u|s|strike|span|sup|sub|small|big|font|del|ins|a|abbr|acronym|cite|code|dfn|kbd|q|samp|tt|var|mark|bdi|bdo|ruby|rt|rp)\s*>""",
        RegexOption.IGNORE_CASE,
    )

    private fun afterInlineClosings(html: String, from: Int): Int {
        var at = from
        while (at < html.length) {
            val close = INLINE_CLOSE.matchAt(html, at) ?: break
            at = close.range.last + 1
        }
        return at
    }

    /** Opening markup before an empty marker contributes no visible offset. No gaps are skipped. */
    private fun onlyTagsBetween(html: String, from: Int, end: Int): Boolean {
        var at = from
        while (at < end) {
            if (html[at] != '<') return false
            val close = html.indexOf('>', at + 1)
            if (close < 0 || close >= end) return false
            at = close + 1
        }
        return at == end
    }

    /**
     * Raw offset just after the [charPos]-th visible char (countVisibleChars convention): past its
     * marker's `</sup>` and adjacent inline closings. Block boundaries stay after the body,
     * including at the end of a line, so the next paragraph's words remain separate.
     */
    private fun insertionPoint(html: String, charPos: Int, sups: List<IntRange>): Int {
        var i = 0
        if (charPos > 0) {
            var count = 0
            var inTag = false
            while (i < html.length && count < charPos) {
                val c = html[i]
                when {
                    inTag -> if (c == '>') inTag = false
                    c == '<' -> inTag = true
                    c == '&' -> {
                        val end = minOf(html.length, i + 10)
                        var j = i + 1
                        while (j < end && html[j] != ';') j++
                        count++
                        if (j < end) i = j
                    }
                    else -> count++
                }
                i++
            }
        }
        val marker = sups.firstOrNull { i > it.first && i <= it.last }
        if (marker != null) i = marker.last + 1
        return afterInlineClosings(html, i)
    }
}
