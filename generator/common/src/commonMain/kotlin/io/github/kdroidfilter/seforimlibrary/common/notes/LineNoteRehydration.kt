package io.github.kdroidfilter.seforimlibrary.common.notes

/**
 * The line as it read before buildLineNoteIndex: each `line_note` body put back at its
 * `charPos` as `<i class="footnote">BODY</i>`. The linker reads this text, so citations
 * inside notes keep producing links from their line.
 */
object LineNoteRehydration {

    data class NoteAt(val charPos: Int, val body: String)

    /** [bodies]: the raw [start, end) of every inserted `<i class="footnote">…</i>` in [text]. */
    class Rehydrated(val text: String, val bodies: List<IntRange>) {
        /**
         * Raw offset in the stored (stripped) line for raw offsets [start, end) of [text],
         * or null when the range touches an inserted body.
         */
        fun storedRange(start: Int, end: Int): Pair<Int, Int>? {
            var shift = 0
            for (body in bodies) {
                val bodyEnd = body.last + 1
                if (start < bodyEnd && end > body.first) return null
                if (bodyEnd <= start) shift += bodyEnd - body.first
            }
            return (start - shift) to (end - shift)
        }
    }

    /** [notes] in noteIndex order (charPos never decreases along it). */
    fun rehydrate(stored: String, notes: List<NoteAt>): Rehydrated {
        if (notes.isEmpty()) return Rehydrated(stored, emptyList())
        val out = StringBuilder(stored.length + notes.sumOf { it.body.length + OPEN.length + CLOSE.length })
        val bodies = ArrayList<IntRange>(notes.size)
        var cursor = 0
        for (note in notes) {
            val at = maxOf(insertionPoint(stored, note.charPos), cursor)
            out.append(stored, cursor, at)
            val start = out.length
            out.append(OPEN).append(note.body).append(CLOSE)
            bodies += start until out.length
            cursor = at
        }
        out.append(stored, cursor, stored.length)
        return Rehydrated(out.toString(), bodies)
    }

    private fun isTagsOnly(html: String, from: Int): Boolean {
        var inTag = false
        for (k in from until html.length) {
            val c = html[k]
            when {
                inTag -> if (c == '>') inTag = false
                c == '<' -> inTag = true
                else -> return false
            }
        }
        return true
    }

    private const val OPEN = "<i class=\"footnote\">"
    private const val CLOSE = "</i>"
    private val SUP = Regex("""<sup\b[^>]*>.*?</sup>""", setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL))

    /**
     * Raw offset just after the [charPos]-th visible char (countVisibleChars convention): past its
     * marker's `</sup>`, else, after the last visible char, at the very end of the line.
     */
    internal fun insertionPoint(html: String, charPos: Int): Int {
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
        val marker = SUP.findAll(html).firstOrNull { i > it.range.first && i <= it.range.last }
        if (marker != null) return marker.range.last + 1
        if (html.startsWith("</sup>", i, ignoreCase = true)) i += "</sup>".length
        else if (isTagsOnly(html, i)) i = html.length
        return i
    }
}
