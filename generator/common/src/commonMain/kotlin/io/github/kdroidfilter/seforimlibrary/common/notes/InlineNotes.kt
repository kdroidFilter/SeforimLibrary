package io.github.kdroidfilter.seforimlibrary.common.notes

/**
 * Inline footnotes in line HTML: `<sup class="footnote-marker">M</sup><i class="footnote">BODY</i>`,
 * the idiom of Sefaria books and of merged 'הערות על' companions.
 *
 * A body ends at the `</i>` that closes its own opening tag, so `<i>` nested in a body
 * belongs to it. A note is anchored when only whitespace separates it from a preceding
 * footnote-marker `<sup>`; that sup is not part of the note.
 */
object InlineNotes {

    /** One note; [start]..[end] (end exclusive) spans `<i …>BODY</i>` in the line. */
    data class Note(
        val start: Int,
        val end: Int,
        /** Inner HTML of the anchoring marker sup, null when the note has none. */
        val marker: String?,
        val body: String,
    )

    sealed interface Parsed {
        data class Notes(val notes: List<Note>) : Parsed

        /** The body opened at [at] has no matching `</i>`. */
        data class Unclosed(val at: Int) : Parsed
    }

    private val NOTE_OPEN = Regex("""<i\b[^>]*\bclass\s*=\s*"footnote"[^>]*>""", RegexOption.IGNORE_CASE)
    private val ITALIC_TAG = Regex("""<i\b[^>]*>|</i>""", RegexOption.IGNORE_CASE)
    private val MARKER = Regex(
        """<sup\b[^>]*\bclass\s*=\s*"footnote-marker"[^>]*>(.*?)</sup>""",
        setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL),
    )

    fun hasNotes(line: String): Boolean = line.contains("footnote") && NOTE_OPEN.containsMatchIn(line)

    /** Top-level notes in reading order; a note nested in another note's body is part of that body. */
    fun parse(line: String): Parsed {
        if (!hasNotes(line)) return Parsed.Notes(emptyList())
        val markersByEnd = MARKER.findAll(line).associate { it.range.last + 1 to it.groupValues[1] }
        val notes = ArrayList<Note>()
        var cursor = 0
        while (true) {
            val open = NOTE_OPEN.find(line, cursor) ?: break
            val close = matchingClose(line, open.range.first) ?: return Parsed.Unclosed(open.range.first)
            var before = open.range.first
            while (before > 0 && line[before - 1].isWhitespace()) before--
            notes += Note(
                start = open.range.first,
                end = close.range.last + 1,
                marker = markersByEnd[before],
                body = line.substring(open.range.last + 1, close.range.first),
            )
            cursor = close.range.last + 1
        }
        return Parsed.Notes(notes)
    }

    /** [line] without the [notes]' `<i …>BODY</i>` spans; marker sups stay. */
    fun strip(line: String, notes: List<Note>): String {
        if (notes.isEmpty()) return line
        val out = StringBuilder(line.length)
        var cursor = 0
        for (note in notes) {
            out.append(line, cursor, note.start)
            cursor = note.end
        }
        return out.append(line, cursor, line.length).toString()
    }

    /** Footnote-marker sups in [line] — after [strip], one per note whose marker is not null. */
    fun countMarkers(line: String): Int = if (!line.contains("footnote-marker")) 0 else MARKER.findAll(line).count()

    /** Plain text use (TOC titles, dropped translator notes): every body and every marker sup removed; an unclosed body runs to the end. */
    fun stripMarkersAndBodies(line: String): String {
        if (!line.contains("footnote")) return line
        val out = StringBuilder(line.length)
        var cursor = 0
        while (true) {
            val open = NOTE_OPEN.find(line, cursor) ?: break
            out.append(line, cursor, open.range.first)
            cursor = matchingClose(line, open.range.first)?.let { it.range.last + 1 } ?: line.length
        }
        out.append(line, cursor, line.length)
        return MARKER.replace(out, "")
    }

    private fun matchingClose(line: String, openAt: Int): MatchResult? {
        var depth = 0
        for (tag in ITALIC_TAG.findAll(line, openAt)) {
            depth += if (tag.value.startsWith("</")) -1 else 1
            if (depth == 0) return tag
        }
        return null
    }
}
