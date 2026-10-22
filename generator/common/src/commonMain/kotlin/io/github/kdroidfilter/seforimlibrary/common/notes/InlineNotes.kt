package io.github.kdroidfilter.seforimlibrary.common.notes

/**
 * Inline footnotes in line HTML: `<sup class="footnote-marker">M</sup><i class="footnote">BODY</i>`,
 * the idiom of Sefaria books and of merged 'הערות על' companions.
 *
 * A body ends at the `</i>` that closes its own opening tag, so `<i>` nested in a body
 * belongs to it. A note is anchored when only whitespace (no-break spaces included, also as
 * entities) separates it from a preceding footnote-marker `<sup>`; that sup is not part of the note.
 */
object InlineNotes {

    /** One note; [start]..[end] (end exclusive) spans `<i …>BODY</i>` in the line. */
    data class Note(
        val start: Int,
        val end: Int,
        /** Inner HTML of the anchoring marker sup, null when the note has none. */
        val marker: String?,
        val body: String,
        /** Opening tag, `<` to `>`, of an anchoring sup without the footnote-marker class. */
        val plainSupTag: IntRange? = null,
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

    private val ANY_SUP = Regex("""<sup\b([^>]*)>(.*?)</sup>""", setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL))
    private val MARKER_CLASS = Regex("""\bclass\s*=\s*"footnote-marker"""", RegexOption.IGNORE_CASE)
    private val CLASS_ATTR = Regex("""\s*\bclass\s*=\s*"[^"]*"""", RegexOption.IGNORE_CASE)

    fun hasNotes(line: String): Boolean = line.contains("footnote") && NOTE_OPEN.containsMatchIn(line)

    /** Top-level notes in reading order; a note nested in another note's body is part of that body. */
    fun parse(line: String): Parsed {
        if (!hasNotes(line)) return Parsed.Notes(emptyList())
        val supsByEnd = ANY_SUP.findAll(line).associateBy { it.range.last + 1 }
        val notes = ArrayList<Note>()
        var cursor = 0
        while (true) {
            val open = NOTE_OPEN.find(line, cursor) ?: break
            val close = matchingClose(line, open.range.first) ?: return Parsed.Unclosed(open.range.first)
            // A footnote tag inside a sup is part of that marker (seen in Sefaria data), not a note.
            if (supsByEnd.values.any { open.range.first in it.range }) {
                cursor = close.range.last + 1
                continue
            }
            val sup =supsByEnd[gapStart(line, open.range.first)]
            val plain = sup != null && !MARKER_CLASS.containsMatchIn(sup.groupValues[1])
            notes += Note(
                start = open.range.first,
                end = close.range.last + 1,
                marker = sup?.groupValues?.get(2),
                body = line.substring(open.range.last + 1, close.range.first),
                plainSupTag = if (plain) sup!!.range.first..line.indexOf('>', sup.range.first) else null,
            )
            cursor = close.range.last + 1
        }
        return Parsed.Notes(notes)
    }

    /**
     * [line] without the [notes]' `<i …>BODY</i>` spans. Marker sups stay; an anchoring sup
     * without the class gains `class="footnote-marker"`, so every anchored note has a classed sup.
     */
    fun strip(line: String, notes: List<Note>): String {
        if (notes.isEmpty()) return line
        val out = StringBuilder(line.length)
        var cursor = 0
        for (note in notes) {
            val tag = note.plainSupTag
            if (tag != null) {
                out.append(line, cursor, tag.first)
                val attrs = line.substring(tag.first + "<sup".length, tag.last)
                out.append("<sup class=\"footnote-marker\"").append(CLASS_ATTR.replace(attrs, "")).append('>')
                cursor = tag.last + 1
            }
            out.append(line, cursor, note.start)
            cursor = note.end
        }
        return out.append(line, cursor, line.length).toString()
    }

    /** Footnote-marker sups in [line] — after [strip], one per note whose marker is not null. */
    fun countMarkers(line: String): Int = if (!line.contains("footnote-marker")) 0 else MARKER.findAll(line).count()

    internal fun markerRanges(line: String): List<IntRange> = MARKER.findAll(line).map { it.range }.toList()

    /** Inner HTML of every footnote-marker sup in [line], in order: `line_note.hasMarker` rows take them by rank. */
    fun markers(line: String): List<String> =
        if (!line.contains("footnote-marker")) emptyList() else MARKER.findAll(line).map { it.groupValues[1] }.toList()

    /**
     * The sups the readers (app `_strippedMarkers`, search engine `stripped_markers`) take as markers of
     * moved-out bodies, with their Dart RegExp semantics: ASCII case folding and word boundaries, JavaScript `\s`.
     */
    internal fun consumerMarkerRanges(line: String): List<IntRange> =
        if (!line.contains("footnote-marker", ignoreCase = true)) {
            emptyList()
        } else {
            CONSUMER_MARKER.findAll(line).filter { CONSUMER_BODY_START.matchAt(line, it.range.last + 1) == null }
                .map { it.range }.toList()
        }

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

    /** Start of the run of whitespace and no-break-space entities that ends at [end]. */
    private fun gapStart(line: String, end: Int): Int {
        var at = end
        while (at > 0) {
            if (line[at - 1].isWhitespace() || line[at - 1] == '\u00A0') { at--; continue }
            val entity = NBSP_ENTITIES.firstOrNull { line.regionMatches(at - it.length, it, 0, it.length, ignoreCase = true) }
                ?: break
            at -= entity.length
        }
        return at
    }

    private val NBSP_ENTITIES = listOf("&nbsp;", "&#160;", "&#xa0;")

    private fun asciiAnyCase(word: String): String =
        word.map { if (it in 'a'..'z') "[$it${it.uppercaseChar()}]" else it.toString() }.joinToString("")

    private const val JS_SPACE = "[\\t\\n\\u000B\\f\\r \\u00A0\\u1680\\u2000-\\u200A\\u2028\\u2029\\u202F\\u205F\\u3000\\uFEFF]"

    // Dart's \b: an ASCII word character on one side only (Java's \b also joins combining marks to a word).
    private const val ASCII_B = "(?:(?<=[A-Za-z0-9_])(?![A-Za-z0-9_])|(?<![A-Za-z0-9_])(?=[A-Za-z0-9_]))"

    private val CONSUMER_MARKER = Regex(
        "<${asciiAnyCase("sup")}$ASCII_B[^>]*$ASCII_B${asciiAnyCase("footnote-marker")}$ASCII_B[^>]*>.*?</${asciiAnyCase("sup")}>",
        RegexOption.DOT_MATCHES_ALL,
    )
    private val CONSUMER_BODY_START = Regex(
        "$JS_SPACE*<${asciiAnyCase("i")}$ASCII_B[^>]*$ASCII_B${asciiAnyCase("class")}$JS_SPACE*=$JS_SPACE*\"[^\"]*$ASCII_B${asciiAnyCase("footnote")}$ASCII_B",
    )

    private fun matchingClose(line: String, openAt: Int): MatchResult? {
        var depth = 0
        for (tag in ITALIC_TAG.findAll(line, openAt)) {
            depth += if (tag.value.startsWith("</")) -1 else 1
            if (depth == 0) return tag
        }
        return null
    }
}
