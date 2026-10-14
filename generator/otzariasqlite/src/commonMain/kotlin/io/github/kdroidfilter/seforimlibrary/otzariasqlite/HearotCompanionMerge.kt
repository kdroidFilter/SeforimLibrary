package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.common.notes.InlineNotes

/**
 * Merges 'הערות על X' companion-note files into the base book's lines as inline
 * footnotes — `<sup class="footnote-marker">N</sup><i class="footnote">body</i>`,
 * the same idiom Sefaria books use and the app's virtual 'הערות' pane renders.
 *
 * A note whose leading marker matches a marker in the base line is injected in
 * place of that marker; each note consumes the leftmost unconsumed occurrence
 * (footnote numbering restarts between sections, so the same number can appear
 * twice in one line and map to two different notes). Notes that cannot be
 * anchored are appended whole at the end of the line — text is never dropped.
 */
internal object HearotCompanionMerge {

    const val COMPANION_PREFIX = "הערות על "
    private const val HAVROUTA_PREFIX = "הערות על חברותא"

    /** The Havrouta grammar applies only to the notes of a Havrouta base book. */
    private fun isHavroutaBase(baseTitle: String): Boolean = (COMPANION_PREFIX + baseTitle).startsWith(HAVROUTA_PREFIX)

    fun isMergeableCompanionTitle(title: String): Boolean = title.startsWith(COMPANION_PREFIX)

    class MergeStats {
        var inPlace: Int = 0
        var appended: Int = 0
    }

    // Leading-marker grammars, tried in order: <small><sup>N</sup> body</small> | <sup [attrs]>N</sup> body |
    // "N body" | Havrouta notes only: "<b>(N)</b> body" (N may end in * or a letter) on a bare <small>(N)</small>
    private val NOTE_SMALL_SUP = Regex("""^﻿?<small><sup>([^<]+)</sup>\s*""")
    private val NOTE_SUP = Regex("""^﻿?<sup(?:\s[^>]*)?>([^<]+)</sup>\s*""")
    private val NOTE_BARE_NUMBER = Regex("""^﻿?(\d+)\s+""")
    private val NOTE_BOLD_PAREN = Regex("""^﻿?<b>(\(\d+[*א-ת]?\))</b>\s*""")
    private val HAVROUTA_TOKEN = Regex("""\(\d+[*א-ת]?\)""")

    /** Strips merged markers+bodies; keeps TOC text of heading lines clean. */
    fun stripFootnotes(line: String): String = InlineNotes.stripMarkersAndBodies(line)

    /**
     * Returns [baseLines] with [notesByLine] (0-based base line → note lines in
     * reading order) injected. Line count is always preserved.
     */
    fun mergeLines(
        bookTitle: String,
        baseLines: List<String>,
        notesByLine: Map<Int, List<String>>,
        stats: MergeStats = MergeStats(),
    ): List<String> {
        if (notesByLine.isEmpty()) return baseLines
        val out = baseLines.toMutableList()
        val havrouta = isHavroutaBase(bookTitle)
        for ((lineIdx, notes) in notesByLine.toSortedMap()) {
            check(lineIdx in out.indices) {
                "hearot merge for '$bookTitle': base line $lineIdx out of range (${out.size} lines)"
            }
            val original = out[lineIdx]
            check(!original.contains("class=\"footnote")) {
                "hearot merge for '$bookTitle': base line $lineIdx already carries inline footnotes"
            }
            val consumed = mutableListOf<IntRange>()
            val placements = mutableListOf<Pair<IntRange, String>>()
            val appendix = StringBuilder()
            for (note in notes) {
                check(note.isNotBlank()) { "hearot merge for '$bookTitle': blank note for line $lineIdx" }
                check(!note.contains("<i>") && !note.contains("<i ") && !note.contains("</i>")) {
                    "hearot merge for '$bookTitle': note carries <i> markup, which breaks the " +
                        "app's footnote regex (first </i> ends the body): ${note.take(80)}"
                }
                val parsed = parseLeadingMarker(note, havrouta)
                val anchor = parsed?.let { findAnchor(original, it.first, consumed, havrouta) }
                if (parsed != null && anchor != null) {
                    consumed += anchor
                    placements += anchor to
                        "<sup class=\"footnote-marker\">${parsed.first}</sup><i class=\"footnote\">${parsed.second}</i>"
                    stats.inPlace++
                } else {
                    appendix.append("<i class=\"footnote\">").append(note.removePrefix("﻿")).append("</i>")
                    stats.appended++
                }
            }
            var line = original
            for ((range, html) in placements.sortedByDescending { it.first.first }) {
                line = line.substring(0, range.first) + html + line.substring(range.last + 1)
            }
            out[lineIdx] = line + appendix
        }
        return out
    }

    /** (marker token, body) when the note opens with a recognized marker, else null. */
    private fun parseLeadingMarker(note: String, havrouta: Boolean): Pair<String, String>? {
        NOTE_SMALL_SUP.find(note)?.let { m ->
            val rest = note.substring(m.value.length).trimEnd()
            // The whole note is wrapped in <small>…</small>; unwrap both ends.
            if (rest.endsWith("</small>")) {
                return m.groupValues[1] to rest.removeSuffix("</small>").trimEnd()
            }
            return null
        }
        NOTE_SUP.find(note)?.let { return it.groupValues[1] to note.substring(it.value.length) }
        NOTE_BARE_NUMBER.find(note)?.let { return it.groupValues[1] to note.substring(it.value.length) }
        if (havrouta) NOTE_BOLD_PAREN.find(note)?.let { return it.groupValues[1] to note.substring(it.value.length) }
        return null
    }

    /**
     * Leftmost unconsumed marker in the base line matching [token]:
     * `<sup>N</sup>`, attribute-carrying `<sup …>N</sup>`, `<small …>N</small>`, or a
     * Havrouta `<small>(N)</small>`.
     */
    private fun findAnchor(line: String, token: String, consumed: List<IntRange>, havrouta: Boolean): IntRange? {
        val esc = Regex.escape(token)
        val patterns = listOfNotNull(
            Regex("<sup>$esc</sup>"),
            Regex("<sup\\s[^>]*>$esc</sup>"),
            Regex("<small\\s[^>]*>\\s*$esc\\s*</small>"),
            Regex("<small>$esc</small>").takeIf { havrouta && HAVROUTA_TOKEN.matches(token) },
        )
        // Leftmost across ALL grammars — a line may mix marker styles.
        return patterns
            .flatMap { it.findAll(line).map(MatchResult::range) }
            .filter { r -> consumed.none { it.first <= r.last && r.first <= it.last } }
            .minByOrNull { it.first }
    }
}
