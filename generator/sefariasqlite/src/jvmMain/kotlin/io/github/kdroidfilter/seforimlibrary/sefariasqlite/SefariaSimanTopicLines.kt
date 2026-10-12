package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Sefaria stores a siman's topic line inside its first se'if, as a leading bold run closed
 * by a line break: `<b>דין השכמת הבוקר. ובו ט סעיפים:</b><br>יתגבר…`. Moves it to its own
 * line above the se'if, still bold and not a TOC heading.
 */
internal object SefariaSimanTopicLines {

    // Books whose every first-segment `<b>…</b><br>` is the section's topic, checked against the export.
    val bookHeTitles: Set<String> = setOf(
        "שולחן ערוך, אורח חיים",
        "שולחן ערוך, יורה דעה",
        "שולחן ערוך, אבן העזר",
        "שולחן ערוך, חושן משפט",
        // In the Tur the bold run is the "הלכות …" heading opening a group of simanim.
        "טור",
        "ערוך השולחן",
        "ערוך השולחן העתיד",
        "קסת הסופר",
        "שמלה חדשה",
        "שערי אפרים",
        "אהבת חסד",
        "כללי התחלת החכמה",
    )

    private val INLINE_ITAG = Regex("""<i data-commentator[^>]*></i>""")
    private const val BOLD_RUN = """<b>((?:(?!</b>).)*)</b>"""

    // Commentator markers may precede the bold (שו"ע יו"ד כט), a space the break (ערוך השולחן יו"ד עב);
    // `<br>` is the cleaned form of every break.
    private val TOPIC = Regex("""^((?:${INLINE_ITAG.pattern})*)$BOLD_RUN\s*<br>\s*""")
    private val TOPIC_LINE = Regex("""^$BOLD_RUN$""")

    data class Split(val line: String, val seif: String)

    /** Splits the topic off the siman's first se'if [line], or null when it has none. */
    fun split(bookHeTitle: String, line: String): Split? {
        if (bookHeTitle !in bookHeTitles) return null
        val match = TOPIC.find(line) ?: return null
        val rest = line.substring(match.range.last + 1)
        if (rest.isEmpty()) return null
        val (leading, inner) = match.destructured
        // Commentator markers keep their line: only the se'if line carries a ref to anchor them.
        val markers = leading + INLINE_ITAG.findAll(inner).joinToString("") { it.value }
        return Split(
            line = "<b>${INLINE_ITAG.replace(inner, "")}</b>",
            seif = markers + rest,
        )
    }

    /** True when stored [content] has the shape of a [Split.line]. */
    fun isTopicLine(content: String?): Boolean = content != null && TOPIC_LINE.matches(content)
}
