package io.github.kdroidfilter.seforimlibrary.reflinks

/**
 * Writes links into a line's HTML: `<a class="ref" href="seforim://line/<id>">(בראשית א, ד)</a>`.
 * Offsets are positions in the tag-stripped text (what the scanner reads); they are mapped back to the raw HTML.
 */
object LinkInjector {

    /** A link to write: [start]/[end] in the tag-stripped text; [existingAnchor] rewrites the `<a>` already there. */
    data class Link(val start: Int, val end: Int, val targetLineId: Long, val existingAnchor: Boolean, val arbitrated: Boolean)

    fun href(lineId: Long) = "seforim://line/$lineId"

    private fun openTag(link: Link): String =
        "<a class=\"ref\" href=\"${href(link.targetLineId)}\"" + (if (link.arbitrated) " data-llm=\"1\"" else "") + ">"

    /** Returns the new HTML, and how many links were written. Links that would cross a tag are skipped. */
    fun inject(html: String, links: List<Link>): Pair<String, Int> {
        // visible index → raw index
        val rawOf = IntArray(html.length + 1)
        var visible = 0
        var i = 0
        while (i < html.length) {
            if (html[i] == '<') {
                val close = html.indexOf('>', i)
                if (close < 0) break
                i = close + 1
                continue
            }
            rawOf[visible++] = i++
        }
        rawOf[visible] = html.length

        val out = StringBuilder(html.length + links.size * 48)
        var copied = 0
        var written = 0
        var lastEnd = -1
        for (link in links.sortedBy { it.start }) {
            if (link.start < lastEnd || link.end > visible || link.start >= link.end) continue
            val rawStart = rawOf[link.start]
            val rawEnd = rawOf[link.end - 1] + 1
            if (link.existingAnchor) {
                // Rewrite the opening tag that ends right before the anchor text
                val tagStart = html.lastIndexOf("<a", rawStart)
                if (tagStart < copied || html.indexOf('>', tagStart) != rawStart - 1) continue
                out.append(html, copied, tagStart).append(openTag(link))
                copied = rawStart
            } else {
                if (html.indexOf('<', rawStart).let { it in rawStart until rawEnd }) continue
                out.append(html, copied, rawStart).append(openTag(link)).append(html, rawStart, rawEnd).append("</a>")
                copied = rawEnd
            }
            lastEnd = link.end
            written++
        }
        out.append(html, copied, html.length)
        return out.toString() to written
    }
}
