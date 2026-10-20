package io.github.kdroidfilter.seforimlibrary.common

/**
 * Fast counter for the number of visible characters inside an HTML snippet.
 *
 * Tag content (`<...>`) is skipped entirely and each HTML entity (`&...;`) is
 * counted as a single visible character. The counter does not allocate a
 * stripped string and walks the input in a single pass, which matters when the
 * whole corpus is ~4M lines processed at generation time.
 *
 * The returned value is intentionally an approximation of what the rendered
 * text will contain: it feeds the content-aware scrollbar, which combines it
 * with a runtime `chars_per_visual_line` measurement to size the thumb.
 */
fun countVisibleChars(html: String): Int = countVisibleChars(html, html.length)

/**
 * Visible-char count of `html[0, endExclusive)` — converts a raw string offset
 * into the visible-char coordinate space of [countVisibleChars]. Used to store
 * word-level link anchors (see the `link_anchor` table) whose sources provide
 * raw offsets into the stored line content.
 */
fun countVisibleChars(html: String, endExclusive: Int): Int {
    if (html.isEmpty() || endExclusive <= 0) return 0
    var count = 0
    var inTag = false
    var i = 0
    val len = minOf(html.length, endExclusive)
    while (i < len) {
        val c = html[i]
        when {
            inTag -> if (c == '>') inTag = false
            c == '<' -> inTag = true
            c == '&' -> {
                // Treat &...; as a single visible character. Bail out after 10 chars
                // if no ';' is found so that stray ampersands still count.
                val end = minOf(len, i + 10)
                var j = i + 1
                var terminated = false
                while (j < end) {
                    if (html[j] == ';') { terminated = true; break }
                    j++
                }
                count++
                i = if (terminated) j else i
            }
            else -> count++
        }
        i++
    }
    return count
}

/**
 * The visible chars of [html], each as [countVisibleChars] counts it: one char outside a tag,
 * or a whole `&...;` entity. Lines with equal units give every visible offset the same meaning.
 */
fun visibleUnits(html: String): List<String> {
    val units = ArrayList<String>()
    var inTag = false
    var i = 0
    while (i < html.length) {
        val c = html[i]
        when {
            inTag -> if (c == '>') inTag = false
            c == '<' -> inTag = true
            c == '&' -> {
                val end = minOf(html.length, i + 10)
                var j = i + 1
                while (j < end && html[j] != ';') j++
                if (j < end) {
                    units += html.substring(i, j + 1)
                    i = j
                } else {
                    units += "&"
                }
            }
            else -> units += c.toString()
        }
        i++
    }
    return units
}
