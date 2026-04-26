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
fun countVisibleChars(html: String): Int {
    if (html.isEmpty()) return 0
    var count = 0
    var inTag = false
    var i = 0
    val len = html.length
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
