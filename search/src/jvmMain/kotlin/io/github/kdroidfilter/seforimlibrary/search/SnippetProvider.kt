package io.github.kdroidfilter.seforimlibrary.search

import org.jsoup.Jsoup
import org.jsoup.safety.Safelist

/**
 * Metadata about a line needed to fetch its snippet source text.
 *
 * @property lineId Unique identifier of the line
 * @property bookId Book containing this line (used for context fetching)
 * @property lineIndex Zero-based position of the line in the book
 */
data class LineSnippetInfo(
    val lineId: Long,
    val bookId: Long,
    val lineIndex: Int
)

/**
 * The text a hit's snippet is cut from: the hit line, HTML-cleaned (escaped), with its neighbors when it is short.
 *
 * @property text the source text
 * @property lineStart start of the hit line in [text]
 * @property lineEnd end of the hit line in [text]
 */
data class SnippetSource(
    val text: String,
    val lineStart: Int,
    val lineEnd: Int
)

/**
 * Provider interface for fetching snippet source text from a data store.
 *
 * The search engine uses this to retrieve the full text content needed for snippet generation. Implementations
 * typically load the lines from a database and build the sources with [SnippetSources.build].
 *
 * @see SearchEngine for how this provider is used during search
 */
fun interface SnippetProvider {
    /**
     * Fetches snippet source text for multiple lines in a single batch.
     *
     * @param lines List of line metadata for which to fetch text
     * @return Map of lineId to its source. Missing lines should be omitted.
     */
    suspend fun getSnippetSources(lines: List<LineSnippetInfo>): Map<Long, SnippetSource>
}

/** Builds snippet sources the same way for every data store. */
object SnippetSources {
    /** Lines kept on each side of a short hit line */
    const val NEIGHBOR_WINDOW = 4

    /** A hit line this long (cleaned) is shown alone */
    const val MIN_LENGTH = 280

    private val WHITESPACE = Regex("\\s+")

    /**
     * Sources for [lines]: each hit line HTML-cleaned, with its [NEIGHBOR_WINDOW] neighbors on each side when shorter
     * than [MIN_LENGTH]. Only the windows around the hits are loaded, through [loadLines] (a book's raw HTML lines
     * from one index to another, both included, by index); only the lines used are cleaned.
     */
    suspend fun build(
        lines: List<LineSnippetInfo>,
        loadLines: suspend (bookId: Long, fromIndex: Int, toIndex: Int) -> Map<Int, String>,
    ): Map<Long, SnippetSource> {
        val result = HashMap<Long, SnippetSource>()
        for ((bookId, bookLines) in lines.groupBy { it.bookId }) {
            // Overlapping windows merged: a book's hits can be thousands of lines apart
            val contentByIndex = HashMap<Int, String>()
            for (range in windows(bookLines.map { it.lineIndex })) {
                contentByIndex.putAll(loadLines(bookId, range.first, range.last))
            }
            val plainByIndex = HashMap<Int, String>()
            fun plain(index: Int): String? =
                plainByIndex[index] ?: contentByIndex[index]?.let { clean(it).also { cleaned -> plainByIndex[index] = cleaned } }

            for (info in bookLines) {
                val line = plain(info.lineIndex) ?: continue
                if (line.length >= MIN_LENGTH) {
                    result[info.lineId] = SnippetSource(line, 0, line.length)
                    continue
                }
                val sb = StringBuilder()
                var lineStart = 0
                for (index in (info.lineIndex - NEIGHBOR_WINDOW).coerceAtLeast(0)..info.lineIndex + NEIGHBOR_WINDOW) {
                    val text = plain(index) ?: continue
                    if (sb.isNotEmpty()) sb.append(' ')
                    if (index == info.lineIndex) lineStart = sb.length
                    sb.append(text)
                }
                result[info.lineId] = SnippetSource(sb.toString(), lineStart, lineStart + line.length)
            }
        }
        return result
    }

    /** [html] as snippet text: tags dropped, entities escaped, whitespace collapsed. */
    fun clean(html: String): String = Jsoup.clean(html, Safelist.none()).replace(WHITESPACE, " ").trim()

    private fun windows(lineIndexes: List<Int>): List<IntRange> {
        val out = ArrayList<IntRange>()
        for (index in lineIndexes.sorted()) {
            val start = (index - NEIGHBOR_WINDOW).coerceAtLeast(0)
            val end = index + NEIGHBOR_WINDOW
            val last = out.lastOrNull()
            if (last != null && start <= last.last + 1) out[out.lastIndex] = last.first..maxOf(last.last, end) else out += start..end
        }
        return out
    }
}
