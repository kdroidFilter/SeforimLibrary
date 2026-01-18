package io.github.kdroidfilter.seforimlibrary.search

/**
 * Info about a line needed to fetch snippet source from DB.
 */
data class LineSnippetInfo(
    val lineId: Long,
    val bookId: Long,
    val lineIndex: Int
)

/**
 * Provider that fetches snippet source text for multiple lines.
 * Returns a map of lineId -> snippetSource (HTML-cleaned, with neighbors if needed).
 */
fun interface SnippetProvider {
    fun getSnippetSources(lines: List<LineSnippetInfo>): Map<Long, String>
}
