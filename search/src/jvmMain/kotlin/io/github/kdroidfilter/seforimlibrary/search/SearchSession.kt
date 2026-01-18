package io.github.kdroidfilter.seforimlibrary.search

import java.io.Closeable

/**
 * A search session that provides paginated access to search results.
 * Must be closed when no longer needed to release resources.
 */
interface SearchSession : Closeable {
    /**
     * Returns the next page of results.
     *
     * @param limit Maximum number of results per page
     * @return SearchPage with results, or null if no more results
     */
    fun nextPage(limit: Int): SearchPage?
}

/**
 * A page of search results.
 */
data class SearchPage(
    val hits: List<LineHit>,
    val totalHits: Long,
    val isLastPage: Boolean
)

/**
 * A single search result representing a line match.
 */
data class LineHit(
    val bookId: Long,
    val bookTitle: String,
    val lineId: Long,
    val lineIndex: Int,
    val snippet: String,
    val score: Float,
    val rawText: String
)
