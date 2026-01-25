package io.github.kdroidfilter.seforimlibrary.search

import java.io.Closeable

/**
 * A stateful search session providing paginated access to search results.
 *
 * Sessions maintain internal cursor state for efficient pagination using
 * Lucene's searchAfter mechanism. Must be closed when no longer needed
 * to release underlying index reader resources.
 *
 * ## Usage
 * ```kotlin
 * engine.openSession("שלום")?.use { session ->
 *     var page = session.nextPage(20)
 *     while (page != null && !page.isLastPage) {
 *         processResults(page.hits)
 *         page = session.nextPage(20)
 *     }
 * }
 * ```
 *
 * ## Thread Safety
 * Sessions are NOT thread-safe. Use one session per thread or synchronize access.
 *
 * @see SearchEngine.openSession to create a session
 * @see SearchPage for the structure of returned pages
 */
interface SearchSession : Closeable {
    /**
     * Retrieves the next page of search results.
     *
     * Each call advances the internal cursor. Results are ordered by relevance score.
     * Returns null when all results have been exhausted.
     *
     * @param limit Maximum number of results to return in this page
     * @return [SearchPage] containing hits and metadata, or null if no more results
     */
    fun nextPage(limit: Int): SearchPage?
}

/**
 * A page of search results with metadata.
 *
 * @property hits List of matching lines for this page
 * @property totalHits Total number of matches across all pages (approximate for large result sets)
 * @property isLastPage True if this is the final page of results
 */
data class SearchPage(
    val hits: List<LineHit>,
    val totalHits: Long,
    val isLastPage: Boolean
)

/**
 * A single search result representing a matched line in a book.
 *
 * @property bookId Unique identifier of the book containing this line
 * @property bookTitle Display title of the book
 * @property lineId Unique identifier of the matched line
 * @property lineIndex Zero-based index of the line within the book
 * @property snippet HTML snippet with highlighted matching terms (contains `<b>` tags)
 * @property score Relevance score (higher = more relevant). Includes boosts for base books.
 * @property rawText Original unprocessed text content of the line
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

/**
 * Aggregated facet counts from a search query.
 * Computed once via a lightweight Lucene collector without loading full results.
 *
 * @property totalHits Total number of matching documents
 * @property categoryCounts Map of categoryId to count (includes ancestor categories)
 * @property bookCounts Map of bookId to count
 */
data class SearchFacets(
    val totalHits: Long,
    val categoryCounts: Map<Long, Int>,
    val bookCounts: Map<Long, Int>,
)
