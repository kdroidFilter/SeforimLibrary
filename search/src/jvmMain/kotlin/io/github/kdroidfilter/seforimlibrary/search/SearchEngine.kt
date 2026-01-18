package io.github.kdroidfilter.seforimlibrary.search

import java.io.Closeable

/**
 * Main interface for full-text search operations on Hebrew religious texts.
 *
 * This interface provides session-based search with pagination support,
 * book title suggestions, and snippet generation with term highlighting.
 *
 * ## Usage Example
 * ```kotlin
 * val engine: SearchEngine = LuceneSearchEngine(indexPath)
 *
 * // Open a search session
 * val session = engine.openSession("בראשית", near = 5)
 * session?.use {
 *     while (true) {
 *         val page = it.nextPage(20) ?: break
 *         page.hits.forEach { hit ->
 *             println("${hit.bookTitle}: ${hit.snippet}")
 *         }
 *         if (page.isLastPage) break
 *     }
 * }
 * ```
 *
 * ## Thread Safety
 * Implementations should be thread-safe for concurrent search operations.
 *
 * @see SearchSession for paginated result access
 * @see LineHit for individual search result structure
 */
interface SearchEngine : Closeable {

    /**
     * Opens a search session for the given query with optional filters.
     *
     * The query is normalized internally (nikud/teamim removed, final letters converted).
     * Returns null if the query is empty, blank, or contains only stop words.
     *
     * @param query The search query in Hebrew (may contain nikud/teamim)
     * @param near Proximity slop for phrase matching. Use 0 for exact phrase,
     *             higher values allow more words between terms (default: 5)
     * @param bookFilter Optional single book ID to restrict results
     * @param categoryFilter Optional category ID to restrict results
     * @param bookIds Optional collection of book IDs to restrict results (OR logic)
     * @param lineIds Optional collection of line IDs to restrict results (OR logic)
     * @return A [SearchSession] for paginated access to results, or null if query is invalid
     */
    fun openSession(
        query: String,
        near: Int = 5,
        bookFilter: Long? = null,
        categoryFilter: Long? = null,
        bookIds: Collection<Long>? = null,
        lineIds: Collection<Long>? = null
    ): SearchSession?

    /**
     * Searches for books whose titles match the given prefix.
     *
     * Useful for autocomplete/typeahead functionality in search UI.
     * The query is normalized before matching.
     *
     * @param query The prefix to search for (e.g., "בראש" matches "בראשית רבה")
     * @param limit Maximum number of book IDs to return (default: 20)
     * @return List of matching book IDs, ordered by relevance
     */
    fun searchBooksByTitlePrefix(query: String, limit: Int = 20): List<Long>

    /**
     * Builds an HTML snippet with highlighted search terms from raw text.
     *
     * The snippet extracts a context window around the first match and wraps
     * matching terms in `<b>` tags for highlighting. Handles Hebrew text with
     * nikud/teamim correctly by matching on normalized forms.
     *
     * @param rawText The raw text content (may contain HTML, will be sanitized)
     * @param query The search query for term highlighting
     * @param near Proximity value affecting context window size
     * @return HTML string with `<b>` tags around matches, possibly with `...` for truncation
     */
    fun buildSnippet(rawText: String, query: String, near: Int): String
}
