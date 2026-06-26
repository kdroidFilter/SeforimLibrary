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
     * @param baseBookOnly If true, restrict results to base books only (default: false)
     * @return A [SearchSession] for paginated access to results, or null if query is invalid
     */
    fun openSession(
        query: String,
        near: Int = 5,
        bookFilter: Long? = null,
        categoryFilter: Long? = null,
        bookIds: Collection<Long>? = null,
        lineIds: Collection<Long>? = null,
        baseBookOnly: Boolean = false
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

    /**
     * Returns the contiguous passage within [text] whose meaning is closest to [query],
     * for semantic highlighting (using the same dense encoder as semantic search).
     *
     * The result is a verbatim substring of [text] so callers can locate it with the
     * usual diacritic-insensitive matching and highlight that single span — instead of
     * scattering dictionary-expanded word matches that don't reflect meaning.
     *
     * @return the best-matching passage, or null when dense search is unavailable or no
     *         passage stands out (e.g. the text is a single short clause).
     */
    suspend fun semanticSpan(query: String, text: String): String? = null

    /**
     * Embedding-based find-in-page within a single book: returns the ids of the lines
     * semantically closest to [query] (dense KNN over the index, scoped to [bookId]),
     * ordered by relevance. Used by the "smart" find mode — the simple mode matches literal
     * words instead. The per-line passage to highlight is computed by the caller via
     * [semanticSpan] on the displayed text. Empty when dense search is unavailable.
     */
    suspend fun semanticFind(query: String, bookId: Long, limit: Int): List<Long> = emptyList()

    /**
     * Ensures the dense backend (embedding model + vector index) is loaded and reports whether
     * it is actually available. Useful for diagnostics and to decide whether semantic features
     * can run. Returns false for engines without a dense path.
     */
    suspend fun denseReady(): Boolean = false

    /**
     * Computes aggregate facet counts without loading full results.
     *
     * Uses a lightweight Lucene collector that only reads book IDs and ancestor
     * category IDs from the index. This is much faster than streaming all results
     * and allows the UI to display the category/book tree immediately.
     *
     * @param query The search query in Hebrew (may contain nikud/teamim)
     * @param near Proximity slop for phrase matching (default: 5)
     * @param bookFilter Optional single book ID to restrict results
     * @param categoryFilter Optional category ID to restrict results
     * @param bookIds Optional collection of book IDs to restrict results (OR logic)
     * @param lineIds Optional collection of line IDs to restrict results (OR logic)
     * @param baseBookOnly If true, restrict results to base books only (default: false)
     * @return [SearchFacets] with counts, or null if query is invalid
     */
    fun computeFacets(
        query: String,
        near: Int = 5,
        bookFilter: Long? = null,
        categoryFilter: Long? = null,
        bookIds: Collection<Long>? = null,
        lineIds: Collection<Long>? = null,
        baseBookOnly: Boolean = false
    ): SearchFacets?
}
