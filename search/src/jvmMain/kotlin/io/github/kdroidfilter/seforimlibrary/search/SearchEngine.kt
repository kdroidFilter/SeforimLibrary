package io.github.kdroidfilter.seforimlibrary.search

import java.io.Closeable

/**
 * Interface for full-text search engine.
 * Provides session-based search with pagination and book title suggestions.
 */
interface SearchEngine : Closeable {

    /**
     * Opens a search session for the given query with optional filters.
     * Returns null if query is empty or invalid.
     *
     * @param query The search query (will be normalized internally)
     * @param near Proximity for phrase matching (default 5)
     * @param bookFilter Optional book ID to filter results
     * @param categoryFilter Optional category ID to filter results
     * @param bookIds Optional collection of book IDs to filter results
     * @param lineIds Optional collection of line IDs to filter results
     * @return A SearchSession for paginated results, or null if query is invalid
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
     * Searches for books by title prefix.
     *
     * @param query The prefix to search for
     * @param limit Maximum number of results to return
     * @return List of book IDs matching the prefix
     */
    fun searchBooksByTitlePrefix(query: String, limit: Int = 20): List<Long>

    /**
     * Builds a snippet with highlighted terms from raw text.
     *
     * @param rawText The raw text to build snippet from
     * @param query The search query for highlighting
     * @param near Proximity for context
     * @return HTML snippet with <b> tags around matching terms
     */
    fun buildSnippet(rawText: String, query: String, near: Int): String
}
