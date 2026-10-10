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
     * The words that stand for [query] in [text] (plain, diacritics allowed), as ranges of [text]: the one rule every
     * view highlights a search with (results' snippets, their preview, the reader's smart find). A query word counts
     * as itself (prefixed, or inside a longer word from 4 letters) or as one of its inflections; consecutive matched
     * words form one range.
     */
    fun highlightRanges(text: String, query: String): List<IntRange> = emptyList()

    /**
     * What to highlight of [query] in found lines [texts] (plain): [highlightRanges] where the query's words show,
     * else, for engines that search by meaning, the passage closest in meaning to the query (the whole line when it
     * is a single clause). One batch, as the meaning takes a model run.
     */
    suspend fun highlights(texts: List<String>, query: String): List<TextHighlights> =
        texts.map { TextHighlights(highlightRanges(it, query)) }

    /** A snippet of [text] (snippet text, see [SnippetSources.clean]) laid out around [ranges], bold. */
    fun rangeSnippet(text: String, ranges: List<IntRange>): String = text

    /**
     * Fills `snippet` and `rawText` of [hits] fetched without snippets (`nextPage(limit, snippets = false)`), so
     * only the hits actually shown pay for their snippet. Order and other fields are kept.
     */
    suspend fun attachSnippets(
        hits: List<LineHit>,
        query: String,
        near: Int,
    ): List<LineHit> = hits

    /**
     * Loads what the first search would otherwise load (index readers, dictionary, models), so it answers at full
     * speed. Meant to run in the background at startup; safe to call more than once.
     */
    suspend fun warmUp() {}

    /**
     * Smart find-in-page within a single book: the ids of its lines the search finds for [query] (its words, fused
     * with the lines closest in meaning where dense search is available), best first. The simple mode matches literal
     * words instead. Each line is highlighted with [highlights]. Empty when dense search is unavailable.
     */
    suspend fun semanticFind(query: String, bookId: Long, limit: Int): List<Long> = emptyList()

    /**
     * Literal find-in-page over a whole book, not just its loaded lines: the ids of the lines
     * of [bookId] that may contain [query] as a substring (nikud, final letters and case
     * ignored), in no particular order.
     *
     * A superset: the index knows which words a line holds, not where, so the caller confirms
     * each candidate against the line's text.
     *
     * @return null when the index can't answer (the book isn't indexed, no index at all, or a
     *         query too short to filter on): the caller must then scan the book itself
     */
    fun findInBookCandidates(query: String, bookId: Long): LongArray? = null

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

/**
 * What a search highlights in a found line.
 *
 * @property ranges the highlighted ranges of the line's text
 * @property byMeaning true when they are the passage closest in meaning, none of the query's words being there
 */
data class TextHighlights(
    val ranges: List<IntRange>,
    val byMeaning: Boolean = false
)
