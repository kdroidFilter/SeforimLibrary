package io.github.kdroidfilter.seforimlibrary.search

/**
 * No-op [SearchEngine] for platforms without a native full-text backend.
 *
 * The JVM "actual" of the search engine is [LuceneSearchEngine]; Android/iOS use this stub
 * (wired through DI) until a mobile search backend is implemented. It returns no results so the
 * app launches and the search UI degrades gracefully rather than crashing.
 */
class StubSearchEngine : SearchEngine {
    override fun openSession(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean,
    ): SearchSession? = null

    override fun searchBooksByTitlePrefix(query: String, limit: Int): List<Long> = emptyList()

    override fun buildSnippet(rawText: String, query: String, near: Int): String = rawText

    override fun buildHighlightTerms(query: String): List<String> = emptyList()

    override fun computeFacets(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean,
    ): SearchFacets? = null

    override fun close() {}
}
