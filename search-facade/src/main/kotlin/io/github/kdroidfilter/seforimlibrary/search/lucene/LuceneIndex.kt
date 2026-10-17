package io.github.kdroidfilter.seforimlibrary.search.lucene

import java.nio.file.Path

/**
 * The Lucene index of a search, read only: the only part of the search that runs Lucene, on a JVM (the desktop's own,
 * the mobile apps' embedded one, through Nucleus JVM Access). Everything else (query building, dictionary, ranking,
 * snippets, highlighting, fusion) is common code.
 *
 * Thread-safe; an [IndexCursor] is used by one caller at a time.
 *
 * @param indexDir the index directory
 * @param cacheQueries whether to cache any query seen often (small indexes: the lookup index); else only the filters
 *   every search repeats
 */
class LuceneIndex(
    indexDir: String,
    cacheQueries: Boolean = false,
) : AutoCloseable {
    private val backend = LuceneIndexBackend(Path.of(indexDir), cacheQueries)

    /** The terms of [text] as the index's analyzer (StandardAnalyzer, the `text` field) splits it. */
    fun analyze(text: String): List<String> = backend.analyze(text)

    /** The best [limit] documents for [query] (by score, or by [sort]), with their stored [fields]. */
    fun search(
        query: IndexQuery,
        limit: Int,
        fields: Set<String>,
        sort: List<SortField>? = null,
    ): IndexPage = backend.search(query, limit, fields, sort)

    /** Pages through the documents of [query] by score, on the index as it is now, with their stored [fields]. */
    fun openCursor(
        query: IndexQuery,
        fields: Set<String>,
    ): IndexCursor = IndexCursor(backend.openCursor(query, fields))

    /** How the documents of [query] spread over the books and categories (`book_id`, `ancestor_category_ids`). */
    fun facets(query: IndexQuery): IndexFacets = backend.facets(query)

    /** The stored numeric [field] of every document of [query], unscored and in no particular order. */
    fun collectLongs(
        query: IndexQuery,
        field: String,
    ): LongArray = backend.collectLongs(query, field)

    fun count(query: IndexQuery): Int = backend.count(query)

    /** How the vectors of [field] are stored, null when no segment has the field. */
    fun vectorEncoding(field: String): VectorEncoding? = backend.vectorEncoding(field)

    /** The nearest documents to a vector (see [KnnRequest]), best first, with their stored [KnnRequest.fields]. */
    fun knn(request: KnnRequest): List<IndexDoc> = backend.knn(request)

    override fun close() = backend.close()
}

/** A page of a [LuceneIndex.openCursor]. */
class IndexCursor internal constructor(
    private val cursor: BackendCursor,
) : AutoCloseable {
    /** The next [limit] documents, none once all were read. */
    fun next(limit: Int): IndexPage = cursor.next(limit)

    override fun close() = cursor.close()
}
