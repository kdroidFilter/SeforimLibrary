package io.github.kdroidfilter.seforimlibrary.search.index

import io.github.kdroidfilter.seforimlibrary.search.SearchFacets
import kotlinx.serialization.Serializable

/**
 * The Lucene index of a search, read only: the only part of the search that runs Lucene. The JVM and Android read it in
 * process; iOS through the Java runtime it embeds (Lucene needs a JVM), with the same implementation. Everything else
 * (query building, dictionary, ranking, snippets, highlighting, fusion) is common code.
 *
 * Implementations are thread-safe; a [IndexCursor] is used by one caller at a time.
 */
interface LuceneIndex : AutoCloseable {
    /** The terms of [text] as the index's analyzer (StandardAnalyzer, the `text` field) splits it. */
    fun analyze(text: String): List<String>

    /** The best [limit] documents for [query] (by score, or by [sort]), with their stored [fields]. */
    fun search(
        query: IndexQuery,
        limit: Int,
        fields: Set<String>,
        sort: List<SortField>? = null,
    ): IndexPage

    /** Pages through the documents of [query] by score, on the index as it is now, with their stored [fields]. */
    fun openCursor(
        query: IndexQuery,
        fields: Set<String>,
    ): IndexCursor

    /** How the documents of [query] spread over the books and categories (`book_id`, `ancestor_category_ids`). */
    fun facets(query: IndexQuery): SearchFacets

    /** The stored numeric [field] of every document of [query], unscored and in no particular order. */
    fun collectLongs(
        query: IndexQuery,
        field: String,
    ): LongArray

    fun count(query: IndexQuery): Int

    /** How the vectors of [field] are stored, null when no segment has the field. */
    fun vectorEncoding(field: String): VectorEncoding?

    /** The nearest documents to a vector (see [KnnRequest]), best first, with their stored [KnnRequest.fields]. */
    fun knn(request: KnnRequest): List<IndexDoc>
}

/** A page of a [LuceneIndex.openCursor]. */
interface IndexCursor : AutoCloseable {
    /** The next [limit] documents, none once all were read. */
    fun next(limit: Int): IndexPage
}

/** A document found: its score and the stored fields asked for, numeric ones in [longs], the others in [strings]. */
@Serializable
class IndexDoc(
    val score: Float,
    val longs: Map<String, Long> = emptyMap(),
    val strings: Map<String, String> = emptyMap(),
)

/** Documents found, with the number of matches ([totalHits] may be a lower bound, as Lucene counts them). */
@Serializable
class IndexPage(
    val docs: List<IndexDoc>,
    val totalHits: Long,
)

@Serializable
data class SortField(
    val field: String,
    val type: Type,
    val reverse: Boolean = false,
) {
    @Serializable
    enum class Type { INT, LONG }
}

@Serializable
enum class VectorEncoding { FLOAT32, BYTE }

/**
 * A nearest neighbors search of [field]: with [byteVector] on a byte field, else with [floatVector]; on a float field
 * quantized by Lucene, [oversample] times more candidates are gathered, then the best [k] kept by their exact
 * similarity. [filter] restricts the documents considered.
 */
@Serializable
class KnnRequest(
    val field: String,
    val k: Int,
    val fields: Set<String>,
    val floatVector: FloatArray? = null,
    val byteVector: ByteArray? = null,
    val filter: IndexQuery? = null,
    val oversample: Int = 1,
) {
    init {
        // A NaN or infinite component means a broken embedding: its neighbors would be meaningless
        require(floatVector?.all { it.isFinite() } != false) { "KNN vector with a NaN or infinite component" }
    }
}
