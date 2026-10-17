package io.github.kdroidfilter.seforimlibrary.search.lucene

/**
 * A Lucene query as plain data: the engines build it in common code, a [LuceneIndex] turns it into Lucene's own
 * query, on a JVM (the desktop's own, the mobile apps' embedded one).
 */
sealed interface IndexQuery {
    /** Documents whose [field] holds the term [text]. */
    data class Term(
        val field: String,
        val text: String,
    ) : IndexQuery

    /** Documents whose [field] holds a term starting with [text]. */
    data class Prefix(
        val field: String,
        val text: String,
    ) : IndexQuery

    /** Documents whose [field] holds a term within [maxEdits] edits of [text]. */
    data class Fuzzy(
        val field: String,
        val text: String,
        val maxEdits: Int,
    ) : IndexQuery

    data class Bool(
        val clauses: List<Clause>,
    ) : IndexQuery

    data class Boost(
        val query: IndexQuery,
        val boost: Float,
    ) : IndexQuery

    /** Each position matches any of its terms, within [slop] moves. */
    data class MultiPhrase(
        val field: String,
        val positions: List<List<String>>,
        val slop: Int,
    ) : IndexQuery

    /**
     * [text] analyzed like the index (StandardAnalyzer) into a phrase within [slop] moves (a term query for one term),
     * every term required when the analysis can't build a phrase, and nothing when it yields no term.
     */
    data class AnalyzedPhrase(
        val field: String,
        val text: String,
        val slop: Int,
    ) : IndexQuery

    /** Documents whose int point [field] is [value]. */
    data class IntExact(
        val field: String,
        val value: Int,
    ) : IndexQuery

    /** Documents whose int point [field] is one of [values]. */
    data class IntSet(
        val field: String,
        val values: IntArray,
    ) : IndexQuery

    /** Every document. */
    data object MatchAll : IndexQuery
}

data class Clause(
    val query: IndexQuery,
    val occur: Occur,
)

enum class Occur { MUST, SHOULD, FILTER, MUST_NOT }

/** A document found: its score and the stored fields asked for, numeric ones in [longs], the others in [strings]. */
data class IndexDoc(
    val score: Float,
    val longs: Map<String, Long> = emptyMap(),
    val strings: Map<String, String> = emptyMap(),
)

/** Documents found, with the number of matches ([totalHits] may be a lower bound, as Lucene counts them). */
data class IndexPage(
    val docs: List<IndexDoc>,
    val totalHits: Long,
)

/** How the documents of a query spread over the books and categories (ancestors included). */
data class IndexFacets(
    val totalHits: Long,
    val categoryCounts: Map<Long, Int>,
    val bookCounts: Map<Long, Int>,
)

enum class SortType { INT, LONG }

data class SortField(
    val field: String,
    val type: SortType,
    val reverse: Boolean = false,
)

enum class VectorEncoding { FLOAT32, BYTE }

/**
 * A nearest neighbors search of [field]: with [byteVector] on a byte field, else with [floatVector]; on a float field
 * quantized by Lucene, [oversample] times more candidates are gathered, then the best [k] kept by their exact
 * similarity. [filter] restricts the documents considered.
 */
data class KnnRequest(
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
