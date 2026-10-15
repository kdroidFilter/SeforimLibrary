package io.github.kdroidfilter.seforimlibrary.search.index

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * A Lucene query as plain data: the engines build it in common code, a [LuceneIndex] turns it into Lucene's own
 * query, in this process (JVM, Android) or in the Java runtime the iOS app embeds.
 */
@Serializable
sealed interface IndexQuery {
    /** Documents whose [field] holds the term [text]. */
    @Serializable
    @SerialName("term")
    data class Term(
        val field: String,
        val text: String,
    ) : IndexQuery

    /** Documents whose [field] holds a term starting with [text]. */
    @Serializable
    @SerialName("prefix")
    data class Prefix(
        val field: String,
        val text: String,
    ) : IndexQuery

    /** Documents whose [field] holds a term within [maxEdits] edits of [text]. */
    @Serializable
    @SerialName("fuzzy")
    data class Fuzzy(
        val field: String,
        val text: String,
        val maxEdits: Int,
    ) : IndexQuery

    @Serializable
    @SerialName("bool")
    data class Bool(
        val clauses: List<Clause>,
    ) : IndexQuery

    @Serializable
    @SerialName("boost")
    data class Boost(
        val query: IndexQuery,
        val boost: Float,
    ) : IndexQuery

    /** Each position matches any of its terms, within [slop] moves. */
    @Serializable
    @SerialName("multiPhrase")
    data class MultiPhrase(
        val field: String,
        val positions: List<List<String>>,
        val slop: Int,
    ) : IndexQuery

    /**
     * [text] analyzed like the index (StandardAnalyzer) into a phrase within [slop] moves (a term query for one term),
     * every term required when the analysis can't build a phrase, and nothing when it yields no term.
     */
    @Serializable
    @SerialName("analyzedPhrase")
    data class AnalyzedPhrase(
        val field: String,
        val text: String,
        val slop: Int,
    ) : IndexQuery

    /** Documents whose int point [field] is [value]. */
    @Serializable
    @SerialName("intExact")
    data class IntExact(
        val field: String,
        val value: Int,
    ) : IndexQuery

    /** Documents whose int point [field] is one of [values]. */
    @Serializable
    @SerialName("intSet")
    class IntSet(
        val field: String,
        val values: IntArray,
    ) : IndexQuery

    /** Every document. */
    @Serializable
    @SerialName("all")
    data object MatchAll : IndexQuery
}

@Serializable
data class Clause(
    val query: IndexQuery,
    val occur: Occur,
)

@Serializable
enum class Occur { MUST, SHOULD, FILTER, MUST_NOT }

/** A [IndexQuery.Bool] built clause by clause. */
class BoolBuilder {
    private val clauses = ArrayList<Clause>()

    fun add(
        query: IndexQuery,
        occur: Occur,
    ): BoolBuilder = apply { clauses += Clause(query, occur) }

    fun build(): IndexQuery.Bool = IndexQuery.Bool(clauses.toList())
}

inline fun bool(block: BoolBuilder.() -> Unit): IndexQuery.Bool = BoolBuilder().apply(block).build()
