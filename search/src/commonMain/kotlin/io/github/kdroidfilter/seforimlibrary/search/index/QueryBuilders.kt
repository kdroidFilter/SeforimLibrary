package io.github.kdroidfilter.seforimlibrary.search.index

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
