package io.github.kdroidfilter.seforimlibrary.searchindex

interface LookupIndexWriter : AutoCloseable {
    fun addBook(
        bookId: Long,
        categoryId: Long,
        displayTitle: String,
        terms: Collection<String>,
        isBaseBook: Boolean = false,
        orderIndex: Int? = null
    )

    fun addAuthor(
        authorId: Long,
        name: String,
        terms: Collection<String>,
        bookCount: Int,
    )

    fun commit()
}
