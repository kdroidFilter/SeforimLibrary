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

    fun addToc(
        tocId: Long,
        bookId: Long,
        categoryId: Long,
        bookTitle: String,
        text: String,
        level: Int
    )

    fun commit()
}
