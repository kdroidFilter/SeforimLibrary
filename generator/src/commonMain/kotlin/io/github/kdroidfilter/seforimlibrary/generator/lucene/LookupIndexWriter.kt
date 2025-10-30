package io.github.kdroidfilter.seforimlibrary.generator.lucene

interface LookupIndexWriter : AutoCloseable {
    fun addBook(
        bookId: Long,
        categoryId: Long,
        displayTitle: String,
        terms: Collection<String>
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

