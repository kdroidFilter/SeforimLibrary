package io.github.kdroidfilter.seforimlibrary.generator.lucene

/**
 * Platform-agnostic interface for text indexing used by the generator.
 * The JVM implementation uses Apache Lucene. Other platforms may provide
 * a no-op or alternate implementation.
 */
interface TextIndexWriter : AutoCloseable {
    /**
     * Index a single content line document.
     * @param bookId The book id
     * @param bookTitle The book title (for display)
     * @param categoryId The category id of the book
     * @param lineId The line id
     * @param lineIndex The 0-based line index within the book
     * @param normalizedText Normalized text to index in the primary field (typically StandardAnalyzer)
     * @param rawPlainText Optional raw plain text (stored) for snippet generation
     * @param normalizedTextHebrew Optional normalized text for a HebMorph-targeted field
     */
    fun addLine(
        bookId: Long,
        bookTitle: String,
        categoryId: Long,
        lineId: Long,
        lineIndex: Int,
        normalizedText: String,
        rawPlainText: String? = null,
        normalizedTextHebrew: String? = null
    )

    /**
     * Index a term for book title suggestions (title and acronyms).
     * Caller passes already-normalized term.
     */
    fun addBookTitleTerm(
        bookId: Long,
        categoryId: Long,
        displayTitle: String,
        term: String
    )

    /**
     * Flush and commit pending writes.
     */
    fun commit()

    override fun close()
}
