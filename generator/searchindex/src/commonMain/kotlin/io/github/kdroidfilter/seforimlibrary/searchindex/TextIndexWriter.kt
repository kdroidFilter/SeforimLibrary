package io.github.kdroidfilter.seforimlibrary.searchindex

/**
 * Platform-agnostic interface for text indexing.
 *
 * The JVM implementation uses Apache Lucene. Other platforms may provide a no-op
 * or alternate implementation.
 */
interface TextIndexWriter : AutoCloseable {
    /**
     * Index a single content line document.
     * @param bookId The book id
     * @param bookTitle The book title (for display)
     * @param categoryId The category id of the book
     * @param ancestorCategoryIds List of ancestor category IDs (including categoryId itself) for filtering
     * @param lineId The line id
     * @param lineIndex The 0-based line index within the book
     * @param normalizedText Normalized text to index in the primary field (typically StandardAnalyzer)
     * @param rawPlainText Optional raw plain text (stored) for snippet generation
     * @param normalizedTextHebrew Optional normalized text for a secondary text field
     * @param orderIndex The order index of the book (for ranking basebooks)
     * @param isBaseBook Whether this book is marked as a base book
     */
    fun addLine(
        bookId: Long,
        bookTitle: String,
        categoryId: Long,
        ancestorCategoryIds: List<Long> = emptyList(),
        lineId: Long,
        lineIndex: Int,
        normalizedText: String,
        rawPlainText: String? = null,
        normalizedTextHebrew: String? = null,
        orderIndex: Int = 999,
        isBaseBook: Boolean = false
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
