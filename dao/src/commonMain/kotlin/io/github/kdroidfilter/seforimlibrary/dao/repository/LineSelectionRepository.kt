package io.github.kdroidfilter.seforimlibrary.dao.repository

import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry

/**
 * Interface for line selection and navigation related repository operations.
 * This interface is extracted to allow mocking in tests.
 */
interface LineSelectionRepository {
    /**
     * Returns the TOC entry whose heading line is the given line id, or null if not a TOC heading.
     */
    suspend fun getHeadingTocEntryByLineId(lineId: Long): TocEntry?

    /**
     * Returns all line ids that belong to the given TOC entry (section), ordered by lineIndex.
     */
    suspend fun getLineIdsForTocEntry(tocEntryId: Long): List<Long>

    /**
     * Returns the TOC entry ID for a given line, or null if the line has no TOC mapping.
     */
    suspend fun getTocEntryIdForLine(lineId: Long): Long?

    /**
     * Returns a TOC entry by its ID.
     */
    suspend fun getTocEntry(id: Long): TocEntry?

    /**
     * Returns a line by its ID.
     */
    suspend fun getLine(id: Long): Line?

    /**
     * Returns the previous line in the book, or null if at the beginning.
     */
    suspend fun getPreviousLine(bookId: Long, currentLineIndex: Int): Line?

    /**
     * Returns the next line in the book, or null if at the end.
     */
    suspend fun getNextLine(bookId: Long, currentLineIndex: Int): Line?

    /**
     * Returns lines in a range for a book.
     */
    suspend fun getLines(bookId: Long, startIndex: Int, endIndex: Int): List<Line>
}
