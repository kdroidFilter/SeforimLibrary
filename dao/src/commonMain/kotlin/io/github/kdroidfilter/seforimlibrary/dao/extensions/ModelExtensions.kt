package io.github.kdroidfilter.seforimlibrary.dao.extensions

import io.github.kdroidfilter.seforimlibrary.core.models.Author
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import io.github.kdroidfilter.seforimlibrary.core.models.PubPlace
import io.github.kdroidfilter.seforimlibrary.core.models.SearchResult
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.Topic
import io.github.kdroidfilter.seforimlibrary.db.SearchAll
import io.github.kdroidfilter.seforimlibrary.db.SearchByAuthor
import io.github.kdroidfilter.seforimlibrary.db.SearchInBook
import io.github.kdroidfilter.seforimlibrary.db.SearchWithBookFilter
import kotlinx.serialization.json.Json
import kotlinx.serialization.decodeFromString
import co.touchlab.kermit.Logger

/**
 * This file contains extension functions to convert database entities to domain models.
 * These functions facilitate the mapping between the database layer and the domain layer.
 */

private val logger = Logger.withTag("ModelExtensions")

/**
 * Converts a database Author entity to a domain Author model.
 *
 * @return The domain Author model
 */
fun io.github.kdroidfilter.seforimlibrary.db.Author.toModel(): Author {
    return Author(
        id = id,
        name = name
    )
}

/**
 * Converts a database Pub_place entity to a domain PubPlace model.
 *
 * @return The domain PubPlace model
 */
fun io.github.kdroidfilter.seforimlibrary.db.Pub_place.toModel(): PubPlace {
    return PubPlace(
        id = id,
        name = name
    )
}

/**
 * Converts a database Pub_date entity to a domain PubDate model.
 *
 * @return The domain PubDate model
 */
fun io.github.kdroidfilter.seforimlibrary.db.Pub_date.toModel(): PubDate {
    return PubDate(
        id = id,
        date = date
    )
}

/**
 * Converts a database Book entity to a domain Book model.
 *
 * @param json The JSON parser for deserialization
 * @param authors The list of authors associated with the book
 * @param pubPlaces The list of publication places associated with the book
 * @param pubDates The list of publication dates associated with the book
 * @return The domain Book model
 */
fun io.github.kdroidfilter.seforimlibrary.db.Book.toModel(json: Json, authors: List<Author> = emptyList(), pubPlaces: List<PubPlace> = emptyList(), pubDates: List<PubDate> = emptyList()): Book {
    return Book(
        id = id,
        categoryId = categoryId,
        title = title,
        authors = authors,
        topics = emptyList(),
        pubPlaces = pubPlaces,
        pubDates = pubDates,
        heShortDesc = heShortDesc,
        order = orderIndex.toFloat(),
        totalLines = totalLines.toInt()
    )
}

/**
 * Converts a database Category entity to a domain Category model.
 *
 * @return The domain Category model
 */
fun io.github.kdroidfilter.seforimlibrary.db.Category.toModel(): Category {
    return Category(
        id = id,
        parentId = parentId,
        title = title,
        level = level.toInt()
    )
}

/**
 * Converts a database Line entity to a domain Line model.
 *
 * @return The domain Line model
 */
fun io.github.kdroidfilter.seforimlibrary.db.Line.toModel(): Line {
    logger.d{"Converting database Line to model with id: $id, bookId: $bookId, tocEntryId: $tocEntryId"}
    return Line(
        id = id,
        bookId = bookId,
        lineIndex = lineIndex.toInt(),
        content = content,
        plainText = plainText
    )
}

/**
 * Converts a database SelectTocById result to a domain TocEntry model.
 * This is used when retrieving a TOC entry by its ID.
 *
 * @return The domain TocEntry model
 */
fun io.github.kdroidfilter.seforimlibrary.db.SelectTocById.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectTocById) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId
    )
}

/**
 * Converts a database SelectByBookId result to a domain TocEntry model.
 * This is used when retrieving TOC entries by book ID.
 *
 * @return The domain TocEntry model
 */
fun io.github.kdroidfilter.seforimlibrary.db.SelectByBookId.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectByBookId) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId
    )
}

/**
 * Converts a database SelectRootByBookId result to a domain TocEntry model.
 * This is used when retrieving root TOC entries for a book.
 *
 * @return The domain TocEntry model
 */
fun io.github.kdroidfilter.seforimlibrary.db.SelectRootByBookId.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectRootByBookId) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId
    )
}

/**
 * Converts a database SelectChildren result to a domain TocEntry model.
 * This is used when retrieving child TOC entries for a parent TOC entry.
 *
 * @return The domain TocEntry model
 */
fun io.github.kdroidfilter.seforimlibrary.db.SelectChildren.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectChildren) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId
    )
}

/**
 * Converts a database Connection_type entity to a domain ConnectionType enum.
 *
 * @return The domain ConnectionType enum
 */
fun io.github.kdroidfilter.seforimlibrary.db.Connection_type.toModel(): ConnectionType {
    return ConnectionType.fromString(name)
}

/**
 * Converts a database SelectLinkById result to a domain Link model.
 *
 * @return The domain Link model
 */
fun io.github.kdroidfilter.seforimlibrary.db.SelectLinkById.toModel(): Link {
    return Link(
        id = id,
        sourceBookId = sourceBookId,
        targetBookId = targetBookId,
        sourceLineId = sourceLineId,
        targetLineId = targetLineId,
        connectionType = ConnectionType.fromString(connectionType)
    )
}

/**
 * Converts a database SelectLinksBySourceLineIds result to a domain Link model.
 *
 * @return The domain Link model
 */
fun io.github.kdroidfilter.seforimlibrary.db.SelectLinksBySourceLineIds.toModel(): Link {
    return Link(
        id = id,
        sourceBookId = sourceBookId,
        targetBookId = targetBookId,
        sourceLineId = sourceLineId,
        targetLineId = targetLineId,
        connectionType = ConnectionType.fromString(connectionType)
    )
}

/**
 * Converts a database SelectLinksBySourceBook result to a domain Link model.
 *
 * @return The domain Link model
 */
fun io.github.kdroidfilter.seforimlibrary.db.SelectLinksBySourceBook.toModel(): Link {
    return Link(
        id = id,
        sourceBookId = sourceBookId,
        targetBookId = targetBookId,
        sourceLineId = sourceLineId,
        targetLineId = targetLineId,
        connectionType = ConnectionType.fromString(connectionType)
    )
}


/**
 * Converts a SearchAll database result to a domain SearchResult model.
 * This is used for global search across all books.
 *
 * @return The domain SearchResult model
 */
fun SearchAll.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId ?: 0,
    bookTitle = bookTitle ?: "",
    lineId = lineId ?: 0,
    lineIndex = lineIndex?.toInt() ?: 0,
    snippet = snippet ?: "",
    rank = rank
)

/**
 * Converts a SearchInBook database result to a domain SearchResult model.
 * This is used for searching within a specific book.
 *
 * @return The domain SearchResult model
 */
fun SearchInBook.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId ?: 0,
    bookTitle = bookTitle ?: "",
    lineId = lineId ?: 0,
    lineIndex = lineIndex?.toInt() ?: 0,
    snippet = snippet ?: "",
    rank = rank
)

/**
 * Converts a SearchByAuthor database result to a domain SearchResult model.
 * This is used for searching books by a specific author.
 *
 * @return The domain SearchResult model
 */
fun SearchByAuthor.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId ?: 0,
    bookTitle = bookTitle ?: "",
    lineId = lineId ?: 0,
    lineIndex = lineIndex?.toInt() ?: 0,
    snippet = snippet ?: "",
    rank = rank
)

/**
 * Converts a SearchWithBookFilter database result to a domain SearchResult model.
 * This is used for searching with specific book filters applied.
 *
 * @return The domain SearchResult model
 */
fun SearchWithBookFilter.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId ?: 0,
    bookTitle = bookTitle ?: "",
    lineId = lineId ?: 0,
    lineIndex = lineIndex?.toInt() ?: 0,
    snippet = snippet ?: "",
    rank = rank
)
