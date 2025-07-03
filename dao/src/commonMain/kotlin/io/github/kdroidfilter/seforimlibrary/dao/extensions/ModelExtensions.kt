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

private val logger = Logger.withTag("ModelExtensions")

fun io.github.kdroidfilter.seforimlibrary.db.Author.toModel(): Author {
    return Author(
        id = id,
        name = name
    )
}

fun io.github.kdroidfilter.seforimlibrary.db.Pub_place.toModel(): PubPlace {
    return PubPlace(
        id = id,
        name = name
    )
}

fun io.github.kdroidfilter.seforimlibrary.db.Pub_date.toModel(): PubDate {
    return PubDate(
        id = id,
        date = date
    )
}

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
        bookType = io.github.kdroidfilter.seforimlibrary.core.models.BookType.valueOf(bookType),
        totalLines = totalLines.toInt()
    )
}

fun io.github.kdroidfilter.seforimlibrary.db.Category.toModel(): Category {
    return Category(
        id = id,
        parentId = parentId,
        title = title,
        level = level.toInt()
    )
}

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

fun io.github.kdroidfilter.seforimlibrary.db.SelectById.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectById) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId,
        lineIndex = lineIndex.toInt(),
        order = orderIndex.toInt(),
        path = path
    )
}

fun io.github.kdroidfilter.seforimlibrary.db.SelectByBookId.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectByBookId) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId,
        lineIndex = lineIndex.toInt(),
        order = orderIndex.toInt(),
        path = path
    )
}

fun io.github.kdroidfilter.seforimlibrary.db.SelectRootByBookId.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectRootByBookId) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId,
        lineIndex = lineIndex.toInt(),
        order = orderIndex.toInt(),
        path = path
    )
}

fun io.github.kdroidfilter.seforimlibrary.db.SelectChildren.toModel(): TocEntry {
    logger.d{"Converting database TocEntry (from SelectChildren) with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
        textId = textId,
        text = text,
        level = level.toInt(),
        lineId = lineId,
        lineIndex = lineIndex.toInt(),
        order = orderIndex.toInt(),
        path = path
    )
}

fun io.github.kdroidfilter.seforimlibrary.db.Link.toModel(): Link {
    return Link(
        id = id,
        sourceBookId = sourceBookId,
        targetBookId = targetBookId,
        heRef = heRef,
        sourceLineId = sourceLineId,
        targetLineId = targetLineId,
        sourceLineIndex = sourceLineIndex.toInt(),
        targetLineIndex = targetLineIndex.toInt(),
        connectionType = ConnectionType.fromString(connectionType)
    )
}


fun SearchAll.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId,
    bookTitle = bookTitle,
    lineId = lineId,
    lineIndex = lineIndex.toInt(),
    snippet = snippet,
    rank = rank.toDouble()
)

fun SearchInBook.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId,
    bookTitle = bookTitle,
    lineId = lineId,
    lineIndex = lineIndex.toInt(),
    snippet = snippet,
    rank = rank.toDouble()
)

fun SearchByAuthor.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId,
    bookTitle = bookTitle,
    lineId = lineId,
    lineIndex = lineIndex.toInt(),
    snippet = snippet,
    rank = rank.toDouble()
)

fun SearchWithBookFilter.toSearchResult(): SearchResult = SearchResult(
    bookId = bookId,
    bookTitle = bookTitle,
    lineId = lineId,
    lineIndex = lineIndex.toInt(),
    snippet = snippet,
    rank = rank.toDouble()
)
