package io.github.kdroidfilter.seforimlibrary.dao.extensions



import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.SearchResult
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.db.SearchAll
import io.github.kdroidfilter.seforimlibrary.db.SearchByAuthor
import io.github.kdroidfilter.seforimlibrary.db.SearchInBook
import io.github.kdroidfilter.seforimlibrary.db.SearchWithBookFilter
import kotlinx.serialization.json.Json
import kotlinx.serialization.decodeFromString
import co.touchlab.kermit.Logger

private val logger = Logger.withTag("ModelExtensions")

fun io.github.kdroidfilter.seforimlibrary.db.Book.toModel(json: Json): Book {
    return Book(
        id = id,
        categoryId = categoryId,
        title = title,
        extraTitles = json.decodeFromString<List<String>>(extraTitles),
        author = author,
        heShortDesc = heShortDesc,
        pubDate = pubDate,
        pubPlace = pubPlace,
        order = orderIndex.toFloat(),
        topics = topics,
        path = path,
        bookType = io.github.kdroidfilter.seforimlibrary.core.models.BookType.valueOf(bookType),
        totalLines = totalLines.toInt(),
        createdAt = createdAt
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

fun io.github.kdroidfilter.seforimlibrary.db.TocEntry.toModel(): TocEntry {
    logger.d{"Converting database TocEntry to model with id: $id, bookId: $bookId, lineId: $lineId"}
    return TocEntry(
        id = id,
        bookId = bookId,
        parentId = parentId,
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
