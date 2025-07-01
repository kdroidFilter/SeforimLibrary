package io.github.kdroidfilter.seforimlibrary.dao.repository



import app.cash.sqldelight.db.SqlDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.SearchResult
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.extensions.toModel
import io.github.kdroidfilter.seforimlibrary.dao.extensions.toSearchResult
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString

class SeforimRepository(databasePath: String, private val driver: SqlDriver) {
    private val database = SeforimDb(driver)
    private val json = Json { ignoreUnknownKeys = true }

    init {
        println("DEBUG: Initializing SeforimRepository")
        // Create the database schema if it doesn't exist
        SeforimDb.Schema.create(driver)
        // Optimisations SQLite
        driver.execute(null, "PRAGMA journal_mode=WAL", 0)
        driver.execute(null, "PRAGMA synchronous=NORMAL", 0)
        driver.execute(null, "PRAGMA cache_size=10000", 0)
        driver.execute(null, "PRAGMA temp_store=MEMORY", 0)

        // Check if the database is empty
        try {
            val bookCount = database.bookQueriesQueries.countAll().executeAsOne()
            println("DEBUG: Database contains $bookCount books")
        } catch (e: Exception) {
            println("DEBUG: Error counting books: ${e.message}")
        }
    }

    // --- Categories ---

    suspend fun getCategory(id: Long): Category? = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel()
    }

    suspend fun getRootCategories(): List<Category> = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectRoot().executeAsList().map { it.toModel() }
    }

    suspend fun getCategoryChildren(parentId: Long): List<Category> = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectByParentId(parentId).executeAsList().map { it.toModel() }
    }

    suspend fun insertCategory(category: Category): Long = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.insert(
            parentId = category.parentId,
            title = category.title,
            description = category.description,
            shortDescription = category.shortDescription,
            orderIndex = category.order.toLong(),
            path = category.path,
            level = category.level.toLong(),
            createdAt = category.createdAt
        )
        database.categoryQueriesQueries.lastInsertRowId().executeAsOne()
    }

    // --- Books ---

    suspend fun getBook(id: Long): Book? = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel(json)
    }

    suspend fun getBooksByCategory(categoryId: Long): List<Book> = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.selectByCategoryId(categoryId).executeAsList()
            .map { it.toModel(json) }
    }



    suspend fun searchBooksByAuthor(author: String): List<Book> = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.selectByAuthor("%$author%").executeAsList()
            .map { it.toModel(json) }
    }

    suspend fun getBookByTitle(title: String): Book? = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.selectByTitle(title).executeAsOneOrNull()?.toModel(json)
    }

    suspend fun insertBook(book: Book): Long = withContext(Dispatchers.IO) {
        println("DEBUG: Repository inserting book '${book.title}' with ID: ${book.id}")

        // Use the ID from the book object if it's greater than 0
        if (book.id > 0) {
            database.bookQueriesQueries.insertWithId(
                id = book.id,
                categoryId = book.categoryId,
                title = book.title,
                extraTitles = json.encodeToString(book.extraTitles),
                author = book.author,
                heShortDesc = book.heShortDesc,
                pubDate = book.pubDate,
                pubPlace = book.pubPlace,
                orderIndex = book.order.toLong(),
                topics = book.topics,
                path = book.path,
                bookType = book.bookType.name,
                totalLines = book.totalLines.toLong(),
                createdAt = book.createdAt
            )
            println("DEBUG: Used insertWithId for book '${book.title}' with ID: ${book.id}")
            return@withContext book.id
        } else {
            // Fall back to auto-generated ID if book.id is 0
            database.bookQueriesQueries.insert(
                categoryId = book.categoryId,
                title = book.title,
                extraTitles = json.encodeToString(book.extraTitles),
                author = book.author,
                heShortDesc = book.heShortDesc,
                pubDate = book.pubDate,
                pubPlace = book.pubPlace,
                orderIndex = book.order.toLong(),
                topics = book.topics,
                path = book.path,
                bookType = book.bookType.name,
                totalLines = book.totalLines.toLong(),
                createdAt = book.createdAt
            )
            val id = database.bookQueriesQueries.lastInsertRowId().executeAsOne()
            println("DEBUG: Used insert for book '${book.title}', got ID: $id")
            return@withContext id
        }
    }

    suspend fun updateBookTotalLines(bookId: Long, totalLines: Int) = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.updateTotalLines(totalLines.toLong(), bookId)
    }

    // --- Lines ---

    suspend fun getLine(id: Long): Line? = withContext(Dispatchers.IO) {
        database.lineQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel()
    }

    suspend fun getLineByIndex(bookId: Long, lineIndex: Int): Line? = withContext(Dispatchers.IO) {
        database.lineQueriesQueries.selectByBookIdAndIndex(bookId, lineIndex.toLong())
            .executeAsOneOrNull()?.toModel()
    }

    suspend fun getLines(bookId: Long, startIndex: Int, endIndex: Int): List<Line> =
        withContext(Dispatchers.IO) {
            database.lineQueriesQueries.selectByBookIdRange(
                bookId = bookId,
                lineIndex = startIndex.toLong(),
                lineIndex_ = endIndex.toLong()
            ).executeAsList().map { it.toModel() }
        }

    suspend fun insertLine(line: Line): Long = withContext(Dispatchers.IO) {
        println("DEBUG: Repository inserting line with bookId: ${line.bookId}")
        database.lineQueriesQueries.insert(
            bookId = line.bookId,
            lineIndex = line.lineIndex.toLong(),
            content = line.content,
            plainText = line.plainText,
            tocEntryId = null
        )
        val lineId = database.lineQueriesQueries.lastInsertRowId().executeAsOne()
        println("DEBUG: Repository inserted line with ID: $lineId and bookId: ${line.bookId}")
        lineId
    }

    suspend fun updateLineTocEntry(lineId: Long, tocEntryId: Long) = withContext(Dispatchers.IO) {
        println("DEBUG: Repository updating line $lineId with tocEntryId: $tocEntryId")
        database.lineQueriesQueries.updateTocEntryId(tocEntryId, lineId)
        println("DEBUG: Repository updated line $lineId with tocEntryId: $tocEntryId")
    }

    // --- Table of Contents ---

    suspend fun getTocEntry(id: Long): TocEntry? = withContext(Dispatchers.IO) {
        database.tocQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel()
    }

    suspend fun getBookToc(bookId: Long): List<TocEntry> = withContext(Dispatchers.IO) {
        database.tocQueriesQueries.selectByBookId(bookId).executeAsList().map { it.toModel() }
    }

    suspend fun getBookRootToc(bookId: Long): List<TocEntry> = withContext(Dispatchers.IO) {
        database.tocQueriesQueries.selectRootByBookId(bookId).executeAsList().map { it.toModel() }
    }

    suspend fun getTocChildren(parentId: Long): List<TocEntry> = withContext(Dispatchers.IO) {
        database.tocQueriesQueries.selectChildren(parentId).executeAsList().map { it.toModel() }
    }

    suspend fun insertTocEntry(entry: TocEntry): Long = withContext(Dispatchers.IO) {
        println("DEBUG: Repository inserting TOC entry with bookId: ${entry.bookId}, lineId: ${entry.lineId}")
        database.tocQueriesQueries.insert(
            bookId = entry.bookId,
            parentId = entry.parentId,
            text = entry.text,
            level = entry.level.toLong(),
            lineId = entry.lineId,
            lineIndex = entry.lineIndex.toLong(),
            orderIndex = entry.order.toLong(),
            path = entry.path
        )
        val tocId = database.tocQueriesQueries.lastInsertRowId().executeAsOne()
        println("DEBUG: Repository inserted TOC entry with ID: $tocId, bookId: ${entry.bookId}, lineId: ${entry.lineId}")
        tocId
    }

    // --- Links ---

    suspend fun getLink(id: Long): Link? = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel()
    }

    suspend fun getCommentariesForLines(
        lineIds: List<Long>,
        activeCommentatorIds: Set<Long> = emptySet()
    ): List<CommentaryWithText> = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectBySourceLineIds(lineIds).executeAsList()
            .filter { activeCommentatorIds.isEmpty() || it.targetBookId in activeCommentatorIds }
            .map {
                CommentaryWithText(
                    link = Link(
                        id = it.id,
                        sourceBookId = it.sourceBookId,
                        targetBookId = it.targetBookId,
                        heRef = it.heRef,
                        sourceLineId = it.sourceLineId,
                        targetLineId = it.targetLineId,
                        sourceLineIndex = it.sourceLineIndex.toInt(),
                        targetLineIndex = it.targetLineIndex.toInt(),
                        connectionType = ConnectionType.fromString(it.connectionType)
                    ),
                    targetBookTitle = it.targetBookTitle,
                    targetText = it.targetText
                )
            }
    }

    suspend fun getAvailableCommentators(bookId: Long): List<CommentatorInfo> =
        withContext(Dispatchers.IO) {
            database.linkQueriesQueries.selectCommentatorsByBook(bookId).executeAsList()
                .map {
                    CommentatorInfo(
                        bookId = it.targetBookId,
                        title = it.targetBookTitle,
                        author = it.author,
                        linkCount = it.linkCount.toInt()
                    )
                }
        }

    suspend fun insertLink(link: Link): Long = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.insert(
            sourceBookId = link.sourceBookId,
            targetBookId = link.targetBookId,
            heRef = link.heRef,
            sourceLineId = link.sourceLineId,
            targetLineId = link.targetLineId,
            sourceLineIndex = link.sourceLineIndex.toLong(),
            targetLineIndex = link.targetLineIndex.toLong(),
            connectionType = link.connectionType.name
        )
        database.linkQueriesQueries.lastInsertRowId().executeAsOne()
    }

    // --- Search ---

    suspend fun search(
        query: String,
        limit: Int = 20,
        offset: Int = 0
    ): List<SearchResult> = withContext(Dispatchers.IO) {
        val ftsQuery = prepareFtsQuery(query)
        database.searchQueriesQueries.searchAll(ftsQuery, limit.toLong(), offset.toLong())
            .executeAsList()
            .map { it.toSearchResult() }
    }

    suspend fun searchInBook(
        bookId: Long,
        query: String,
        limit: Int = 20,
        offset: Int = 0
    ): List<SearchResult> = withContext(Dispatchers.IO) {
        val ftsQuery = prepareFtsQuery(query)
        database.searchQueriesQueries.searchInBook(
            ftsQuery, bookId, limit.toLong(), offset.toLong()
        ).executeAsList().map { it.toSearchResult() }
    }

    suspend fun searchByAuthor(
        author: String,
        query: String,
        limit: Int = 20,
        offset: Int = 0
    ): List<SearchResult> = withContext(Dispatchers.IO) {
        val ftsQuery = prepareFtsQuery(query)
        database.searchQueriesQueries.searchByAuthor(
            ftsQuery, author, limit.toLong(), offset.toLong()
        ).executeAsList().map { it.toSearchResult() }
    }

    // --- Helpers ---

    private fun prepareFtsQuery(query: String): String {
        return query.trim()
            .split("\\s+".toRegex())
            .filter { it.isNotBlank() }
            .joinToString(" ") { "\"$it\"*" }
    }

    fun close() {
        driver.close()
    }
}

// Data classes pour résultats enrichis
data class CommentatorInfo(
    val bookId: Long,
    val title: String,
    val author: String?,
    val linkCount: Int
)

data class CommentaryWithText(
    val link: Link,
    val targetBookTitle: String,
    val targetText: String
)
