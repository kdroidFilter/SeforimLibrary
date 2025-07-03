package io.github.kdroidfilter.seforimlibrary.dao.repository



import app.cash.sqldelight.db.SqlDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.extensions.toModel
import io.github.kdroidfilter.seforimlibrary.dao.extensions.toSearchResult
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json

class SeforimRepository(databasePath: String, private val driver: SqlDriver) {
    private val database = SeforimDb(driver)
    private val json = Json { ignoreUnknownKeys = true }
    private val logger = Logger.withTag("SeforimRepository")

    init {

        logger.d{"Initializing SeforimRepository"}
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
            logger.d{"Database contains $bookCount books"}
        } catch (e: Exception) {
            logger.d{"Error counting books: ${e.message}"}
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
        logger.d { "🔧 Repository: Attempting to insert category '${category.title}'" }
        logger.d { "🔧 Category details: parentId=${category.parentId}, path='${category.path}', level=${category.level}" }

        try {
            // Vérifier s'il y a déjà une catégorie avec ce path
            val existingCategory = database.categoryQueriesQueries.selectByPath(category.path).executeAsOneOrNull()
            if (existingCategory != null) {
                logger.w { "⚠️ Category with path '${category.path}' already exists with ID: ${existingCategory.id}" }
                return@withContext existingCategory.id
            }

            // Essayer l'insertion
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

            val insertedId = database.categoryQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d { "✅ Repository: Category inserted with ID: $insertedId" }

            if (insertedId == 0L) {
                logger.e { "❌ Repository: lastInsertRowId() returned 0! This indicates insertion failed." }

                // Diagnostiquer le problème
                val categoryCount = database.categoryQueriesQueries.countAll().executeAsOne()
                logger.d { "📊 Total categories in database: $categoryCount" }

                // Vérifier si la catégorie existe maintenant (peut-être un conflit résolu)
                val retryCategory = database.categoryQueriesQueries.selectByPath(category.path).executeAsOneOrNull()
                if (retryCategory != null) {
                    logger.w { "🔄 Category found after failed insertion, returning existing ID: ${retryCategory.id}" }
                    return@withContext retryCategory.id
                }

                throw RuntimeException("Failed to insert category '${category.title}' - insertion returned ID 0")
            }

            return@withContext insertedId

        } catch (e: Exception) {
            logger.e(e) { "❌ Repository: Error inserting category '${category.title}': ${e.message}" }

            // En cas d'erreur, vérifier si la catégorie existe quand même
            val existingCategory = database.categoryQueriesQueries.selectByPath(category.path).executeAsOneOrNull()
            if (existingCategory != null) {
                logger.w { "🔄 Category exists after error, returning existing ID: ${existingCategory.id}" }
                return@withContext existingCategory.id
            }

            throw e
        }
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
        logger.d{"Repository inserting book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}"}

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
            logger.d{"Used insertWithId for book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}"}

            // ✅ Vérifier que l'insertion s'est bien passée
            val insertedBook = database.bookQueriesQueries.selectById(book.id).executeAsOneOrNull()
            if (insertedBook?.categoryId != book.categoryId) {
                logger.e{"ERROR: Book inserted with wrong categoryId! Expected: ${book.categoryId}, Got: ${insertedBook?.categoryId}"}
                // Corriger immédiatement
                database.bookQueriesQueries.updateCategoryId(book.categoryId, book.id)
                logger.d{"Corrected categoryId for book ID: ${book.id}"}
            }

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
            logger.d{"Used insert for book '${book.title}', got ID: $id with categoryId: ${book.categoryId}"}
            return@withContext id
        }
    }

    suspend fun updateBookTotalLines(bookId: Long, totalLines: Int) = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.updateTotalLines(totalLines.toLong(), bookId)
    }

    suspend fun updateBookCategoryId(bookId: Long, categoryId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Updating book $bookId with categoryId: $categoryId"}
        database.bookQueriesQueries.updateCategoryId(categoryId, bookId)
        logger.d{"Updated book $bookId with categoryId: $categoryId"}
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
        logger.d{"Repository inserting line with bookId: ${line.bookId}"}

        // Use the ID from the line object if it's greater than 0
        if (line.id > 0) {
            database.lineQueriesQueries.insertWithId(
                id = line.id,
                bookId = line.bookId,
                lineIndex = line.lineIndex.toLong(),
                content = line.content,
                plainText = line.plainText,
                tocEntryId = null
            )
            logger.d{"Repository inserted line with explicit ID: ${line.id} and bookId: ${line.bookId}"}
            return@withContext line.id
        } else {
            // Fall back to auto-generated ID if line.id is 0
            database.lineQueriesQueries.insert(
                bookId = line.bookId,
                lineIndex = line.lineIndex.toLong(),
                content = line.content,
                plainText = line.plainText,
                tocEntryId = null
            )
            val lineId = database.lineQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"Repository inserted line with auto-generated ID: $lineId and bookId: ${line.bookId}"}
            return@withContext lineId
        }
    }

    suspend fun updateLineTocEntry(lineId: Long, tocEntryId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Repository updating line $lineId with tocEntryId: $tocEntryId"}
        database.lineQueriesQueries.updateTocEntryId(tocEntryId, lineId)
        logger.d{"Repository updated line $lineId with tocEntryId: $tocEntryId"}
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
        logger.d{"Repository inserting TOC entry with bookId: ${entry.bookId}, lineId: ${entry.lineId}"}

        // Use the ID from the entry object if it's greater than 0
        if (entry.id > 0) {
            database.tocQueriesQueries.insertWithId(
                id = entry.id,
                bookId = entry.bookId,
                parentId = entry.parentId,
                text = entry.text,
                level = entry.level.toLong(),
                lineId = entry.lineId,
                lineIndex = entry.lineIndex.toLong(),
                orderIndex = entry.order.toLong(),
                path = entry.path
            )
            logger.d{"Repository inserted TOC entry with explicit ID: ${entry.id}, bookId: ${entry.bookId}, lineId: ${entry.lineId}"}
            return@withContext entry.id
        } else {
            // Fall back to auto-generated ID if entry.id is 0
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
            logger.d{"Repository inserted TOC entry with auto-generated ID: $tocId, bookId: ${entry.bookId}, lineId: ${entry.lineId}"}
            return@withContext tocId
        }
    }

    suspend fun updateTocEntryLineId(tocEntryId: Long, lineId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Repository updating TOC entry $tocEntryId with lineId: $lineId"}
        database.tocQueriesQueries.updateLineId(lineId, tocEntryId)
        logger.d{"Repository updated TOC entry $tocEntryId with lineId: $lineId"}
    }

    // --- Links ---

    suspend fun getLink(id: Long): Link? = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel()
    }

    suspend fun countLinks(): Long = withContext(Dispatchers.IO) {
        logger.d{"Counting links in database"}
        val count = database.linkQueriesQueries.countAll().executeAsOne()
        logger.d{"Found $count links in database"}
        count
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
        logger.d{"Repository inserting link from book ${link.sourceBookId} to book ${link.targetBookId}"}
        logger.d{"Link details - sourceLineId: ${link.sourceLineId}, targetLineId: ${link.targetLineId}"}
        logger.d{"Link details - sourceLineIndex: ${link.sourceLineIndex}, targetLineIndex: ${link.targetLineIndex}"}

        try {
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
            val linkId = database.linkQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"Repository inserted link with ID: $linkId"}
            return@withContext linkId
        } catch (e: Exception) {
            logger.e(e){"Error inserting link: ${e.message}"}
            throw e
        }
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
