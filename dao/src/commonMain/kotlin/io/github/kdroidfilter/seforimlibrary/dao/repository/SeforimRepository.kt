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
        logger.d { "🔧 Category details: parentId=${category.parentId}, level=${category.level}" }

        try {
            // Check if a category with the same title already exists
            val existingCategories = database.categoryQueriesQueries.selectAll().executeAsList()
            val existingCategory = existingCategories.find { it.title == category.title }

            if (existingCategory != null) {
                logger.d { "⚠️ Category with title '${category.title}' already exists with ID: ${existingCategory.id}" }
                return@withContext existingCategory.id
            }

            // Essayer l'insertion
            database.categoryQueriesQueries.insert(
                parentId = category.parentId,
                title = category.title,
                level = category.level.toLong()
            )

            val insertedId = database.categoryQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d { "✅ Repository: Category inserted with ID: $insertedId" }

            if (insertedId == 0L) {
                logger.e { "❌ Repository: lastInsertRowId() returned 0! This indicates insertion failed." }

                // Diagnostiquer le problème
                val categoryCount = database.categoryQueriesQueries.countAll().executeAsOne()
                logger.d { "📊 Total categories in database: $categoryCount" }

                // Check again if the category was inserted despite lastInsertRowId() returning 0
                val updatedCategories = database.categoryQueriesQueries.selectAll().executeAsList()
                val newCategory = updatedCategories.find { it.title == category.title }

                if (newCategory != null) {
                    logger.d { "🔄 Category found after failed insertion, returning existing ID: ${newCategory.id}" }
                    return@withContext newCategory.id
                }

                throw RuntimeException("Failed to insert category '${category.title}' - insertion returned ID 0")
            }

            return@withContext insertedId

        } catch (e: Exception) {
            logger.e(e) { "❌ Repository: Error inserting category '${category.title}': ${e.message}" }

            // In case of error, check if the category exists anyway
            val categories = database.categoryQueriesQueries.selectAll().executeAsList()
            val existingCategory = categories.find { it.title == category.title }

            if (existingCategory != null) {
                logger.d { "🔄 Category exists after error, returning existing ID: ${existingCategory.id}" }
                return@withContext existingCategory.id
            }

            throw e
        }
    }

    // --- Books ---

    suspend fun getBook(id: Long): Book? = withContext(Dispatchers.IO) {
        val bookData = database.bookQueriesQueries.selectById(id).executeAsOneOrNull() ?: return@withContext null
        val authors = getBookAuthors(bookData.id)
        val topics = getBookTopics(bookData.id)
        val pubPlaces = getBookPubPlaces(bookData.id)
        return@withContext bookData.toModel(json, authors, pubPlaces).copy(topics = topics)
    }

    suspend fun getBooksByCategory(categoryId: Long): List<Book> = withContext(Dispatchers.IO) {
        val books = database.bookQueriesQueries.selectByCategoryId(categoryId).executeAsList()
        return@withContext books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            bookData.toModel(json, authors, pubPlaces).copy(topics = topics)
        }
    }



    suspend fun searchBooksByAuthor(authorName: String): List<Book> = withContext(Dispatchers.IO) {
        val books = database.bookQueriesQueries.selectByAuthor("%$authorName%").executeAsList()
        return@withContext books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            bookData.toModel(json, authors, pubPlaces).copy(topics = topics)
        }
    }

    // Get all authors for a book
    private suspend fun getBookAuthors(bookId: Long): List<Author> = withContext(Dispatchers.IO) {
        logger.d{"Getting authors for book ID: $bookId"}
        val authors = database.authorQueriesQueries.selectByBookId(bookId).executeAsList()
        logger.d{"Found ${authors.size} authors for book ID: $bookId"}
        return@withContext authors.map { it.toModel() }
    }

    // Get all topics for a book
    private suspend fun getBookTopics(bookId: Long): List<Topic> = withContext(Dispatchers.IO) {
        logger.d{"Getting topics for book ID: $bookId"}
        val topics = database.topicQueriesQueries.selectByBookId(bookId).executeAsList()
        logger.d{"Found ${topics.size} topics for book ID: $bookId"}
        return@withContext topics.map { Topic(id = it.id, name = it.name) }
    }

    // Get all publication places for a book
    private suspend fun getBookPubPlaces(bookId: Long): List<PubPlace> = withContext(Dispatchers.IO) {
        logger.d{"Getting publication places for book ID: $bookId"}
        val pubPlaces = database.pubPlaceQueriesQueries.selectByBookId(bookId).executeAsList()
        logger.d{"Found ${pubPlaces.size} publication places for book ID: $bookId"}
        return@withContext pubPlaces.map { it.toModel() }
    }

    // Get an author by name, returns null if not found
    suspend fun getAuthorByName(name: String): Author? = withContext(Dispatchers.IO) {
        logger.d{"Looking for author with name: $name"}
        val author = database.authorQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (author != null) {
            logger.d{"Found author with ID: ${author.id}"}
        } else {
            logger.d{"Author not found: $name"}
        }
        return@withContext author?.toModel()
    }

    // Insert an author and return its ID
    suspend fun insertAuthor(name: String): Long = withContext(Dispatchers.IO) {
        logger.d{"Inserting author: $name"}

        // Check if author already exists
        val existingAuthor = database.authorQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (existingAuthor != null) {
            logger.d{"Author already exists with ID: ${existingAuthor.id}"}
            return@withContext existingAuthor.id
        }

        // Insert the author
        database.authorQueriesQueries.insert(name)

        // Get the ID of the inserted author
        val authorId = database.authorQueriesQueries.lastInsertRowId().executeAsOne()

        // If lastInsertRowId returns 0, try to get the ID by name
        if (authorId == 0L) {
            logger.w{"lastInsertRowId() returned 0 for author insertion, trying to get ID by name"}
            val insertedAuthor = database.authorQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (insertedAuthor != null) {
                logger.d{"Found author after insertion with ID: ${insertedAuthor.id}"}
                return@withContext insertedAuthor.id
            }
            throw RuntimeException("Failed to insert author '$name'")
        }

        logger.d{"Author inserted with ID: $authorId"}
        return@withContext authorId
    }

    // Link an author to a book
    suspend fun linkAuthorToBook(authorId: Long, bookId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Linking author $authorId to book $bookId"}
        database.authorQueriesQueries.linkBookAuthor(bookId, authorId)
        logger.d{"Linked author $authorId to book $bookId"}
    }

    suspend fun getBookByTitle(title: String): Book? = withContext(Dispatchers.IO) {
        val bookData = database.bookQueriesQueries.selectByTitle(title).executeAsOneOrNull() ?: return@withContext null
        val authors = getBookAuthors(bookData.id)
        val topics = getBookTopics(bookData.id)
        val pubPlaces = getBookPubPlaces(bookData.id)
        return@withContext bookData.toModel(json, authors, pubPlaces).copy(topics = topics)
    }

    // Get a topic by name, returns null if not found
    suspend fun getTopicByName(name: String): Topic? = withContext(Dispatchers.IO) {
        logger.d{"Looking for topic with name: $name"}
        val topic = database.topicQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (topic != null) {
            logger.d{"Found topic with ID: ${topic.id}"}
        } else {
            logger.d{"Topic not found: $name"}
        }
        return@withContext topic?.let { Topic(id = it.id, name = it.name) }
    }

    // Get a publication place by name, returns null if not found
    suspend fun getPubPlaceByName(name: String): PubPlace? = withContext(Dispatchers.IO) {
        logger.d{"Looking for publication place with name: $name"}
        val pubPlace = database.pubPlaceQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (pubPlace != null) {
            logger.d{"Found publication place with ID: ${pubPlace.id}"}
        } else {
            logger.d{"Publication place not found: $name"}
        }
        return@withContext pubPlace?.toModel()
    }

    // Insert a topic and return its ID
    suspend fun insertTopic(name: String): Long = withContext(Dispatchers.IO) {
        logger.d{"Inserting topic: $name"}

        // Check if topic already exists
        val existingTopic = database.topicQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (existingTopic != null) {
            logger.d{"Topic already exists with ID: ${existingTopic.id}"}
            return@withContext existingTopic.id
        }

        // Insert the topic
        database.topicQueriesQueries.insert(name)

        // Get the ID of the inserted topic
        val topicId = database.topicQueriesQueries.lastInsertRowId().executeAsOne()

        // If lastInsertRowId returns 0, try to get the ID by name
        if (topicId == 0L) {
            logger.w{"lastInsertRowId() returned 0 for topic insertion, trying to get ID by name"}
            val insertedTopic = database.topicQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (insertedTopic != null) {
                logger.d{"Found topic after insertion with ID: ${insertedTopic.id}"}
                return@withContext insertedTopic.id
            }
            throw RuntimeException("Failed to insert topic '$name'")
        }

        logger.d{"Topic inserted with ID: $topicId"}
        return@withContext topicId
    }

    // Link a topic to a book
    suspend fun linkTopicToBook(topicId: Long, bookId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Linking topic $topicId to book $bookId"}
        database.topicQueriesQueries.linkBookTopic(bookId, topicId)
        logger.d{"Linked topic $topicId to book $bookId"}
    }

    // Insert a publication place and return its ID
    suspend fun insertPubPlace(name: String): Long = withContext(Dispatchers.IO) {
        logger.d{"Inserting publication place: $name"}

        // Check if publication place already exists
        val existingPubPlace = database.pubPlaceQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (existingPubPlace != null) {
            logger.d{"Publication place already exists with ID: ${existingPubPlace.id}"}
            return@withContext existingPubPlace.id
        }

        // Insert the publication place
        database.pubPlaceQueriesQueries.insert(name)

        // Get the ID of the inserted publication place
        val pubPlaceId = database.pubPlaceQueriesQueries.lastInsertRowId().executeAsOne()

        // If lastInsertRowId returns 0, try to get the ID by name
        if (pubPlaceId == 0L) {
            logger.w{"lastInsertRowId() returned 0 for publication place insertion, trying to get ID by name"}
            val insertedPubPlace = database.pubPlaceQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (insertedPubPlace != null) {
                logger.d{"Found publication place after insertion with ID: ${insertedPubPlace.id}"}
                return@withContext insertedPubPlace.id
            }
            throw RuntimeException("Failed to insert publication place '$name'")
        }

        logger.d{"Publication place inserted with ID: $pubPlaceId"}
        return@withContext pubPlaceId
    }

    // Link a publication place to a book
    suspend fun linkPubPlaceToBook(pubPlaceId: Long, bookId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Linking publication place $pubPlaceId to book $bookId"}
        database.pubPlaceQueriesQueries.linkBookPubPlace(bookId, pubPlaceId)
        logger.d{"Linked publication place $pubPlaceId to book $bookId"}
    }

    suspend fun insertBook(book: Book): Long = withContext(Dispatchers.IO) {
        logger.d{"Repository inserting book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}"}

        // Use the ID from the book object if it's greater than 0
        if (book.id > 0) {
            database.bookQueriesQueries.insertWithId(
                id = book.id,
                categoryId = book.categoryId,
                title = book.title,
                heShortDesc = book.heShortDesc,
                pubDate = book.pubDate,
                orderIndex = book.order.toLong(),
                bookType = book.bookType.name,
                totalLines = book.totalLines.toLong()
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

            // Process authors
            for (author in book.authors) {
                val authorId = insertAuthor(author.name)
                linkAuthorToBook(authorId, book.id)
                logger.d{"Processed author '${author.name}' (ID: $authorId) for book '${book.title}' (ID: ${book.id})"}
            }

            // Process topics
            for (topic in book.topics) {
                val topicId = insertTopic(topic.name)
                linkTopicToBook(topicId, book.id)
                logger.d{"Processed topic '${topic.name}' (ID: $topicId) for book '${book.title}' (ID: ${book.id})"}
            }

            // Process publication places
            for (pubPlace in book.pubPlaces) {
                val pubPlaceId = insertPubPlace(pubPlace.name)
                linkPubPlaceToBook(pubPlaceId, book.id)
                logger.d{"Processed publication place '${pubPlace.name}' (ID: $pubPlaceId) for book '${book.title}' (ID: ${book.id})"}
            }

            return@withContext book.id
        } else {
            // Fall back to auto-generated ID if book.id is 0
            database.bookQueriesQueries.insert(
                categoryId = book.categoryId,
                title = book.title,
                heShortDesc = book.heShortDesc,
                pubDate = book.pubDate,
                orderIndex = book.order.toLong(),
                bookType = book.bookType.name,
                totalLines = book.totalLines.toLong()
            )
            val id = database.bookQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"Used insert for book '${book.title}', got ID: $id with categoryId: ${book.categoryId}"}

            // Process authors
            for (author in book.authors) {
                val authorId = insertAuthor(author.name)
                linkAuthorToBook(authorId, id)
                logger.d{"Processed author '${author.name}' (ID: $authorId) for book '${book.title}' (ID: $id)"}
            }

            // Process topics
            for (topic in book.topics) {
                val topicId = insertTopic(topic.name)
                linkTopicToBook(topicId, id)
                logger.d{"Processed topic '${topic.name}' (ID: $topicId) for book '${book.title}' (ID: $id)"}
            }

            // Process publication places
            for (pubPlace in book.pubPlaces) {
                val pubPlaceId = insertPubPlace(pubPlace.name)
                linkPubPlaceToBook(pubPlaceId, id)
                logger.d{"Processed publication place '${pubPlace.name}' (ID: $pubPlaceId) for book '${book.title}' (ID: $id)"}
            }

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

    // --- TocText methods ---

    // Get or create a tocText entry and return its ID
    private suspend fun getOrCreateTocText(text: String): Long = withContext(Dispatchers.IO) {
        logger.d{"Getting or creating tocText entry for text: $text"}

        // Check if the text already exists
        val existingId = database.tocTextQueriesQueries.selectIdByText(text).executeAsOneOrNull()
        if (existingId != null) {
            logger.d{"Found existing tocText entry with ID: $existingId"}
            return@withContext existingId
        }

        // Insert the text
        database.tocTextQueriesQueries.insertAndGetId(text)

        // Get the ID of the inserted text
        val textId = database.tocTextQueriesQueries.lastInsertRowId().executeAsOne()

        // If lastInsertRowId returns 0, try to get the ID by text
        if (textId == 0L) {
            logger.w{"lastInsertRowId() returned 0 for tocText insertion, trying to get ID by text"}
            val insertedId = database.tocTextQueriesQueries.selectIdByText(text).executeAsOneOrNull()
            if (insertedId != null) {
                logger.d{"Found tocText after insertion with ID: $insertedId"}
                return@withContext insertedId
            }
            throw RuntimeException("Failed to insert tocText '$text'")
        }

        logger.d{"Created new tocText entry with ID: $textId"}
        return@withContext textId
    }

    suspend fun insertTocEntry(entry: TocEntry): Long = withContext(Dispatchers.IO) {
        logger.d{"Repository inserting TOC entry with bookId: ${entry.bookId}, lineId: ${entry.lineId}"}

        // Get or create the tocText entry
        val textId = entry.textId ?: getOrCreateTocText(entry.text)
        logger.d{"Using tocText ID: $textId for text: ${entry.text}"}

        // Use the ID from the entry object if it's greater than 0
        if (entry.id > 0) {
            database.tocQueriesQueries.insertWithId(
                id = entry.id,
                bookId = entry.bookId,
                parentId = entry.parentId,
                textId = textId,
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
                textId = textId,
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
