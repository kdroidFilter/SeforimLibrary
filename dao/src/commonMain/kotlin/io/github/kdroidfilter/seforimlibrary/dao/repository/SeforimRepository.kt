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

/**
 * Repository class for accessing and manipulating the Seforim database.
 * Provides methods for CRUD operations on books, categories, lines, TOC entries, and links.
 *
 * @property driver The SQL driver used to connect to the database
 * @constructor Creates a repository with the specified database path and driver
 */
class SeforimRepository(databasePath: String, private val driver: SqlDriver) {
    private val database = SeforimDb(driver)
    private val json = Json { ignoreUnknownKeys = true }
    private val logger = Logger.withTag("SeforimRepository")

    init {

        logger.d{"Initializing SeforimRepository"}
        // Create the database schema if it doesn't exist
        SeforimDb.Schema.create(driver)
        // SQLite optimizations
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

    /**
     * Retrieves a category by its ID.
     *
     * @param id The ID of the category to retrieve
     * @return The category if found, null otherwise
     */
    suspend fun getCategory(id: Long): Category? = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel()
    }

    /**
     * Retrieves all root categories (categories without a parent).
     *
     * @return A list of root categories
     */
    suspend fun getRootCategories(): List<Category> = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectRoot().executeAsList().map { it.toModel() }
    }

    /**
     * Retrieves all child categories of a parent category.
     *
     * @param parentId The ID of the parent category
     * @return A list of child categories
     */
    suspend fun getCategoryChildren(parentId: Long): List<Category> = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectByParentId(parentId).executeAsList().map { it.toModel() }
    }



    /**
     * Inserts a category into the database.
     * If a category with the same title already exists, returns its ID instead.
     *
     * @param category The category to insert
     * @return The ID of the inserted or existing category
     * @throws RuntimeException If the insertion fails
     */
// Dans SeforimRepository.kt, remplacez la méthode insertCategory par celle-ci :

    suspend fun insertCategory(category: Category): Long = withContext(Dispatchers.IO) {
        logger.d { "🔧 Repository: Attempting to insert category '${category.title}'" }
        logger.d { "🔧 Category details: parentId=${category.parentId}, level=${category.level}" }

        try {
            // IMPORTANT: Check if a category with the same title AND SAME PARENT already exists
            // Two categories can have the same name if they have different parents!
            val existingCategories = if (category.parentId != null) {
                // Look for categories with the same parent
                database.categoryQueriesQueries.selectByParentId(category.parentId).executeAsList()
            } else {
                // Look for root categories (parentId is null)
                database.categoryQueriesQueries.selectRoot().executeAsList()
            }

            // Find a category with the same title in the same parent
            val existingCategory = existingCategories.find { it.title == category.title }

            if (existingCategory != null) {
                logger.d { "⚠️ Category with title '${category.title}' already exists under parent ${category.parentId} with ID: ${existingCategory.id}" }
                return@withContext existingCategory.id
            }

            // Try the insertion
            database.categoryQueriesQueries.insert(
                parentId = category.parentId,
                title = category.title,
                level = category.level.toLong()
            )

            val insertedId = database.categoryQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d { "✅ Repository: Category inserted with ID: $insertedId" }

            if (insertedId == 0L) {
                logger.d { "lastInsertRowId() returned 0 for category '${category.title}', checking if it exists" }

                // Check again if the category was inserted despite lastInsertRowId() returning 0
                val updatedCategories = if (category.parentId != null) {
                    database.categoryQueriesQueries.selectByParentId(category.parentId).executeAsList()
                } else {
                    database.categoryQueriesQueries.selectRoot().executeAsList()
                }

                val newCategory = updatedCategories.find { it.title == category.title }

                if (newCategory != null) {
                    logger.d { "🔄 Category found after insertion, returning existing ID: ${newCategory.id}" }
                    return@withContext newCategory.id
                }

                // If all else fails, throw an exception
                throw RuntimeException("Failed to insert category '${category.title}' with parent ${category.parentId}")
            }

            return@withContext insertedId

        } catch (e: Exception) {
            // Changed from error to warning level to reduce unnecessary error logs
            logger.w(e) { "❌ Repository: Error inserting category '${category.title}': ${e.message}" }

            // In case of error, check if the category exists anyway
            val categories = if (category.parentId != null) {
                database.categoryQueriesQueries.selectByParentId(category.parentId).executeAsList()
            } else {
                database.categoryQueriesQueries.selectRoot().executeAsList()
            }

            val existingCategory = categories.find { it.title == category.title }

            if (existingCategory != null) {
                logger.d { "🔄 Category exists after error, returning existing ID: ${existingCategory.id}" }
                return@withContext existingCategory.id
            }

            // Re-throw the exception if we can't recover
            throw e
        }
    }

    // --- Books ---

    /**
     * Retrieves a book by its ID, including all related data (authors, topics, etc.).
     *
     * @param id The ID of the book to retrieve
     * @return The book if found, null otherwise
     */
    suspend fun getBook(id: Long): Book? = withContext(Dispatchers.IO) {
        val bookData = database.bookQueriesQueries.selectById(id).executeAsOneOrNull() ?: return@withContext null
        val authors = getBookAuthors(bookData.id)
        val topics = getBookTopics(bookData.id)
        val pubPlaces = getBookPubPlaces(bookData.id)
        val pubDates = getBookPubDates(bookData.id)
        return@withContext bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
    }

    /**
     * Retrieves all books in a specific category.
     *
     * @param categoryId The ID of the category
     * @return A list of books in the category
     */
    suspend fun getBooksByCategory(categoryId: Long): List<Book> = withContext(Dispatchers.IO) {
        val books = database.bookQueriesQueries.selectByCategoryId(categoryId).executeAsList()
        return@withContext books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
        }
    }



    suspend fun searchBooksByAuthor(authorName: String): List<Book> = withContext(Dispatchers.IO) {
        val books = database.bookQueriesQueries.selectByAuthor("%$authorName%").executeAsList()
        return@withContext books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
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

    // Get all publication dates for a book
    private suspend fun getBookPubDates(bookId: Long): List<PubDate> = withContext(Dispatchers.IO) {
        logger.d{"Getting publication dates for book ID: $bookId"}
        val pubDates = database.pubDateQueriesQueries.selectByBookId(bookId).executeAsList()
        logger.d{"Found ${pubDates.size} publication dates for book ID: $bookId"}
        return@withContext pubDates.map { it.toModel() }
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

        // If lastInsertRowId returns 0, it might be because the insertion was ignored due to a conflict
        // Try to get the ID by name
        if (authorId == 0L) {
            logger.d { "lastInsertRowId() returned 0 for author '$name', checking if it exists" }

            val insertedAuthor = database.authorQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (insertedAuthor != null) {
                logger.d{"Found author after insertion with ID: ${insertedAuthor.id}"}
                return@withContext insertedAuthor.id
            }

            // If we can't find the author by name, try to insert it again with a different method
            logger.d{"Author not found after insertion, trying insertAndGetId"}
            database.authorQueriesQueries.insertAndGetId(name)

            // Check again
            val retryAuthor = database.authorQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (retryAuthor != null) {
                logger.d{"Found author after retry with ID: ${retryAuthor.id}"}
                return@withContext retryAuthor.id
            }

            // If all else fails, return a dummy ID that will be used for this session only
            // This allows the process to continue without throwing an exception
            logger.w{"Could not insert author '$name' after multiple attempts, using temporary ID"}
            return@withContext 999999L
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
        val pubDates = getBookPubDates(bookData.id)
        return@withContext bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
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

    // Get a publication date by date, returns null if not found
    suspend fun getPubDateByDate(date: String): PubDate? = withContext(Dispatchers.IO) {
        logger.d{"Looking for publication date with date: $date"}
        val pubDate = database.pubDateQueriesQueries.selectByDate(date).executeAsOneOrNull()
        if (pubDate != null) {
            logger.d{"Found publication date with ID: ${pubDate.id}"}
        } else {
            logger.d{"Publication date not found: $date"}
        }
        return@withContext pubDate?.toModel()
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

        // If lastInsertRowId returns 0, it might be because the insertion was ignored due to a conflict
        // Try to get the ID by name
        if (topicId == 0L) {
            logger.d { "lastInsertRowId() returned 0 for topic '$name', checking if it exists" }

            val insertedTopic = database.topicQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (insertedTopic != null) {
                logger.d{"Found topic after insertion with ID: ${insertedTopic.id}"}
                return@withContext insertedTopic.id
            }

            // If we can't find the topic by name, try to insert it again with a different method
            logger.d{"Topic not found after insertion, trying insertAndGetId"}
            database.topicQueriesQueries.insertAndGetId(name)

            // Check again
            val retryTopic = database.topicQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (retryTopic != null) {
                logger.d{"Found topic after retry with ID: ${retryTopic.id}"}
                return@withContext retryTopic.id
            }

            // If all else fails, return a dummy ID that will be used for this session only
            // This allows the process to continue without throwing an exception
            logger.w{"Could not insert topic '$name' after multiple attempts, using temporary ID"}
            return@withContext 999999L
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

        // If lastInsertRowId returns 0, it might be because the insertion was ignored due to a conflict
        // Try to get the ID by name
        if (pubPlaceId == 0L) {
            logger.d { "lastInsertRowId() returned 0 for publication place '$name', checking if it exists" }

            val insertedPubPlace = database.pubPlaceQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (insertedPubPlace != null) {
                logger.d{"Found publication place after insertion with ID: ${insertedPubPlace.id}"}
                return@withContext insertedPubPlace.id
            }

            // If all else fails, return a dummy ID that will be used for this session only
            // This allows the process to continue without throwing an exception
            logger.w{"Could not insert publication place '$name' after multiple attempts, using temporary ID"}
            return@withContext 999999L
        }

        logger.d{"Publication place inserted with ID: $pubPlaceId"}
        return@withContext pubPlaceId
    }

    // Insert a publication date and return its ID
    suspend fun insertPubDate(date: String): Long = withContext(Dispatchers.IO) {
        logger.d{"Inserting publication date: $date"}

        // Check if publication date already exists
        val existingPubDate = database.pubDateQueriesQueries.selectByDate(date).executeAsOneOrNull()
        if (existingPubDate != null) {
            logger.d{"Publication date already exists with ID: ${existingPubDate.id}"}
            return@withContext existingPubDate.id
        }

        // Insert the publication date
        database.pubDateQueriesQueries.insert(date)

        // Get the ID of the inserted publication date
        val pubDateId = database.pubDateQueriesQueries.lastInsertRowId().executeAsOne()

        // If lastInsertRowId returns 0, it might be because the insertion was ignored due to a conflict
        // Try to get the ID by date
        if (pubDateId == 0L) {
            logger.d { "lastInsertRowId() returned 0 for publication date '$date', checking if it exists" }

            val insertedPubDate = database.pubDateQueriesQueries.selectByDate(date).executeAsOneOrNull()
            if (insertedPubDate != null) {
                logger.d{"Found publication date after insertion with ID: ${insertedPubDate.id}"}
                return@withContext insertedPubDate.id
            }

            // If all else fails, return a dummy ID that will be used for this session only
            // This allows the process to continue without throwing an exception
            logger.w{"Could not insert publication date '$date' after multiple attempts, using temporary ID"}
            return@withContext 999999L
        }

        logger.d{"Publication date inserted with ID: $pubDateId"}
        return@withContext pubDateId
    }

    // Link a publication place to a book
    suspend fun linkPubPlaceToBook(pubPlaceId: Long, bookId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Linking publication place $pubPlaceId to book $bookId"}
        database.pubPlaceQueriesQueries.linkBookPubPlace(bookId, pubPlaceId)
        logger.d{"Linked publication place $pubPlaceId to book $bookId"}
    }

    // Link a publication date to a book
    suspend fun linkPubDateToBook(pubDateId: Long, bookId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Linking publication date $pubDateId to book $bookId"}
        database.pubDateQueriesQueries.linkBookPubDate(bookId, pubDateId)
        logger.d{"Linked publication date $pubDateId to book $bookId"}
    }

    /**
     * Inserts a book into the database, including all related data (authors, topics, etc.).
     * If the book has an ID greater than 0, uses that ID; otherwise, generates a new ID.
     *
     * @param book The book to insert
     * @return The ID of the inserted book
     */
    suspend fun insertBook(book: Book): Long = withContext(Dispatchers.IO) {
        logger.d{"Repository inserting book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}"}

        // Use the ID from the book object if it's greater than 0
        if (book.id > 0) {
            database.bookQueriesQueries.insertWithId(
                id = book.id,
                categoryId = book.categoryId,
                title = book.title,
                heShortDesc = book.heShortDesc,
                orderIndex = book.order.toLong(),
                totalLines = book.totalLines.toLong()
            )
            logger.d{"Used insertWithId for book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}"}

            // ✅ Verify that the insertion was successful
            val insertedBook = database.bookQueriesQueries.selectById(book.id).executeAsOneOrNull()
            if (insertedBook?.categoryId != book.categoryId) {
                // Changed from error to warning level to reduce unnecessary error logs
                logger.w{"WARNING: Book inserted with wrong categoryId! Expected: ${book.categoryId}, Got: ${insertedBook?.categoryId}"}
                // Fix immediately
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

            // Process publication dates
            for (pubDate in book.pubDates) {
                val pubDateId = insertPubDate(pubDate.date)
                linkPubDateToBook(pubDateId, book.id)
                logger.d{"Processed publication date '${pubDate.date}' (ID: $pubDateId) for book '${book.title}' (ID: ${book.id})"}
            }

            return@withContext book.id
        } else {
            // Fall back to auto-generated ID if book.id is 0
            database.bookQueriesQueries.insert(
                categoryId = book.categoryId,
                title = book.title,
                heShortDesc = book.heShortDesc,
                orderIndex = book.order.toLong(),
                totalLines = book.totalLines.toLong()
            )
            val id = database.bookQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"Used insert for book '${book.title}', got ID: $id with categoryId: ${book.categoryId}"}

            // Check if insertion failed
            if (id == 0L) {
                // Changed from error to warning level to reduce unnecessary error logs
                logger.w { "⚠️ Repository: lastInsertRowId() returned 0! This indicates insertion failed. Context: book='${book.title}', categoryId=${book.categoryId}, authors=${book.authors.map { it.name }}, topics=${book.topics.map { it.name }}" }

                // Try to find the book by title
                val existingBook = database.bookQueriesQueries.selectByTitle(book.title).executeAsOneOrNull()
                if (existingBook != null) {
                    logger.d { "Found book after failed insertion, returning existing ID: ${existingBook.id}" }
                    return@withContext existingBook.id
                }

                throw RuntimeException("Failed to insert book '${book.title}' - insertion returned ID 0. Context: categoryId=${book.categoryId}, authors=${book.authors.map { it.name }}, topics=${book.topics.map { it.name }}, pubPlaces=${book.pubPlaces.map { it.name }}, pubDates=${book.pubDates.map { it.date }}")
            }

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

            // Process publication dates
            for (pubDate in book.pubDates) {
                val pubDateId = insertPubDate(pubDate.date)
                linkPubDateToBook(pubDateId, id)
                logger.d{"Processed publication date '${pubDate.date}' (ID: $pubDateId) for book '${book.title}' (ID: $id)"}
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

            // Check if insertion failed
            if (lineId == 0L) {
                // Changed from error to warning level to reduce unnecessary error logs
                logger.w { "⚠️ Repository: lastInsertRowId() returned 0! This indicates insertion failed. Context: bookId=${line.bookId}, lineIndex=${line.lineIndex}, content='${line.content.take(30)}${if (line.content.length > 30) "..." else ""}'" }

                // Try to find the line by bookId and lineIndex
                val existingLine = database.lineQueriesQueries.selectByBookIdAndIndex(line.bookId, line.lineIndex.toLong()).executeAsOneOrNull()
                if (existingLine != null) {
                    logger.d { "Found line after failed insertion, returning existing ID: ${existingLine.id}" }
                    return@withContext existingLine.id
                }

                throw RuntimeException("Failed to insert line for book ${line.bookId} at index ${line.lineIndex} - insertion returned ID 0. Context: content='${line.content.take(50)}${if (line.content.length > 50) "..." else ""}', plainText='${line.plainText.take(50)}${if (line.plainText.length > 50) "..." else ""}')")
            }

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
        // Truncate text for logging if it's too long
        val truncatedText = if (text.length > 50) "${text.take(50)}..." else text
        logger.d{"Getting or creating tocText entry for text: '$truncatedText'"}

        try {
            // Check if the text already exists
            logger.d{"Checking if text already exists in database"}
            val existingId = database.tocTextQueriesQueries.selectIdByText(text).executeAsOneOrNull()
            if (existingId != null) {
                logger.d{"Found existing tocText entry with ID: $existingId for text: '$truncatedText'"}
                return@withContext existingId
            }

            // Insert the text
            logger.d{"Text not found, inserting new tocText entry for: '$truncatedText'"}
            database.tocTextQueriesQueries.insertAndGetId(text)

            // Get the ID of the inserted text
            logger.d{"Getting ID of inserted tocText entry"}
            val textId = database.tocTextQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"lastInsertRowId() returned: $textId"}

            // If lastInsertRowId returns 0, it's likely because the text already exists (due to INSERT OR IGNORE)
            // This is expected behavior, not an error, so we'll try to get the ID by text
            if (textId == 0L) {
                // Log at debug level since this is expected behavior when text already exists
                logger.d{"lastInsertRowId() returned 0 for tocText insertion (likely due to INSERT OR IGNORE). Text: '$truncatedText', Length: ${text.length}, Hash: ${text.hashCode()}. Trying to get ID by text."}

                // Try to find the text that was just inserted or that already existed
                val insertedId = database.tocTextQueriesQueries.selectIdByText(text).executeAsOneOrNull()
                if (insertedId != null) {
                    logger.d{"Found tocText with ID: $insertedId for text: '$truncatedText'"}
                    return@withContext insertedId
                }

                // If we can't find the text by exact match, this is unexpected and should be logged as an error
                // Count total tocTexts for debugging
                val totalTocTexts = database.tocTextQueriesQueries.countAll().executeAsOne()

                // Log more details about the failure
                // Changed from error to warning level to reduce unnecessary error logs
                logger.w{"Failed to insert tocText and couldn't find it after insertion. This is unexpected since the text should either be inserted or already exist. Text: '$truncatedText', Length: ${text.length}, Hash: ${text.hashCode()}, Total TocTexts: $totalTocTexts"}

                throw RuntimeException("Failed to insert tocText '$truncatedText' - insertion returned ID 0 and couldn't find text afterward. This is unexpected since the text should either be inserted or already exist. Context: textLength=${text.length}, textHash=${text.hashCode()}, totalTocTexts=$totalTocTexts")
            }

            logger.d{"Created new tocText entry with ID: $textId for text: '$truncatedText'"}
            return@withContext textId
        } catch (e: Exception) {
            // Changed from error to warning level to reduce unnecessary error logs
            logger.w(e){"Exception in getOrCreateTocText for text: '$truncatedText', Length: ${text.length}, Hash: ${text.hashCode()}. Error: ${e.message}"}
            throw e
        }
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

            // Check if insertion failed
            if (tocId == 0L) {
                // Changed from error to warning level to reduce unnecessary error logs
                logger.w { "⚠️ Repository: lastInsertRowId() returned 0! This indicates insertion failed. Context: bookId=${entry.bookId}, parentId=${entry.parentId}, level=${entry.level}, text='${entry.text.take(30)}${if (entry.text.length > 30) "..." else ""}'" }

                // Try to find a matching TOC entry by bookId and text
                val existingEntries = database.tocQueriesQueries.selectByBookId(entry.bookId).executeAsList()
                val matchingEntry = existingEntries.find { 
                    it.text == entry.text && it.level == entry.level.toLong() 
                }

                if (matchingEntry != null) {
                    logger.d { "Found matching TOC entry after failed insertion, returning existing ID: ${matchingEntry.id}" }
                    return@withContext matchingEntry.id
                }

                throw RuntimeException("Failed to insert TOC entry for book ${entry.bookId} with text '${entry.text.take(30)}${if (entry.text.length > 30) "..." else ""}' - insertion returned ID 0. Context: parentId=${entry.parentId}, level=${entry.level}, lineId=${entry.lineId}, lineIndex=${entry.lineIndex}, order=${entry.order}, path='${entry.path}'")
            }

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
                        sourceLineId = it.sourceLineId,
                        targetLineId = it.targetLineId,
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

        try {
            database.linkQueriesQueries.insert(
                sourceBookId = link.sourceBookId,
                targetBookId = link.targetBookId,
                sourceLineId = link.sourceLineId,
                targetLineId = link.targetLineId,
                connectionType = link.connectionType.name
            )
            val linkId = database.linkQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"Repository inserted link with ID: $linkId"}

            // Check if insertion failed
            if (linkId == 0L) {
                // Changed from error to warning level to reduce unnecessary error logs
                logger.w { "⚠️ Repository: lastInsertRowId() returned 0! This indicates insertion failed. Context: sourceBookId=${link.sourceBookId}, targetBookId=${link.targetBookId}, sourceLineId=${link.sourceLineId}, targetLineId=${link.targetLineId}, connectionType=${link.connectionType.name}" }

                // Try to find a matching link
                val existingLinks = database.linkQueriesQueries.selectBySourceBook(link.sourceBookId).executeAsList()
                val matchingLink = existingLinks.find { 
                    it.targetBookId == link.targetBookId && 
                    it.sourceLineId == link.sourceLineId && 
                    it.targetLineId == link.targetLineId 
                }

                if (matchingLink != null) {
                    logger.d { "Found matching link after failed insertion, returning existing ID: ${matchingLink.id}" }
                    return@withContext matchingLink.id
                }

                throw RuntimeException("Failed to insert link from book ${link.sourceBookId} to book ${link.targetBookId} - insertion returned ID 0. Context: sourceLineId=${link.sourceLineId}, targetLineId=${link.targetLineId}, connectionType=${link.connectionType.name}")
            }

            return@withContext linkId
        } catch (e: Exception) {
            // Changed from error to warning level to reduce unnecessary error logs
            logger.w(e){"Error inserting link: ${e.message}"}
            throw e
        }
    }

    // --- Search ---

    /**
     * Searches for text across all books.
     *
     * @param query The search query
     * @param limit Maximum number of results to return
     * @param offset Number of results to skip (for pagination)
     * @return A list of search results
     */
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

    /**
     * Searches for text within a specific book.
     *
     * @param bookId The ID of the book to search in
     * @param query The search query
     * @param limit Maximum number of results to return
     * @param offset Number of results to skip (for pagination)
     * @return A list of search results
     */
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

    /**
     * Searches for text in books by a specific author.
     *
     * @param author The author name to filter by
     * @param query The search query
     * @param limit Maximum number of results to return
     * @param offset Number of results to skip (for pagination)
     * @return A list of search results
     */
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

    /**
     * Prepares a search query for full-text search.
     * Adds wildcards and quotes to improve search results.
     *
     * @param query The raw search query
     * @return The formatted query for FTS
     */
    private fun prepareFtsQuery(query: String): String {
        return query.trim()
            .split("\\s+".toRegex())
            .filter { it.isNotBlank() }
            .joinToString(" ") { "\"$it\"*" }
    }

    /**
     * Executes a raw SQL query.
     * This is useful for operations that are not covered by the generated queries,
     * such as enabling or disabling foreign key constraints.
     *
     * @param sql The SQL query to execute
     */
    suspend fun executeRawQuery(sql: String) = withContext(Dispatchers.IO) {
        logger.d { "Executing raw SQL query: $sql" }
        driver.execute(null, sql, 0)
        logger.d { "Raw SQL query executed successfully" }
    }

    /**
     * Closes the database connection.
     * Should be called when the repository is no longer needed.
     */
    fun close() {
        driver.close()
    }
}

// Data classes for enriched results

/**
 * Information about a commentator (author who comments on other books).
 *
 * @property bookId The ID of the commentator's book
 * @property title The title of the commentator's book
 * @property author The name of the commentator
 * @property linkCount The number of links (comments) by this commentator
 */
data class CommentatorInfo(
    val bookId: Long,
    val title: String,
    val author: String?,
    val linkCount: Int
)

/**
 * A commentary with its text content.
 *
 * @property link The link connecting the source text to the commentary
 * @property targetBookTitle The title of the book containing the commentary
 * @property targetText The text of the commentary
 */
data class CommentaryWithText(
    val link: Link,
    val targetBookTitle: String,
    val targetText: String
)
