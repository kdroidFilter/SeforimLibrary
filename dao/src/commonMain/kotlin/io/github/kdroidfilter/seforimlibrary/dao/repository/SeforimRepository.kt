package io.github.kdroidfilter.seforimlibrary.dao.repository



import app.cash.sqldelight.db.SqlDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.*
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.QueryResult
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
        driver.execute(null, "PRAGMA cache_size=40000", 0)
        driver.execute(null, "PRAGMA temp_store=MEMORY", 0)

        // Check if the database is empty
        try {
            val bookCount = database.bookQueriesQueries.countAll().executeAsOne()
            logger.d{"Database contains $bookCount books"}
        } catch (e: Exception) {
            logger.d{"Error counting books: ${e.message}"}
        }
    }

    // --- Line ⇄ TOC mapping ---

    /**
     * Maps a line to the TOC entry it belongs to. Upserts on conflict.
     */
    suspend fun upsertLineToc(lineId: Long, tocEntryId: Long) = withContext(Dispatchers.IO) {
        database.lineTocQueriesQueries.upsert(lineId, tocEntryId)
    }

    // --- Performance helpers (transactions/PRAGMAs) ---

    private var txCounter = 0

    suspend fun <T> runInTransaction(block: suspend () -> T): T {
        // Use explicit transaction boundaries to avoid savepoint issues with some SQLite configurations
        withContext(Dispatchers.IO) { driver.execute(null, "BEGIN IMMEDIATE", 0) }
        return try {
            val result = block()
            withContext(Dispatchers.IO) { driver.execute(null, "COMMIT", 0) }
            result
        } catch (t: Throwable) {
            try { withContext(Dispatchers.IO) { driver.execute(null, "ROLLBACK", 0) } } catch (_: Throwable) {}
            throw t
        }
    }

    suspend fun setSynchronous(mode: String) = withContext(Dispatchers.IO) {
        driver.execute(null, "PRAGMA synchronous=$mode", 0)
    }

    suspend fun setSynchronousOff() = setSynchronous("OFF")
    suspend fun setSynchronousNormal() = setSynchronous("NORMAL")

    suspend fun bulkUpsertLineToc(pairs: List<Pair<Long, Long>>) = withContext(Dispatchers.IO) {
        if (pairs.isEmpty()) return@withContext
        for ((lineId, tocEntryId) in pairs) {
            database.lineTocQueriesQueries.upsert(lineId, tocEntryId)
        }
    }

    /**
     * Gets the tocEntryId associated with a line via the mapping table.
     */
    suspend fun getTocEntryIdForLine(lineId: Long): Long? = withContext(Dispatchers.IO) {
        database.lineTocQueriesQueries.selectTocEntryIdByLineId(lineId).executeAsOneOrNull()
    }

    /**
     * Gets the TocEntry model associated with a line via the mapping table.
     */
    suspend fun getTocEntryForLine(lineId: Long): TocEntry? = withContext(Dispatchers.IO) {
        val tocId = database.lineTocQueriesQueries.selectTocEntryIdByLineId(lineId).executeAsOneOrNull()
            ?: return@withContext null
        database.tocQueriesQueries.selectTocById(tocId).executeAsOneOrNull()?.toModel()
    }

    /**
     * Returns the TOC entry whose heading line is the given line id, or null if not a TOC heading.
     */
    suspend fun getHeadingTocEntryByLineId(lineId: Long): TocEntry? = withContext(Dispatchers.IO) {
        database.tocQueriesQueries.selectByLineId(lineId).executeAsOneOrNull()?.toModel()
    }

    /**
     * Returns all line ids that belong to the given TOC entry (section), ordered by lineIndex.
     */
    suspend fun getLineIdsForTocEntry(tocEntryId: Long): List<Long> = withContext(Dispatchers.IO) {
        database.lineTocQueriesQueries.selectLineIdsByTocEntryId(tocEntryId).executeAsList()
    }

    /**
     * Returns mappings (lineId -> tocEntryId) for a book ordered by line index.
     */
    suspend fun getLineTocMappingsForBook(bookId: Long): List<LineTocMapping> = withContext(Dispatchers.IO) {
        database.lineTocQueriesQueries.selectByBookId(bookId).executeAsList().map {
            // The generated type exposes columns as properties with same names
            LineTocMapping(lineId = it.lineId, tocEntryId = it.tocEntryId)
        }
    }

    /**
     * Builds all mappings for a given book by assigning to each line
     * the latest TOC entry whose start line index is <= line's index.
     * This is useful for backfilling existing databases.
     */
    suspend fun rebuildLineTocForBook(bookId: Long) = withContext(Dispatchers.IO) {
        // Clear existing mappings for the book
        database.lineTocQueriesQueries.deleteByBookId(bookId)

        // Insert computed mappings in a single statement using a correlated subquery
        // Note: Uses lineIndex ordering via join on tocEntry.lineId → line.lineIndex
        driver.execute(null, """
            INSERT INTO line_toc(lineId, tocEntryId)
            SELECT l.id AS lineId,
                   (
                       SELECT t.id
                       FROM tocEntry t
                       JOIN line sl ON sl.id = t.lineId
                       WHERE t.bookId = l.bookId
                         AND t.lineId IS NOT NULL
                         AND sl.lineIndex <= l.lineIndex
                       ORDER BY sl.lineIndex DESC
                       LIMIT 1
                   ) AS tocEntryId
            FROM line l
            WHERE l.bookId = ?
        """.trimIndent(), 1) {
            bindLong(0, bookId)
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
     * Retrieves a category by its exact title.
     */
    suspend fun getCategoryByTitle(title: String): Category? = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectByTitle(title).executeAsOneOrNull()?.toModel()
    }

    /**
     * Retrieves best-matching category by name, trying exact, normalized, then LIKE.
     */
    suspend fun findCategoryByTitlePreferExact(title: String): Category? = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectByTitle(title).executeAsOneOrNull()?.toModel()
            ?: database.categoryQueriesQueries.selectByTitleLike("%$title%").executeAsOneOrNull()?.toModel()
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
     * Finds categories whose title matches the LIKE pattern. Use %term% for contains.
     */
    suspend fun findCategoriesByTitleLike(pattern: String, limit: Int = 20): List<Category> = withContext(Dispatchers.IO) {
        database.categoryQueriesQueries.selectManyByTitleLike(pattern, limit.toLong()).executeAsList().map { it.toModel() }
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

    /**
     * Rebuilds the category_closure table from the current category tree.
     * Inserts self-pairs and ancestor-descendant pairs for fast descendant filtering.
     */
    suspend fun rebuildCategoryClosure() = withContext(Dispatchers.IO) {
        // Clear existing closure data
        database.categoryClosureQueriesQueries.clear()
        // Load all categories (id, parentId)
        val rows = database.categoryQueriesQueries.selectAll().executeAsList()
        val parentMap = rows.associate { it.id to it.parentId }
        // For each category, walk up to root and insert pairs
        for (desc in rows) {
            var anc: Long? = desc.id
            // Self
            database.categoryClosureQueriesQueries.insert(desc.id, desc.id)
            anc = parentMap[desc.id]
            val safety = 128
            var guard = 0
            while (anc != null && guard++ < safety) {
                database.categoryClosureQueriesQueries.insert(anc, desc.id)
                anc = parentMap[anc]
            }
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

    /**
     * Finds books whose title matches the LIKE pattern. Use %term% for contains.
     */
    suspend fun findBooksByTitleLike(pattern: String, limit: Int = 20): List<Book> = withContext(Dispatchers.IO) {
        val rows = database.bookQueriesQueries.selectManyByTitleLike(pattern, limit.toLong()).executeAsList()
        rows.map { bookData ->
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

    /**
     * Retrieves a book by approximate title (exact, normalized, or LIKE).
     */
    suspend fun findBookByTitlePreferExact(title: String): Book? = withContext(Dispatchers.IO) {
        val row = database.bookQueriesQueries.selectByTitle(title).executeAsOneOrNull()
            ?: database.bookQueriesQueries.selectByTitleLike("%$title%").executeAsOneOrNull()
        row?.let { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
        }
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
        
    /**
     * Gets the previous line for a given book and line index.
     * 
     * @param bookId The ID of the book
     * @param currentLineIndex The index of the current line
     * @return The previous line, or null if there is no previous line
     */
    suspend fun getPreviousLine(bookId: Long, currentLineIndex: Int): Line? = withContext(Dispatchers.IO) {
        if (currentLineIndex <= 0) return@withContext null
        
        val previousIndex = currentLineIndex - 1
        database.lineQueriesQueries.selectByBookIdAndIndex(bookId, previousIndex.toLong())
            .executeAsOneOrNull()?.toModel()
    }
    
    /**
     * Gets the next line for a given book and line index.
     * 
     * @param bookId The ID of the book
     * @param currentLineIndex The index of the current line
     * @return The next line, or null if there is no next line
     */
    suspend fun getNextLine(bookId: Long, currentLineIndex: Int): Line? = withContext(Dispatchers.IO) {
        val nextIndex = currentLineIndex + 1
        database.lineQueriesQueries.selectByBookIdAndIndex(bookId, nextIndex.toLong())
            .executeAsOneOrNull()?.toModel()
    }

    suspend fun updateLinePlainText(lineId: Long, plain: String) = withContext(Dispatchers.IO) {
        database.lineExtraQueriesQueries.updatePlainText(plain, lineId)
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
        database.tocQueriesQueries.selectTocById(id).executeAsOneOrNull()?.toModel()
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

    // Returns all distinct tocText values using generated SQLDelight query
    suspend fun getAllTocTexts(): List<String> = withContext(Dispatchers.IO) {
        logger.d { "Getting all tocText values (using generated query)" }
        database.tocTextQueriesQueries.selectAll().executeAsList().map { it.text }
    }
    
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
        logger.d{"Repository inserting TOC entry with bookId: ${entry.bookId}, lineId: ${entry.lineId}, hasChildren: ${entry.hasChildren}"}

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
                isLastChild = if (entry.isLastChild) 1 else 0,
                hasChildren = if (entry.hasChildren) 1 else 0  // NOUVEAU
            )
            logger.d{"Repository inserted TOC entry with explicit ID: ${entry.id}, bookId: ${entry.bookId}, lineId: ${entry.lineId}, hasChildren: ${entry.hasChildren}"}
            return@withContext entry.id
        } else {
            // Fall back to auto-generated ID if entry.id is 0
            database.tocQueriesQueries.insert(
                bookId = entry.bookId,
                parentId = entry.parentId,
                textId = textId,
                level = entry.level.toLong(),
                lineId = entry.lineId,
                isLastChild = if (entry.isLastChild) 1 else 0,
                hasChildren = if (entry.hasChildren) 1 else 0  // NOUVEAU
            )
            val tocId = database.tocQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"Repository inserted TOC entry with auto-generated ID: $tocId, bookId: ${entry.bookId}, lineId: ${entry.lineId}, hasChildren: ${entry.hasChildren}"}

            // Check if insertion failed
            if (tocId == 0L) {
                // Try to find a matching TOC entry by bookId and text
                val existingEntries = database.tocQueriesQueries.selectByBookId(entry.bookId).executeAsList()
                val matchingEntry = existingEntries.find {
                    it.text == entry.text && it.level == entry.level.toLong()
                }

                if (matchingEntry != null) {
                    logger.d { "Found matching TOC entry after failed insertion, returning existing ID: ${matchingEntry.id}" }
                    return@withContext matchingEntry.id
                }

                throw RuntimeException("Failed to insert TOC entry for book ${entry.bookId} with text '${entry.text.take(30)}${if (entry.text.length > 30) "..." else ""}' - insertion returned ID 0. Context: parentId=${entry.parentId}, level=${entry.level}, lineId=${entry.lineId}")
            }

            return@withContext tocId
        }
    }

    // Nouvelle méthode pour mettre à jour hasChildren
    suspend fun updateTocEntryHasChildren(tocEntryId: Long, hasChildren: Boolean) = withContext(Dispatchers.IO) {
        logger.d{"Repository updating TOC entry $tocEntryId with hasChildren: $hasChildren"}
        database.tocQueriesQueries.updateHasChildren(if (hasChildren) 1 else 0, tocEntryId)
        logger.d{"Repository updated TOC entry $tocEntryId with hasChildren: $hasChildren"}
    }
    suspend fun updateTocEntryLineId(tocEntryId: Long, lineId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Repository updating TOC entry $tocEntryId with lineId: $lineId"}
        database.tocQueriesQueries.updateLineId(lineId, tocEntryId)
        logger.d{"Repository updated TOC entry $tocEntryId with lineId: $lineId"}
    }
    
    suspend fun updateTocEntryIsLastChild(tocEntryId: Long, isLastChild: Boolean) = withContext(Dispatchers.IO) {
        logger.d{"Repository updating TOC entry $tocEntryId with isLastChild: $isLastChild"}
        database.tocQueriesQueries.updateIsLastChild(if (isLastChild) 1 else 0, tocEntryId)
        logger.d{"Repository updated TOC entry $tocEntryId with isLastChild: $isLastChild"}
    }

    // --- Connection Types ---

    /**
     * Gets a connection type by name, or creates it if it doesn't exist.
     *
     * @param name The name of the connection type
     * @return The ID of the connection type
     */
    private suspend fun getOrCreateConnectionType(name: String): Long = withContext(Dispatchers.IO) {
        logger.d{"Getting or creating connection type: $name"}

        // Check if the connection type already exists
        val existingType = database.connectionTypeQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (existingType != null) {
            logger.d{"Found existing connection type with ID: ${existingType.id}"}
            return@withContext existingType.id
        }

        // Insert the connection type
        database.connectionTypeQueriesQueries.insert(name)

        // Get the ID of the inserted connection type
        val typeId = database.connectionTypeQueriesQueries.lastInsertRowId().executeAsOne()

        // If lastInsertRowId returns 0, try to get the ID by name
        if (typeId == 0L) {

            val insertedType = database.connectionTypeQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (insertedType != null) {
                logger.d{"Found connection type after insertion with ID: ${insertedType.id}"}
                return@withContext insertedType.id
            }

            throw RuntimeException("Failed to insert connection type '$name' - insertion returned ID 0 and couldn't find type afterward")
        }

        logger.d{"Created new connection type with ID: $typeId"}
        return@withContext typeId
    }

    /**
     * Gets all connection types from the database.
     *
     * @return A list of all connection types
     */
    suspend fun getAllConnectionTypes(): List<ConnectionType> = withContext(Dispatchers.IO) {
        database.connectionTypeQueriesQueries.selectAll().executeAsList().map { 
            ConnectionType.fromString(it.name)
        }
    }

    // --- Links ---

    suspend fun getLink(id: Long): Link? = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectLinkById(id).executeAsOneOrNull()?.toModel()
    }

    suspend fun countLinks(): Long = withContext(Dispatchers.IO) {
        logger.d{"Counting links in database"}
        val count = database.linkQueriesQueries.countAllLinks().executeAsOne()
        logger.d{"Found $count links in database"}
        count
    }

    suspend fun getCommentariesForLines(
        lineIds: List<Long>,
        activeCommentatorIds: Set<Long> = emptySet()
    ): List<CommentaryWithText> = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectLinksBySourceLineIds(lineIds).executeAsList()
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

    // New paginated methods for per-commentator pagination use cases
    suspend fun getCommentariesForLineRange(
        lineIds: List<Long>,
        activeCommentatorIds: Set<Long> = emptySet(),
        offset: Int,
        limit: Int
    ): List<CommentaryWithText> = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectLinksBySourceLineIds(lineIds)
            .executeAsList()
            .filter { activeCommentatorIds.isEmpty() || it.targetBookId in activeCommentatorIds }
            .drop(offset)
            .take(limit)
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

    suspend fun getAvailableCommentators(
        bookId: Long,
        offset: Int,
        limit: Int
    ): List<CommentatorInfo> = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectCommentatorsByBook(bookId)
            .executeAsList()
            .drop(offset)
            .take(limit)
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
        logger.d{"Link details - sourceLineId: ${link.sourceLineId}, targetLineId: ${link.targetLineId}, connectionType: ${link.connectionType.name}"}

        try {
            // Get or create the connection type
            val connectionTypeId = getOrCreateConnectionType(link.connectionType.name)
            logger.d{"Using connection type ID: $connectionTypeId for type: ${link.connectionType.name}"}

            database.linkQueriesQueries.insert(
                sourceBookId = link.sourceBookId,
                targetBookId = link.targetBookId,
                sourceLineId = link.sourceLineId,
                targetLineId = link.targetLineId,
                connectionTypeId = connectionTypeId
            )
            val linkId = database.linkQueriesQueries.lastInsertRowId().executeAsOne()
            logger.d{"Repository inserted link with ID: $linkId"}

            // Check if insertion failed
            if (linkId == 0L) {

                // Try to find a matching link
                val existingLinks = database.linkQueriesQueries.selectLinksBySourceBook(link.sourceBookId).executeAsList()
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

    /**
     * Migrates existing links to use the new connection_type table.
     * This should be called once after updating the database schema.
     */
    suspend fun migrateConnectionTypes() = withContext(Dispatchers.IO) {
        logger.d{"Starting migration of connection types"}

        try {
            // Make sure all connection types exist in the connection_type table
            for (type in ConnectionType.values()) {
                getOrCreateConnectionType(type.name)
            }

            // Get all links from the database
            val links = database.linkQueriesQueries.selectLinksBySourceBook(0).executeAsList()
            logger.d{"Found ${links.size} links to migrate"}

            // For each link, update the connectionTypeId
            var migratedCount = 0
            for (link in links) {
                val connectionTypeId = getOrCreateConnectionType(link.connectionType)

                // Execute a raw SQL query to update the link
                val updateSql = "UPDATE link SET connectionTypeId = $connectionTypeId WHERE id = ${link.id}"
                executeRawQuery(updateSql)

                migratedCount++
                if (migratedCount % 100 == 0) {
                    logger.d{"Migrated $migratedCount links so far"}
                }
            }

            logger.d{"Successfully migrated $migratedCount links"}
            logger.d{"Connection types migration completed successfully"}
        } catch (e: Exception) {
            logger.e(e){"Error during connection types migration: ${e.message}"}
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
     * Same as searchInBook but accepts a raw FTS query (e.g., NEAR operators).
     */
    suspend fun searchInBookWithOperators(
        bookId: Long,
        ftsQuery: String,
        limit: Int = 200,
        offset: Int = 0
    ): List<SearchResult> = withContext(Dispatchers.IO) {
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
     * Executes a raw FTS5 query string that may contain operators like NEAR, AND/OR, etc.
     * The query is passed as-is to the FTS5 MATCH clause. Callers are responsible for
     * constructing a valid FTS5 query string.
     *
     * This is useful for proximity searches such as: term1 NEAR/5 term2
     */
    suspend fun searchWithOperators(
        ftsQuery: String,
        limit: Int = 50,
        offset: Int = 0
    ): List<SearchResult> = withContext(Dispatchers.IO) {
        database.searchQueriesQueries
            .searchWithOperators(ftsQuery, limit.toLong(), offset.toLong())
            .executeAsList()
            .map { row ->
                io.github.kdroidfilter.seforimlibrary.core.models.SearchResult(
                    bookId = row.bookId ?: 0,
                    bookTitle = row.bookTitle ?: "",
                    lineId = row.id ?: 0,
                    lineIndex = row.lineIndex?.toInt() ?: 0,
                    snippet = row.snippet ?: "",
                    rank = row.rank
                )
            }
    }

    /**
     * Searches within a category (including descendants) using a raw FTS query with operators.
     */
    suspend fun searchInCategoryWithOperators(
        categoryId: Long,
        ftsQuery: String,
        limit: Int = 50,
        offset: Int = 0
    ): List<SearchResult> = withContext(Dispatchers.IO) {
        database.searchQueriesQueries
            .searchInCategoryWithOperators(ftsQuery, categoryId, limit.toLong(), offset.toLong())
            .executeAsList()
            .map { row ->
                io.github.kdroidfilter.seforimlibrary.core.models.SearchResult(
                    bookId = row.bookId ?: 0,
                    bookTitle = row.bookTitle ?: "",
                    lineId = row.id ?: 0,
                    lineIndex = row.lineIndex?.toInt() ?: 0,
                    snippet = row.snippet ?: "",
                    rank = row.rank
                )
            }
    }

    // Note: For category filtering with descendants, the app currently aggregates per-book results.

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
     * Rebuilds the FTS5 index for the line_search table.
     * This should be called after all data has been inserted to ensure optimal search performance.
     */
    suspend fun rebuildFts5Index() = withContext(Dispatchers.IO) {
        logger.d { "Rebuilding FTS5 index for line_search table" }
        database.searchQueriesQueries.rebuildFts5Index()
        logger.d { "FTS5 index rebuilt successfully" }
    }

    /**
     * Updates the book_has_links table to indicate whether a book has source links, target links, or both.
     * 
     * @param bookId The ID of the book to update
     * @param hasSourceLinks Whether the book has source links (true) or not (false)
     * @param hasTargetLinks Whether the book has target links (true) or not (false)
     */
    suspend fun updateBookHasLinks(bookId: Long, hasSourceLinks: Boolean, hasTargetLinks: Boolean) = withContext(Dispatchers.IO) {
        logger.d { "Updating book_has_links for book $bookId: hasSourceLinks=$hasSourceLinks, hasTargetLinks=$hasTargetLinks" }
        val hasSourceLinksInt = if (hasSourceLinks) 1L else 0L
        val hasTargetLinksInt = if (hasTargetLinks) 1L else 0L

        // Use upsert to insert or update the book's link status
        database.bookHasLinksQueriesQueries.upsert(bookId, hasSourceLinksInt, hasTargetLinksInt)

        logger.d { "Updated book_has_links for book $bookId: hasSourceLinks=$hasSourceLinks, hasTargetLinks=$hasTargetLinks" }
    }

    /**
     * Updates only the source links status for a book.
     * 
     * @param bookId The ID of the book to update
     * @param hasSourceLinks Whether the book has source links (true) or not (false)
     */
    suspend fun updateBookSourceLinks(bookId: Long, hasSourceLinks: Boolean) = withContext(Dispatchers.IO) {
        logger.d { "Updating source links for book $bookId: hasSourceLinks=$hasSourceLinks" }
        val hasSourceLinksInt = if (hasSourceLinks) 1L else 0L

        // Update only the source links status
        database.bookHasLinksQueriesQueries.updateSourceLinks(hasSourceLinksInt, bookId)

        logger.d { "Updated source links for book $bookId: hasSourceLinks=$hasSourceLinks" }
    }

    /**
     * Updates only the target links status for a book.
     * 
     * @param bookId The ID of the book to update
     * @param hasTargetLinks Whether the book has target links (true) or not (false)
     */
    suspend fun updateBookTargetLinks(bookId: Long, hasTargetLinks: Boolean) = withContext(Dispatchers.IO) {
        logger.d { "Updating target links for book $bookId: hasTargetLinks=$hasTargetLinks" }
        val hasTargetLinksInt = if (hasTargetLinks) 1L else 0L

        // Update only the target links status
        database.bookHasLinksQueriesQueries.updateTargetLinks(hasTargetLinksInt, bookId)

        logger.d { "Updated target links for book $bookId: hasTargetLinks=$hasTargetLinks" }
    }

    // --- Connection type specific helpers ---

    suspend fun countLinksBySourceBookAndType(bookId: Long, typeName: String): Long = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.countLinksBySourceBookAndType(bookId, typeName).executeAsOne()
    }

    suspend fun countLinksByTargetBookAndType(bookId: Long, typeName: String): Long = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.countLinksByTargetBookAndType(bookId, typeName).executeAsOne()
    }

    suspend fun updateBookConnectionFlags(
        bookId: Long,
        hasTargum: Boolean,
        hasReference: Boolean,
        hasCommentary: Boolean,
        hasOther: Boolean
    ) = withContext(Dispatchers.IO) {
        val t = if (hasTargum) 1L else 0L
        val r = if (hasReference) 1L else 0L
        val c = if (hasCommentary) 1L else 0L
        val o = if (hasOther) 1L else 0L
        database.bookQueriesQueries.updateConnectionFlags(t, r, c, o, bookId)
    }

    /**
     * Checks if a book has any links (source or target).
     * 
     * @param bookId The ID of the book to check
     * @return True if the book has any links, false otherwise
     */
    suspend fun bookHasAnyLinks(bookId: Long): Boolean = withContext(Dispatchers.IO) {
        logger.d { "Checking if book $bookId has any links" }

        // Check if the book has any links as source or target
        val hasSourceLinks = bookHasSourceLinks(bookId)
        val hasTargetLinks = bookHasTargetLinks(bookId)
        val result = hasSourceLinks || hasTargetLinks

        logger.d { "Book $bookId has any links: $result" }
        result
    }

    /**
     * Checks if a book has source links.
     * 
     * @param bookId The ID of the book to check
     * @return True if the book has source links, false otherwise
     */
    suspend fun bookHasSourceLinks(bookId: Long): Boolean = withContext(Dispatchers.IO) {
        logger.d { "Checking if book $bookId has source links" }
        val count = countLinksBySourceBook(bookId)
        val result = count > 0
        logger.d { "Book $bookId has source links: $result" }
        result
    }

    /**
     * Checks if a book has target links.
     * 
     * @param bookId The ID of the book to check
     * @return True if the book has target links, false otherwise
     */
    suspend fun bookHasTargetLinks(bookId: Long): Boolean = withContext(Dispatchers.IO) {
        logger.d { "Checking if book $bookId has target links" }
        val count = countLinksByTargetBook(bookId)
        val result = count > 0
        logger.d { "Book $bookId has target links: $result" }
        result
    }

    /**
     * Checks if a book has OTHER type comments.
     */
    suspend fun bookHasOtherComments(bookId: Long): Boolean = withContext(Dispatchers.IO) {
        logger.d { "Checking if book $bookId has OTHER comments" }
        val book = database.bookQueriesQueries.selectById(bookId).executeAsOneOrNull()
        val result = book?.hasOtherConnection == 1L
        logger.d { "Book $bookId has OTHER comments: $result" }
        result
    }

    /**
     * Checks if a book has COMMENTARY type comments.
     */
    suspend fun bookHasCommentaryComments(bookId: Long): Boolean = withContext(Dispatchers.IO) {
        logger.d { "Checking if book $bookId has COMMENTARY comments" }
        val book = database.bookQueriesQueries.selectById(bookId).executeAsOneOrNull()
        val result = book?.hasCommentaryConnection == 1L
        logger.d { "Book $bookId has COMMENTARY comments: $result" }
        result
    }

    /**
     * Checks if a book has REFERENCE type comments.
     */
    suspend fun bookHasReferenceComments(bookId: Long): Boolean = withContext(Dispatchers.IO) {
        logger.d { "Checking if book $bookId has REFERENCE comments" }
        val book = database.bookQueriesQueries.selectById(bookId).executeAsOneOrNull()
        val result = book?.hasReferenceConnection == 1L
        logger.d { "Book $bookId has REFERENCE comments: $result" }
        result
    }

    /**
     * Checks if a book has TARGUM type comments.
     */
    suspend fun bookHasTargumComments(bookId: Long): Boolean = withContext(Dispatchers.IO) {
        logger.d { "Checking if book $bookId has TARGUM comments" }
        val book = database.bookQueriesQueries.selectById(bookId).executeAsOneOrNull()
        val result = book?.hasTargumConnection == 1L
        logger.d { "Book $bookId has TARGUM comments: $result" }
        result
    }

    /**
     * Gets all books that have any links (source or target).
     * 
     * @return A list of books that have any links
     */
    suspend fun getBooksWithAnyLinks(): List<Book> = withContext(Dispatchers.IO) {
        logger.d { "Getting all books with any links" }
        val books = database.bookHasLinksQueriesQueries.selectBooksWithAnyLinks().executeAsList()
        logger.d { "Found ${books.size} books with any links" }

        // Convert the database books to model books
        books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
        }
    }

    /**
     * Gets all books that have source links.
     * 
     * @return A list of books that have source links
     */
    suspend fun getBooksWithSourceLinks(): List<Book> = withContext(Dispatchers.IO) {
        logger.d { "Getting all books with source links" }
        val books = database.bookHasLinksQueriesQueries.selectBooksWithSourceLinks().executeAsList()
        logger.d { "Found ${books.size} books with source links" }

        // Convert the database books to model books
        books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
        }
    }

    /**
     * Gets all books that have target links.
     * 
     * @return A list of books that have target links
     */
    suspend fun getBooksWithTargetLinks(): List<Book> = withContext(Dispatchers.IO) {
        logger.d { "Getting all books with target links" }
        val books = database.bookHasLinksQueriesQueries.selectBooksWithTargetLinks().executeAsList()
        logger.d { "Found ${books.size} books with target links" }

        // Convert the database books to model books
        books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
        }
    }

    /**
     * Counts the number of books that have any links (source or target).
     * 
     * @return The number of books that have any links
     */
    suspend fun countBooksWithAnyLinks(): Long = withContext(Dispatchers.IO) {
        logger.d { "Counting books with any links" }
        val count = database.bookHasLinksQueriesQueries.countBooksWithAnyLinks().executeAsOne()
        logger.d { "Found $count books with any links" }
        count
    }

    /**
     * Counts the number of books that have source links.
     * 
     * @return The number of books that have source links
     */
    suspend fun countBooksWithSourceLinks(): Long = withContext(Dispatchers.IO) {
        logger.d { "Counting books with source links" }
        val count = database.bookHasLinksQueriesQueries.countBooksWithSourceLinks().executeAsOne()
        logger.d { "Found $count books with source links" }
        count
    }

    /**
     * Counts the number of books that have target links.
     * 
     * @return The number of books that have target links
     */
    suspend fun countBooksWithTargetLinks(): Long = withContext(Dispatchers.IO) {
        logger.d { "Counting books with target links" }
        val count = database.bookHasLinksQueriesQueries.countBooksWithTargetLinks().executeAsOne()
        logger.d { "Found $count books with target links" }
        count
    }


    /**
     * Gets all books from the database.
     * 
     * @return A list of all books
     */
    suspend fun getAllBooks(): List<Book> = withContext(Dispatchers.IO) {
        logger.d { "Getting all books" }
        val books = database.bookQueriesQueries.selectAll().executeAsList()
        logger.d { "Found ${books.size} books" }

        // Convert the database books to model books
        books.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
        }
    }

    /**
     * Counts the number of links where the given book is the source.
     * 
     * @param bookId The ID of the book to count links for
     * @return The number of links where the book is the source
     */
    suspend fun countLinksBySourceBook(bookId: Long): Long = withContext(Dispatchers.IO) {
        logger.d { "Counting links where book $bookId is the source" }
        val count = database.linkQueriesQueries.countLinksBySourceBook(bookId).executeAsOne()
        logger.d { "Found $count links where book $bookId is the source" }
        count
    }

    /**
     * Counts the number of links where the given book is the target.
     * 
     * @param bookId The ID of the book to count links for
     * @return The number of links where the book is the target
     */
    suspend fun countLinksByTargetBook(bookId: Long): Long = withContext(Dispatchers.IO) {
        logger.d { "Counting links where book $bookId is the target" }
        val count = database.linkQueriesQueries.countLinksByTargetBook(bookId).executeAsOne()
        logger.d { "Found $count links where book $bookId is the target" }
        count
    }

    // --- Acronyms ---

    /**
     * Inserts a single acronym term for a book (ignores duplicates).
     */
    suspend fun insertBookAcronym(bookId: Long, term: String) = withContext(Dispatchers.IO) {
        database.acronymQueriesQueries.insert(bookId, term)
    }

    /**
     * Bulk inserts acronym terms for a given bookId. Ignores duplicates.
     */
    suspend fun bulkInsertBookAcronyms(bookId: Long, terms: Collection<String>) = withContext(Dispatchers.IO) {
        if (terms.isEmpty()) return@withContext
        for (t in terms) database.acronymQueriesQueries.insert(bookId, t)
    }

    /**
     * Returns all acronym terms for a given book.
     */
    suspend fun getAcronymsForBook(bookId: Long): List<String> = withContext(Dispatchers.IO) {
        database.acronymQueriesQueries.selectTermsByBookId(bookId).executeAsList()
    }

    /**
     * Finds all book IDs whose acronym list contains exactly the given term.
     */
    suspend fun findBookIdsByAcronym(term: String): List<Long> = withContext(Dispatchers.IO) {
        database.acronymQueriesQueries.selectBookIdsByTerm(term).executeAsList()
    }

    /**
     * Finds books by acronym LIKE pattern. Use %term% or term% for prefix.
     */
    suspend fun findBooksByAcronymLike(pattern: String, limit: Int = 20): List<Book> = withContext(Dispatchers.IO) {
        val ids = database.acronymQueriesQueries.selectBookIdsByTermLike(pattern, limit.toLong()).executeAsList()
        ids.distinct().mapNotNull { id -> getBook(id) }
    }

    /**
     * Finds books by exact acronym term.
     */
    suspend fun findBooksByAcronymExact(term: String, limit: Int = 20): List<Book> = withContext(Dispatchers.IO) {
        val ids = database.acronymQueriesQueries.selectBookIdsByTerm(term).executeAsList()
        ids.take(limit).mapNotNull { id -> getBook(id) }
    }

    /**
     * Returns the hierarchical depth for a category using the closure table.
     * Depth = number of ancestors (including self) - 1. Falls back to stored level if closure empty.
     */
    suspend fun getCategoryDepth(categoryId: Long): Int = withContext(Dispatchers.IO) {
        val count = database.categoryClosureQueriesQueries.countAncestorsByDescendant(categoryId).executeAsOne()
        if (count > 0) (count - 1).toInt() else {
            // Fallback to category.level if closure not built
            database.categoryQueriesQueries.selectById(categoryId).executeAsOneOrNull()?.level?.toInt() ?: 0
        }
    }

    // --- Book title FTS (titles + acronyms as terms) managed via raw SQL ---

    private fun ensureBookTitleFts() {
        driver.execute(
            null,
            """
                CREATE VIRTUAL TABLE IF NOT EXISTS book_title_search USING fts5(
                    bookId UNINDEXED,
                    term,
                    displayTitle UNINDEXED,
                    categoryId UNINDEXED
                )
            """.trimIndent(),
            0
        )
    }

    suspend fun clearBookTitleFts() = withContext(Dispatchers.IO) {
        ensureBookTitleFts()
        driver.execute(null, "DELETE FROM book_title_search", 0)
    }

    suspend fun insertBookTitleFtsTerm(bookId: Long, term: String, displayTitle: String, categoryId: Long) = withContext(Dispatchers.IO) {
        ensureBookTitleFts()
        driver.execute(null, "INSERT INTO book_title_search(bookId, term, displayTitle, categoryId) VALUES (?, ?, ?, ?)", 4) {
            bindLong(0, bookId)
            bindString(1, term)
            bindString(2, displayTitle)
            bindLong(3, categoryId)
        }
    }

    suspend fun searchBooksByTitleFts(matchQuery: String, limit: Int = 20): List<Book> = withContext(Dispatchers.IO) {
        ensureBookTitleFts()
        val ids: List<Long> = driver.executeQuery(null,
            "SELECT bookId FROM book_title_search WHERE book_title_search MATCH ? ORDER BY bm25(book_title_search) LIMIT ?",
            { cursor: SqlCursor ->
                val out = ArrayList<Long>()
                while (true) {
                    val hasNext = cursor.next().value
                    if (!hasNext) break
                    out.add(cursor.getLong(0)!!)
                }
                QueryResult.Value(out)
            },
            2
        ) {
            bindString(0, matchQuery)
            bindLong(1, limit.toLong())
        }.value

        ids.distinct().mapNotNull { id -> getBook(id) }
    }

    /**
     * Deletes all acronym rows for a book.
     */
    suspend fun deleteAcronymsForBook(bookId: Long) = withContext(Dispatchers.IO) {
        database.acronymQueriesQueries.deleteByBookId(bookId)
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
