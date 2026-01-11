package io.github.kdroidfilter.seforimlibrary.dao.repository



import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.SqlDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocStructure
import io.github.kdroidfilter.seforimlibrary.core.models.Author
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.LineAltTocMapping
import io.github.kdroidfilter.seforimlibrary.core.models.LineTocMapping
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import io.github.kdroidfilter.seforimlibrary.core.models.PubPlace
import io.github.kdroidfilter.seforimlibrary.core.models.Source
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.Topic
import io.github.kdroidfilter.seforimlibrary.dao.extensions.toModel
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.env.getEnvironmentVariable
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
        val repositoryLoggingEnv = getEnvironmentVariable("SEFORIMAPP_REPOSITORY_LOGGING")?.lowercase()
        if (repositoryLoggingEnv == "true" || repositoryLoggingEnv == "1" || repositoryLoggingEnv == "yes") {
            Logger.setMinSeverity(Severity.Verbose)
        } else {
            Logger.setMinSeverity(Severity.Assert)
        }
        logger.d{"Initializing SeforimRepository"}
        // Create the database schema (fresh builds only; no runtime migrations needed)
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

    // --- Transactions ---
    // Avoid custom transaction management that wraps the entire generation.
    // Use SQLDelight's default behavior (per‑statement auto-commit) at call sites.
    // Keep signature for compatibility with existing callers.
    suspend fun <T> runInTransaction(block: suspend () -> T): T = block()

    // --- ID helpers ---

    suspend fun getMaxBookId(): Long = withContext(Dispatchers.IO) {
        driver.executeQuery(
            identifier = null,
            sql = "SELECT COALESCE(MAX(id),0) FROM book",
            mapper = { c: SqlCursor ->
                val v = if (c.next().value) c.getLong(0) else null
                QueryResult.Value(v ?: 0L)
            },
            parameters = 0
        ).value
    }

    suspend fun getMaxLineId(): Long = withContext(Dispatchers.IO) {
        driver.executeQuery(
            identifier = null,
            sql = "SELECT COALESCE(MAX(id),0) FROM line",
            mapper = { c: SqlCursor ->
                val v = if (c.next().value) c.getLong(0) else null
                QueryResult.Value(v ?: 0L)
            },
            parameters = 0
        ).value
    }

    suspend fun getMaxTocEntryId(): Long = withContext(Dispatchers.IO) {
        driver.executeQuery(
            identifier = null,
            sql = "SELECT COALESCE(MAX(id),0) FROM tocEntry",
            mapper = { c: SqlCursor ->
                val v = if (c.next().value) c.getLong(0) else null
                QueryResult.Value(v ?: 0L)
            },
            parameters = 0
        ).value
    }

    suspend fun setSynchronous(mode: String) = withContext(Dispatchers.IO) {
        driver.execute(null, "PRAGMA synchronous=$mode", 0)
    }

    suspend fun setSynchronousOff() = setSynchronous("OFF")
    suspend fun setSynchronousNormal() = setSynchronous("NORMAL")

    suspend fun setJournalMode(mode: String) = withContext(Dispatchers.IO) {
        driver.execute(null, "PRAGMA journal_mode=$mode", 0)
    }
    suspend fun setJournalModeOff() = setJournalMode("OFF")
    suspend fun setJournalModeWal() = setJournalMode("WAL")

    /**
     * Inserts multiple line_toc mappings in a single batch.
     * Assumes the database is in a fresh import state (no existing mappings for these lineIds).
     */
    suspend fun insertLineTocBatch(mappings: List<Pair<Long, Long>>) = withContext(Dispatchers.IO) {
        if (mappings.isEmpty()) return@withContext
        database.transaction {
            mappings.forEach { (lineId, tocEntryId) ->
                database.lineTocQueriesQueries.insert(
                    lineId = lineId,
                    tocEntryId = tocEntryId
                )
            }
        }
    }

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
     * Returns all TOC entries for a book (flat list).
     */
    suspend fun getTocEntriesForBook(bookId: Long): List<TocEntry> = withContext(Dispatchers.IO) {
        database.tocQueriesQueries.selectByBookId(bookId).executeAsList().map { it.toModel() }
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
     * Returns all descendant category IDs (including the category itself) using the
     * category_closure table. This is a bulk way to scope by category without recursive calls.
     */
    suspend fun getDescendantCategoryIds(ancestorId: Long): List<Long> = withContext(Dispatchers.IO) {
        database.categoryClosureQueriesQueries.selectDescendants(ancestorId).executeAsList()
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
                level = category.level.toLong(),
                orderIndex = category.order.toLong()
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
     * This is a heavy call: it performs additional queries to fetch authors, topics,
     * publication places and dates. Prefer [getBookCore] in hot paths where only
     * basic book metadata (id, title, categoryId, order, totalLines, flags) is needed.
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
     * Lightweight variant of [getBook] that only reads the core book row without
     * joining authors, topics, publication places or dates.
     *
     * Suitable for search suggestions, navigation trees and other scenarios where
     * only id/title/category information is required.
     */
    suspend fun getBookCore(id: Long): Book? = withContext(Dispatchers.IO) {
        val bookData = database.bookQueriesQueries.selectById(id).executeAsOneOrNull() ?: return@withContext null
        return@withContext bookData.toModel(json)
    }

    /**
     * Lightweight helper for commentary flows: loads core book data and publication
     * dates without joining authors, topics, or publication places.
     */
    suspend fun getBookWithPubDates(id: Long): Book? = withContext(Dispatchers.IO) {
        val bookData = database.bookQueriesQueries.selectById(id).executeAsOneOrNull() ?: return@withContext null
        val pubDates = getBookPubDates(bookData.id)
        return@withContext bookData.toModel(json, pubDates = pubDates)
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
     * Retrieves all books under the given ancestor category (including the category itself)
     * using the category_closure table in a single query.
     */
    suspend fun getBooksUnderCategoryTree(ancestorCategoryId: Long): List<Book> = withContext(Dispatchers.IO) {
        val rows = database.bookQueriesQueries.selectByAncestorCategory(ancestorCategoryId).executeAsList()
        rows.map { bookData ->
            val authors = getBookAuthors(bookData.id)
            val topics = getBookTopics(bookData.id)
            val pubPlaces = getBookPubPlaces(bookData.id)
            val pubDates = getBookPubDates(bookData.id)
            bookData.toModel(json, authors, pubPlaces, pubDates).copy(topics = topics)
        }
    }

    suspend fun getAllBookAltFlags(): Map<Long, Boolean> = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.selectAltFlags().executeAsList()
            .associate { row -> row.id to (row.hasAltStructures == 1L) }
    }

    /**
     * Finds books whose title matches the LIKE pattern. Use %term% for contains.
     *
     * This version loads full metadata (authors, topics, publication info) for each match.
     * For lightweight use cases (e.g., typeahead suggestions), prefer [findBooksByTitleLikeCore].
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

    /**
     * Lightweight variant of [findBooksByTitleLike] that returns only core book metadata
     * without issuing extra queries for authors, topics, publication places or dates.
     */
    suspend fun findBooksByTitleLikeCore(pattern: String, limit: Int = 20): List<Book> = withContext(Dispatchers.IO) {
        val rows = database.bookQueriesQueries.selectManyByTitleLike(pattern, limit.toLong()).executeAsList()
        rows.map { bookData -> bookData.toModel(json) }
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
                sourceId = book.sourceId,
                title = book.title,
                heShortDesc = book.heShortDesc,
                notesContent = book.notesContent,
                orderIndex = book.order.toLong(),
                totalLines = book.totalLines.toLong(),
                isBaseBook = if (book.isBaseBook) 1 else 0,
                hasSourceConnection = if (book.hasSourceConnection) 1 else 0,
                hasAltStructures = if (book.hasAltStructures) 1 else 0
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
                sourceId = book.sourceId,
                title = book.title,
                heShortDesc = book.heShortDesc,
                notesContent = book.notesContent,
                orderIndex = book.order.toLong(),
                totalLines = book.totalLines.toLong(),
                isBaseBook = if (book.isBaseBook) 1 else 0,
                hasSourceConnection = if (book.hasSourceConnection) 1 else 0,
                hasAltStructures = if (book.hasAltStructures) 1 else 0
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

    // --- Sources ---

    /**
     * Returns a Source by name, or null if not found.
     */
    suspend fun getSourceByName(name: String): Source? = withContext(Dispatchers.IO) {
        database.sourceQueriesQueries.selectByName(name).executeAsOneOrNull()?.toModel()
    }

    /**
     * Returns a Source by id, or null if not found.
     */
    suspend fun getSourceById(id: Long): Source? = withContext(Dispatchers.IO) {
        database.sourceQueriesQueries.selectById(id).executeAsOneOrNull()?.toModel()
    }

    /**
     * Inserts a source if missing and returns its id.
     */
    suspend fun insertSource(name: String): Long = withContext(Dispatchers.IO) {
        // Check existing
        val existing = database.sourceQueriesQueries.selectByName(name).executeAsOneOrNull()
        if (existing != null) return@withContext existing.id

        database.sourceQueriesQueries.insert(name)
        val id = database.sourceQueriesQueries.lastInsertRowId().executeAsOne()
        if (id == 0L) {
            // Try to read back just in case
            val again = database.sourceQueriesQueries.selectByName(name).executeAsOneOrNull()
            if (again != null) return@withContext again.id
            throw RuntimeException("Failed to insert source '$name'")
        }
        id
    }

    suspend fun updateBookTotalLines(bookId: Long, totalLines: Int) = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.updateTotalLines(totalLines.toLong(), bookId)
    }

    suspend fun updateBookCategoryId(bookId: Long, categoryId: Long) = withContext(Dispatchers.IO) {
        logger.d{"Updating book $bookId with categoryId: $categoryId"}
        database.bookQueriesQueries.updateCategoryId(categoryId, bookId)
        logger.d{"Updated book $bookId with categoryId: $categoryId"}
    }

    suspend fun updateHasAltStructures(bookId: Long, hasAltStructures: Boolean) = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.updateHasAltStructures(if (hasAltStructures) 1 else 0, bookId)
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

    suspend fun getLinesByIds(ids: Collection<Long>): List<Line> =
        withContext(Dispatchers.IO) {
            if (ids.isEmpty()) return@withContext emptyList()
            database.lineQueriesQueries.selectByIds(ids).executeAsList().map { it.toModel() }
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

    /**
     * Inserts multiple lines in a single transaction.
     * This is optimized for bulk import where IDs are provided explicitly.
     */
    suspend fun insertLinesBatch(lines: List<Line>) = withContext(Dispatchers.IO) {
        if (lines.isEmpty()) return@withContext

        database.transaction {
            lines.forEach { line ->
                // For batch import we expect explicit IDs > 0
                database.lineQueriesQueries.insertWithId(
                    id = line.id,
                    bookId = line.bookId,
                    lineIndex = line.lineIndex.toLong(),
                    content = line.content,
                    heRef = line.heRef,
                    tocEntryId = null
                )
            }
        }
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
                heRef = line.heRef,
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
                heRef = line.heRef,
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

                throw RuntimeException("Failed to insert line for book ${line.bookId} at index ${line.lineIndex} - insertion returned ID 0. Context: content='${line.content.take(50)}${if (line.content.length > 50) "..." else ""}')")
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

    // --- Alternative TOC structures ---

    suspend fun getAltTocStructuresForBook(bookId: Long): List<AltTocStructure> = withContext(Dispatchers.IO) {
        database.altTocStructureQueriesQueries.selectByBookId(bookId).executeAsList().map { it.toModel() }
    }

    suspend fun clearAltTocForBook(bookId: Long) = withContext(Dispatchers.IO) {
        database.altTocStructureQueriesQueries.deleteByBookId(bookId)
        database.lineAltTocQueriesQueries.deleteByBookId(bookId)
    }

    suspend fun upsertAltTocStructure(structure: AltTocStructure): Long = withContext(Dispatchers.IO) {
        val existing = database.altTocStructureQueriesQueries.selectByBookId(structure.bookId)
            .executeAsList()
            .firstOrNull { it.key == structure.key }
        if (existing != null) return@withContext existing.id

        if (structure.id > 0) {
            database.altTocStructureQueriesQueries.insertWithId(
                id = structure.id,
                bookId = structure.bookId,
                key = structure.key,
                title = structure.title,
                heTitle = structure.heTitle
            )
            return@withContext structure.id
        }

        database.altTocStructureQueriesQueries.insert(
            bookId = structure.bookId,
            key = structure.key,
            title = structure.title,
            heTitle = structure.heTitle
        )
        val id = database.altTocStructureQueriesQueries.lastInsertRowId().executeAsOne()
        if (id == 0L) {
            val fallback = database.altTocStructureQueriesQueries.selectByBookId(structure.bookId)
                .executeAsList()
                .firstOrNull { it.key == structure.key }?.id
            if (fallback != null) return@withContext fallback
            throw RuntimeException("Failed to insert alt_toc_structure for book ${structure.bookId} with key ${structure.key}")
        }
        id
    }

    suspend fun insertAltTocEntry(entry: AltTocEntry): Long = withContext(Dispatchers.IO) {
        val textId = entry.textId ?: getOrCreateTocText(entry.text)
        if (entry.id > 0) {
            database.altTocEntryQueriesQueries.insertWithId(
                id = entry.id,
                structureId = entry.structureId,
                parentId = entry.parentId,
                textId = textId,
                level = entry.level.toLong(),
                lineId = entry.lineId,
                isLastChild = if (entry.isLastChild) 1 else 0,
                hasChildren = if (entry.hasChildren) 1 else 0
            )
            return@withContext entry.id
        }

        database.altTocEntryQueriesQueries.insert(
            structureId = entry.structureId,
            parentId = entry.parentId,
            textId = textId,
            level = entry.level.toLong(),
            lineId = entry.lineId,
            isLastChild = if (entry.isLastChild) 1 else 0,
            hasChildren = if (entry.hasChildren) 1 else 0
        )
        val id = database.altTocEntryQueriesQueries.lastInsertRowId().executeAsOne()
        if (id == 0L) {
            val fallback = database.altTocEntryQueriesQueries.selectAltEntriesByStructureId(entry.structureId)
                .executeAsList()
                .firstOrNull { it.textId == textId && it.parentId == entry.parentId && it.level == entry.level.toLong() }?.id
            if (fallback != null) return@withContext fallback
            throw RuntimeException("Failed to insert alt_toc_entry for structure ${entry.structureId} with text '${entry.text}'")
        }
        id
    }

    suspend fun updateAltTocEntryHasChildren(altTocEntryId: Long, hasChildren: Boolean) = withContext(Dispatchers.IO) {
        database.altTocEntryQueriesQueries.updateHasChildren(if (hasChildren) 1 else 0, altTocEntryId)
    }

    suspend fun updateAltTocEntryIsLastChild(altTocEntryId: Long, isLastChild: Boolean) = withContext(Dispatchers.IO) {
        database.altTocEntryQueriesQueries.updateIsLastChild(if (isLastChild) 1 else 0, altTocEntryId)
    }

    suspend fun updateAltTocEntryLineId(altTocEntryId: Long, lineId: Long) = withContext(Dispatchers.IO) {
        database.altTocEntryQueriesQueries.updateLineId(lineId, altTocEntryId)
    }

    suspend fun getAltTocEntry(id: Long): AltTocEntry? = withContext(Dispatchers.IO) {
        database.altTocEntryQueriesQueries.selectAltTocEntryById(id).executeAsOneOrNull()?.toModel()
    }

    suspend fun getAltRootToc(structureId: Long): List<AltTocEntry> = withContext(Dispatchers.IO) {
        database.altTocEntryQueriesQueries.selectAltRootByStructureId(structureId).executeAsList().map { it.toModel() }
    }

    suspend fun getAltTocChildren(parentId: Long): List<AltTocEntry> = withContext(Dispatchers.IO) {
        database.altTocEntryQueriesQueries.selectAltChildren(parentId).executeAsList().map { it.toModel() }
    }

    suspend fun getAltTocEntriesForStructure(structureId: Long): List<AltTocEntry> = withContext(Dispatchers.IO) {
        database.altTocEntryQueriesQueries.selectAltEntriesByStructureId(structureId).executeAsList().map { it.toModel() }
    }

    // --- Alternative line ⇄ TOC mapping ---

    suspend fun upsertLineAltToc(lineId: Long, structureId: Long, altTocEntryId: Long) = withContext(Dispatchers.IO) {
        database.lineAltTocQueriesQueries.upsert(lineId, structureId, altTocEntryId)
    }

    suspend fun getAltTocEntryIdForLine(lineId: Long, structureId: Long): Long? = withContext(Dispatchers.IO) {
        database.lineAltTocQueriesQueries.selectAltTocEntryIdByLineAndStructure(lineId, structureId).executeAsOneOrNull()
    }

    suspend fun getLineAltTocMappings(structureId: Long): List<LineAltTocMapping> = withContext(Dispatchers.IO) {
        database.lineAltTocQueriesQueries.selectByStructure(structureId).executeAsList().map {
            LineAltTocMapping(
                lineId = it.lineId,
                structureId = it.structureId,
                altTocEntryId = it.altTocEntryId
            )
        }
    }

    suspend fun getLineIdsForAltTocEntry(altTocEntryId: Long): List<Long> = withContext(Dispatchers.IO) {
        database.lineAltTocQueriesQueries.selectLineIdsByAltTocEntry(altTocEntryId).executeAsList()
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

    /**
     * Lightweight variant for prefetch/navigation use cases; omits target text content.
     */
    suspend fun getCommentarySummariesForLines(
        lineIds: List<Long>,
        activeCommentatorIds: Set<Long> = emptySet()
    ): List<CommentarySummary> = withContext(Dispatchers.IO) {
        database.linkQueriesQueries.selectLinkSummariesBySourceLineIds(lineIds).executeAsList()
            .filter { activeCommentatorIds.isEmpty() || it.targetBookId in activeCommentatorIds }
            .map {
                CommentarySummary(
                    link = Link(
                        id = it.id,
                        sourceBookId = it.sourceBookId,
                        targetBookId = it.targetBookId,
                        sourceLineId = it.sourceLineId,
                        targetLineId = it.targetLineId,
                        connectionType = ConnectionType.fromString(it.connectionType)
                    ),
                    targetBookTitle = it.targetBookTitle
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
        connectionTypes: Set<ConnectionType> = setOf(ConnectionType.COMMENTARY),
        offset: Int,
        limit: Int
    ): List<CommentaryWithText> = withContext(Dispatchers.IO) {
        if (lineIds.isEmpty()) return@withContext emptyList()
        if (connectionTypes.isEmpty()) return@withContext emptyList()
        val typeNames = connectionTypes.map { it.name }
        if (activeCommentatorIds.isEmpty()) {
            database.linkQueriesQueries.selectLinksBySourceLineIdsAndTypesPaged(
                lineIds,
                typeNames,
                limit.toLong(),
                offset.toLong()
            ).executeAsList().map {
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
        } else {
            database.linkQueriesQueries.selectLinksBySourceLineIdsTargetsAndTypesPaged(
                lineIds,
                activeCommentatorIds.toList(),
                typeNames,
                limit.toLong(),
                offset.toLong()
            ).executeAsList().map {
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
            // Dedup: return existing link id if already present
            val existingId = driver.executeQuery(
                identifier = null,
                sql = "SELECT id FROM link WHERE sourceBookId=? AND targetBookId=? AND sourceLineId=? AND targetLineId=? LIMIT 1",
                mapper = { c: SqlCursor ->
                    val v = if (c.next().value) c.getLong(0) else null
                    QueryResult.Value(v)
                },
                parameters = 4
            ) {
                bindLong(0, link.sourceBookId)
                bindLong(1, link.targetBookId)
                bindLong(2, link.sourceLineId)
                bindLong(3, link.targetLineId)
            }.value
            if (existingId != null) {
                logger.d { "Link already exists with ID $existingId, skipping insert." }
                return@withContext existingId
            }

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
     * Inserts multiple links in a single transaction.
     * Optimized for bulk import; assumes no duplicate (sourceBookId, targetBookId, sourceLineId, targetLineId) rows.
     */
    suspend fun insertLinksBatch(links: List<Link>) = withContext(Dispatchers.IO) {
        if (links.isEmpty()) return@withContext

        // Pre-resolve connection type IDs per type name to avoid repeated lookups.
        val typeIdCache = mutableMapOf<String, Long>()
        for (link in links) {
            val name = link.connectionType.name
            if (name !in typeIdCache) {
                typeIdCache[name] = getOrCreateConnectionType(name)
            }
        }

        database.transaction {
            links.forEach { link ->
                val connectionTypeId = typeIdCache[link.connectionType.name]
                    ?: error("Missing connection type id for ${link.connectionType.name}")

                database.linkQueriesQueries.insert(
                    sourceBookId = link.sourceBookId,
                    targetBookId = link.targetBookId,
                    sourceLineId = link.sourceLineId,
                    targetLineId = link.targetLineId,
                    connectionTypeId = connectionTypeId
                )
            }
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

    // Search functions removed (migrated to Lucene in app layer).

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

    // FTS rebuild removed (Lucene managed externally by generator).

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
        hasSource: Boolean,
        hasCommentary: Boolean,
        hasOther: Boolean
    ) = withContext(Dispatchers.IO) {
        val t = if (hasTargum) 1L else 0L
        val r = if (hasReference) 1L else 0L
        val s = if (hasSource) 1L else 0L
        val c = if (hasCommentary) 1L else 0L
        val o = if (hasOther) 1L else 0L
        database.bookQueriesQueries.updateConnectionFlags(t, r, s, c, o, bookId)
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
     * Returns the IDs of all base books (isBaseBook = 1).
     */
    suspend fun getBaseBookIds(): List<Long> = withContext(Dispatchers.IO) {
        database.bookQueriesQueries.selectBaseIds().executeAsList()
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

    // --- Default commentators ---

    /**
     * Returns the list of commentator book IDs configured as defaults for the given book.
     */
    suspend fun getDefaultCommentatorIdsForBook(bookId: Long): List<Long> = withContext(Dispatchers.IO) {
        database.defaultCommentatorQueriesQueries.selectByBookId(bookId).executeAsList()
    }

    /**
     * Replaces the default commentators list for a given book with the provided ordered IDs.
     */
    suspend fun setDefaultCommentatorsForBook(bookId: Long, commentatorBookIds: List<Long>) = withContext(Dispatchers.IO) {
        database.defaultCommentatorQueriesQueries.deleteByBookId(bookId)
        commentatorBookIds.forEachIndexed { index, commentatorBookId ->
            database.defaultCommentatorQueriesQueries.insert(
                bookId = bookId,
                commentatorBookId = commentatorBookId,
                position = index.toLong()
            )
        }
    }

    /**
     * Deletes all default commentator mappings from the database.
     * Intended for use during full database regeneration.
     */
    suspend fun clearAllDefaultCommentators() = withContext(Dispatchers.IO) {
        database.defaultCommentatorQueriesQueries.deleteAll()
    }

    // --- Default targumim ---

    /**
     * Returns the list of targum book IDs configured as defaults for the given book.
     */
    suspend fun getDefaultTargumIdsForBook(bookId: Long): List<Long> = withContext(Dispatchers.IO) {
        database.defaultTargumQueriesQueries.selectByBookId(bookId).executeAsList()
    }

    /**
     * Replaces the default targum list for a given book with the provided ordered IDs.
     */
    suspend fun setDefaultTargumForBook(bookId: Long, targumBookIds: List<Long>) = withContext(Dispatchers.IO) {
        database.defaultTargumQueriesQueries.deleteByBookId(bookId)
        targumBookIds.forEachIndexed { index, targumBookId ->
            database.defaultTargumQueriesQueries.insert(
                bookId = bookId,
                targumBookId = targumBookId,
                position = index.toLong()
            )
        }
    }

    /**
     * Deletes all default targum mappings from the database.
     * Intended for use during full database regeneration.
     */
    suspend fun clearAllDefaultTargum() = withContext(Dispatchers.IO) {
        database.defaultTargumQueriesQueries.deleteAll()
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

    // Legacy book-title FTS removed (handled by Lucene index in generator/app).

    /**
     * Deletes all acronym rows for a book.
     */
    suspend fun deleteAcronymsForBook(bookId: Long) = withContext(Dispatchers.IO) {
        database.acronymQueriesQueries.deleteByBookId(bookId)
    }

    /**
     * Returns all line IDs that are heading lines (content starts with <h1, <h2, <h3, or <h4).
     * Used during link processing to filter out links to heading lines.
     */
    suspend fun getHeadingLineIds(): Set<Long> = withContext(Dispatchers.IO) {
        val result = mutableSetOf<Long>()
        driver.executeQuery(
            identifier = null,
            sql = """
                SELECT id FROM line
                WHERE content LIKE '<h1%'
                   OR content LIKE '<h2%'
                   OR content LIKE '<h3%'
                   OR content LIKE '<h4%'
            """.trimIndent(),
            mapper = { cursor: SqlCursor ->
                while (cursor.next().value) {
                    cursor.getLong(0)?.let { result.add(it) }
                }
                QueryResult.Value(Unit)
            },
            parameters = 0
        ).await()
        result
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

data class CommentarySummary(
    val link: Link,
    val targetBookTitle: String
)
