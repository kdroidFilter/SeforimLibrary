package io.github.kdroidfilter.seforimlibrary.generator


import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.nameWithoutExtension
import kotlin.io.path.readText

/**
 * DatabaseGenerator is responsible for generating the Otzaria database from source files.
 * It processes directories, books, and links to create a structured database.
 *
 * @property sourceDirectory The path to the source directory containing the data files
 * @property repository The repository used to store the generated data
 */
class DatabaseGenerator(
    private val sourceDirectory: Path,
    private val repository: SeforimRepository
) {

    private val logger = Logger.withTag("DatabaseGenerator")


    private val json = Json {
        ignoreUnknownKeys = true
        coerceInputValues = true
    }
    private var nextBookId = 1L // Counter for book IDs
    private var nextLineId = 1L // Counter for line IDs
    private var nextTocEntryId = 1L // Counter for TOC entry IDs


    /**
     * Generates the database by processing metadata, directories, and links.
     * This is the main entry point for the database generation process.
     */
    suspend fun generate(): Unit = coroutineScope {
        logger.i { "Starting database generation..." }
        logger.i { "Source directory: $sourceDirectory" }

        try {
            // Disable foreign keys for better performance during bulk insertion
            logger.i { "Disabling foreign keys for better performance..." }
            disableForeignKeys()

            // Load metadata
            val metadata = loadMetadata()
            logger.i { "Metadata loaded: ${metadata.size} entries" }

            // Process hierarchy
            val libraryPath = sourceDirectory.resolve("אוצריא")
            if (!libraryPath.exists()) {
                throw IllegalStateException("The directory אוצריא does not exist in $sourceDirectory")
            }

            logger.i { "🚀 Starting to process library directory: $libraryPath" }
            processDirectory(libraryPath, null, 0, metadata)

            // Process links
            processLinks()

            // Re-enable foreign keys after all data is inserted
            logger.i { "Re-enabling foreign keys..." }
            enableForeignKeys()

            // Rebuild FTS5 index
            logger.i { "Rebuilding FTS5 index..." }
            rebuildFts5Index()

            logger.i { "Generation completed successfully!" }
        } catch (e: Exception) {
            // Make sure to re-enable foreign keys even if an error occurs
            try {
                enableForeignKeys()
            } catch (innerEx: Exception) {
                logger.w(innerEx) { "Error re-enabling foreign keys after failure" }
            }

            logger.e(e) { "Error during generation" }
            throw e
        }
    }

    /**
     * Loads book metadata from the metadata.json file.
     * Attempts to parse the file in different formats (Map or List).
     *
     * @return A map of book titles to their metadata
     */
    private suspend fun loadMetadata(): Map<String, BookMetadata> {
        val metadataFile = sourceDirectory.resolve("metadata.json")
        return if (metadataFile.exists()) {
            val content = metadataFile.readText()
            try {
                // Try to parse as Map first (original format)
                json.decodeFromString<Map<String, BookMetadata>>(content)
            } catch (e: Exception) {
                // If that fails, try to parse as List and convert to Map
                try {
                    val metadataList = json.decodeFromString<List<BookMetadata>>(content)
                    logger.i { "Parsed metadata as List with ${metadataList.size} entries" }
                    // Convert list to map using title as key
                    metadataList.associateBy { it.title }
                } catch (e: Exception) {
                    logger.i(e) { "Failed to parse metadata.json" }
                    emptyMap()
                }
            }
        } else {
            logger.w { "Metadata file metadata.json not found" }
            emptyMap()
        }
    }


    /**
     * Processes a directory recursively, creating categories and books.
     *
     * @param directory The directory to process
     * @param parentCategoryId The ID of the parent category, if any
     * @param level The current level in the directory hierarchy
     * @param metadata The metadata for books
     */
    private suspend fun processDirectory(
        directory: Path,
        parentCategoryId: Long?,
        level: Int,
        metadata: Map<String, BookMetadata>
    ) {
        logger.i { "=== Processing directory: ${directory.fileName} with parentCategoryId: $parentCategoryId (level: $level) ===" }

        Files.list(directory).use { stream ->
            val entries = stream.sorted { a, b ->
                a.fileName.toString().compareTo(b.fileName.toString())
            }.toList()

            logger.d { "Found ${entries.size} entries in directory ${directory.fileName}" }

            for (entry in entries) {
                when {
                    Files.isDirectory(entry) -> {
                        logger.d { "Processing subdirectory: ${entry.fileName} with parentId: $parentCategoryId" }
                        val categoryId = createCategory(entry, parentCategoryId, level)
                        logger.i { "✅ Created category '${entry.fileName}' with ID: $categoryId (parent: $parentCategoryId)" }
                        processDirectory(entry, categoryId, level + 1, metadata)
                    }

                    Files.isRegularFile(entry) && entry.extension == "txt" -> {
                        if (parentCategoryId == null) {
                            logger.w { "❌ Book found without category: $entry" }
                            continue
                        }
                        logger.i { "📚 Processing book ${entry.fileName} with categoryId: $parentCategoryId" }
                        createAndProcessBook(entry, parentCategoryId, metadata)
                    }

                    else -> {
                        logger.d { "Skipping entry: ${entry.fileName} (not a supported file type)" }
                    }
                }
            }
        }
        logger.i { "=== Finished processing directory: ${directory.fileName} ===" }
    }

    /**
     * Creates a category in the database.
     *
     * @param path The path representing the category
     * @param parentId The ID of the parent category, if any
     * @param level The level in the category hierarchy
     * @return The ID of the created category
     */
    private suspend fun createCategory(
        path: Path,
        parentId: Long?,
        level: Int
    ): Long {
        val title = path.fileName.toString()
        logger.i { "🏗️ Creating category: '$title' (level $level, parent: $parentId)" }

        val category = Category(
            parentId = parentId,
            title = title,
            level = level
        )

        val insertedId = repository.insertCategory(category)
        logger.i { "✅ Category '$title' created with ID: $insertedId" }

        // Additional verification
        val insertedCategory = repository.getCategory(insertedId)
        if (insertedCategory == null) {
            // Changed from error to warning level to reduce unnecessary error logs
            logger.w { "❌ WARNING: Unable to retrieve the category that was just inserted (ID: $insertedId)" }
        } else {
            logger.d { "✅ Verification: category retrieved with ID: ${insertedCategory.id}, parent: ${insertedCategory.parentId}" }
        }

        return insertedId
    }


    /**
     * Creates a book in the database and processes its content.
     *
     * @param path The path to the book file
     * @param categoryId The ID of the category the book belongs to
     * @param metadata The metadata for the book
     */
    private suspend fun createAndProcessBook(
        path: Path,
        categoryId: Long,
        metadata: Map<String, BookMetadata>
    ) {
        val filename = path.fileName.toString()
        val title = filename.substringBeforeLast('.')
        val meta = metadata[title]

        logger.i { "Processing book: $title with categoryId: $categoryId" }

        // Assign a unique ID to this book
        val currentBookId = nextBookId++
        logger.d { "Assigning ID $currentBookId to book '$title' with categoryId: $categoryId" }

        // Create author list if author is available in metadata
        val authors = meta?.author?.let { authorName ->
            listOf(Author(name = authorName))
        } ?: emptyList()

        // Create publication places list if pubPlace is available in metadata
        val pubPlaces = meta?.pubPlace?.let { pubPlaceName ->
            listOf(PubPlace(name = pubPlaceName))
        } ?: emptyList()

        // Create publication dates list if pubDate is available in metadata
        val pubDates = meta?.pubDate?.let { pubDateValue ->
            listOf(PubDate(date = pubDateValue))
        } ?: emptyList()

        val book = Book(
            id = currentBookId,
            categoryId = categoryId,
            title = title,
            authors = authors,
            pubPlaces = pubPlaces,
            pubDates = pubDates,
            heShortDesc = meta?.heShortDesc,
            order = meta?.order ?: 999f,
            topics = extractTopics(path)
        )

        logger.d { "Inserting book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}" }
        val insertedBookId = repository.insertBook(book)

        // ✅ Important verification: ensure that ID and categoryId are correct
        val insertedBook = repository.getBook(insertedBookId)
        if (insertedBook?.categoryId != categoryId) {
            logger.w { "WARNING: Book inserted with wrong categoryId! Expected: $categoryId, Got: ${insertedBook?.categoryId}" }
            // Correct the categoryId if necessary
            repository.updateBookCategoryId(insertedBookId, categoryId)
        }

        logger.d { "Book '${book.title}' inserted with ID: $insertedBookId and categoryId: $categoryId" }

        // Process content of the book
        processBookContent(path, insertedBookId)
    }

    /**
     * Processes the content of a book, extracting lines and TOC entries.
     *
     * @param path The path to the book file
     * @param bookId The ID of the book in the database
     */
    private suspend fun processBookContent(path: Path, bookId: Long) = coroutineScope {
        logger.d { "Processing content for book ID: $bookId" }
        logger.i { "Processing content of book ID: $bookId (ID generated by the database)" }

        val content = path.readText(Charsets.UTF_8)

        val lines = content.lines()
        logger.i { "Number of lines: ${lines.size}" }

        // Process each line one by one, handling TOC entries as we go
        processLinesWithTocEntries(bookId, lines)

        // Update the total number of lines
        repository.updateBookTotalLines(bookId, lines.size)

        logger.i { "Content processed successfully for book ID: $bookId (ID generated by the database)" }
    }


    /**
     * Processes lines of a book, identifying and creating TOC entries.
     *
     * @param bookId The ID of the book in the database
     * @param lines The lines of the book content
     */
    private suspend fun processLinesWithTocEntries(bookId: Long, lines: List<String>) {
        logger.d { "Processing lines and TOC entries together for book ID: $bookId" }

        // Map to track TOC entry parent relationships by level
        val parentStack = mutableMapOf<Int, Long>()

        for ((lineIndex, line) in lines.withIndex()) {
            val plainText = cleanHtml(line)
            val level = detectHeaderLevel(line)

            if (level > 0) {
                if (plainText.isBlank()) {
                    // Header is empty: skip creating a TOC entry
                    logger.d { "⚠️ Skipping empty header at level $level (line $lineIndex)" }
                    parentStack.remove(level)
                    continue
                }

                // Find parent: walk up until we find a valid parent
                val parentId = (level - 1 downTo 1).firstNotNullOfOrNull { parentStack[it] }

                val currentTocEntryId = nextTocEntryId++
                val currentLineId = nextLineId++

                val tocEntry = TocEntry(
                    id = currentTocEntryId,
                    bookId = bookId,
                    parentId = parentId,
                    text = plainText,
                    level = level,
                    lineId = null
                )

                val tocEntryId = repository.insertTocEntry(tocEntry)
                parentStack[level] = tocEntryId

                val lineId = repository.insertLine(
                    Line(
                        id = currentLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        plainText = plainText
                    )
                )
                repository.updateTocEntryLineId(tocEntryId, lineId)
                repository.updateLineTocEntry(lineId, tocEntryId)
            } else {
                // Regular line
                val currentLineId = nextLineId++
                repository.insertLine(
                    Line(
                        id = currentLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        plainText = plainText
                    )
                )
            }

            if (lineIndex % 1000 == 0) {
                logger.i { "Progress: $lineIndex/${lines.size} lines" }
            }
        }

        logger.i { "✅ Finished processing lines and TOC entries for book ID: $bookId" }
    }


    private fun cleanHtml(html: String): String {
        return HebrewTextUtils.removeNikud(
            Jsoup.clean(html, Safelist.none())
                .trim()
                .replace("\\s+".toRegex(), " ")
        )
    }


    private fun detectHeaderLevel(line: String): Int {
        return when {
            line.startsWith("<h1", ignoreCase = true) -> 1
            line.startsWith("<h2", ignoreCase = true) -> 2
            line.startsWith("<h3", ignoreCase = true) -> 3
            line.startsWith("<h4", ignoreCase = true) -> 4
            line.startsWith("<h5", ignoreCase = true) -> 5
            line.startsWith("<h6", ignoreCase = true) -> 6
            else -> 0
        }
    }

    /**
     * Processes all link files in the links directory.
     * Links connect lines between different books.
     */
    private suspend fun processLinks() {
        val linksDir = sourceDirectory.resolve("links")
        if (!linksDir.exists()) {
            logger.w { "Links directory not found" }
            return
        }

        // Count links before processing
        val linksBefore = repository.countLinks()
        logger.d { "Links in database before processing: $linksBefore" }

        logger.i { "Processing links..." }
        var totalLinks = 0

        Files.list(linksDir).use { stream ->
            stream.filter { it.extension == "json" }.forEach { linkFile ->
                runBlocking {
                    val processedLinks = processLinkFile(linkFile)
                    totalLinks += processedLinks
                    logger.d { "Processed $processedLinks links from ${linkFile.fileName}, total so far: $totalLinks" }
                }
            }
        }

        // Count links after processing
        val linksAfter = repository.countLinks()
        logger.d { "Links in database after processing: $linksAfter" }
        logger.d { "Added ${linksAfter - linksBefore} links to the database" }

        logger.i { "Total of $totalLinks links processed" }

        // Update the book_has_links table
        updateBookHasLinksTable()
    }

    /**
     * Processes a single link file, creating links between books.
     *
     * @param linkFile The path to the link file
     * @return The number of links successfully processed
     */
    private suspend fun processLinkFile(linkFile: Path): Int {
        val bookTitle = linkFile.nameWithoutExtension.removeSuffix("_links")
        logger.d { "Processing link file for book: $bookTitle" }

        // Find the source book
        val sourceBook = repository.getBookByTitle(bookTitle)

        if (sourceBook == null) {
            logger.w { "Source book not found for links: $bookTitle" }
            return 0
        }
        logger.d { "Found source book with ID: ${sourceBook.id}" }

        try {
            val content = linkFile.readText()
            logger.d { "Link file content length: ${content.length}" }
            val links = json.decodeFromString<List<LinkData>>(content)
            logger.d { "Decoded ${links.size} links from file" }
            var processed = 0

            for ((index, linkData) in links.withIndex()) {
                try {
                    // Find the target book
                    // Handle paths with backslashes
                    val path = linkData.path_2
                    val targetTitle = if (path.contains('\\')) {
                        // Extract the last component of a backslash-separated path
                        val lastComponent = path.split('\\').last()
                        // Remove file extension if present
                        lastComponent.substringBeforeLast('.', lastComponent)
                    } else {
                        // Use the standard path handling for forward slash paths
                        val targetPath = Paths.get(path)
                        targetPath.fileName.toString().substringBeforeLast('.')
                    }
                    logger.d { "Link ${index + 1}/${links.size} - Target book title: $targetTitle" }

                    // Try to find the target book
                    val targetBook = repository.getBookByTitle(targetTitle)
                    if (targetBook == null) {
                        // Enhanced logging for debugging
                        logger.i { "Link ${index + 1}/${links.size} - Target book not found: $targetTitle" }
                        logger.i { "Original path: ${linkData.path_2}" }
                        continue
                    }
                    logger.d { "Using target book with ID: ${targetBook.id}" }

                    // Find the lines
                    // Adjust indices from 1-based to 0-based
                    val sourceLineIndex = (linkData.line_index_1.toInt() - 1).coerceAtLeast(0)
                    val targetLineIndex = (linkData.line_index_2.toInt() - 1).coerceAtLeast(0)

                    logger.d { "Looking for source line at index: $sourceLineIndex (original: ${linkData.line_index_1}) in book ${sourceBook.id}" }

                    // Try to find the source line
                    val sourceLine = repository.getLineByIndex(sourceBook.id, sourceLineIndex)
                    if (sourceLine == null) {
                        logger.d { "Source line not found at index: $sourceLineIndex, skipping this link but continuing with others" }
                        continue
                    }
                    logger.d { "Using source line with ID: ${sourceLine.id}" }

                    logger.d { "Looking for target line at index: $targetLineIndex (original: ${linkData.line_index_2}) in book ${targetBook.id}" }

                    // Try to find the target line
                    val targetLine = repository.getLineByIndex(targetBook.id, targetLineIndex)
                    if (targetLine == null) {
                        logger.d { "Target line not found at index: $targetLineIndex, skipping this link but continuing with others" }
                        continue
                    }
                    logger.d { "Using target line with ID: ${targetLine.id}" }

                    val link = Link(
                        sourceBookId = sourceBook.id,
                        targetBookId = targetBook.id,
                        sourceLineId = sourceLine.id,
                        targetLineId = targetLine.id,
                        connectionType = ConnectionType.fromString(linkData.connectionType)
                    )

                    logger.d { "Inserting link from book ${sourceBook.id} to book ${targetBook.id}" }
                    val linkId = repository.insertLink(link)
                    logger.d { "Link inserted with ID: $linkId" }
                    processed++
                } catch (e: Exception) {
                    // Changed from error to debug level to reduce unnecessary error logs
                    logger.d(e) { "Error processing link: ${linkData.heRef_2}" }
                    logger.d { "Error processing link: ${e.message}" }
                }
            }
            logger.d { "Processed $processed links out of ${links.size}" }
            return processed
        } catch (e: Exception) {
            // Changed from error to warning level to reduce unnecessary error logs
            logger.w(e) { "Error processing link file: ${linkFile.fileName}" }
            logger.d { "Error processing link file: ${e.message}" }
            return 0
        }
    }

    /**
     * Extracts topics from the file path.
     * Topics are derived from the directory structure.
     *
     * @param path The path to the book file
     * @return A list of topics extracted from the path
     */
    private fun extractTopics(path: Path): List<Topic> {
        // Extract topics from the path
        val parts = path.toString().split(File.separator)
        val topicNames = parts.dropLast(1).takeLast(2)

        return topicNames.map { name ->
            Topic(name = name)
        }
    }

    /**
     * Disables foreign key constraints in the database to improve performance during bulk insertion.
     * This should be called before starting the data generation process.
     */
    private suspend fun disableForeignKeys() {
        logger.d { "Disabling foreign key constraints" }
        repository.executeRawQuery("PRAGMA foreign_keys = OFF")
    }

    /**
     * Re-enables foreign key constraints in the database after data insertion is complete.
     * This should be called after all data has been inserted to ensure data integrity.
     */
    private suspend fun enableForeignKeys() {
        logger.d { "Re-enabling foreign key constraints" }
        repository.executeRawQuery("PRAGMA foreign_keys = ON")
    }

    /**
     * Rebuilds the FTS5 index for the line_search table.
     * This should be called after all data has been inserted to ensure optimal search performance.
     */
    private suspend fun rebuildFts5Index() {
        logger.d { "Rebuilding FTS5 index for line_search table" }
        repository.rebuildFts5Index()
        logger.i { "FTS5 index rebuilt successfully" }
    }

    /**
     * Updates the book_has_links table to indicate which books have source links, target links, or both.
     * This should be called after all links have been processed.
     */
    private suspend fun updateBookHasLinksTable() {
        logger.i { "Updating book_has_links table with separate source and target link flags..." }

        // Get all books
        val books = repository.getAllBooks()
        logger.d { "Found ${books.size} books to check for links" }

        var booksWithSourceLinks = 0
        var booksWithTargetLinks = 0
        var booksWithAnyLinks = 0
        var processedBooks = 0

        // For each book, check if it has source links and/or target links
        for (book in books) {
            // Check if the book has any links as source
            val hasSourceLinks = repository.countLinksBySourceBook(book.id) > 0

            // Check if the book has any links as target
            val hasTargetLinks = repository.countLinksByTargetBook(book.id) > 0

            // Update the book_has_links table with separate flags for source and target links
            repository.updateBookHasLinks(book.id, hasSourceLinks, hasTargetLinks)

            // Update counters
            if (hasSourceLinks) {
                booksWithSourceLinks++
            }
            if (hasTargetLinks) {
                booksWithTargetLinks++
            }
            if (hasSourceLinks || hasTargetLinks) {
                booksWithAnyLinks++
            }

            processedBooks++

            // Log progress every 100 books
            if (processedBooks % 100 == 0) {
                logger.d { "Processed $processedBooks/${books.size} books: " +
                        "$booksWithSourceLinks with source links, " +
                        "$booksWithTargetLinks with target links, " +
                        "$booksWithAnyLinks with any links" }
            }
        }

        logger.i { "Book_has_links table updated. Found:" }
        logger.i { "- $booksWithSourceLinks books with source links" }
        logger.i { "- $booksWithTargetLinks books with target links" }
        logger.i { "- $booksWithAnyLinks books with any links (source or target)" }
        logger.i { "- ${books.size} total books" }
    }



    // Internal classes

    /**
     * Data class representing a link between two books.
     * Used for deserializing link data from JSON files.
     */
    @Serializable
    private data class LinkData(
        val heRef_2: String,
        val line_index_1: Double,
        val path_2: String,
        val line_index_2: Double,
        @SerialName("Conection Type")
        val connectionType: String = ""
    )
}
