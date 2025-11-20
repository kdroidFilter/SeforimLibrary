package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory
import java.io.File

/**
 * Converts Sefaria data to SQLite database
 */
class SefariaToSQLiteConverter(
    private val repository: SeforimRepository,
    private val sefariaBaseDir: File
) {
    private val logger = LoggerFactory.getLogger(SefariaToSQLiteConverter::class.java)
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
        coerceInputValues = true
    }
    private val tableOfContents = loadTableOfContents()

    // Maps to track IDs
    private val categoryMap = mutableMapOf<String, Long>() // Hebrew title -> DB ID
    private val bookMap = mutableMapOf<String, Long>() // Hebrew title -> DB ID
    private val bookMapByEnglish = mutableMapOf<String, Long>() // English title -> DB ID
    private val authorMap = mutableMapOf<String, Long>() // Hebrew name -> DB ID
    private val topicMap = mutableMapOf<String, Long>() // Hebrew name -> DB ID
    private val pubPlaceMap = mutableMapOf<String, Long>() // Hebrew name -> DB ID
    private val pubDateMap = mutableMapOf<String, Long>() // Date string -> DB ID
    private val sourceMap = mutableMapOf<String, Long>() // Source name -> DB ID

    // Parser instances
    private val textParser = SefariaTextParser()
    private val citationParser = SefariaCitationParser()

    // Schema cache for later text processing
    private val schemaCache = mutableMapOf<String, SefariaSchema>()

    // Book structures for citation-to-line-index calculation
    private val bookStructures = mutableMapOf<String, SefariaCitationParser.BookStructure>()

    // Index of merged.json files by English title (mergedText.title)
    private val mergedJsonFileByTitle = mutableMapOf<String, File>()

    // Per-book, per-section seif start indices: bookTitle -> sectionName -> (siman, seif) -> first lineIndex
    private val seifIndexCache = mutableMapOf<String, MutableMap<String, MutableMap<Pair<Int, Int>, Int>>>()

    // Track line counts per book for offset calculation
    private val bookLineOffsets = mutableMapOf<Long, Int>()

    // Cache line IDs by book and index for faster link processing
    private val lineIdCache = mutableMapOf<Long, MutableMap<Int, Long>>()

    // Statistics
    data class ConversionStats(
        var schemasProcessed: Int = 0,
        var schemaErrors: Int = 0,
        var textsProcessed: Int = 0,
        var textErrors: Int = 0,
        var linksProcessed: Int = 0,
        var linksCreated: Int = 0,
        var categoriesCreated: Int = 0,
        var booksCreated: Int = 0,
        var linesCreated: Int = 0
    )

    private val stats = ConversionStats()

    suspend fun initialize() {
        // Initialize source for Sefaria
        val sefariaSourceId = repository.insertSource("Sefaria")
        sourceMap["Sefaria"] = sefariaSourceId

        logger.info("Initialized Sefaria source with ID: $sefariaSourceId")
        // Connection types will be created automatically when inserting links
    }

    suspend fun convert() {
        logger.info("Starting Sefaria to SQLite conversion...")

        withContext(Dispatchers.IO) {
            // Step 1: Process schemas to create categories and books
            logger.info("Processing schemas...")
            processSchemas()

            // Step 2: Process merged.json files to create lines and TOC
            logger.info("Processing texts...")
            processTexts()

            // Step 3: Process links
            logger.info("Processing links...")
            processLinks()

            // Print statistics
            printStatistics()

            logger.info("Conversion complete!")
        }
    }

    private fun printStatistics() {
        logger.info("============================================================")
        logger.info("Conversion Statistics:")
        logger.info("  Schemas: ${stats.schemasProcessed} processed, ${stats.schemaErrors} errors")
        logger.info("  Categories: ${stats.categoriesCreated} created")
        logger.info("  Books: ${stats.booksCreated} created")
        logger.info("  Texts: ${stats.textsProcessed} processed, ${stats.textErrors} errors")
        logger.info("  Lines: ${stats.linesCreated} created")
        logger.info("  Links: ${stats.linksCreated} created from ${stats.linksProcessed} processed")
        logger.info("============================================================")
    }

    private fun loadTableOfContents(): SefariaTableOfContents? {
        val tocFile = File(sefariaBaseDir, "table_of_contents.json")
        if (!tocFile.exists()) {
            logger.warn("table_of_contents.json not found in ${sefariaBaseDir.absolutePath}; falling back to English category names")
            return null
        }

        return runCatching { SefariaTableOfContents.fromFile(tocFile, json) }
            .onFailure { logger.error("Failed to parse table_of_contents.json at ${tocFile.absolutePath}", it) }
            .getOrNull()
    }

    private suspend fun processSchemas() {
        val schemasDir = File(sefariaBaseDir, "schemas")
        if (!schemasDir.exists()) {
            logger.error("Schemas directory not found: ${schemasDir.absolutePath}")
            return
        }

        val schemaFiles = schemasDir.listFiles { file -> file.extension == "json" }
        logger.info("Found ${schemaFiles?.size ?: 0} schema files")

        schemaFiles?.forEach { schemaFile ->
            try {
                processSchemaFile(schemaFile)
                stats.schemasProcessed++
            } catch (e: Exception) {
                logger.error("Error processing schema file ${schemaFile.name}", e)
                stats.schemaErrors++
            }
        }
    }

    private suspend fun processSchemaFile(file: File) {
        // Read and validate file content
        val content = file.readText().trim()
        if (content.isEmpty()) {
            logger.warn("Skipping empty schema file: ${file.name}")
            return
        }

        val schema = json.decodeFromString<SefariaSchema>(content)

        // Cache schema for later use
        schemaCache[schema.title] = schema

        // Build book structure for citation parsing
        buildBookStructure(schema)

        // Create category hierarchy
        val categoryId = createCategoryHierarchy(schema.categories)

        // Get or create authors
        val authorIds = schema.authors.map { author ->
            authorMap.getOrPut(author.he) {
                repository.insertAuthor(author.he)
            }
        }

        // Get or create publication places
        val pubPlaceIds = mutableListOf<Long>()
        schema.pubPlaceString?.he?.let { hePlace ->
            pubPlaceIds.add(pubPlaceMap.getOrPut(hePlace) {
                repository.insertPubPlace(hePlace)
            })
        }

        // Get or create publication dates
        val pubDateIds = mutableListOf<Long>()
        schema.pubDate?.forEach { year ->
            val dateStr = year.toString()
            pubDateIds.add(pubDateMap.getOrPut(dateStr) {
                repository.insertPubDate(dateStr)
            })
        }

        // Determine if this is a base book or commentary
        val isBaseBook = schema.dependence != "Commentary"

        val tocBookInfo = tableOfContents?.bookInfo(schema.title)
        val bookOrder = tocBookInfo?.order ?: 999f
        val hebrewBookTitle = tocBookInfo?.hebrewTitle ?: schema.heTitle

        // Create book
        val book = Book(
            categoryId = categoryId,
            sourceId = sourceMap["Sefaria"]!!,
            title = hebrewBookTitle,
            heShortDesc = schema.heShortDesc ?: schema.heDesc,
            order = bookOrder.toFloat(),
            isBaseBook = isBaseBook
        )

        val bookId = repository.insertBook(book)
        bookMap[hebrewBookTitle] = bookId
        bookMapByEnglish[schema.title] = bookId
        stats.booksCreated++

        // TODO: Associate authors, publication places and dates with books
        // The repository doesn't currently have insertBookAuthor, insertBookPubPlace methods
        // These would need to be added to the repository or handled differently

        logger.debug("Created book: ${schema.heTitle} (ID: $bookId)")
    }

    private suspend fun createCategoryHierarchy(categories: List<String>): Long {
        if (categories.isEmpty()) {
            // Create default category
            return categoryMap.getOrPut("אחר") {
                repository.insertCategory(Category(title = "אחר", level = 0))
            }
        }

        var parentId: Long? = null
        var currentLevel = 0

        val categoryNames = tableOfContents?.hebrewCategoryPath(categories) ?: run {
            if (tableOfContents != null) {
                logger.warn("Could not resolve Hebrew names for categories ${categories.joinToString(" > ")} from table_of_contents.json; using English names")
            }
            categories
        }

        categoryNames.forEach { hebrewCategoryName ->
            val key = if (parentId == null) hebrewCategoryName else "$parentId/$hebrewCategoryName"

            val categoryId = categoryMap.getOrPut(key) {
                val category = Category(
                    parentId = parentId,
                    title = hebrewCategoryName,
                    level = currentLevel
                )
                val id = repository.insertCategory(category)
                stats.categoriesCreated++

                // TODO: Category closure table would be rebuilt at the end
                // repository.insertCategoryClosure doesn't exist yet
                // It should be built with repository.rebuildCategoryClosure() at the end

                id
            }

            parentId = categoryId
            currentLevel++
        }

        return parentId!!
    }

    private suspend fun processTexts() {
        val jsonDir = File(sefariaBaseDir, "json")
        if (!jsonDir.exists()) {
            logger.error("JSON directory not found: ${jsonDir.absolutePath}")
            return
        }

        // Recursively find all merged.json files
        val mergedFiles = findMergedJsonFiles(jsonDir)
        logger.info("Found ${mergedFiles.size} merged.json files")

        mergedFiles.forEach { file ->
            try {
                processTextFile(file)
            } catch (e: Exception) {
                logger.error("Error processing text file ${file.absolutePath}", e)
            }
        }
    }

    private fun findMergedJsonFiles(dir: File): List<File> {
        val result = mutableListOf<File>()

        dir.listFiles()?.forEach { file ->
            when {
                file.isDirectory -> result.addAll(findMergedJsonFiles(file))
                file.name == "merged.json" -> result.add(file)
            }
        }

        return result
    }

    private suspend fun processTextFile(file: File) {
        try {
            val mergedText = json.decodeFromString<SefariaMergedText>(file.readText())

            // Find book by English title
            val bookId = bookMapByEnglish[mergedText.title]
            if (bookId == null) {
                logger.warn("Could not find book for title: ${mergedText.title}")
                return
            }

            // Parse the text structure and create lines and TOC
            parseAndInsertText(bookId, mergedText)

            logger.debug("Processed text for book: ${mergedText.title} (ID: $bookId)")
        } catch (e: Exception) {
            logger.error("Error processing text file ${file.absolutePath}", e)
        }
    }

    private suspend fun parseAndInsertText(bookId: Long, mergedText: SefariaMergedText) {
        try {
            // Get schema for this book
            val schema = schemaCache[mergedText.title]

            // Parse the text using the text parser
            val parsed = textParser.parse(bookId, mergedText, schema)

            // Initialize cache for this book
            val bookCache = mutableMapOf<Int, Long>()
            lineIdCache[bookId] = bookCache

            // Insert lines and cache their IDs
            parsed.lines.forEachIndexed { index, line ->
                val lineId = repository.insertLine(line)
                bookCache[index] = lineId
            }

            // Insert TOC entries with temporary ID mapping
            // Map temporary parser IDs to real database-generated IDs
            val tempIdToRealId = mutableMapOf<Long, Long>()

            parsed.tocEntries.forEach { tocEntry ->
                // Get the line index for this TOC entry
                val lineIndex = parsed.tocLineIndices[tocEntry.id]
                val lineId = if (lineIndex != null && lineIndex < parsed.lines.size) {
                    bookCache[lineIndex]
                } else null

                // Map parent ID from temporary to real
                val realParentId = if (tocEntry.parentId != null) {
                    tempIdToRealId[tocEntry.parentId]
                } else null

                // Create TOC entry with real parent ID and line ID
                val tocToInsert = TocEntry(
                    id = 0,  // Let database auto-generate
                    bookId = tocEntry.bookId,
                    parentId = realParentId,
                    level = tocEntry.level,
                    text = tocEntry.text,
                    lineId = lineId
                )

                val realTocId = repository.insertTocEntry(tocToInsert)
                tempIdToRealId[tocEntry.id] = realTocId

                // Update line's TOC entry reference if we have a line
                if (lineId != null) {
                    repository.updateLineTocEntry(lineId, realTocId)
                }
            }

            // Second pass: update hasChildren and isLastChild flags
            if (parsed.tocEntries.isNotEmpty()) {
                updateTocEntryFlags(bookId, parsed.tocEntries, tempIdToRealId)
            }

            // Build line→TOC ownership so features relying on line_toc (breadcrumbs, filters)
            // behave like the main Otzaria generator.
            repository.rebuildLineTocForBook(bookId)

            // Update book's total lines
            repository.updateBookTotalLines(bookId, parsed.lines.size)

            // Store line count for this book (for link processing)
            bookLineOffsets[bookId] = parsed.lines.size

            logger.info("Inserted ${parsed.lines.size} lines and ${parsed.tocEntries.size} TOC entries for book ID: $bookId")
        } catch (e: Exception) {
            logger.error("Error parsing and inserting text for book ID: $bookId", e)
        }
    }

    /**
     * Second pass: update hasChildren and isLastChild flags for TOC entries
     * Inspired by Otzaria generator approach
     */
    private suspend fun updateTocEntryFlags(
        bookId: Long,
        tocEntries: List<TocEntry>,
        tempIdToRealId: Map<Long, Long>
    ) {
        try {
            // Group entries by real parent ID
            val entriesByRealParent = tocEntries.groupBy { entry ->
                if (entry.parentId != null) tempIdToRealId[entry.parentId] else null
            }

            // Create a set of real IDs that have children (map temp parent IDs to real IDs)
            val realParentIds = tocEntries.mapNotNull { entry ->
                entry.parentId?.let { tempIdToRealId[it] }
            }.toSet()

            // Update hasChildren flag for all entries that are parents
            for (entry in tocEntries) {
                val realTocId = tempIdToRealId[entry.id]
                if (realTocId != null && realTocId in realParentIds) {
                    repository.updateTocEntryHasChildren(realTocId, true)
                }
            }

            // Update isLastChild flag for the last child of each parent
            for ((realParentId, children) in entriesByRealParent) {
                if (children.isNotEmpty()) {
                    // The last child in the list is the last child
                    val lastChildTempId = children.last().id
                    val lastChildRealId = tempIdToRealId[lastChildTempId]
                    if (lastChildRealId != null) {
                        repository.updateTocEntryIsLastChild(lastChildRealId, true)
                    }
                }
            }

            logger.debug("Updated TOC entry flags for ${tocEntries.size} entries in book $bookId")
        } catch (e: Exception) {
            logger.error("Error updating TOC entry flags for book $bookId", e)
        }
    }

    /**
     * Build book structure from schema for citation-to-line-index calculation
     */
    private fun buildBookStructure(schema: SefariaSchema) {
        try {
            val schemaNode = schema.schema ?: return

            // Simple case: JaggedArrayNode with lengths
            if (schemaNode.nodeType == "JaggedArrayNode" && schemaNode.lengths != null) {
                val depth = schemaNode.depth ?: schemaNode.lengths.size

                when (depth) {
                    1 -> {
                        // Single dimension - simple list
                        bookStructures[schema.title] = SefariaCitationParser.BookStructure()
                    }
                    2 -> {
                        // Two dimensions (e.g., chapter:verse)
                        // Use lengths to build chapter lengths
                        val chapterCount = schemaNode.lengths[0]
                        val totalVerses = if (schemaNode.lengths.size > 1) schemaNode.lengths[1] else 0

                        // We don't have individual chapter lengths, so use average
                        val avgVersesPerChapter = if (chapterCount > 0) totalVerses / chapterCount else 0
                        val chapterLengths = List(chapterCount) { avgVersesPerChapter }

                        bookStructures[schema.title] = SefariaCitationParser.BookStructure(
                            chapterLengths = chapterLengths
                        )
                    }
                    3 -> {
                        // Three dimensions (e.g., chapter:verse:comment)
                        // Similar to depth 2 but with additional nesting
                        val level1Count = schemaNode.lengths.getOrNull(0) ?: 0
                        val level2Count = schemaNode.lengths.getOrNull(1) ?: 0

                        val avgPerLevel1 = if (level1Count > 0) level2Count / level1Count else 0
                        val chapterLengths = List(level1Count) { avgPerLevel1 }

                        bookStructures[schema.title] = SefariaCitationParser.BookStructure(
                            chapterLengths = chapterLengths
                        )
                    }
                }
            } else if (schemaNode.nodeType == null && schemaNode.nodes != null) {
                // Schema without nodeType but with nodes - use default structure
                bookStructures[schema.title] = SefariaCitationParser.BookStructure()
            }
        } catch (e: Exception) {
            logger.debug("Failed to build structure for ${schema.title}", e)
        }
    }

    private suspend fun processLinks() {
        val linksDir = File(sefariaBaseDir, "links")
        if (!linksDir.exists()) {
            logger.error("Links directory not found: ${linksDir.absolutePath}")
            return
        }

        // Process all links*.csv files
        val linkFiles = linksDir.listFiles { file ->
            file.name.startsWith("links") &&
            file.name.endsWith(".csv") &&
            !file.name.contains("by_book")
        }

        logger.info("Found ${linkFiles?.size ?: 0} link CSV files")

        var totalLinksProcessed = 0
        var totalLinksCreated = 0

        linkFiles?.forEach { file ->
            try {
                val stats = processLinkFile(file)
                totalLinksProcessed += stats.first
                totalLinksCreated += stats.second
                logger.info("Processed ${stats.first} links from ${file.name}, created ${stats.second} links")
            } catch (e: Exception) {
                logger.error("Error processing link file ${file.name}", e)
            }
        }

        logger.info("Total links processed: $totalLinksProcessed, created: $totalLinksCreated")

        // After all links are inserted, update per-book link flags
        if (totalLinksCreated > 0) {
            logger.info("Updating book_has_links and connection flags for Sefaria-generated links...")
            updateBookLinkFlags()
        } else {
            logger.info("No links created; skipping book_has_links / connection flag update")
        }
    }

    private suspend fun processLinkFile(file: File): Pair<Int, Int> {
        var processed = 0
        var created = 0

        file.useLines { lines ->
            lines.drop(1).forEach { line -> // Skip header
                try {
                    val parts = parseCsvLine(line)
                    if (parts.size >= 7) {
                        processed++
                        val link = SefariaLink(
                            citation1 = parts[0],
                            citation2 = parts[1],
                            connectionType = parts[2],
                            text1 = parts[3],
                            text2 = parts[4],
                            category1 = parts[5],
                            category2 = parts[6]
                        )
                        // Process link
                        if (processLink(link)) {
                            created++
                        }
                    }
                } catch (e: Exception) {
                    logger.debug("Error parsing CSV line: $line", e)
                }
            }
        }

        return Pair(processed, created)
    }

    private fun parseCsvLine(line: String): List<String> {
        val result = mutableListOf<String>()
        var current = StringBuilder()
        var inQuotes = false

        for (char in line) {
            when {
                char == '"' -> inQuotes = !inQuotes
                char == ',' && !inQuotes -> {
                    result.add(current.toString())
                    current = StringBuilder()
                }
                else -> current.append(char)
            }
        }
        result.add(current.toString())

        return result
    }

    private suspend fun processLink(link: SefariaLink): Boolean {
        try {
            // Parse citations
            val citation1 = citationParser.parse(link.citation1)
            val citation2 = citationParser.parse(link.citation2)

            if (citation1 == null || citation2 == null) {
                logger.debug("Could not parse citations: ${link.citation1} -> ${link.citation2}")
                return false
            }

            // Find books by title (English title from schema / merged.json)
            val bookId1 = bookMapByEnglish[citation1.bookTitle]
            val bookId2 = bookMapByEnglish[citation2.bookTitle]

            if (bookId1 == null || bookId2 == null) {
                logger.debug("Could not find books for link: ${citation1.bookTitle} -> ${citation2.bookTitle}")
                return false
            }

            // Validate line indices
            val maxLines1 = bookLineOffsets[bookId1] ?: 0
            val maxLines2 = bookLineOffsets[bookId2] ?: 0

            if (maxLines1 <= 0 || maxLines2 <= 0) {
                logger.debug("No lines for books in link: ${citation1.bookTitle} (lines=$maxLines1), ${citation2.bookTitle} (lines=$maxLines2)")
                return false
            }

            // Calculate line indices for both sides (prefer schema-based structure, fall back to heuristic)
            val lineIndex1 = calculateLineIndexForBook(citation1, bookId1, maxLines1)
            val lineIndex2 = calculateLineIndexForBook(citation2, bookId2, maxLines2)

            if (lineIndex1 < 0 || lineIndex1 >= maxLines1) {
                logger.debug("Line index for first citation out of range: $lineIndex1 (max: $maxLines1) for ${link.citation1}")
                return false
            }

            if (lineIndex2 < 0 || lineIndex2 >= maxLines2) {
                logger.debug("Line index for second citation out of range: $lineIndex2 (max: $maxLines2) for ${link.citation2}")
                return false
            }

            // Get line IDs
            val lineId1 = getLineIdByBookAndIndex(bookId1, lineIndex1)
            val lineId2 = getLineIdByBookAndIndex(bookId2, lineIndex2)

            if (lineId1 == null || lineId2 == null) {
                logger.debug("Could not find line IDs for link: ${link.citation1} -> ${link.citation2}")
                return false
            }

            val schema1 = schemaCache[citation1.bookTitle]
            val schema2 = schemaCache[citation2.bookTitle]

            fun isDependent(schema: SefariaSchema?): Boolean {
                val dep = schema?.dependence ?: return false
                return dep.equals("Commentary", ignoreCase = true) ||
                        dep.equals("Targum", ignoreCase = true)
            }

            fun isTargum(schema: SefariaSchema?): Boolean {
                val dep = schema?.dependence ?: return false
                return dep.equals("Targum", ignoreCase = true)
            }

            // Determine connection type
            val rawType = link.connectionType.lowercase()
            val connectionType = when {
                rawType == "commentary" ->
                    io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.COMMENTARY
                rawType == "targum" ->
                    io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.TARGUM
                rawType == "quotation" ->
                    io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.REFERENCE
                rawType.isBlank() && isDependent(schema1) != isDependent(schema2) -> {
                    // Infer when CSV omits connection type but one side is a commentary/targum
                    if (isTargum(schema1) || isTargum(schema2)) {
                        io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.TARGUM
                    } else {
                        io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.COMMENTARY
                    }
                }
                else ->
                    io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.OTHER
            }

            // Decide link orientation:
            // For COMMENTARY/TARGUM, CSV semantics (see LINKS_GUIDE) are Text1 = commentary on Text2.
            // We want base text as sourceBook/sourceLine so UI can fetch commentaries from base lines.
            // Use schema.dependence when available, otherwise keep the CSV convention.
            val sourceBookId: Long
            val sourceLineId: Long
            val targetBookId: Long
            val targetLineId: Long

            if (connectionType == io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.COMMENTARY ||
                connectionType == io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.TARGUM
            ) {
                // Default: Text2 is base, Text1 is commentary (per LINKS_GUIDE)
                var srcBook = bookId2
                var srcLine = lineId2
                var tgtBook = bookId1
                var tgtLine = lineId1

                val dep1 = isDependent(schema1)
                val dep2 = isDependent(schema2)

                when {
                    dep1 && !dep2 -> {
                        // citation1 is commentary/targum on citation2 (keep default orientation)
                    }
                    dep2 && !dep1 -> {
                        // citation2 is commentary/targum on citation1 -> flip
                        srcBook = bookId1
                        srcLine = lineId1
                        tgtBook = bookId2
                        tgtLine = lineId2
                    }
                    // else: keep CSV convention
                }

                sourceBookId = srcBook
                sourceLineId = srcLine
                targetBookId = tgtBook
                targetLineId = tgtLine
            } else {
                // Reference/other: respect CSV ordering
                sourceBookId = bookId1
                sourceLineId = lineId1
                targetBookId = bookId2
                targetLineId = lineId2
            }

            // Create the link
            val linkModel = Link(
                sourceBookId = sourceBookId,
                targetBookId = targetBookId,
                sourceLineId = sourceLineId,
                targetLineId = targetLineId,
                connectionType = connectionType
            )

            repository.insertLink(linkModel)
            logger.debug("Created link: ${link.text1} -> ${link.text2} (type: $connectionType)")
            return true

        } catch (e: Exception) {
            logger.debug("Error processing link: ${link.citation1} -> ${link.citation2}", e)
            return false
        }
    }

    /**
     * Calculate a line index for a given citation and book.
     *
     * Strategy:
     * - If a structured schema-based mapping exists (bookStructures), use it and keep it if in range.
     * - Otherwise, or if out of range, fall back to a simple heuristic that maps references into
     *   the available line range [0, maxLines).
     *
     * This allows link resolution to work even when we only imported a subset of books/texts
     * (for example, only Tur + classic commentaries) without full per-chapter lengths.
     */
    private fun calculateLineIndexForBook(
        citation: SefariaCitationParser.Citation,
        bookId: Long,
        maxLines: Int
    ): Int {
        if (maxLines <= 0) return -1

        // 0) Try precise seif-based index using merged.json structure when we have a section and at least Siman/Seif
        val section = citation.section
        if (section != null && citation.references.size >= 2) {
            val siman = citation.references[0]
            val seif = citation.references[1]
            val fromSeifIndex = getLineIndexFromSeifIndex(citation.bookTitle, section, siman, seif)
            if (fromSeifIndex != null && fromSeifIndex in 0 until maxLines) {
                return fromSeifIndex
            }
        }

        // 1) Try schema-based structure if available
        val structure = bookStructures[citation.bookTitle]
        if (structure != null) {
            val idx = citationParser.calculateLineIndex(citation, structure)
            if (idx in 0 until maxLines) {
                return idx
            }
        }

        // 2) Fallback heuristic based only on reference numbers
        val refs = citation.references
        if (refs.isEmpty()) {
            return 0
        }

        fun Int.safe() = if (this <= 0) 1 else this

        val idx = when (refs.size) {
            1 -> refs[0].safe() - 1
            2 -> {
                // Treat first ref as a coarse "chapter"/"siman" index and the second as a finer index
                val level1 = refs[0].safe() - 1
                val level2 = refs[1].safe() - 1
                level1 * 50 + level2
            }
            else -> {
                // Three-level address (e.g., Siman:Seif:Paragraph) – spread more coarsely
                val level1 = refs[0].safe() - 1
                val level2 = refs[1].safe() - 1
                val level3 = refs[2].safe() - 1
                level1 * 200 + level2 * 10 + level3
            }
        }

        return idx.coerceIn(0, maxLines - 1)
    }

    /**
     * Returns the starting line index for a given (section, siman, seif) if we have a precise
     * mapping built from merged.json for this book; otherwise returns null.
     */
    private fun getLineIndexFromSeifIndex(
        bookTitle: String,
        section: String,
        siman: Int,
        seif: Int
    ): Int? {
        if (siman <= 0 || seif <= 0) return null
        ensureSeifIndexForBook(bookTitle)
        val perSection = seifIndexCache[bookTitle] ?: return null
        val seifMap = perSection[section] ?: return null
        return seifMap[siman to seif]
    }

    /**
     * Ensure we have a seif index built for the given book title.
     * This lazily scans merged.json files under sefariaBaseDir/json and builds
     * a mapping from (Siman, Seif) to first line index for each section.
     */
    private fun ensureSeifIndexForBook(bookTitle: String) {
        if (seifIndexCache.containsKey(bookTitle)) return
        ensureMergedJsonIndex()
        val file = mergedJsonFileByTitle[bookTitle] ?: return
        runCatching {
            val merged = json.decodeFromString<SefariaMergedText>(file.readText())
            val index = buildSeifIndexFromMerged(merged)
            seifIndexCache[bookTitle] = index
        }
    }

    /**
     * Build an index of (Siman, Seif) -> first lineIndex for each section in a merged.json file.
     * This mirrors the traversal order used by SefariaTextParser for complex object-based texts.
     */
    private fun buildSeifIndexFromMerged(
        merged: SefariaMergedText
    ): MutableMap<String, MutableMap<Pair<Int, Int>, Int>> {
        val result = mutableMapOf<String, MutableMap<Pair<Int, Int>, Int>>()
        var currentLineIndex = 0
        var currentSiman = 0
        var currentSeif = 0

        fun indexElement(
            sectionName: String,
            element: JsonElement,
            depth: Int,
            seifMap: MutableMap<Pair<Int, Int>, Int>
        ) {
            when (element) {
                is JsonArray -> {
                    element.forEachIndexed { idx, child ->
                        when {
                            depth == 0 -> {
                                // Arrays directly under a section: treat as Simanim
                                currentSiman = idx + 1
                                currentSeif = 0
                            }
                            depth == 1 && currentSiman == 0 -> {
                                // Special case: arrays under an object inside a section
                                // (e.g., Tur: Orach Chayim -> { Introduction, \"\" -> [Simanim] })
                                // where depth 1 still represents Simanim.
                                currentSiman = idx + 1
                                currentSeif = 0
                            }
                            depth == 1 -> {
                                // Second level under Siman: treat as Seif
                                currentSeif = idx + 1
                            }
                        }
                        indexElement(sectionName, child, depth + 1, seifMap)
                    }
                }
                is JsonObject -> {
                    element.forEach { (_, value) ->
                        indexElement(sectionName, value, depth + 1, seifMap)
                    }
                }
                is JsonPrimitive -> {
                    if (element.isString && element.content.isNotBlank()) {
                        if (currentSiman > 0 && currentSeif > 0 && depth >= 2) {
                            val key = currentSiman to currentSeif
                            if (!seifMap.containsKey(key)) {
                                seifMap[key] = currentLineIndex
                            }
                        }
                        currentLineIndex++
                    }
                }
            }
        }

        val textElement = merged.text
        if (textElement is JsonObject) {
            textElement.forEach { (sectionKey, sectionValue) ->
                val sectionName = sectionKey
                val seifMap = result.getOrPut(sectionName) { mutableMapOf() }

                if (sectionValue is JsonObject && sectionValue.isNotEmpty() && sectionValue[""] != null) {
                    // Special case: section is an object with a default \"\" key holding the main
                    // JaggedArray (e.g., Tur: Orach Chayim / Yoreh De'ah / Even HaEzer / Choshen Mishpat).
                    // First, walk non-default keys (Introductions etc.) only to advance currentLineIndex,
                    // then reset Siman/Seif and build the seif index from the default array.
                    sectionValue.forEach { (innerKey, innerValue) ->
                        if (innerKey.isNotEmpty()) {
                            indexElement(sectionName, innerValue, 0, seifMap)
                        }
                    }
                    currentSiman = 0
                    currentSeif = 0
                    sectionValue[""]?.let { defaultArray ->
                        indexElement(sectionName, defaultArray, 0, seifMap)
                    }
                } else {
                    indexElement(sectionName, sectionValue, 0, seifMap)
                }

                // Reset structural indices between sections
                currentSiman = 0
                currentSeif = 0
            }
        }
        return result
    }

    /**
     * Build an index of merged.json files under sefariaBaseDir/json keyed by mergedText.title.
     */
    private fun ensureMergedJsonIndex() {
        if (mergedJsonFileByTitle.isNotEmpty()) return
        val jsonRoot = File(sefariaBaseDir, "json")
        if (!jsonRoot.exists()) return

        jsonRoot.walkTopDown()
            .filter { it.isFile && it.name == "merged.json" }
            .forEach { file ->
                runCatching {
                    val text = file.readText()
                    val merged = json.decodeFromString<SefariaMergedText>(text)
                    if (merged.title.isNotBlank()) {
                        mergedJsonFileByTitle[merged.title] = file
                    }
                }
            }
    }

    /**
     * Update book_has_links table and per-book connection-type flags
     * (hasTargumConnection, hasReferenceConnection, hasCommentaryConnection, hasOtherConnection)
     * based on the links currently present in the database.
     *
     * This mirrors the behavior of the main Otzaria generator so that Sefaria-generated
     * links integrate cleanly with the rest of the tooling.
     */
    private suspend fun updateBookLinkFlags() {
        val allBooks = repository.getAllBooks()

        for (book in allBooks) {
            val bookId = book.id

            // Source/target presence
            val hasSourceLinks = repository.bookHasSourceLinks(bookId)
            val hasTargetLinks = repository.bookHasTargetLinks(bookId)
            repository.updateBookHasLinks(bookId, hasSourceLinks, hasTargetLinks)

            // Per-connection-type flags (TARGUM, REFERENCE, COMMENTARY, OTHER)
            val hasTargum =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.TARGUM.name) +
                        repository.countLinksByTargetBookAndType(bookId, ConnectionType.TARGUM.name) > 0
            val hasReference =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.REFERENCE.name) +
                        repository.countLinksByTargetBookAndType(bookId, ConnectionType.REFERENCE.name) > 0
            val hasCommentary =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.COMMENTARY.name) +
                        repository.countLinksByTargetBookAndType(bookId, ConnectionType.COMMENTARY.name) > 0
            val hasOther =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.OTHER.name) +
                        repository.countLinksByTargetBookAndType(bookId, ConnectionType.OTHER.name) > 0

            repository.updateBookConnectionFlags(
                bookId = bookId,
                hasTargum = hasTargum,
                hasReference = hasReference,
                hasCommentary = hasCommentary,
                hasOther = hasOther
            )
        }
    }

    /**
     * Get line ID by book ID and line index
     * Uses cache for performance
     */
    private suspend fun getLineIdByBookAndIndex(bookId: Long, lineIndex: Int): Long? {
        return try {
            // Try cache first
            lineIdCache[bookId]?.get(lineIndex)?.let { return it }

            // If not in cache, load from DB
            val lines = repository.getLines(bookId, lineIndex, lineIndex)
            val lineId = lines.firstOrNull()?.id

            // Cache for next time
            if (lineId != null) {
                lineIdCache.getOrPut(bookId) { mutableMapOf() }[lineIndex] = lineId
            }

            lineId
        } catch (e: Exception) {
            logger.debug("Error getting line ID for book $bookId at index $lineIndex", e)
            null
        }
    }

}
