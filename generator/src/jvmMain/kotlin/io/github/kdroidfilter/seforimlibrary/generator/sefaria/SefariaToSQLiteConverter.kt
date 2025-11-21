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

    private data class PreloadedSefariaData(
        val schemas: Map<File, SefariaSchema>,
        val mergedTexts: Map<File, SefariaMergedText>,
        val linkLines: Map<File, List<String>>
    )

    private var preloadedData: PreloadedSefariaData? = null

    internal companion object {
        /**
         * Build a list of English aliases for a sectioned book (base title + ", " + section title).
         * This lets links like "Tur, Orach Chayim 1:1" resolve to the same book ID as "Tur".
         */
        internal fun collectEnglishSectionAliases(baseTitle: String, rootNode: SchemaNode?): List<String> {
            if (rootNode == null) return emptyList()
            val sections = rootNode.nodes ?: return emptyList()
            val seen = LinkedHashSet<String>()

            sections.forEach { node ->
                val title = englishTitleForNode(node)
                if (!title.isNullOrBlank()) {
                    val alias = "$baseTitle, $title"
                    seen.add(alias)
                }
            }

            return seen.toList()
        }

        private fun englishTitleForNode(node: SchemaNode): String? {
            val fromTitles = node.titles
                ?.firstOrNull { it.lang.equals("en", ignoreCase = true) && (it.primary == true) }
                ?: node.titles?.firstOrNull { it.lang.equals("en", ignoreCase = true) }

            return node.enTitle
                ?: node.title
                ?: fromTitles?.text
                ?: node.key?.takeIf { it.isNotBlank() && it != "default" }
        }
    }

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
            // Load all assets into memory to minimize disk I/O during conversion
            val preload = preloadedData ?: preloadAllSefariaData()
            preloadedData = preload

            // Step 1: Process schemas to create categories and books
            logger.info("Processing schemas (preloaded=${preload.schemas.size})...")
            processSchemas(preload.schemas)

            // Step 2: Process merged.json files to create lines and TOC
            logger.info("Processing texts (preloaded=${preload.mergedTexts.size})...")
            processTexts(preload.mergedTexts)

            // Step 3: Process links
            logger.info("Processing links (preloaded=${preload.linkLines.size})...")
            processLinks(preload.linkLines)

            // Print statistics
            printStatistics()

            logger.info("Conversion complete!")
        }
    }

    /**
     * Preload schemas, merged.json files and link CSVs into memory to reduce disk access.
     */
    private fun preloadAllSefariaData(): PreloadedSefariaData {
        logger.info("Preloading Sefaria assets into memory...")

        val schemasDir = File(sefariaBaseDir, "schemas")
        val schemaFiles = schemasDir.listFiles { file -> file.extension == "json" }.orEmpty()
        val schemas = mutableMapOf<File, SefariaSchema>()
        schemaFiles.forEach { file ->
            val text = file.readText()
            if (text.isBlank()) {
                logger.warn("Skipping empty schema file: ${file.name}")
                return@forEach
            }
            runCatching { json.decodeFromString<SefariaSchema>(text) }
                .onSuccess { schemas[file] = it }
                .onFailure { e ->
                    logger.warn("Skipping schema file ${file.name}: ${e.message}")
                }
        }
        logger.info("  - Schemas loaded: ${schemas.size} (of ${schemaFiles.size})")

        val mergedFiles = findMergedJsonFiles(File(sefariaBaseDir, "json"))
        val mergedTexts = mutableMapOf<File, SefariaMergedText>()
        mergedFiles.forEach { file ->
            val text = file.readText()
            if (text.isBlank()) {
                logger.warn("Skipping empty merged.json: ${file.absolutePath}")
                return@forEach
            }
            runCatching { json.decodeFromString<SefariaMergedText>(text) }
                .onSuccess { mergedTexts[file] = it }
                .onFailure { e ->
                    logger.warn("Skipping merged.json ${file.absolutePath}: ${e.message}")
                }
        }
        logger.info("  - merged.json loaded: ${mergedTexts.size} (of ${mergedFiles.size})")

        val linksDir = File(sefariaBaseDir, "links")
        val linkFiles = linksDir.listFiles { file ->
            file.name.startsWith("links") &&
                file.name.endsWith(".csv") &&
                !file.name.contains("by_book")
        }.orEmpty()
        val linkLines = mutableMapOf<File, List<String>>()
        linkFiles.forEach { file ->
            runCatching { file.readLines() }
                .onSuccess { linkLines[file] = it }
                .onFailure { e ->
                    logger.warn("Skipping link CSV ${file.name}: ${e.message}")
                }
        }
        logger.info("  - Link CSVs loaded: ${linkLines.size} (of ${linkFiles.size})")

        return PreloadedSefariaData(
            schemas = schemas,
            mergedTexts = mergedTexts,
            linkLines = linkLines
        )
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

    private suspend fun processSchemas(preloadedSchemas: Map<File, SefariaSchema>? = null) {
        val schemasDir = File(sefariaBaseDir, "schemas")
        if (!schemasDir.exists()) {
            logger.error("Schemas directory not found: ${schemasDir.absolutePath}")
            return
        }

        val schemaEntries: List<Pair<File, SefariaSchema?>> = when {
            preloadedSchemas != null -> preloadedSchemas.entries.map { it.key to it.value }
            else -> {
                val files = schemasDir.listFiles { file -> file.extension == "json" }.orEmpty()
                logger.info("Found ${files.size} schema files")
                files.map { it to null }
            }
        }

        schemaEntries.forEach { (schemaFile, schema) ->
            try {
                processSchemaFile(schemaFile, schema)
                stats.schemasProcessed++
            } catch (e: Exception) {
                logger.error("Error processing schema file ${schemaFile.name}", e)
                stats.schemaErrors++
            }
        }
    }

    private suspend fun processSchemaFile(file: File, preloadedSchema: SefariaSchema? = null) {
        val schema = preloadedSchema ?: run {
            // Read and validate file content
            val content = file.readText().trim()
            if (content.isEmpty()) {
                logger.warn("Skipping empty schema file: ${file.name}")
                return
            }
            json.decodeFromString<SefariaSchema>(content)
        }

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

        // Register aliases for sectioned books (e.g., "Tur, Orach Chayim") so links resolve correctly
        collectEnglishSectionAliases(schema.title, schema.schema).forEach { alias ->
            if (!bookMapByEnglish.containsKey(alias)) {
                bookMapByEnglish[alias] = bookId
                logger.debug("Registered section alias '$alias' for book ${schema.title} -> $bookId")
            }
        }

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

    private suspend fun processTexts(preloadedMergedTexts: Map<File, SefariaMergedText>? = null) {
        val jsonDir = File(sefariaBaseDir, "json")
        if (!jsonDir.exists()) {
            logger.error("JSON directory not found: ${jsonDir.absolutePath}")
            return
        }

        val mergedEntries: List<Pair<File, SefariaMergedText?>> = when {
            preloadedMergedTexts != null -> preloadedMergedTexts.entries.map { it.key to it.value }
            else -> {
                val mergedFiles = findMergedJsonFiles(jsonDir)
                logger.info("Found ${mergedFiles.size} merged.json files")
                mergedFiles.map { it to null }
            }
        }

        mergedEntries.forEach { (file, mergedText) ->
            try {
                processTextFile(file, mergedText)
                stats.textsProcessed++
            } catch (e: Exception) {
                logger.error("Error processing text file ${file.absolutePath}", e)
                stats.textErrors++
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

    private suspend fun processTextFile(file: File, preloadedText: SefariaMergedText? = null) {
        val mergedText = preloadedText ?: json.decodeFromString<SefariaMergedText>(file.readText())

        // Find book by English title
        val bookId = bookMapByEnglish[mergedText.title]
        if (bookId == null) {
            logger.warn("Could not find book for title: ${mergedText.title}")
            return
        }

        // Parse the text structure and create lines and TOC
        parseAndInsertText(bookId, mergedText)

        logger.debug("Processed text for book: ${mergedText.title} (ID: $bookId)")
    }

    private suspend fun parseAndInsertText(bookId: Long, mergedText: SefariaMergedText) {
        try {
            // Get schema for this book
            val schema = schemaCache[mergedText.title]

            // Parse the text using the text parser
            val parsed = textParser.parse(bookId, mergedText, schema)
            val (collapsedTocEntries, collapsedLineIndices) = collapseSingleChildToc(parsed.tocEntries, parsed.tocLineIndices)

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

            collapsedTocEntries.forEach { tocEntry ->
                // Get the line index for this TOC entry
                val lineIndex = collapsedLineIndices[tocEntry.id]
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
            if (collapsedTocEntries.isNotEmpty()) {
                updateTocEntryFlags(bookId, collapsedTocEntries, tempIdToRealId)
            }

            // Build line→TOC ownership so features relying on line_toc (breadcrumbs, filters)
            // behave like the main Otzaria generator.
            repository.rebuildLineTocForBook(bookId)

            // Update book's total lines
            repository.updateBookTotalLines(bookId, parsed.lines.size)

            // Store line count for this book (for link processing)
            bookLineOffsets[bookId] = parsed.lines.size

            logger.info("Inserted ${parsed.lines.size} lines and ${collapsedTocEntries.size} TOC entries for book ID: $bookId")
        } catch (e: Exception) {
            logger.error("Error parsing and inserting text for book ID: $bookId", e)
        }
    }

    /**
     * Collapse single-child TOC chains (a>b>c -> a) to reduce depth and improve performance.
     * Preserves ordering and keeps the first available line index along the collapsed chain.
     */
    private fun collapseSingleChildToc(
        tocEntries: List<TocEntry>,
        tocLineIndices: Map<Long, Int>
    ): Pair<List<TocEntry>, Map<Long, Int>> {
        if (tocEntries.isEmpty()) return tocEntries to tocLineIndices

        val childrenByParent = mutableMapOf<Long?, MutableList<TocEntry>>()
        tocEntries.forEach { entry ->
            val key = entry.parentId
            childrenByParent.getOrPut(key) { mutableListOf() }.add(entry)
        }

        var nextId = 1L
        val newEntries = mutableListOf<TocEntry>()
        val newLineIndices = mutableMapOf<Long, Int>()

        fun emit(entry: TocEntry, parentId: Long?, level: Int) {
            var current = entry
            var lineIdx = tocLineIndices[current.id]
            var childList = childrenByParent[current.id].orEmpty()

            // Collapse chains of single children
            while (childList.size == 1) {
                val child = childList.first()
                if (lineIdx == null) {
                    lineIdx = tocLineIndices[child.id]
                }
                current = current.copy() // keep current text/metadata
                childList = childrenByParent[child.id].orEmpty()
            }

            val newId = nextId++
            val newEntry = current.copy(id = newId, parentId = parentId, level = level)
            newEntries += newEntry
            lineIdx?.let { newLineIndices[newId] = it }

            childList.forEachIndexed { _, child ->
                emit(child, newId, level + 1)
            }
        }

        val roots = childrenByParent[null].orEmpty()
        roots.forEach { root ->
            emit(root, null, 0)
        }

        return newEntries to newLineIndices
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

    private suspend fun processLinks(preloadedLinkLines: Map<File, List<String>>? = null) {
        val linksDir = File(sefariaBaseDir, "links")
        if (!linksDir.exists()) {
            logger.error("Links directory not found: ${linksDir.absolutePath}")
            return
        }

        // Process all links*.csv files (preloaded if available)
        val linkFiles = preloadedLinkLines?.keys?.toList() ?: linksDir.listFiles { file ->
            file.name.startsWith("links") &&
            file.name.endsWith(".csv") &&
            !file.name.contains("by_book")
        }?.toList().orEmpty()

        logger.info("Found ${linkFiles.size} link CSV files")

        var totalLinksProcessed = 0
        var totalLinksCreated = 0

        linkFiles.forEach { file ->
            try {
                val lines = preloadedLinkLines?.get(file)
                val stats = processLinkFile(file, lines)
                totalLinksProcessed += stats.first
                totalLinksCreated += stats.second
                logger.info("Processed ${stats.first} links from ${file.name}, created ${stats.second} links")
            } catch (e: Exception) {
                logger.error("Error processing link file ${file.name}", e)
            }
        }

        logger.info("Total links processed: $totalLinksProcessed, created: $totalLinksCreated")
        stats.linksProcessed += totalLinksProcessed
        stats.linksCreated += totalLinksCreated

        // After all links are inserted, update per-book link flags
        if (totalLinksCreated > 0) {
            logger.info("Updating book_has_links and connection flags for Sefaria-generated links...")
            updateBookLinkFlags()
        } else {
            logger.info("No links created; skipping book_has_links / connection flag update")
        }
    }

    private suspend fun processLinkFile(file: File, preloadedLines: List<String>? = null): Pair<Int, Int> {
        var processed = 0
        var created = 0

        val linesSeq = preloadedLines?.asSequence() ?: file.useLines { sequence -> sequence.toList().asSequence() }
        linesSeq.drop(1).forEach { line -> // Skip header
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
            // For books with sections (e.g., "Shulchan Arukh, Orach Chayim"), reconstruct full title
            val fullTitle1 = if (citation1.section != null) {
                "${citation1.bookTitle}, ${citation1.section}"
            } else {
                citation1.bookTitle
            }
            val fullTitle2 = if (citation2.section != null) {
                "${citation2.bookTitle}, ${citation2.section}"
            } else {
                citation2.bookTitle
            }

            val bookId1 = bookMapByEnglish[fullTitle1]
            val bookId2 = bookMapByEnglish[fullTitle2]

            if (bookId1 == null || bookId2 == null) {
                logger.debug("Could not find books for link: $fullTitle1 -> $fullTitle2 (from citations: ${link.citation1} -> ${link.citation2})")
                return false
            }

            // Validate line indices
            val maxLines1 = bookLineOffsets[bookId1] ?: 0
            val maxLines2 = bookLineOffsets[bookId2] ?: 0

            if (maxLines1 <= 0 || maxLines2 <= 0) {
                logger.debug("No lines for books in link: $fullTitle1 (lines=$maxLines1), $fullTitle2 (lines=$maxLines2)")
                return false
            }

            // Calculate line indices for both sides (prefer schema-based structure, fall back to heuristic)
            val lineIndex1 = calculateLineIndexForBook(citation1, bookId1, maxLines1)
            val lineIndex2 = calculateLineIndexForBook(citation2, bookId2, maxLines2)

            if (lineIndex1 == null || lineIndex1 < 0 || lineIndex1 >= maxLines1) {
                logger.debug("Line index for first citation out of range or null: $lineIndex1 (max: $maxLines1) for ${link.citation1}")
                return false
            }

            if (lineIndex2 == null || lineIndex2 < 0 || lineIndex2 >= maxLines2) {
                logger.debug("Line index for second citation out of range or null: $lineIndex2 (max: $maxLines2) for ${link.citation2}")
                return false
            }

            // Get line IDs
            val lineId1 = getLineIdByBookAndIndex(bookId1, lineIndex1)
            val lineId2 = getLineIdByBookAndIndex(bookId2, lineIndex2)

            if (lineId1 == null || lineId2 == null) {
                logger.debug("Could not find line IDs for link: ${link.citation1} -> ${link.citation2}")
                return false
            }

            // Look up schemas using full titles (same as bookMapByEnglish)
            val schema1 = schemaCache[fullTitle1]
            val schema2 = schemaCache[fullTitle2]

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
    ): Int? {
        if (maxLines <= 0) return null

        // Reconstruct full book title for lookups (same logic as in processLink)
        val fullBookTitle = if (citation.section != null) {
            "${citation.bookTitle}, ${citation.section}"
        } else {
            citation.bookTitle
        }
        val baseBookTitle = citation.bookTitle

        // 0) Try precise seif-based index using merged.json structure when we have a section and at least Siman/Seif
        val section = citation.section
        if (section != null && citation.references.size >= 2) {
            val siman = citation.references[0]
            val seif = citation.references[1]
            val fromSeifIndex = getLineIndexFromSeifIndex(fullBookTitle, section, siman, seif)
                ?: getLineIndexFromSeifIndex(baseBookTitle, section, siman, seif)
            if (fromSeifIndex != null && fromSeifIndex in 0 until maxLines) {
                return fromSeifIndex
            }
        }

        // 0b) Use merged.json to map citation references directly to line index (no heuristics)
        val mergedIndex = calculateLineIndexFromMerged(fullBookTitle, citation)
            ?: calculateLineIndexFromMerged(baseBookTitle, citation)
        if (mergedIndex != null && mergedIndex in 0 until maxLines) {
            return mergedIndex
        }

        // If we cannot map using merged.json, do not guess.
        return null
    }

    /**
     * Map a citation directly to the line index by traversing the merged.json content in the
     * same order as parsing, avoiding heuristics.
     */
    private fun calculateLineIndexFromMerged(
        bookTitle: String,
        citation: SefariaCitationParser.Citation
    ): Int? {
        ensureMergedJsonIndex()
        val file = mergedJsonFileByTitle[bookTitle] ?: return null
        val merged = preloadedData?.mergedTexts?.get(file)
            ?: runCatching { json.decodeFromString<SefariaMergedText>(file.readText()) }.getOrNull()
            ?: return null

        val root = merged.text

        // Handle sections with subsections like "Orach Chayim, Introduction"
        var sectionElement: JsonElement? = if (citation.section != null && root is JsonObject) {
            // First try direct lookup
            root[citation.section] ?: run {
                // If not found and section contains comma, try splitting into section and subsection
                if (citation.section.contains(",")) {
                    val parts = citation.section.split(",", limit = 2)
                    val mainSection = parts[0].trim()
                    val subSection = parts[1].trim()
                    val mainElement = root[mainSection] as? JsonObject
                    mainElement?.get(subSection)
                } else {
                    null
                }
            }
        } else {
            root
        }

        if (sectionElement == null) return null

        // Special handling for subsections within an object-based section
        // When citation points to a subsection like "Introduction" within "Orach Chayim"
        // we need to calculate the offset of lines before this subsection
        val offsetBeforeSubsection = if (citation.section?.contains(",") == true && root is JsonObject) {
            val parts = citation.section.split(",", limit = 2)
            val mainSection = parts[0].trim()
            val subSection = parts[1].trim()
            val mainElement = root[mainSection] as? JsonObject
            if (mainElement != null) {
                calculateOffsetBeforeKey(mainElement, subSection)
            } else {
                0
            }
        } else {
            0
        }

        val (targetArray, introLines) = unwrapSectionElement(sectionElement) ?: return null
        val idx = lineIndexInArray(targetArray, citation.references) ?: return null
        return offsetBeforeSubsection + introLines + idx
    }

    /**
     * Calculate how many lines exist in the object before the specified key,
     * following the traversal order used during text parsing.
     */
    private fun calculateOffsetBeforeKey(obj: JsonObject, targetKey: String): Int {
        var offset = 0
        for ((key, value) in obj) {
            if (key == targetKey) break
            offset += countLinesInElement(value)
        }
        return offset
    }

    /**
     * If the section element contains a default "" array, return that array plus the number of
     * lines contributed by non-default keys (Introductions) that precede it.
     */
    private fun unwrapSectionElement(element: JsonElement): Pair<JsonArray, Int>? {
        return when (element) {
            is JsonArray -> element to 0
            is JsonObject -> {
                if (element.containsKey("")) {
                    val introLines = element
                        .filterKeys { it.isNotEmpty() }
                        .values
                        .sumOf { countLinesInElement(it) }
                    val defaultVal = element[""] as? JsonArray ?: return null
                    defaultVal to introLines
                } else {
                    null
                }
            }
            else -> null
        }
    }

    /**
     * Recursively compute the line index inside a JsonArray using 1-based references list.
     * When refs is empty, returns 0 (first line of the array/section).
     */
    private fun lineIndexInArray(array: JsonArray, refs: List<Int>): Int? {
        // If no references, return first line (index 0)
        if (refs.isEmpty()) return 0
        val targetIdx = refs.first() - 1
        if (targetIdx !in array.indices) return null
        val prefix = array.take(targetIdx).sumOf { countLinesInElement(it) }
        val child = array[targetIdx]
        if (refs.size == 1) {
            return prefix
        }
        return when (child) {
            is JsonArray -> {
                val inner = lineIndexInArray(child, refs.drop(1)) ?: return null
                prefix + inner
            }
            is JsonObject -> {
                val (innerArray, introLines) = unwrapSectionElement(child) ?: return null
                val inner = lineIndexInArray(innerArray, refs.drop(1)) ?: return null
                prefix + introLines + inner
            }
            else -> null
        }
    }

    /**
     * Count how many lines (strings) are inside this JsonElement, following the same traversal
     * order as text parsing: introduction keys before default "" when present.
     */
    private fun countLinesInElement(element: JsonElement): Int {
        return when (element) {
            is JsonPrimitive -> if (element.isString && element.content.isNotBlank()) 1 else 0
            is JsonArray -> element.sumOf { countLinesInElement(it) }
            is JsonObject -> {
                if (element.containsKey("")) {
                    val intro = element.filterKeys { it.isNotEmpty() }.values.sumOf { countLinesInElement(it) }
                    val defaultLines = element[""]?.let { countLinesInElement(it) } ?: 0
                    intro + defaultLines
                } else {
                    element.values.sumOf { countLinesInElement(it) }
                }
            }
            else -> 0
        }
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
        // For sectioned titles like "Tur, Orach Chayim", also try the base title "Tur"
        val baseTitle = bookTitle.substringBefore(",").trim()
        if (seifIndexCache.containsKey(baseTitle)) {
            // Mirror existing index under the full title key for faster subsequent lookups
            seifIndexCache[bookTitle] = seifIndexCache[baseTitle]!!
            return
        }

        ensureMergedJsonIndex()
        val file = mergedJsonFileByTitle[bookTitle] ?: mergedJsonFileByTitle[baseTitle] ?: return
        runCatching {
            val merged = preloadedData?.mergedTexts?.get(file)
                ?: json.decodeFromString<SefariaMergedText>(file.readText())
            val index = buildSeifIndexFromMerged(merged)
            seifIndexCache[bookTitle] = index
            if (bookTitle != baseTitle) {
                seifIndexCache[baseTitle] = index
            }
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
        preloadedData?.mergedTexts?.forEach { (file, merged) ->
            if (merged.title.isNotBlank()) {
                mergedJsonFileByTitle[merged.title] = file
            }
        }
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
            // Only consider the book as a *source* for per-book connection flags used by the UI.
            // This avoids marking commentary books as if they have commentaries on themselves.
            val hasTargum =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.TARGUM.name) > 0
            val hasReference =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.REFERENCE.name) > 0
            val hasCommentary =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.COMMENTARY.name) > 0
            val hasOther =
                repository.countLinksBySourceBookAndType(bookId, ConnectionType.OTHER.name) > 0

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
