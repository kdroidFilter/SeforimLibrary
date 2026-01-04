package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.core.text.HebrewTextUtils
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
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
    private val repository: SeforimRepository,
    private val acronymDbPath: String? = null,
    private val filterSourcesForLinks: Boolean = true
) {

    private val logger = Logger.withTag("DatabaseGenerator")


    private val json = Json {
        ignoreUnknownKeys = true
        coerceInputValues = true
    }
    private var nextBookId = 1L // Counter for book IDs
    private var nextLineId = 1L // Counter for line IDs
    private var nextTocEntryId = 1L // Counter for TOC entry IDs

    // Optional connection to the Acronymizer DB (opened lazily)
    private var acronymDb: java.sql.Connection? = null

    // Library root used for relative path normalization
    private lateinit var libraryRoot: Path

    // Map from library-relative book key (e.g. "תנ"ך/בראשית.txt") to source name (e.g. "sefariaToOtzaria")
    private val manifestSourcesByRel = mutableMapOf<String, String>()
    // Cache of source name -> id from DB
    private val sourceNameToId = mutableMapOf<String, Long>()

    // Source blacklist loaded from resources (fallback to default)
    private val sourceBlacklist: Set<String> = loadSourceBlacklistFromResources()

    // File name blacklist loaded from resources; entries are plain filenames such as "book.txt"
    private val fileNameBlacklist: Set<String> = loadFileNameBlacklistFromResources()

    // In-memory caches to accelerate link processing using available RAM
    private var booksByTitle: Map<String, Book> = emptyMap()
    private var booksById: Map<Long, Book> = emptyMap()
    // For each bookId, maps lineIndex -> lineId (0 means missing)
    private val lineIdCache = mutableMapOf<Long, LongArray>()

    // Tracks books processed from the priority list to avoid double insertion
    private val processedPriorityBookKeys = mutableSetOf<String>()

    // Overall progress across books
    private var totalBooksToProcess: Int = 0
    private var processedBooksCount: Int = 0

    // Normalization helpers for categories/titles
    private fun normalizeHebrewLabel(raw: String): String {
        var s = raw.trim()
        // Normalize common quote variants to Hebrew gershayim/geresh
        s = s.replace('\u201C', '"').replace('\u201D', '"')
        s = s.replace('\u2018', '\'').replace('\u2019', '\'')
        s = s.replace("\"", "״")
        s = s.replace("''", "״")
        s = s.replace("׳׳", "״")
        s = s.replace("`", "׳")
        s = s.replace("\u05f3", "׳")
        s = s.replace("\\s+".toRegex(), " ").trim()
        return s
    }

    private fun comparableLabel(raw: String): String {
        fun stripCorpusSuffix(s: String): String {
            val pattern = "(?i)\\s+על\\s+(התנ\"ך|התורה|התלמוד|המשנה|תנך|תורה|תלמוד|משנה)$".toRegex()
            return s.replace(pattern, "").trim()
        }

        val base = normalizeHebrewLabel(raw)
            .replace("״", "")
            .replace("\"", "")
            .replace("׳", "")
            .replace("'", "")
            .replace("\\s+".toRegex(), " ")
            .trim()
        return stripCorpusSuffix(base)
    }

    private fun normalizeCategorySegments(rawTitle: String): List<String> {
        val cleaned = normalizeHebrewLabel(rawTitle)
        return when (cleaned) {
            "תלמוד בבלי" -> listOf("תלמוד", "בבלי")
            "תלמוד ירושלמי", "תלמוד ירושלים" -> listOf("תלמוד", "ירושלמי")
            "תנך", "תנ\"ך", "תנ״ך" -> listOf("תנ״ך")
            "שות", "שו\"ת", "שו״ת" -> listOf("שו״ת")
            else -> listOf(cleaned)
        }
    }

    private data class CategoryPlacement(
        val id: Long,
        val leafLevel: Int,
        val normalizedPath: List<String>
    )

    private suspend fun findExistingCategory(parentId: Long?, title: String): Category? {
        val targetKey = comparableLabel(title)
        val candidates = if (parentId == null) repository.getRootCategories() else repository.getCategoryChildren(parentId)
        return candidates.firstOrNull { comparableLabel(it.title) == targetKey }
    }

    private suspend fun ensureCategoryHierarchy(rawTitle: String, parentId: Long?, startLevel: Int): CategoryPlacement {
        val normalizedSegments = normalizeCategorySegments(rawTitle)
        var currentParent = parentId
        var currentLevel = startLevel
        var lastId: Long? = null

        for (title in normalizedSegments) {
            val existing = findExistingCategory(currentParent, title)
            val categoryId = existing?.id ?: repository.insertCategory(
                Category(
                    parentId = currentParent,
                    title = title,
                    level = currentLevel
                )
            )
            lastId = categoryId
            currentParent = categoryId
            currentLevel += 1
        }

        val finalId = lastId ?: throw IllegalStateException("Failed to ensure category hierarchy for $rawTitle")
        return CategoryPlacement(
            id = finalId,
            leafLevel = currentLevel - 1,
            normalizedPath = normalizedSegments
        )
    }

    private fun normalizeBookTitle(rawTitle: String): String {
        val base = normalizeHebrewLabel(rawTitle)
        return when (base) {
            "תנך", "תנ\"ך" -> "תנ״ך"
            else -> base
        }
    }

    private fun stripQuotesForLookup(title: String): String {
        return title.replace("״", "")
            .replace("\"", "")
            .replace("׳", "")
            .replace("'", "")
            .trim()
    }

    // Book contents cache: maps library-relative key -> list of lines
    private val bookContentCache = mutableMapOf<String, List<String>>()

    // Tracks whether ID counters have been initialized from an existing DB
    private var idCountersInitialized = false

    private suspend fun initializeIdCountersFromExistingDb() {
        if (idCountersInitialized) return
        idCountersInitialized = true
        val maxBookId = try { repository.getMaxBookId() } catch (_: Exception) { 0L }
        val maxLineId = try { repository.getMaxLineId() } catch (_: Exception) { 0L }
        val maxTocEntryId = try { repository.getMaxTocEntryId() } catch (_: Exception) { 0L }
        nextBookId = (maxBookId + 1).coerceAtLeast(1)
        nextLineId = (maxLineId + 1).coerceAtLeast(1)
        nextTocEntryId = (maxTocEntryId + 1).coerceAtLeast(1)
        if (maxBookId > 0 || maxLineId > 0 || maxTocEntryId > 0) {
            logger.i { "Continuing from existing DB ids: nextBookId=$nextBookId, nextLineId=$nextLineId, nextTocEntryId=$nextTocEntryId" }
        } else {
            logger.d { "No existing rows found; starting ID counters at 1" }
        }
    }


    /**
     * Generates the database by processing metadata, directories, and links.
     * This is the main entry point for the database generation process.
     */
    suspend fun generate(): Unit = coroutineScope {
        logger.i { "Starting database generation..." }
        logger.i { "Source directory: $sourceDirectory" }

        try {
            // Continue IDs from existing DB if present (append/incremental)
            initializeIdCountersFromExistingDb()
            // Disable foreign keys for better performance during bulk insertion
            logger.i { "Disabling foreign keys for better performance..." }
            disableForeignKeys()

            // Lower durability for faster bulk writes (restored afterward)
            logger.i { "Setting PRAGMA synchronous=OFF for bulk generation" }
            repository.setSynchronousOff()
            logger.i { "Setting PRAGMA journal_mode=OFF for bulk generation" }
            repository.setJournalModeOff()

            // Wrap the entire generation in a single transaction for major SQLite speedups
            repository.runInTransaction {
                // Load metadata
                val metadata = loadMetadata()
                logger.i { "Metadata loaded: ${metadata.size} entries" }

                // Load sources from files_manifest.json and upsert source table
                loadSourcesFromManifest()
                precreateSourceEntries()
                backfillAcronymsForExistingBooks()

                // Process hierarchy
                val libraryPath = sourceDirectory.resolve("אוצריא")
                if (!libraryPath.exists()) {
                    throw IllegalStateException("The directory אוצריא does not exist in $sourceDirectory")
                }

                // Save for relative path computations
                libraryRoot = libraryPath

                // Estimate total number of books (txt files) for progress tracking
                totalBooksToProcess = try {
                    Files.walk(libraryRoot).use { s ->
                        s.filter { Files.isRegularFile(it) && it.extension == "txt" }
                            .filter { !it.fileName.toString().substringBeforeLast('.')
                                .startsWith("הערות על ") }
                            .count().toInt()
                    }
                } catch (_: Exception) { 0 }
                logger.i { "Planned to process approximately $totalBooksToProcess books" }

                // Process priority books first (if any), then process the full library
                runCatching {
                    processPriorityBooks(loadMetadata = { metadata })
                }.onFailure { e ->
                    logger.w(e) { "Failed processing priority list; continuing with full generation" }
                }

                logger.i { "🚀 Starting to process library directory: $libraryPath" }
                // Preload all book .txt contents into RAM for faster processing
                preloadAllBookContents(libraryPath)
                processDirectory(libraryPath, null, 0, metadata)

                // Process links
                processLinks()

                // Build category closure table for fast descendant queries
                logger.i { "Building category_closure (ancestor-descendant) table..." }
                repository.rebuildCategoryClosure()
            }
            // Restore PRAGMAs after commit
            logger.i { "Re-enabling foreign keys..." }
            enableForeignKeys()
            logger.i { "Restoring PRAGMA synchronous=NORMAL" }
            repository.setSynchronousNormal()
            // Restore journal mode after commit
            logger.i { "Restoring PRAGMA journal_mode=WAL" }
            repository.setJournalModeWal()
            logger.i { "Generation completed successfully!" }
        } catch (e: Exception) {
            // Make sure to re-enable foreign keys even if an error occurs
            try {
                enableForeignKeys()
            } catch (innerEx: Exception) {
                logger.w(innerEx) { "Error re-enabling foreign keys after failure" }
            }
            try {
                repository.setSynchronousNormal()
            } catch (_: Exception) {}
            try {
                repository.setJournalModeWal()
            } catch (_: Exception) {}

            logger.e(e) { "Error during generation" }
            throw e
        }
    }

    /**
     * Phase 1: Generate categories, books, TOCs and lines only (no links).
     */
    suspend fun generateLinesOnly(): Unit = coroutineScope {
        logger.i { "Starting phase 1: categories/books/lines generation..." }
        try {
            initializeIdCountersFromExistingDb()
            // Performance PRAGMAs
            disableForeignKeys()
            repository.setSynchronousOff()
            repository.setJournalModeOff()

            repository.runInTransaction {
                val metadata = loadMetadata()
                // Load sources and create entries upfront
                loadSourcesFromManifest()
                precreateSourceEntries()
                backfillAcronymsForExistingBooks()
                val libraryPath = sourceDirectory.resolve("אוצריא")
                if (!libraryPath.exists()) {
                    throw IllegalStateException("The directory אוצריא does not exist in $sourceDirectory")
                }
                libraryRoot = libraryPath

                totalBooksToProcess = try {
                    Files.walk(libraryRoot).use { s ->
                        s.filter { Files.isRegularFile(it) && it.extension == "txt" }
                            .filter { !it.fileName.toString().substringBeforeLast('.')
                                .startsWith("הערות על ") }
                            .count().toInt()
                    }
                } catch (_: Exception) { 0 }
                logger.i { "Planned to process approximately $totalBooksToProcess books (phase 1)" }

                runCatching { processPriorityBooks(loadMetadata = { metadata }) }
                    .onFailure { e -> logger.w(e) { "Failed processing priority list; continuing with full generation (phase 1)" } }
                // Preload all book .txt contents into RAM for faster processing
                preloadAllBookContents(libraryPath)
                processDirectory(libraryPath, null, 0, metadata)

                // Build category closure after categories insertion
                logger.i { "Building category_closure table (phase 1)..." }
                repository.rebuildCategoryClosure()
            }
        } finally {
            runCatching { enableForeignKeys() }
            runCatching { repository.setSynchronousNormal() }
            runCatching { repository.setJournalModeWal() }
            logger.i { "Phase 1 completed." }
        }
    }

    /**
     * Phase 2: Process links only and update link-related flags.
     */
    suspend fun generateLinksOnly(): Unit = coroutineScope {
        logger.i { "Starting phase 2: links processing..." }
        try {
            disableForeignKeys()
            repository.setSynchronousOff()
            repository.setJournalModeOff()
            repository.runInTransaction {
                processLinks()
            }
        } finally {
            runCatching { enableForeignKeys() }
            runCatching { repository.setSynchronousNormal() }
            runCatching { repository.setJournalModeWal() }
            logger.i { "Phase 2 completed." }
        }
    }

    // Prepare caches so that link resolution uses RAM instead of round-trips
    private suspend fun ensureCachesLoaded() {
        if (booksByTitle.isEmpty()) {
            val allBooks = repository.getAllBooks()
            val filtered = if (filterSourcesForLinks) {
                allBooks.filter { book ->
                    val src = runCatching { repository.getSourceById(book.sourceId) }.getOrNull()
                    val name = src?.name ?: "Unknown"
                    val normalized = comparableLabel(name)
                    !sourceBlacklist.any { comparableLabel(it) == normalized }
                }
            } else {
                allBooks
            }
            booksByTitle = filtered.associateBy { it.title }
            booksById = filtered.associateBy { it.id }
            val skipped = allBooks.size - filtered.size
            if (filterSourcesForLinks && skipped > 0) {
                logger.i { "Preloaded ${filtered.size} books into memory for fast link processing (skipped $skipped by source blacklist)" }
            } else {
                logger.i { "Preloaded ${filtered.size} books into memory for fast link processing" }
            }
        }
    }

    // Ensure we have lineIndex -> lineId mapping for the given book in memory
    private suspend fun ensureLineIndexCache(bookId: Long) {
        if (lineIdCache.containsKey(bookId)) return
        val totalLines = booksById[bookId]?.totalLines ?: repository.getBook(bookId)?.totalLines ?: 0
        val arr = LongArray(totalLines.coerceAtLeast(0)) { 0L }
        // Use existing repository.getLines to load ids and indices; content is ignored here
        val lines = if (totalLines > 0) repository.getLines(bookId, 0, totalLines - 1) else emptyList()
        for (ln in lines) {
            val idx = ln.lineIndex
            if (idx >= 0 && idx < arr.size) arr[idx] = ln.id
        }
        lineIdCache[bookId] = arr
        logger.d { "Loaded ${lines.size} line id/index pairs for book $bookId into memory" }
    }

    // Lookup helper using the RAM cache
    private suspend fun getLineIdCached(bookId: Long, lineIndex: Int): Long? {
        ensureLineIndexCache(bookId)
        val arr = lineIdCache[bookId] ?: return null
        if (lineIndex < 0 || lineIndex >= arr.size) return null
        val id = arr[lineIndex]
        return if (id == 0L) null else id
    }

    // Preload all book file contents into RAM, keyed by library-relative path
    private suspend fun preloadAllBookContents(libraryPath: Path) {
        // Avoid reloading if already populated
        if (bookContentCache.isNotEmpty()) return
        logger.i { "Preloading book contents into RAM from $libraryPath ..." }
        val files = Files.walk(libraryPath).use { s ->
            s.filter { Files.isRegularFile(it) && it.extension == "txt" }
                .filter { p ->
                    val fileName = p.fileName.toString()
                    // Skip notes files
                    val titleNoExt = fileName.substringBeforeLast('.')
                    if (titleNoExt.startsWith("הערות על ")) return@filter false
                    // Skip files explicitly blacklisted by name
                    if (fileNameBlacklist.contains(fileName)) {
                        logger.d { "Skipping preload for blacklisted file '$fileName'" }
                        return@filter false
                    }
                    // Skip blacklisted sources when known
                    val rel = runCatching { toLibraryRelativeKey(p) }.getOrElse { p.fileName.toString() }
                    val src = manifestSourcesByRel[rel] ?: "Unknown"
                    if (sourceBlacklist.contains(src)) {
                        logger.d { "Skipping preload for blacklisted source '$src': $rel" }
                        return@filter false
                    }
                    true
                }
                .toList()
        }
        // Parallelize reads
        val loaded = coroutineScope {
            files.map { p ->
                async {
                    val key = toLibraryRelativeKey(p)
                    val content = p.readText(Charsets.UTF_8)
                    key to content.lines()
                }
            }.awaitAll()
        }
        for ((k, v) in loaded) bookContentCache[k] = v
        logger.i { "Preloaded ${bookContentCache.size} books into RAM" }
    }

    /**
     * Loads book metadata from the metadata.json file.
     * Attempts to parse the file in different formats (Map or List).
     *
     * @return A map of book titles to their metadata
     */
    private fun loadMetadata(): Map<String, BookMetadata> {
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

    @Serializable
    private data class ManifestEntry(
        val hash: String? = null
    )

    /**
     * Loads `files_manifest.json` and builds a mapping from library-relative path
     * (under אוצריא) to a source name (top-level directory of the manifest entry).
     */
    private fun loadSourcesFromManifest() {
        manifestSourcesByRel.clear()
        // Prefer manifest in the provided source directory; fallback to repo path if present.
        //
        // Otzaria releases may ship both:
        // - files_manifest_new.json: includes the originating source in the prefix (e.g. wiki_jewish_books/…/אוצריא/…)
        // - files_manifest.json: legacy format without per-book source information (e.g. אוצריא/…)
        //
        // For source blacklisting to work, we must prefer the "new" manifest when available.
        val primaryNew = sourceDirectory.resolve("files_manifest_new.json")
        val primaryOld = sourceDirectory.resolve("files_manifest.json")
        val fallbackNew = Paths.get("otzaria-library/files_manifest_new.json")
        val fallbackOld = Paths.get("otzaria-library/files_manifest.json")
        val manifestPath = when {
            primaryNew.exists() -> primaryNew
            primaryOld.exists() -> primaryOld
            fallbackNew.exists() -> fallbackNew
            fallbackOld.exists() -> fallbackOld
            else -> null
        }
        if (manifestPath == null) {
            logger.w { "files_manifest*.json not found; assigning source 'Unknown' to all books" }
            return
        }
        logger.i { "Loading sources from manifest: ${manifestPath.toAbsolutePath()}" }
        runCatching {
            val content = manifestPath.readText()
            val map = json.decodeFromString<Map<String, ManifestEntry>>(content)
            // For every manifest key, if it contains "/אוצריא/", index the subpath after it
            for ((path, _) in map) {
                val parts = path.split('/')
                if (parts.isEmpty()) continue
                val idx = parts.indexOf("אוצריא")
                if (idx < 0 || idx == parts.size - 1) continue
                val rel = parts.drop(idx + 1).joinToString("/")
                val sourceName = if (idx > 0) parts.first() else "Unknown"
                // Keep first assignment, warn on duplicates
                val prev = manifestSourcesByRel.putIfAbsent(rel, sourceName)
                if (prev != null && prev != sourceName) {
                    logger.w { "Duplicate source mapping for '$rel': existing=$prev new=$sourceName; keeping existing" }
                }
            }
            val uniqueSources = manifestSourcesByRel.values.toSet()
            logger.i {
                "Loaded ${manifestSourcesByRel.size} book→source mappings from manifest " +
                    "(unique sources: ${uniqueSources.size})"
            }
        }.onFailure { e ->
            logger.w(e) { "Failed to parse files_manifest.json; sources will be 'Unknown'" }
        }
    }

    /**
     * Ensure all known source names from manifest are present in DB, including 'Unknown'.
     */
    private suspend fun precreateSourceEntries() {
        // Always ensure 'Unknown' exists
        val unknownId = repository.insertSource("Unknown")
        sourceNameToId["Unknown"] = unknownId
        // Insert all discovered sources
        val uniqueSources = manifestSourcesByRel.values.toSet()
        for (name in uniqueSources) {
            val id = repository.insertSource(name)
            sourceNameToId[name] = id
        }
        logger.i { "Prepared ${sourceNameToId.size} sources in DB" }
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
                        val placement = ensureCategoryHierarchy(entry.fileName.toString(), parentCategoryId, level)
                        val normalizedPath = placement.normalizedPath.joinToString(" / ")
                        logger.i { "✅ Category '${entry.fileName}' normalized to '$normalizedPath' with ID: ${placement.id} (parent: $parentCategoryId)" }
                        processDirectory(entry, placement.id, placement.leafLevel + 1, metadata)
                    }

                    Files.isRegularFile(entry) && entry.extension == "txt" -> {
                        // Skip if already processed from the priority list
                        val key = toLibraryRelativeKey(entry)
                        if (processedPriorityBookKeys.contains(key)) {
                            logger.i { "⏭️ Skipping already-processed priority book: $key" }
                            continue
                        }
                        // Skip companion notes files named 'הערות על <title>.txt'.
                        val fname = entry.fileName.toString()
                        val titleNoExt = fname.substringBeforeLast('.')
                        if (titleNoExt.startsWith("הערות על ")) {
                            logger.i { "📝 Skipping notes file '$fname' (will be attached to base book if present)" }
                            continue
                        }
                        // Skip files explicitly blacklisted by name
                        if (fileNameBlacklist.contains(fname)) {
                            logger.i { "⛔ Skipping blacklisted file '$fname' by name" }
                            continue
                        }
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
     * Creates a book in the database and processes its content.
     *
     * @param path The path to the book file
     * @param categoryId The ID of the category the book belongs to
     * @param metadata The metadata for the book
     */
    private suspend fun createAndProcessBook(
        path: Path,
        categoryId: Long,
        metadata: Map<String, BookMetadata>,
        isBaseBook: Boolean = false
    ) {
        val filename = path.fileName.toString()
        val rawTitle = filename.substringBeforeLast('.')
        val title = normalizeBookTitle(rawTitle)
        val meta = metadata[rawTitle] ?: metadata[title] ?: metadata[stripQuotesForLookup(rawTitle)]

        logger.i { "Processing book: $title with categoryId: $categoryId" }

        // Apply source blacklist
        val srcName = getSourceNameFor(path)
        if (sourceBlacklist.contains(srcName)) {
            logger.i { "⛔ Skipping '$title' from blacklisted source '$srcName'" }
            processedBooksCount += 1
            val pct = if (totalBooksToProcess > 0) (processedBooksCount * 100 / totalBooksToProcess) else 0
            logger.i { "Books progress: $processedBooksCount/$totalBooksToProcess (${pct}%)" }
            return
        }

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

        // Detect companion notes file named 'הערות על <title>.txt' in the same directory
        val notesContent: String? = runCatching {
            val dir = path.parent
            val possibleTitles = listOf(title, rawTitle).distinct()
            val candidate = possibleTitles
                .map { dir.resolve("הערות על $it.txt") }
                .firstOrNull { Files.isRegularFile(it) }
            if (candidate != null) {
                if (fileNameBlacklist.contains(candidate.fileName.toString())) {
                    logger.i { "📝 Notes file '${candidate.fileName}' is blacklisted by name; skipping attachment" }
                    return@runCatching null
                }
                val key = toLibraryRelativeKey(candidate)
                val lines = bookContentCache[key]
                if (lines != null) lines.joinToString("\n") else candidate.readText(Charsets.UTF_8)
            } else null
        }.getOrNull()

        val sourceId = resolveSourceIdFor(path)
        val book = Book(
            id = currentBookId,
            categoryId = categoryId,
            sourceId = sourceId,
            title = title,
            authors = authors,
            pubPlaces = pubPlaces,
            pubDates = pubDates,
            heShortDesc = meta?.heShortDesc,
            notesContent = notesContent,
            order = meta?.order ?: 999f,
            topics = extractTopics(path),
            isBaseBook = isBaseBook
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

        // Insert acronyms for this book if an Acronymizer DB is available
        try {
            val terms = fetchAcronymsForTitle(title)
            if (terms.isNotEmpty()) {
                repository.bulkInsertBookAcronyms(insertedBookId, terms)
                logger.i { "Inserted ${terms.size} acronyms for '${title}'" }
            }
        } catch (e: Exception) {
            logger.w(e) { "Failed to insert acronyms for '$title'" }
        }

        // Process content of the book
        processBookContent(path, insertedBookId, title, categoryId)

        // Book-level progress
        processedBooksCount += 1
        val pct = if (totalBooksToProcess > 0) (processedBooksCount * 100 / totalBooksToProcess) else 0
        logger.i { "Books progress: $processedBooksCount/$totalBooksToProcess (${pct}%)" }
    }

    /**
     * Processes the content of a book, extracting lines and TOC entries.
     *
     * @param path The path to the book file
     * @param bookId The ID of the book in the database
     */
    private suspend fun processBookContent(path: Path, bookId: Long, bookTitle: String, categoryId: Long) = coroutineScope {
        logger.d { "Processing content for book ID: $bookId" }
        logger.i { "Processing content of book ID: $bookId (ID generated by the database)" }

        // Prefer preloaded content from RAM if available
        val key = toLibraryRelativeKey(path)
        val lines = bookContentCache[key] ?: run {
            val content = path.readText(Charsets.UTF_8)
            content.lines()
        }
        logger.i { "Number of lines: ${lines.size}" }

        // Process each line one by one, handling TOC entries as we go
        processLinesWithTocEntries(bookId, bookTitle, categoryId, lines)

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
    private suspend fun processLinesWithTocEntries(bookId: Long, bookTitle: String, categoryId: Long, lines: List<String>) {
        logger.d { "Processing lines and TOC entries together for book ID: $bookId" }

        // Structure pour stocker toutes les entrées TOC créées
        data class TocEntryData(
            val id: Long,
            val parentId: Long?,
            val level: Int,
            val text: String,
            val lineIndex: Int
        )

        val allTocEntries = mutableListOf<TocEntryData>()
        val parentStack = mutableMapOf<Int, Long>()
        val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
        var currentOwningTocEntryId: Long? = null
        val lineTocBuffer = ArrayList<Pair<Long, Long>>(minOf(lines.size, 200_000))

        // PREMIÈRE PASSE : Créer toutes les entrées et lignes
        for ((lineIndex, line) in lines.withIndex()) {
            val level = detectHeaderLevel(line)
            val plainText = if (level > 0) cleanHtml(line) else ""

            if (level > 0) {
                if (plainText.isBlank()) {
                    logger.d { "⚠️ Skipping empty header at level $level (line $lineIndex)" }
                    parentStack.remove(level)
                    continue
                }

                val parentId = (level - 1 downTo 1).firstNotNullOfOrNull { parentStack[it] }
                val currentTocEntryId = nextTocEntryId++
                val currentLineId = nextLineId++

                // Stocker l'info de cette entrée pour la deuxième passe
                allTocEntries.add(TocEntryData(
                    id = currentTocEntryId,
                    parentId = parentId,
                    level = level,
                    text = plainText,
                    lineIndex = lineIndex
                ))

                // Créer l'entrée TOC avec hasChildren = false par défaut
                val tocEntry = TocEntry(
                    id = currentTocEntryId,
                    bookId = bookId,
                    parentId = parentId,
                    text = plainText,
                    level = level,
                    lineId = null,
                    isLastChild = false,
                    hasChildren = false  // Par défaut, sera mis à jour dans la deuxième passe
                )

                val tocEntryId = repository.insertTocEntry(tocEntry)
                parentStack[level] = tocEntryId
                entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocEntryId)
                currentOwningTocEntryId = tocEntryId

                val lineId = repository.insertLine(
                    Line(
                        id = currentLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line
                    )
                )
                repository.updateTocEntryLineId(tocEntryId, lineId)
                repository.updateLineTocEntry(lineId, tocEntryId)
                // Buffer this mapping instead of writing immediately
                lineTocBuffer.add(lineId to tocEntryId)
                if (lineTocBuffer.size >= 200_000) {
                    repository.bulkUpsertLineToc(lineTocBuffer)
                    lineTocBuffer.clear()
                }
            } else {
                // Regular line
                val currentLineId = nextLineId++
                val insertedLineId = repository.insertLine(
                    Line(
                        id = currentLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        heRef = buildOtzariaRef(bookTitle, lineIndex)
                    )
                )
                // Buffer mapping for regular line if there is a current owner
                currentOwningTocEntryId?.let { ownerId ->
                    lineTocBuffer.add(insertedLineId to ownerId)
                    if (lineTocBuffer.size >= 200_000) {
                        repository.bulkUpsertLineToc(lineTocBuffer)
                        lineTocBuffer.clear()
                    }
                }
            }

            if (lineIndex % 1000 == 0) {
                val pct = if (lines.isNotEmpty()) (lineIndex * 100 / lines.size) else 0
                logger.i { "Book $bookId '$bookTitle': $lineIndex/${lines.size} lines (${pct}%)" }
            }
        }

        // Flush buffered line→toc mappings in bulk
        repository.bulkUpsertLineToc(lineTocBuffer)

        // DEUXIÈME PASSE : Mettre à jour isLastChild et hasChildren
        logger.d { "Updating isLastChild and hasChildren for book ID: $bookId" }

        // Créer un set des IDs qui ont des enfants
        val parentIds = allTocEntries.mapNotNull { it.parentId }.toSet()

        // Mettre à jour hasChildren pour les entrées qui ont des enfants
        for (entry in allTocEntries) {
            if (entry.id in parentIds) {
                logger.d { "Updating TOC entry ${entry.id} as having children" }
                repository.updateTocEntryHasChildren(entry.id, true)
            }
        }

        // Mettre à jour isLastChild
        for ((parentId, children) in entriesByParent) {
            if (children.isNotEmpty()) {
                val lastChildId = children.last()
                logger.d { "Marking TOC entry $lastChildId as last child of parent $parentId" }
                repository.updateTocEntryIsLastChild(lastChildId, true)
            }
        }

        logger.i { "✅ Finished processing lines and TOC entries for book ID: $bookId" }
        logger.i { "   Total TOC entries: ${allTocEntries.size}" }
        logger.i { "   Entries with children: ${parentIds.size}" }
    }

    private fun cleanHtml(html: String): String {
        val cleaned = Jsoup.clean(html, Safelist.none())
            .trim()
            .replace("\\s+".toRegex(), " ")
        val withoutMaqaf = HebrewTextUtils.replaceMaqaf(cleaned, " ")
        return HebrewTextUtils.removeAllDiacritics(withoutMaqaf)
    }

    private fun buildOtzariaRef(bookTitle: String, lineIndex: Int): String {
        val safeTitle = bookTitle.trim()
        val oneBasedIndex = lineIndex + 1
        return "$safeTitle $oneBasedIndex"
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

    // ===== Priority handling =====

    /**
     * Reads the priority list from resources and returns normalized relative paths under the library root.
     */
    private fun loadPriorityList(): List<String> {
        return try {
            val stream = this::class.java.classLoader.getResourceAsStream("priority.txt")
                ?: return emptyList()
            stream.bufferedReader(Charsets.UTF_8).use { reader ->
                reader.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .map { raw ->
                        // Normalize separators
                        var s = raw.replace('\\', '/')
                        // Remove BOM if present
                        if (s.isNotEmpty() && s[0].code == 0xFEFF) s = s.substring(1)
                        // Remove leading slash
                        s = s.removePrefix("/")
                        // Try to start from 'אוצריא' if present
                        val idx = s.indexOf("אוצריא")
                        if (idx >= 0) s = s.substring(idx + "אוצריא".length).removePrefix("/")
                        s
                    }
                    .filter { it.endsWith(".txt", ignoreCase = true) }
                    .toList()
            }
        } catch (e: Exception) {
            logger.w(e) { "Unable to read priority.txt from resources" }
            emptyList()
        }
    }

    /**
     * Processes books listed in priority.txt before the normal directory traversal.
     * Ensures categories exist and records processed books to avoid duplicates.
     */
    private suspend fun processPriorityBooks(loadMetadata: () -> Map<String, BookMetadata>) {
        val entries = loadPriorityList()
        if (entries.isEmpty()) {
            logger.i { "No priority entries found" }
            return
        }

        logger.i { "Processing ${entries.size} priority entries first" }
        val metadata = loadMetadata()

        outer@ for ((idx, relative) in entries.withIndex()) {
            // Build the absolute path under the library root
            val parts = relative.split('/').filter { it.isNotEmpty() }
            if (parts.isEmpty()) continue

            // Last part is the book filename, everything before are categories
            val categories = if (parts.size > 1) parts.dropLast(1) else emptyList()
            val bookFileName = parts.last()
            // Skip notes-only entries from priority list
            if (bookFileName.substringBeforeLast('.').startsWith("הערות על ")) {
                logger.i { "⏭️ Skipping notes file in priority list: $bookFileName" }
                continue@outer
            }
            // Skip files explicitly blacklisted by name
            if (fileNameBlacklist.contains(bookFileName)) {
                logger.i { "⛔ Skipping blacklisted file in priority list: $bookFileName" }
                continue@outer
            }

            // Fold into actual filesystem path
            var currentPath = libraryRoot
            for (p in categories) currentPath = currentPath.resolve(p)
            val bookPath = currentPath.resolve(bookFileName)

            if (!Files.isRegularFile(bookPath)) {
                logger.w { "Priority entry ${idx + 1}/${entries.size}: file not found: $bookPath" }
                continue@outer
            }

            // Avoid processing duplicates listed multiple times
            val key = toLibraryRelativeKey(bookPath)
            if (processedPriorityBookKeys.contains(key)) {
                logger.d { "Priority entry ${idx + 1}/${entries.size}: already processed (dup in list): $key" }
                continue@outer
            }

            // Ensure categories exist and get the final parent category id
            var parentId: Long? = null
            var level = 0
            for (cat in categories) {
                val placement = ensureCategoryHierarchy(cat, parentId, level)
                parentId = placement.id
                level = placement.leafLevel + 1
            }

            if (parentId == null) {
                logger.w { "Priority entry ${idx + 1}/${entries.size}: missing parent category for $bookPath; skipping" }
                continue@outer
            }

            logger.i { "⭐ Priority ${idx + 1}/${entries.size}: processing $bookFileName under categories ${categories.joinToString("/")}" }
            createAndProcessBook(bookPath, parentId, metadata, isBaseBook = true)

            // Mark as processed to avoid double insertion during full traversal
            processedPriorityBookKeys.add(key)
        }
    }

    /**
     * Compute a normalized key for a book file relative to the library root.
     */
    private fun toLibraryRelativeKey(file: Path): String {
        return try {
            val rel = libraryRoot.relativize(file).toString().replace('\\', '/')
            rel
        } catch (_: Exception) {
            // Fallback to filename
            file.fileName.toString()
        }
    }

    // Resolve a source id for a book file using the manifest mapping
    private suspend fun resolveSourceIdFor(file: Path): Long {
        val rel = toLibraryRelativeKey(file)
        val sourceName = manifestSourcesByRel[rel] ?: "Unknown"
        val cached = sourceNameToId[sourceName]
        if (cached != null) return cached
        val id = repository.insertSource(sourceName)
        sourceNameToId[sourceName] = id
        return id
    }

    private fun getSourceNameFor(file: Path): String {
        val rel = toLibraryRelativeKey(file)
        return manifestSourcesByRel[rel] ?: "Unknown"
    }

    private fun loadSourceBlacklistFromResources(): Set<String> {
        val fallback = setOf("wiki_jewish_books")
        return try {
            val resourceNames = listOf("source-blacklist.txt", "/source-blacklist.txt")
            val cl = Thread.currentThread().contextClassLoader
            val stream = resourceNames.asSequence()
                .mapNotNull { name ->
                    cl?.getResourceAsStream(name) ?: DatabaseGenerator::class.java.getResourceAsStream(name)
                }
                .firstOrNull()
            if (stream == null) return fallback
            stream.bufferedReader(Charsets.UTF_8).use { br ->
                br.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .toSet()
            }.ifEmpty { fallback }
        } catch (e: Exception) {
            Logger.withTag("DatabaseGenerator").w(e) { "Failed to load source-blacklist.txt; using fallback" }
            fallback
        }
    }

    private fun loadFileNameBlacklistFromResources(): Set<String> {
        return try {
            val resourceNames = listOf("files-blacklist.txt", "/files-blacklist.txt")
            val cl = Thread.currentThread().contextClassLoader
            val stream = resourceNames.asSequence()
                .mapNotNull { name ->
                    cl?.getResourceAsStream(name) ?: DatabaseGenerator::class.java.getResourceAsStream(name)
                }
                .firstOrNull()
                ?: return emptySet()
            stream.bufferedReader(Charsets.UTF_8).use { br ->
                br.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .toSet()
            }
        } catch (e: Exception) {
            Logger.withTag("DatabaseGenerator").w(e) { "Failed to load files-blacklist.txt; ignoring file-name blacklist" }
            emptySet()
        }
    }

    /**
     * Processes all link files in the links directory.
     * Links connect lines between different books.
     */
    private suspend fun processLinks() {
        // Load caches once so most lookups stay in memory
        ensureCachesLoaded()
        val linksDir = sourceDirectory.resolve("links")
        if (!linksDir.exists()) {
            logger.w { "Links directory not found" }
            return
        }

        // Count links before processing
        val linksBefore = repository.countLinks()
        logger.d { "Links in database before processing: $linksBefore" }

        logger.i { "Loading all link JSON files into RAM..." }
        // Preload all links JSON into memory to minimize IO
        val linkFiles = Files.list(linksDir).use { s -> s.filter { it.extension == "json" }.toList() }
        val linksByBook = coroutineScope {
            linkFiles.map { file ->
                async {
                    val bookTitle = file.nameWithoutExtension.removeSuffix("_links")
                    val content = file.readText()
                    val links = parseLinksFromJson(content, bookTitle)
                    bookTitle to links
                }
            }.mapNotNull { deferred ->
                runCatching { deferred.await() }
                    .onFailure { e -> logger.w(e) { "Failed to preload links from file" } }
                    .getOrNull()
            }.toMap(mutableMapOf())
        }

        logger.i { "Processing links from RAM..." }
        var totalLinks = 0
        for ((bookTitle, links) in linksByBook) {
            val processedLinks = processLinksForBook(bookTitle, links)
            totalLinks += processedLinks
            logger.d { "Processed $processedLinks links for $bookTitle, total so far: $totalLinks" }
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

        // Find the source book (use RAM cache)
        val sourceBook = booksByTitle[bookTitle]

        if (sourceBook == null) {
            logger.w { "Source book not found for links: $bookTitle" }
            return 0
        }
        logger.d { "Found source book with ID: ${sourceBook.id}" }

        try {
            val content = linkFile.readText()
            logger.d { "Link file content length: ${content.length}" }
            val links = parseLinksFromJson(content, bookTitle)
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

                    // Try to find the target book (use RAM cache)
                    val targetBook = booksByTitle[targetTitle]
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

                    // Try to find the source line (use RAM cache)
                    val sourceLineId = getLineIdCached(sourceBook.id, sourceLineIndex)
                    if (sourceLineId == null) {
                        logger.d { "Source line not found at index: $sourceLineIndex, skipping this link but continuing with others" }
                        continue
                    }
                    logger.d { "Using source line with ID: $sourceLineId" }

                    logger.d { "Looking for target line at index: $targetLineIndex (original: ${linkData.line_index_2}) in book ${targetBook.id}" }

                    // Try to find the target line (use RAM cache)
                    val targetLineId = getLineIdCached(targetBook.id, targetLineIndex)
                    if (targetLineId == null) {
                        logger.d { "Target line not found at index: $targetLineIndex, skipping this link but continuing with others" }
                        continue
                    }
                    logger.d { "Using target line with ID: $targetLineId" }

                    val link = Link(
                        sourceBookId = sourceBook.id,
                        targetBookId = targetBook.id,
                        sourceLineId = sourceLineId,
                        targetLineId = targetLineId,
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
     * Processes links that were preloaded in memory for a given source book.
     */
    private suspend fun processLinksForBook(bookTitle: String, links: List<LinkData>): Int {
        val sourceBook = booksByTitle[bookTitle]
        if (sourceBook == null) {
            logger.w { "Source book not found for links: $bookTitle" }
            return 0
        }
        var processed = 0
        for ((index, linkData) in links.withIndex()) {
            try {
                val path = linkData.path_2
                val targetTitle = if (path.contains('\\')) {
                    val lastComponent = path.split('\\').last()
                    lastComponent.substringBeforeLast('.', lastComponent)
                } else {
                    val targetPath = Paths.get(path)
                    targetPath.fileName.toString().substringBeforeLast('.')
                }

                val targetBook = booksByTitle[targetTitle]
                if (targetBook == null) {
                    logger.i { "Link ${index + 1}/${links.size} - Target book not found: $targetTitle" }
                    logger.i { "Original path: ${linkData.path_2}" }
                    continue
                }

                val sourceLineIndex = (linkData.line_index_1.toInt() - 1).coerceAtLeast(0)
                val targetLineIndex = (linkData.line_index_2.toInt() - 1).coerceAtLeast(0)

                val sourceLineId = getLineIdCached(sourceBook.id, sourceLineIndex)
                if (sourceLineId == null) continue

                val targetLineId = getLineIdCached(targetBook.id, targetLineIndex)
                if (targetLineId == null) continue

                val link = Link(
                    sourceBookId = sourceBook.id,
                    targetBookId = targetBook.id,
                    sourceLineId = sourceLineId,
                    targetLineId = targetLineId,
                    connectionType = ConnectionType.fromString(linkData.connectionType)
                )
                repository.insertLink(link)
                processed++
            } catch (_: Exception) {
                // Skip malformed entries but continue
            }
        }
        return processed
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
     * Fetch and sanitize acronym terms for a given book title from the Acronymizer DB.
     */
    private fun fetchAcronymsForTitle(title: String): List<String> {
        val path = acronymDbPath ?: return emptyList()
        try {
            if (acronymDb == null) {
                acronymDb = java.sql.DriverManager.getConnection("jdbc:sqlite:$path")
            }
            val conn = acronymDb ?: return emptyList()

            val lookupTitles = buildList {
                add(title)
                val stripped = stripQuotesForLookup(title)
                if (stripped.isNotBlank()) add(stripped)
                val noComma = title.replace(",", " ").trim()
                if (noComma.isNotBlank()) add(noComma)
                val noPunct = title.replace("[\\p{Punct}]".toRegex(), " ").replace("\\s+".toRegex(), " ").trim()
                if (noPunct.isNotBlank()) add(noPunct)
                val sanitized = sanitizeAcronymTerm(title)
                if (sanitized.isNotBlank()) add(sanitized)
            }.distinct()
            var rawTerms: String? = null
            for (candidate in lookupTitles) {
                conn.prepareStatement(
                    "SELECT terms FROM AcronymResults WHERE book_title = ? ORDER BY id DESC LIMIT 1"
                ).use { ps ->
                    ps.setString(1, candidate)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) {
                            rawTerms = rs.getString(1)
                            if (!rawTerms.isNullOrBlank()) break
                        }
                    }
                }
            }

            val raw = rawTerms ?: return emptyList()
            val parts = raw.split(',')
            val clean = parts
                .map { sanitizeAcronymTerm(it) }
                .map { it.trim() }
                .filter { it.isNotEmpty() }

            // De-duplicate and drop items identical to the title after normalization
            val titleNormalized = sanitizeAcronymTerm(title)
            return clean
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .filter { !it.equals(title, ignoreCase = true) }
                .filter { !it.equals(titleNormalized, ignoreCase = true) }
                .distinct()
        } catch (e: Exception) {
            logger.w(e) { "Error reading acronyms for '$title' from $path" }
            return emptyList()
        }
    }

    private suspend fun backfillAcronymsForExistingBooks() {
        val path = acronymDbPath ?: return
        logger.i { "Backfilling missing acronyms from $path for existing books..." }
        val books = runCatching { repository.getAllBooks() }.getOrDefault(emptyList())
        var insertedCount = 0
        var touchedBooks = 0
        for (book in books) {
            val existing = runCatching { repository.getAcronymsForBook(book.id) }.getOrDefault(emptyList())
            if (existing.isNotEmpty()) continue
            val terms = fetchAcronymsForTitle(book.title)
            if (terms.isNotEmpty()) {
                repository.bulkInsertBookAcronyms(book.id, terms)
                insertedCount += terms.size
                touchedBooks += 1
            }
        }
        logger.i { "Backfill complete: added $insertedCount acronyms to $touchedBooks books" }
    }

    // Clean an acronym using HebrewTextUtils and remove gershayim
    private fun sanitizeAcronymTerm(raw: String): String {
        var s = raw.trim()
        if (s.isEmpty()) return ""
        s = HebrewTextUtils.removeAllDiacritics(s)
        s = HebrewTextUtils.replaceMaqaf(s, " ")
        s = s.replace("\u05F4", "") // remove Hebrew gershayim (״)
        s = s.replace("\u05F3", "") // remove Hebrew geresh (׳)
        s = s.replace("\\s+".toRegex(), " ").trim()
        return s
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

    // Removed: FTS rebuild is obsolete (Lucene index is committed in this run)

    /**
     * Updates the book_has_links table to indicate which books have source links, target links, or both.
     * This should be called after all links have been processed.
     */
    private suspend fun updateBookHasLinksTable() {
        logger.i { "Updating book_has_links table with separate source and target link flags..." }

        // Ensure all books are present in book_has_links
        runCatching {
            repository.executeRawQuery(
                "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
                        "SELECT id, 0, 0 FROM book"
            )
        }

        // Reset flags, then set from aggregated distinct sets
        runCatching { repository.executeRawQuery("UPDATE book_has_links SET hasSourceLinks=0, hasTargetLinks=0") }
        runCatching {
            repository.executeRawQuery(
                "UPDATE book_has_links SET hasSourceLinks=1 " +
                        "WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
            )
        }
        runCatching {
            repository.executeRawQuery(
                "UPDATE book_has_links SET hasTargetLinks=1 " +
                        "WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
            )
        }

        // Reset book per-connection-type flags
        runCatching {
            repository.executeRawQuery(
                "UPDATE book SET hasTargumConnection=0, hasReferenceConnection=0, hasCommentaryConnection=0, hasOtherConnection=0"
            )
        }

        // Helper to set a type flag in one statement
        suspend fun setConnFlag(typeName: String, column: String) {
            val sql = "UPDATE book SET $column=1 WHERE id IN (" +
                    "SELECT DISTINCT bId FROM (" +
                    "SELECT sourceBookId AS bId FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId WHERE ct.name='$typeName' " +
                    "UNION " +
                    "SELECT targetBookId AS bId FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId WHERE ct.name='$typeName'" +
                    ")" +
                    ")"
            repository.executeRawQuery(sql)
        }

        runCatching { setConnFlag("TARGUM", "hasTargumConnection") }
        runCatching { setConnFlag("REFERENCE", "hasReferenceConnection") }
        runCatching { setConnFlag("COMMENTARY", "hasCommentaryConnection") }
        runCatching { setConnFlag("OTHER", "hasOtherConnection") }

        // Quick summary counts (single queries)
        val totalBooks = repository.getAllBooks().size
        val booksWithSourceLinks = repository.countBooksWithSourceLinks().toInt()
        val booksWithTargetLinks = repository.countBooksWithTargetLinks().toInt()
        val booksWithAnyLinks = repository.countBooksWithAnyLinks().toInt()

        logger.i { "Book_has_links table updated. Found:" }
        logger.i { "- $booksWithSourceLinks books with source links" }
        logger.i { "- $booksWithTargetLinks books with target links" }
        logger.i { "- $booksWithAnyLinks books with any links (source or target)" }
        logger.i { "- $totalBooks total books" }
    }



    /**
     * Parses links from JSON content, handling both Ben-YehudaToOtzaria and DictaToOtzaria formats
     */
    private fun parseLinksFromJson(content: String, bookTitle: String): List<LinkData> {
        return try {
            // First, try to parse as Ben-YehudaToOtzaria format (List<LinkData>)
            json.decodeFromString<List<LinkData>>(content)
        } catch (e: Exception) {
            // If that fails, try to parse as DictaToOtzaria format (Map<String, List<DictaLinkData>>)
            try {
                val dictaLinksMap = json.decodeFromString<Map<String, List<DictaLinkData>>>(content)
                logger.d { "Successfully parsed DictaToOtzaria format for $bookTitle with ${dictaLinksMap.size} line groups" }
                
                // Convert DictaToOtzaria format to LinkData format
                // Note: DictaToOtzaria format doesn't map perfectly to LinkData structure
                // We'll need to handle this conversion carefully or skip for now
                logger.w { "DictaToOtzaria format conversion not yet implemented for $bookTitle" }
                emptyList<LinkData>()
            } catch (e2: Exception) {
                logger.w(e2) { "Failed to parse links from file for $bookTitle in any known format" }
                emptyList<LinkData>()
            }
        }
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

    /**
     * Data class for DictaToOtzaria link format
     */
    @Serializable
    private data class DictaLinkData(
        val start: Int,
        val end: Int,
        val refs: Map<String, String>
    )

}
