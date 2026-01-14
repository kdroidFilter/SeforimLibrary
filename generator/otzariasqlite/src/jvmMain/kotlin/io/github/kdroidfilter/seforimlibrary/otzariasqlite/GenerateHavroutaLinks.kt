package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File
import java.nio.file.Paths

/**
 * Generates links between Havrouta commentaries and their corresponding Talmud tractates.
 *
 * Havrouta books contain the original Talmud text in bold (<b>...</b> tags).
 * This script extracts the bold text and matches it to the corresponding Talmud lines.
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateHavroutaLinks -PseforimDb=/path/to.db
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateHavroutaLinks")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()

    val jdbcUrl = "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repository = SeforimRepository(dbPath, driver)

    val sourceDir = System.getProperty("sourceDir")
        ?: System.getenv("OTZARIA_SOURCE_DIR")
        ?: OtzariaFetcher.ensureLocalSource(logger).toString()

    try {
        logger.i { "Starting Havrouta-Talmud link generation..." }
        val talmudLinksCreated = generateHavroutaLinks(repository, logger)
        logger.i { "Havrouta-Talmud link generation completed. Created $talmudLinksCreated links." }

        logger.i { "Starting Havrouta-Hearot link generation from Otzaria files..." }
        val hearotLinksCreated = generateHavroutaHearotLinks(repository, logger, sourceDir)
        logger.i { "Havrouta-Hearot link generation completed. Created $hearotLinksCreated links." }

        logger.i { "Total links created: ${talmudLinksCreated + hearotLinksCreated}" }
    } catch (e: Exception) {
        logger.e(e) { "Error generating Havrouta links" }
        throw e
    } finally {
        repository.close()
    }
}

/**
 * Data class representing a daf section with its line range.
 */
private data class DafSection(
    val dafRef: String,
    val startLineIndex: Int,
    val endLineIndex: Int  // exclusive
)

/**
 * Mapping of Havrouta tractate names to Talmud tractate names (for special cases).
 */
private val tractateNameMapping = mapOf(
    "נידה" to "נדה"
)

/**
 * Regex to extract bold text from Havrouta lines.
 */
private val boldPattern = Regex("""<b>([^<]+)</b>""")

/**
 * Regex to extract daf headers.
 */
private val havroutaDafPattern = Regex("""<h3>\s*דף\s+([^<]+)</h3>""")
private val talmudDafPattern = Regex("""<h2>דף\s*([^<]+)</h2>""")

/**
 * Normalizes text for comparison by removing nikud, punctuation, and extra spaces.
 */
private fun normalizeText(text: String): String {
    return text
        // Remove Hebrew nikud (vowel marks)
        .replace(Regex("[\u0591-\u05C7]"), "")
        // Remove punctuation and special characters
        .replace(Regex("[()\\[\\]{}\"',.;:!?׳״]"), "")
        // Normalize spaces
        .replace(Regex("\\s+"), " ")
        .trim()
        .lowercase()
}

/**
 * Extracts bold text from a Havrouta line.
 */
private fun extractBoldText(content: String): String {
    val matches = boldPattern.findAll(content)
    return matches.map { it.groupValues[1] }.joinToString(" ")
}

/**
 * Checks if a line is a section header (מתניתין, גמרא, etc.)
 */
private fun isSectionHeader(content: String): Boolean {
    val headerPatterns = listOf(
        "<big><b>מתניתין",
        "<big><b>גמרא",
        "<big><b>הדרן",
        "<h2>", "<h3>", "<h4>"
    )
    return headerPatterns.any { content.contains(it) }
}

/**
 * Generates links between Havrouta books and their corresponding Talmud tractates.
 */
private suspend fun generateHavroutaLinks(
    repository: SeforimRepository,
    logger: Logger
): Int {
    // Find all Havrouta books
    val havroutaBooks = repository.getAllBooks().filter { it.title.startsWith("חברותא על ") }
    logger.i { "Found ${havroutaBooks.size} Havrouta books" }

    // Find all Talmud Bavli tractates
    val talmudBooks = repository.getAllBooks().filter { book ->
        book.sourceId == 1L &&
            !book.title.startsWith("משנה") &&
            !book.title.startsWith("תלמוד ירושלמי") &&
            !book.title.startsWith("תוספתא")
    }.associateBy { it.title }

    var totalLinksCreated = 0

    // Delete any existing Havrouta-Talmud links before creating new ones
    for (havroutaBook in havroutaBooks) {
        repository.executeRawQuery(
            "DELETE FROM link WHERE sourceBookId = ${havroutaBook.id} OR targetBookId = ${havroutaBook.id}"
        )
    }
    logger.i { "Deleted existing Havrouta links" }

    for (havroutaBook in havroutaBooks) {
        val tractateName = havroutaBook.title.removePrefix("חברותא על ")
        val talmudTractateName = tractateNameMapping[tractateName] ?: tractateName

        val talmudBook = talmudBooks[talmudTractateName]
        if (talmudBook == null) {
            logger.w { "No Talmud match for: ${havroutaBook.title}" }
            continue
        }

        logger.i { "Processing: ${havroutaBook.title} -> ${talmudBook.title}" }

        val linksForBook = processBookPair(
            repository = repository,
            havroutaBookId = havroutaBook.id,
            talmudBookId = talmudBook.id,
            havroutaTotalLines = havroutaBook.totalLines,
            talmudTotalLines = talmudBook.totalLines,
            logger = logger
        )

        logger.i { "  Created $linksForBook links" }
        totalLinksCreated += linksForBook
    }

    // Update book_has_links table
    updateBookHasLinks(repository, logger)

    return totalLinksCreated
}

/**
 * Processes a single Havrouta-Talmud book pair and creates links.
 */
private suspend fun processBookPair(
    repository: SeforimRepository,
    havroutaBookId: Long,
    talmudBookId: Long,
    havroutaTotalLines: Int,
    talmudTotalLines: Int,
    logger: Logger
): Int {
    // Load all lines for both books
    val havroutaLines = repository.getLines(havroutaBookId, 0, havroutaTotalLines - 1)
    val talmudLines = repository.getLines(talmudBookId, 0, talmudTotalLines - 1)

    // Extract daf sections from both books
    val havroutaDafs = extractDafSections(havroutaLines.map { it.content }, havroutaDafPattern)
    val talmudDafs = extractDafSections(talmudLines.map { it.content }, talmudDafPattern)

    // Create map of dafRef -> Talmud lines in that daf
    val talmudLinesByDaf = mutableMapOf<String, List<IndexedValue<String>>>()
    for (daf in talmudDafs) {
        val linesInDaf = talmudLines
            .filter { it.lineIndex >= daf.startLineIndex && it.lineIndex < daf.endLineIndex }
            .map { IndexedValue(it.lineIndex, normalizeText(it.content)) }
        talmudLinesByDaf[daf.dafRef] = linesInDaf
    }

    // Create map of lineIndex -> lineId for Talmud
    val talmudLineIdByIndex = talmudLines.associate { it.lineIndex to it.id }

    var linksCreated = 0
    val linkBatch = mutableListOf<Link>()

    // Process each Havrouta line
    var currentDafRef: String? = null
    var lastMatchedIndex = 0  // Track last matched line for sequential reading
    for (havroutaLine in havroutaLines) {
        // Check if this is a daf header
        val dafMatch = havroutaDafPattern.find(havroutaLine.content)
        if (dafMatch != null) {
            currentDafRef = dafMatch.groupValues[1].trim()
            lastMatchedIndex = 0  // Reset for new daf
            continue
        }

        // Skip if we haven't encountered a daf yet
        if (currentDafRef == null) continue

        // Skip section headers
        if (isSectionHeader(havroutaLine.content)) continue

        // Extract bold text
        val boldText = extractBoldText(havroutaLine.content)
        if (boldText.isBlank()) continue

        val normalizedBold = normalizeText(boldText)
        if (normalizedBold.length < 5) continue  // Skip very short matches

        // Find matching Talmud line in the same daf
        val talmudLinesInDaf = talmudLinesByDaf[currentDafRef] ?: continue

        // Find the best matching line
        val matchingLineIndex = findBestMatch(normalizedBold, talmudLinesInDaf, lastMatchedIndex)
        if (matchingLineIndex == null) continue

        lastMatchedIndex = matchingLineIndex  // Update for sequential reading

        val talmudLineId = talmudLineIdByIndex[matchingLineIndex] ?: continue

        // Create link: Talmud -> Havrouta (Talmud has Havrouta as commentary)
        linkBatch.add(Link(
            sourceBookId = talmudBookId,
            targetBookId = havroutaBookId,
            sourceLineId = talmudLineId,
            targetLineId = havroutaLine.id,
            connectionType = ConnectionType.COMMENTARY
        ))

        // Create reverse link: Havrouta -> Talmud (Havrouta's source is Talmud)
        linkBatch.add(Link(
            sourceBookId = havroutaBookId,
            targetBookId = talmudBookId,
            sourceLineId = havroutaLine.id,
            targetLineId = talmudLineId,
            connectionType = ConnectionType.SOURCE
        ))

        linksCreated += 2

        // Batch insert
        if (linkBatch.size >= 1000) {
            repository.insertLinksBatch(linkBatch)
            linkBatch.clear()
        }
    }

    // Insert remaining links
    if (linkBatch.isNotEmpty()) {
        repository.insertLinksBatch(linkBatch)
    }

    return linksCreated
}

/**
 * Extracts daf sections from a list of line contents.
 */
private fun extractDafSections(contents: List<String>, pattern: Regex): List<DafSection> {
    val sections = mutableListOf<DafSection>()
    var lastDafRef: String? = null
    var lastStartIndex = 0

    for ((index, content) in contents.withIndex()) {
        val match = pattern.find(content)
        if (match != null) {
            // Close previous section
            if (lastDafRef != null) {
                sections.add(DafSection(lastDafRef, lastStartIndex, index))
            }
            lastDafRef = match.groupValues[1].trim()
            lastStartIndex = index
        }
    }

    // Close last section
    if (lastDafRef != null) {
        sections.add(DafSection(lastDafRef, lastStartIndex, contents.size))
    }

    return sections
}

/**
 * Finds the best matching Talmud line for a given bold text.
 * Uses a sequential approach where we start searching from lastMatchedIndex.
 * Returns the lineIndex of the best match, or null if no good match found.
 */
private fun findBestMatch(
    normalizedBoldText: String,
    talmudLines: List<IndexedValue<String>>,
    lastMatchedIndex: Int
): Int? {
    if (normalizedBoldText.length < 3) return null

    // Search all lines but prefer lines >= lastMatchedIndex
    val linesAfter = talmudLines.filter { it.index >= lastMatchedIndex }
    val linesBefore = talmudLines.filter { it.index < lastMatchedIndex }

    // First try exact substring match on lines after lastMatchedIndex
    for (line in linesAfter) {
        if (line.value.contains(normalizedBoldText)) {
            return line.index
        }
    }

    // Try matching first significant words on lines after
    val words = normalizedBoldText.split(" ").filter { it.length > 1 }
    if (words.size >= 2) {
        val firstWords = words.take(3).joinToString(" ")
        for (line in linesAfter) {
            if (line.value.contains(firstWords)) {
                return line.index
            }
        }
    }

    // Try significant overlap (but only forward) - this catches minor text variations
    if (normalizedBoldText.length >= 8) {
        for (line in linesAfter) {
            if (hasSignificantOverlap(normalizedBoldText, line.value, 8)) {
                return line.index
            }
        }
    }

    // If nothing found forward, search backwards (but be stricter)
    // Only use exact substring match when going backwards
    for (line in linesBefore) {
        if (line.value.contains(normalizedBoldText)) {
            return line.index
        }
    }

    // Last resort: try first 2-3 words on all lines
    if (words.size >= 2) {
        val twoWords = words.take(2).joinToString(" ")
        for (line in linesAfter + linesBefore) {
            if (line.value.contains(twoWords)) {
                return line.index
            }
        }
    }

    return null
}

/**
 * Checks if two strings have significant overlap (shared substring of minLen+ chars).
 */
private fun hasSignificantOverlap(text1: String, text2: String, minLen: Int = 6): Boolean {
    if (text1.length < minLen || text2.length < minLen) return false

    // Check overlapping substrings from text1
    for (i in 0..text1.length - minLen) {
        val sub = text1.substring(i, minOf(i + minLen + 4, text1.length))
        if (text2.contains(sub)) return true
    }
    return false
}

/**
 * Updates the book_has_links table after generating links.
 */
private suspend fun updateBookHasLinks(repository: SeforimRepository, logger: Logger) {
    logger.i { "Updating book_has_links table..." }

    repository.executeRawQuery(
        "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
            "SELECT id, 0, 0 FROM book"
    )

    repository.executeRawQuery(
        "UPDATE book_has_links SET hasSourceLinks=1 " +
            "WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
    )

    repository.executeRawQuery(
        "UPDATE book_has_links SET hasTargetLinks=1 " +
            "WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
    )

    repository.executeRawQuery(
        "UPDATE book SET hasCommentaryConnection=1 WHERE id IN (" +
            "SELECT DISTINCT sourceBookId FROM link l " +
            "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
            "WHERE ct.name='COMMENTARY'" +
            ")"
    )

    repository.executeRawQuery(
        "UPDATE book SET hasSourceConnection=1 WHERE id IN (" +
            "SELECT DISTINCT sourceBookId FROM link l " +
            "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
            "WHERE ct.name='SOURCE'" +
            ")"
    )

    logger.i { "book_has_links table updated" }
}

/**
 * Data class for parsing Otzaria link JSON files.
 */
@Serializable
private data class OtzariaLinkData(
    val line_index_1: Long,
    val line_index_2: Long,
    val heRef_2: String,
    val path_2: String,
    @SerialName("Conection Type")
    val connectionType: String
)

private val json = Json {
    ignoreUnknownKeys = true
    coerceInputValues = true
}

/**
 * Generates links between Havrouta and Hearot al Havrouta using Otzaria link files.
 */
private suspend fun generateHavroutaHearotLinks(
    repository: SeforimRepository,
    logger: Logger,
    sourceDir: String
): Int {
    val linksDir = File(sourceDir, "links")
    if (!linksDir.exists()) {
        logger.w { "Links directory not found: ${linksDir.absolutePath}" }
        return 0
    }

    // Get all Havrouta books
    val allBooks = repository.getAllBooks()
    val havroutaBooks = allBooks.filter { it.title.startsWith("חברותא על ") }
    val booksByTitle = allBooks.associateBy { it.title }

    logger.i { "Found ${havroutaBooks.size} Havrouta books" }

    var totalLinksCreated = 0

    for (havroutaBook in havroutaBooks) {
        val linkFileName = "${havroutaBook.title}_links.json"
        val linkFile = File(linksDir, linkFileName)

        if (!linkFile.exists()) {
            logger.d { "No link file found for ${havroutaBook.title}" }
            continue
        }

        logger.i { "Processing links for ${havroutaBook.title}" }

        try {
            val content = linkFile.readText()
            val links: List<OtzariaLinkData> = json.decodeFromString(content)

            // Filter for links to הערות על חברותא
            val hearotLinks = links.filter { it.path_2.contains("הערות על חברותא") }

            if (hearotLinks.isEmpty()) {
                logger.d { "No Hearot links found in ${linkFileName}" }
                continue
            }

            logger.i { "Found ${hearotLinks.size} links to Hearot" }

            // Get lines for the Havrouta book
            val havroutaLines = repository.getLines(havroutaBook.id, 0, havroutaBook.totalLines - 1)
            val havroutaLinesByIndex = havroutaLines.associateBy { it.lineIndex }

            var linksCreated = 0
            val linkBatch = mutableListOf<Link>()

            for (linkData in hearotLinks) {
                // Extract target book title from path
                val targetTitle = linkData.path_2.split('\\').last().substringBeforeLast('.')
                val targetBook = booksByTitle[targetTitle]

                if (targetBook == null) {
                    logger.d { "Target book not found: $targetTitle" }
                    continue
                }

                // Get line indices (Otzaria uses 1-based indices)
                val sourceLineIndex = (linkData.line_index_1.toInt() - 1).coerceAtLeast(0)
                val targetLineIndex = (linkData.line_index_2.toInt() - 1).coerceAtLeast(0)

                val sourceLine = havroutaLinesByIndex[sourceLineIndex]
                if (sourceLine == null) {
                    logger.d { "Source line not found at index $sourceLineIndex" }
                    continue
                }

                // Get target line
                val targetLines = repository.getLines(targetBook.id, targetLineIndex, targetLineIndex)
                if (targetLines.isEmpty()) {
                    logger.d { "Target line not found at index $targetLineIndex in ${targetBook.title}" }
                    continue
                }
                val targetLine = targetLines.first()

                // Create link: Havrouta -> Hearot (COMMENTARY)
                linkBatch.add(Link(
                    sourceBookId = havroutaBook.id,
                    targetBookId = targetBook.id,
                    sourceLineId = sourceLine.id,
                    targetLineId = targetLine.id,
                    connectionType = ConnectionType.COMMENTARY
                ))

                // Create reverse link: Hearot -> Havrouta (SOURCE)
                linkBatch.add(Link(
                    sourceBookId = targetBook.id,
                    targetBookId = havroutaBook.id,
                    sourceLineId = targetLine.id,
                    targetLineId = sourceLine.id,
                    connectionType = ConnectionType.SOURCE
                ))

                linksCreated += 2

                // Batch insert
                if (linkBatch.size >= 1000) {
                    for (link in linkBatch) {
                        repository.insertLink(link)
                    }
                    linkBatch.clear()
                }
            }

            // Insert remaining links
            for (link in linkBatch) {
                repository.insertLink(link)
            }
            linkBatch.clear()

            totalLinksCreated += linksCreated
            logger.i { "Created $linksCreated links for ${havroutaBook.title}" }

        } catch (e: Exception) {
            logger.e(e) { "Error processing links for ${havroutaBook.title}" }
        }
    }

    return totalLinksCreated
}
