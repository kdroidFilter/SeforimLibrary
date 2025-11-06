package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.name
import kotlin.io.path.nameWithoutExtension
import kotlin.io.path.readLines

/**
 * Generator for converting the Sefaria dump located under generator/build/Sefaria
 * into the SQLite schema expected by the DAO (same format as the Otzaria pipeline).
 *
 * - Creates categories based on the directory structure under txt/
 * - Creates books per text folder (language-specific; defaults to Hebrew)
 * - Parses merged.txt to insert lines and TOC entries (Chapter/Paragraph patterns)
 * - Fills line_toc mapping for fast line → section resolution
 *
 * Links (export_links) parsing is left as a follow-up (varies by corpus; requires
 * per‑text citation parsing). This class focuses on faithfully populating the
 * core schema (category, book, line, tocEntry, line_toc) without changing structure.
 */
class SefariaGenerator(
    private val sefariaRoot: Path,
    private val repository: SeforimRepository,
    private val languageWhitelist: Set<String> = setOf("Hebrew")
) {
    private val logger = Logger.withTag("SefariaGenerator")

    private val json = Json { ignoreUnknownKeys = true }

    // Running explicit IDs to mirror the Otzaria generator behavior.
    // Assumes generation into a fresh database.
    private var nextBookId = 1L
    private var nextLineId = 1L
    private var nextTocEntryId = 1L

    // Cache for category paths we already created: key = joined path, value = id
    private val categoryPathCache = mutableMapOf<String, Long>()
    // Library root used for relative keys under txt/
    private lateinit var libraryRoot: Path
    // Preloaded file contents (key = library-relative merged.txt path)
    private val fileContentCache = mutableMapOf<String, List<String>>()
    // Book content cache: bookId -> list of lines (for in-RAM link mapping)
    private val bookContentCache = mutableMapOf<Long, List<String>>()
    // For each book, an array mapping lineIndex -> inserted lineId
    private val lineIdsByBook = mutableMapOf<Long, LongArray>()
    // English title -> Hebrew title from Sefaria sources (TOC or schema)
    private val heTitleByEnTitle = mutableMapOf<String, String>()
    // English title -> Hebrew section names per level (from schema.heSectionNames)
    private val heSectionNamesByTitle = mutableMapOf<String, List<String>>()
    // English title -> Hebrew short description
    private val heShortDescByEnTitle = mutableMapOf<String, String>()
    // English category name -> Hebrew category name (from schema.heCategories)
    private val heCategoryByEnCategory = mutableMapOf<String, String>()

    suspend fun generate() = coroutineScope {
        logger.i { "Starting Sefaria generation from $sefariaRoot" }

        val txtRoot = sefariaRoot.resolve("txt")
        require(Files.isDirectory(txtRoot)) { "Missing txt folder at $txtRoot" }

        // SQLite perf toggles (same spirit as Otzaria generator)
        beginBulkMode()

        // Discover all merged.txt files for configured languages
        libraryRoot = txtRoot
        val files = Files.walk(libraryRoot).use { s ->
            s.filter { Files.isRegularFile(it) && it.name == "merged.txt" }.toList()
        }

        logger.i { "Found ${files.size} Sefaria merged.txt files" }
        // Preload all contents into RAM
        preloadAllBookContents(files)

        // Load Hebrew titles from TOC and schemas
        loadHeTitlesFromToc()
        loadSectionNamesFromSchemas()

        for ((index, file) in files.withIndex()) {
            val relative = txtRoot.relativize(file)
            val parts = relative.map { it.toString() }
            if (parts.size < 2) continue // at least <Title>/<Lang>/merged.txt

            // Expect .../<Title>/<Language>/merged.txt, possibly deeper categories before <Title>
            val language = parts.getOrNull(parts.size - 2) ?: ""
            if (languageWhitelist.isNotEmpty() && language !in languageWhitelist) {
                logger.d { "Skipping non-whitelisted language '$language' at $relative" }
                continue
            }

            // Category path excludes the last two segments (<Title>/<Lang>) and file name
            val categorySegments = parts.dropLast(2)
            val title = parts[parts.size - 3].ifEmpty { "Untitled" }

            val categoryId = ensureCategoryPath(categorySegments)

            // Prefer Hebrew title from TOC/schema; fallback to merged.txt headers
            val hebTitle = heTitleByEnTitle[title]
                ?: extractHebrewTitle(file)
                ?: title
            val heShort = heShortDescByEnTitle[title]
            val bookId = createBook(hebTitle, categoryId, heShort)
            processBookContents(bookId, title, file)

            if ((index + 1) % 50 == 0) {
                logger.i { "Progress: ${index + 1}/${files.size} books" }
            }
        }

        // Rebuild category_closure for hierarchical queries
        runCatching { repository.rebuildCategoryClosure() }

        logger.i { "Sefaria generation completed" }
    }

    // =========================
    // Links (CSV) processing
    // =========================

    private data class BookNavIndex(
        val chapterToBlocks: MutableMap<Int, MutableList<Int>> = mutableMapOf(),
        val chapterHeadingStart: MutableMap<Int, Int> = mutableMapOf()
    )

    private val navIndexCache = mutableMapOf<Long, BookNavIndex>()
    private var booksByTitle: Map<String, Long> = emptyMap()

    suspend fun processLinks(): Unit = coroutineScope {
        val linksDir = sefariaRoot.resolve("export_links").resolve("links")
        if (!Files.isDirectory(linksDir)) {
            logger.w { "No links directory found at $linksDir; skipping link import" }
            return@coroutineScope
        }

        // Build title → bookId map from DB
        runCatching {
            booksByTitle = repository.getAllBooks().associate { it.title to it.id }
            logger.i { "Loaded ${booksByTitle.size} books for link mapping" }
        }.onFailure { e ->
            logger.w(e) { "Failed to preload books; skipping link import" }
            return@coroutineScope
        }

        val csvFiles = Files.list(linksDir).use { s ->
            s.filter { Files.isRegularFile(it) && it.fileName.toString().startsWith("links") && !it.fileName.toString().contains("by_book") }
                .toList()
        }

        var inserted = 0
        var skipped = 0

        for (csv in csvFiles) {
            logger.i { "Processing links file: ${csv.fileName}" }
            val lines = csv.readLines(Charsets.UTF_8)
            if (lines.isEmpty()) continue
            // First line is header
            for (raw in lines.drop(1)) {
                if (raw.isBlank()) continue
                val cols = parseCsvLine(raw)
                if (cols.size < 7) { skipped++; continue }

                val citation1 = cols[0].trim()
                val citation2 = cols[1].trim()
                val typeRaw = cols[2].trim().lowercase()
                val text1 = cols[3].trim()
                val text2 = cols[4].trim()
                val cat1 = cols.getOrNull(5)?.trim()
                val cat2 = cols.getOrNull(6)?.trim()

                val srcBookId = booksByTitle[text1]
                val tgtBookId = booksByTitle[text2]
                if (srcBookId == null || tgtBookId == null) { skipped++; continue }

                val srcLineId = mapCitationToLineId(srcBookId, text1, citation1)
                val tgtLineId = mapCitationToLineId(tgtBookId, text2, citation2)
                if (srcLineId == null || tgtLineId == null) { skipped++; continue }

                val connType = mapConnectionTypeNoHeuristic(typeRaw)

                runCatching {
                    repository.insertLink(
                        io.github.kdroidfilter.seforimlibrary.core.models.Link(
                            sourceBookId = srcBookId,
                            targetBookId = tgtBookId,
                            sourceLineId = srcLineId,
                            targetLineId = tgtLineId,
                            connectionType = connType
                        )
                    )
                    inserted++
                }.onFailure { skipped++ }
            }
        }

        logger.i { "Links import completed. Inserted=$inserted, Skipped=$skipped" }

        // Update book_has_links and per-type flags (same logic as in Otzaria generator)
        runCatching { updateBookHasLinksTable() }

        // Restore PRAGMAs and VACUUM at the end of the full run
        endBulkModeAndVacuum()
    }

    private fun parseCsvLine(line: String): List<String> {
        val out = ArrayList<String>(8)
        val sb = StringBuilder()
        var i = 0
        var inQuotes = false
        while (i < line.length) {
            val c = line[i]
            when (c) {
                '"' -> {
                    if (inQuotes && i + 1 < line.length && line[i + 1] == '"') {
                        // Escaped quote
                        sb.append('"'); i += 1
                    } else {
                        inQuotes = !inQuotes
                    }
                }
                ',' -> {
                    if (inQuotes) sb.append(c) else {
                        out.add(sb.toString())
                        sb.setLength(0)
                    }
                }
                else -> sb.append(c)
            }
            i += 1
        }
        out.add(sb.toString())
        return out
    }

    // No heuristic: only respect explicit types; default to REFERENCE
    private fun mapConnectionTypeNoHeuristic(raw: String): io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType =
        if (raw.equals("commentary", ignoreCase = true))
            io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.COMMENTARY
        else
            io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType.REFERENCE

    private suspend fun mapCitationToLineId(bookId: Long, bookTitle: String, citation: String): Long? {
        val idx = buildNavIndexForBook(bookId) ?: return null
        val ref = parseNumericRef(citation)
        val lineIndex = when {
            ref == null -> null
            ref.size >= 2 -> {
                val chap = ref[0]
                val para = ref[1]
                val blocks = idx.chapterToBlocks[chap]
                if (blocks == null || blocks.isEmpty()) null
                else {
                    val i = (para - 1).coerceAtLeast(0)
                    if (i < blocks.size) blocks[i] else blocks.last()
                }
            }
            ref.size == 1 -> {
                val chap = ref[0]
                idx.chapterHeadingStart[chap]
            }
            else -> null
        }
        if (lineIndex == null) return null

        // Resolve from in-RAM mapping if available, otherwise fallback to DB
        lineIdsByBook[bookId]?.let { arr ->
            if (lineIndex in arr.indices) {
                val id = arr[lineIndex]
                if (id != 0L) return id
            }
        }
        val single = repository.getLines(bookId, lineIndex, lineIndex)
        return single.firstOrNull()?.id
    }

    // Extract ref numbers from a citation string. Supports forms like:
    // "Shulchan Arukh, Orach Chayim 355:1", "Mishnah Peah 6:6", "Sanhedrin 74b:9" -> [355,1], [6,6], [74,9]
    private fun parseNumericRef(s: String): List<Int>? {
        // Find trailing numeric group(s)
        val trimmed = s.trim()
        // Take last token sequence after last space that's likely the citation
        val tail = trimmed.substringAfterLast(' ', trimmed)

        // Normalize daf like 74b -> 74
        val normalized = tail.replace(Regex("([0-9]+)[abAB]"), "$1")
        val parts = normalized.split(':')
        val nums = parts.mapNotNull {
            it.trim().takeIf { it.isNotEmpty() }?.let { t -> t.toIntOrNull() }
        }
        return if (nums.isNotEmpty()) nums else null
    }

    private suspend fun buildNavIndexForBook(bookId: Long): BookNavIndex? {
        navIndexCache[bookId]?.let { return it }

        val idx = BookNavIndex()
        var currentChapter: Int? = null
        var lastWasBlank = true

        val content = bookContentCache[bookId]
        if (content != null) {
            content.forEachIndexed { lineIdx, text ->
                val t = text.trim()
                val level = detectHeaderLevel(t)
                if (level > 0) {
                    val chapNum = extractLeadingNumber(t)
                    if (chapNum != null) {
                        currentChapter = chapNum
                        idx.chapterHeadingStart[chapNum] = lineIdx
                        idx.chapterToBlocks.getOrPut(chapNum) { mutableListOf() }
                    }
                    lastWasBlank = true
                } else {
                    val isBlank = t.isEmpty()
                    if (!isBlank && lastWasBlank) {
                        val chap = currentChapter
                        if (chap != null) idx.chapterToBlocks.getOrPut(chap) { mutableListOf() }.add(lineIdx)
                    }
                    lastWasBlank = isBlank
                }
            }
            navIndexCache[bookId] = idx
            return idx
        }

        // Fallback to DB if no in-RAM content (e.g., links-only run)
        val book = repository.getBook(bookId) ?: return null
        val total = book.totalLines
        if (total <= 0) return null
        val lines = repository.getLines(bookId, 0, total - 1)

        for (ln in lines) {
            val t = ln.content.trim()
            val level = detectHeaderLevel(t)
            if (level > 0) {
                val chapNum = extractLeadingNumber(t)
                if (chapNum != null) {
                    currentChapter = chapNum
                    idx.chapterHeadingStart[chapNum] = ln.lineIndex
                    idx.chapterToBlocks.getOrPut(chapNum) { mutableListOf() }
                }
                lastWasBlank = true
            } else {
                val isBlank = t.isEmpty()
                if (!isBlank && lastWasBlank) {
                    val chap = currentChapter
                    if (chap != null) idx.chapterToBlocks.getOrPut(chap) { mutableListOf() }.add(ln.lineIndex)
                }
                lastWasBlank = isBlank
            }
        }
        navIndexCache[bookId] = idx
        return idx
    }

    private fun extractLeadingNumber(header: String): Int? {
        // Examples: "Chapter 12", "Siman 355", "Psalm 1", "פרק 6", "דף 12"
        val m = Regex("^(?:[A-Za-z]+|[\u0590-\u05FF]+)\\s+([0-9]+)").find(header)
        return m?.groupValues?.getOrNull(1)?.toIntOrNull()
    }

    // Borrowed (adapted) from Generator.kt
    private suspend fun updateBookHasLinksTable() {
        logger.i { "Updating book_has_links table for Sefaria links..." }

        runCatching {
            repository.executeRawQuery(
                "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
                        "SELECT id, 0, 0 FROM book"
            )
        }

        runCatching { repository.executeRawQuery("UPDATE book_has_links SET hasSourceLinks=0, hasTargetLinks=0") }
        runCatching {
            repository.executeRawQuery(
                "UPDATE book_has_links SET hasSourceLinks=1 WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
            )
        }
        runCatching {
            repository.executeRawQuery(
                "UPDATE book_has_links SET hasTargetLinks=1 WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
            )
        }

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
    }

    private suspend fun ensureCategoryPath(segments: List<String>): Long {
        if (segments.isEmpty()) return ensureCategory(null, "Sefaria", 0)

        var parentId: Long? = null
        var level = 0
        val keyParts = mutableListOf<String>()

        // Root marker to make keys unique under this generator
        keyParts.add("Sefaria")
        parentId = ensureCategory(null, "Sefaria", level)
        level += 1

        for (segEn in segments) {
            val seg = heCategoryByEnCategory[segEn] ?: segEn
            keyParts.add(seg)
            val key = keyParts.joinToString("/")
            val cached = categoryPathCache[key]
            if (cached != null) {
                parentId = cached
                level += 1
                continue
            }
            val id = ensureCategory(parentId, seg, level)
            categoryPathCache[key] = id
            parentId = id
            level += 1
        }
        return parentId ?: ensureCategory(null, "Sefaria", 0)
    }

    private suspend fun ensureCategory(parentId: Long?, title: String, level: Int): Long {
        val cat = Category(parentId = parentId, title = title, level = level)
        return repository.insertCategory(cat)
    }

    private suspend fun createBook(title: String, categoryId: Long, heShortDesc: String?): Long {
        val id = nextBookId++
        val book = Book(
            id = id,
            categoryId = categoryId,
            title = title,
            heShortDesc = heShortDesc,
            order = 999f,
            totalLines = 0
        )
        repository.insertBook(book)
        return id
    }

    private suspend fun processBookContents(bookId: Long, title: String, file: Path) {
        logger.i { "Processing '$title' (bookId=$bookId)" }
        val key = toLibraryRelativeKey(file)
        val rawLines = fileContentCache[key] ?: file.readLines(Charsets.UTF_8)
        val start = guessContentStartIndex(rawLines)
        val lines = if (start in rawLines.indices) rawLines.drop(start) else rawLines

        // Keep in RAM for link processing
        bookContentCache[bookId] = lines
        val lineIds = LongArray(lines.size) { 0L }

        var lineIndex = 0
        val parentStack = mutableMapOf<Int, Long>()
        var currentOwningTocId: Long? = null
        val childMap = mutableMapOf<Long?, MutableList<Long>>()
        val lineTocPairs = ArrayList<Pair<Long, Long>>(minOf(lines.size, 200_000))

        for (l in lines) {
            val trimmed = l.trim()
            val level = detectHeaderLevel(trimmed)
            if (level > 0) {
                // Create TOC entry and a line for the heading itself
                val parentId = (level - 1 downTo 1).firstNotNullOfOrNull { parentStack[it] }
                val tocId = nextTocEntryId++
                val headingLineId = nextLineId++

                // Insert heading line
                repository.insertLine(
                    Line(
                        id = headingLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = trimmed
                    )
                )
                if (lineIndex < lineIds.size) lineIds[lineIndex] = headingLineId

                // Insert TOC entry (temporary: no children/last flags yet)
                val tocText = computeHebrewTocTextForLevel(title, level, trimmed)
                val tocEntry = TocEntry(
                    id = tocId,
                    bookId = bookId,
                    parentId = parentId,
                    text = tocText,
                    level = level,
                    lineId = null,
                    isLastChild = false,
                    hasChildren = false
                )
                repository.insertTocEntry(tocEntry)

                // Link toc entry to heading line
                repository.updateTocEntryLineId(tocId, headingLineId)

                // Update stacks and child maps
                parentStack[level] = tocId
                currentOwningTocId = tocId
                childMap.getOrPut(parentId) { mutableListOf() }.add(tocId)

                // Heading line belongs to itself
                lineTocPairs.add(headingLineId to tocId)
                lineIndex += 1
            } else {
                // Regular content line
                val contentLineId = nextLineId++
                repository.insertLine(
                    Line(
                        id = contentLineId,
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = l
                    )
                )
                if (lineIndex < lineIds.size) lineIds[lineIndex] = contentLineId
                currentOwningTocId?.let { lineTocPairs.add(contentLineId to it) }
                lineIndex += 1
            }
        }

        // Compute hasChildren and isLastChild
        for ((parent, children) in childMap) {
            if (children.isEmpty()) continue
            for ((i, childId) in children.withIndex()) {
                if (i == children.lastIndex) repository.updateTocEntryIsLastChild(childId, true)
            }
            parent?.let { repository.updateTocEntryHasChildren(it, true) }
        }

        // Bulk mapping line → owning toc entry
        repository.bulkUpsertLineToc(lineTocPairs)

        // Update book lines count
        repository.updateBookTotalLines(bookId, lineIndex)
        // Store line ids mapping
        lineIdsByBook[bookId] = lineIds
    }

    // Heuristics to skip the Sefaria merged.txt banner/header.
    private fun guessContentStartIndex(lines: List<String>): Int {
        if (lines.isEmpty()) return 0
        // Look for the first structural heading (Chapter/Paragraph/etc.).
        for ((i, raw) in lines.withIndex()) {
            val t = raw.trim()
            val lvl = detectHeaderLevel(t)
            if (lvl > 0) return i
        }
        return 0
    }

    // Extract the textual part to store in tocText (drop numbering keywords when possible).
    // Compute Hebrew TOC text from schema section names when available
    private fun computeHebrewTocTextForLevel(enTitle: String, level: Int, rawHeader: String): String {
        val heNames = heSectionNamesByTitle[enTitle]
        if (heNames != null && level in 1..heNames.size) {
            val n = extractLeadingNumber(rawHeader)
            val label = heNames[level - 1]
            if (n != null) return "$label $n"
            return label
        }
        // Fallback: return the raw header as-is
        return rawHeader.trim()
    }

    // Detects heading levels for common Sefaria merged.txt patterns.
    // Level 1: Chapter, Psalm, Perek, Siman, Daf, Book, Introduction, Preface, Mishnah, Masachet, Petichta
    // Level 2: Paragraph, Halakha/Seif/Verse/Letter, Segment
    private fun detectHeaderLevel(t: String): Int {
        if (t.isEmpty()) return 0

        val lower = t.lowercase()
        // Common English
        if (lower.startsWith("chapter ") || lower.startsWith("psalm ") || lower.startsWith("book ") ||
            lower.startsWith("introduction") || lower.startsWith("preface") || lower.startsWith("mishnah ") ||
            lower.startsWith("daf ") || lower.startsWith("massechet ") || lower.startsWith("tractate ") ||
            lower.startsWith("petichta "))
            return 1

        if (lower.startsWith("paragraph ") || lower.startsWith("halakha ") || lower.startsWith("seif ") ||
            lower.startsWith("verse ") || lower.startsWith("letter ") || lower.startsWith("segment "))
            return 2

        // Hebrew patterns
        if (t.startsWith("פרק ") || t.startsWith("תהילים ") || t.startsWith("תהלים ") ||
            t.startsWith("סימן ") || t.startsWith("דף ") || t.startsWith("מסכת ") ||
            t.startsWith("פתיחתא ") || t.startsWith("פתיחה "))
            return 1

        if (t.startsWith("הלכה ") || t.startsWith("סעיף ") || t.startsWith("אות ") || t.startsWith("פסוק "))
            return 2

        return 0
    }

    // ===== Hebrew normalization =====

    private fun containsHebrew(s: String): Boolean = s.any { ch -> ch.code in 0x0590..0x05FF }

    private fun extractHebrewTitle(file: Path): String? {
        val key = toLibraryRelativeKey(file)
        val lines = fileContentCache[key] ?: return null
        // Inspect the first ~8 lines to find a strong Hebrew title candidate
        val head = lines.take(8)
        var best: String? = null
        var bestScore = -1
        for (ln in head) {
            val score = ln.count { it.code in 0x0590..0x05FF }
            if (score > bestScore) {
                bestScore = score
                best = ln.trim()
            }
        }
        // Filter out boilerplate markers
        if (best != null && best!!.isNotBlank() && !best.equals("merged", ignoreCase = true) && !best.startsWith("http", true)) {
            return best
        }
        return null
    }

    // ===== Load Hebrew metadata from Sefaria exports =====

    private fun loadHeTitlesFromToc() {
        val tocFile = sefariaRoot.resolve("export_toc").resolve("table_of_contents.json")
        if (!Files.isRegularFile(tocFile)) return
        runCatching {
            val text = tocFile.readLines(Charsets.UTF_8).joinToString("\n")
            val root = json.parseToJsonElement(text)
            fun visit(node: kotlinx.serialization.json.JsonElement) {
                val obj = node as? kotlinx.serialization.json.JsonObject ?: return
                val title = obj["title"]?.toString()?.trim('"')
                val he = obj["heTitle"]?.toString()?.trim('"')
                val heShort = obj["heShortDesc"]?.toString()?.trim('"')
                if (!title.isNullOrBlank() && !he.isNullOrBlank()) heTitleByEnTitle.putIfAbsent(title, he)
                if (!title.isNullOrBlank() && !heShort.isNullOrBlank()) heShortDescByEnTitle.putIfAbsent(title, heShort)
                val contents = obj["contents"] as? kotlinx.serialization.json.JsonArray
                contents?.forEach { visit(it) }
            }
            when (root) {
                is kotlinx.serialization.json.JsonArray -> root.forEach { visit(it) }
                is kotlinx.serialization.json.JsonObject -> visit(root)
                else -> { /* ignore */ }
            }
        }.onFailure { /* ignore */ }
    }

    private fun loadSectionNamesFromSchemas() {
        val dir = sefariaRoot.resolve("export_schemas").resolve("schemas")
        if (!Files.isDirectory(dir)) return
        runCatching {
            Files.list(dir).use { s ->
                s.filter { Files.isRegularFile(it) && it.toString().endsWith(".json") }.forEach { p ->
                    runCatching {
                        val text = p.readLines(Charsets.UTF_8).joinToString("\n")
                        val el = json.parseToJsonElement(text) as? kotlinx.serialization.json.JsonObject ?: return@runCatching
                        val title = el["title"]?.toString()?.trim('"')
                        val heTitle = el["heTitle"]?.toString()?.trim('"')
                        val schema = el["schema"] as? kotlinx.serialization.json.JsonObject
                        val heSec = schema?.get("heSectionNames") as? kotlinx.serialization.json.JsonArray
                        val heList = heSec?.mapNotNull { it.toString().trim('"') } ?: emptyList()
                        // heShortDesc at top-level schema
                        val heShort = el["heShortDesc"]?.toString()?.trim('"')
                        // categories and heCategories arrays for category mapping
                        val catsEn = (el["categories"] as? kotlinx.serialization.json.JsonArray)?.mapNotNull { it.toString().trim('"') } ?: emptyList()
                        val catsHe = (el["heCategories"] as? kotlinx.serialization.json.JsonArray)?.mapNotNull { it.toString().trim('"') } ?: emptyList()
                        if (!title.isNullOrBlank()) {
                            if (!heTitle.isNullOrBlank()) heTitleByEnTitle.putIfAbsent(title, heTitle)
                            if (heList.isNotEmpty()) heSectionNamesByTitle.putIfAbsent(title, heList)
                            if (!heShort.isNullOrBlank()) heShortDescByEnTitle.putIfAbsent(title, heShort)
                        }
                        if (catsEn.isNotEmpty() && catsEn.size == catsHe.size) {
                            for (i in catsEn.indices) {
                                val en = catsEn[i]
                                val he = catsHe[i]
                                if (en.isNotBlank() && he.isNotBlank()) heCategoryByEnCategory.putIfAbsent(en, he)
                            }
                        }
                    }
                }
            }
        }.onFailure { /* ignore */ }
    }

    // ===== Bulk mode & preload helpers =====

    private suspend fun beginBulkMode() {
        runCatching { repository.executeRawQuery("PRAGMA foreign_keys = OFF") }
        runCatching { repository.setSynchronousOff() }
        runCatching { repository.setJournalModeOff() }
        runCatching { repository.executeRawQuery("PRAGMA cache_size=40000") }
        runCatching { repository.executeRawQuery("PRAGMA temp_store=MEMORY") }
    }

    private suspend fun endBulkModeAndVacuum() {
        runCatching { repository.setJournalModeWal() }
        runCatching { repository.setSynchronousNormal() }
        runCatching { repository.executeRawQuery("PRAGMA foreign_keys = ON") }
        runCatching { repository.executeRawQuery("VACUUM") }
    }

    private fun toLibraryRelativeKey(file: Path): String = try {
        libraryRoot.relativize(file).toString().replace('\\', '/')
    } catch (_: Exception) {
        file.fileName.toString()
    }

    private fun preloadAllBookContents(files: List<Path>) {
        if (fileContentCache.isNotEmpty()) return
        logger.i { "Preloading ${files.size} merged.txt files into RAM" }
        for (p in files) {
            runCatching {
                val key = toLibraryRelativeKey(p)
                fileContentCache[key] = p.readLines(Charsets.UTF_8)
            }
        }
        logger.i { "Preloaded ${fileContentCache.size} files into RAM" }
    }
}
