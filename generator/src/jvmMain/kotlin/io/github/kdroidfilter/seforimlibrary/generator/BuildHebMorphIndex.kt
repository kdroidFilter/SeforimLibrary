package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.QueryResult
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import com.code972.hebmorph.datastructures.DictHebMorph
import com.code972.hebmorph.hspell.HSpellDictionaryLoader
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.Dispatchers
import org.apache.lucene.analysis.hebrew.HebrewLegacyIndexingAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneLookupIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneTextIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.utils.HebrewTextUtils
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.Comparator
import kotlin.math.min

/**
 * Build a Lucene index using HebMorph's HebrewIndexingAnalyzer against an existing SQLite DB.
 *
 * Input: path to an existing SQLite database file (seforim.db).
 * Looks for hspell data files via system property 'hebmorph.hspell.path', environment
 * variable 'HEBMORPH_HSPELL_PATH', or common relative locations (including the
 * SeforimLibrary/HebMorph checkout). See [resolveHSpellPath].
 *
 * Usage examples:
 *   ./gradlew -p SeforimLibrary :generator:buildHebMorphIndex -PseforimDb=/path/to/seforim.db \
 *       [-Phebmorph.hspell.path=/path/to/hspell-data-files]
 */
fun main() = runBlocking {
    Logger.setMinSeverity(Severity.Warn)
    val logger = Logger.withTag("BuildHebMorphIndex")

    // Resolve DB path: prefer -PseforimDb / SEFORIM_DB, fallback to build/seforim.db
    val dbPath: String =
        System.getProperty("seforimDb")
            ?: System.getenv("SEFORIM_DB")
            ?: Paths.get("build", "seforim.db").toString()

    val dbFile = File(dbPath)
    require(dbFile.exists()) { "Database not found at $dbPath" }

    // Resolve and load HebMorph dictionary (hspell)
    val hspellPath = resolveHSpellPath(logger, dbFile.parentFile)
    System.setProperty("hebmorph.hspell.path", hspellPath)
    logger.i { "Using hspell path: $hspellPath" }
    val dict: DictHebMorph = HSpellDictionaryLoader().loadDictionaryFromPath(hspellPath)

    // Final index output paths next to the DB
    val finalIndexDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lucene") else Paths.get("$dbPath.luceneindex")
    val finalLookupDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lookup.lucene") else Paths.get("$dbPath.lookupindex")
    runCatching { Files.createDirectories(finalIndexDir.parent) }
    runCatching { Files.createDirectories(finalLookupDir.parent) }

    // By default, write indices directly to disk. Tmpfs can be enabled explicitly via -DuseTmpfsForIndex=true
    val useTmpfsForIndex = (System.getProperty("useTmpfsForIndex") ?: "false").equals("true", ignoreCase = true)
    // Prefer tmpfs (/dev/shm or /mnt/ramdisk) only when explicitly requested
    val tmpfsRoot = if (useTmpfsForIndex) {
        System.getProperty("tmpfsDir")
            ?: (Paths.get("/dev/shm").takeIf { Files.isDirectory(it) }?.toString())
            ?: (Paths.get("/mnt/ramdisk").takeIf { Files.isDirectory(it) }?.toString())
    } else null

    val copyDbToTmpfs = (System.getProperty("copyDbToTmpfs") == "true")
    var needCopyBack = false // for indices
    val processDbPath: String // DB path to open (original path by default)
    val indexDir: Path
    val lookupDir: Path

    if (tmpfsRoot != null) {
        val workDir = Paths.get(tmpfsRoot, "seforim-index-${System.currentTimeMillis()}")
        Files.createDirectories(workDir)
        logger.i { "[Index] Using tmpfs work dir: $workDir" }
        // By default, DO NOT copy the DB. Only if explicitly requested via -DcopyDbToTmpfs=true
        processDbPath = if (copyDbToTmpfs) {
            val workDb = workDir.resolve("seforim.db")
            Files.copy(dbFile.toPath(), workDb, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)
            logger.i { "[Index] Copied DB to tmpfs: $workDb" }
            workDb.toString()
        } else dbPath
        indexDir = workDir.resolve("lucene")
        lookupDir = workDir.resolve("lookup")
        Files.createDirectories(indexDir)
        Files.createDirectories(lookupDir)
        needCopyBack = true
    } else {
        processDbPath = dbPath
        indexDir = finalIndexDir
        lookupDir = finalLookupDir
        Files.createDirectories(indexDir)
        Files.createDirectories(lookupDir)
    }

    // Open repository on the chosen DB path (default to in-memory DB)
    val useMemoryDb = (System.getProperty("inMemoryDb") ?: "true") != "false"
    // Use shared in-memory DB so multiple connections can read concurrently
    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite:file:seforim_index?mode=memory&cache=shared" else "jdbc:sqlite:$processDbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repo = SeforimRepository(processDbPath, driver)

    // If using in-memory DB, seed it from the disk DB (original dbPath)
    if (useMemoryDb) {
        val baseDb = dbPath
        if (File(baseDb).exists()) {
            logger.i { "[Index] Seeding in-memory DB from $baseDb" }
            runCatching {
                val escaped = baseDb.replace("'", "''")
                repo.executeRawQuery("PRAGMA foreign_keys=OFF")
                repo.executeRawQuery("ATTACH DATABASE '$escaped' AS disk")
                val tables = driver.executeQuery(null,
                    "SELECT name FROM disk.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                    { c: SqlCursor ->
                        val list = mutableListOf<String>()
                        while (c.next().value) c.getString(0)?.let { list.add(it) }
                        QueryResult.Value(list)
                    }, 0
                ).value
                for (t in tables) {
                    repo.executeRawQuery("DELETE FROM \"$t\"")
                    repo.executeRawQuery("INSERT INTO \"$t\" SELECT * FROM disk.\"$t\"")
                }
                repo.executeRawQuery("DETACH DATABASE disk")
                repo.executeRawQuery("PRAGMA foreign_keys=ON")
                logger.i { "[Index] Seed complete: ${'$'}{tables.size} tables copied" }
            }.onFailure { e ->
                logger.e(e) { "[Index] Failed to seed in-memory DB; falling back to file DB reads" }
            }
        } else {
            logger.w { "[Index] Base DB not found at $baseDb; cannot seed in-memory DB" }
        }
    }

    // Build analyzers and writers
    val hebrewAnalyzer = HebrewLegacyIndexingAnalyzer(dict)
    // Precision-first: use HebMorph analyzer as default (FIELD_TEXT), and for FIELD_TEXT_HE as well.
    val perFieldAnalyzer = PerFieldAnalyzerWrapper(
        hebrewAnalyzer,
        mapOf(
            LuceneTextIndexWriter.FIELD_TEXT_HE to hebrewAnalyzer
        )
    )

    LuceneTextIndexWriter(indexDir, analyzer = perFieldAnalyzer, indexHebrewField = true, indexPrimaryText = false).use { writer ->
        LuceneLookupIndexWriter(lookupDir, analyzer = io.github.kdroidfilter.seforimlibrary.analysis.HebrewCliticAwareAnalyzer()).use { lookup ->
            val books = repo.getAllBooks()
            val indexThreads = (System.getProperty("indexThreads") ?: Runtime.getRuntime().availableProcessors().toString()).toInt().coerceAtLeast(1)
            val workerDispatcher = Dispatchers.Default.limitedParallelism(indexThreads)
            val totalBooks = books.size
            var processedBooks = 0
            logger.i { "Indexing $totalBooks books into $indexDir using HebMorph" }
            val progress = java.util.concurrent.atomic.AtomicInteger(0)
            books.map { book ->
                async(workerDispatcher) {
                    val current = progress.incrementAndGet()
                    val globalPct = if (totalBooks > 0) (current * 100 / totalBooks) else 0
                    logger.i { "[$current/$totalBooks | ${globalPct}%] Indexing book: '${book.title}' (id=${book.id})" }

                    // Create a separate read-only connection for concurrent reads
                    val localRepo = SeforimRepository(processDbPath, JdbcSqliteDriver(url = jdbcUrl))
                    try {
                        // Titles
                        writer.addBookTitleTerm(book.id, book.categoryId, book.title, book.title)
                        val titleSan = sanitizeAcronymTerm(book.title)
                        if (titleSan.isNotBlank() && !titleSan.equals(book.title, ignoreCase = true)) {
                            writer.addBookTitleTerm(book.id, book.categoryId, book.title, titleSan)
                        }
                        // Topics → title terms
                        runCatching {
                            val topicTerms = book.topics.asSequence()
                                .map { sanitizeAcronymTerm(it.name) }
                                .map { it.trim() }
                                .filter { it.isNotEmpty() }
                                .distinct()
                                .toList()
                            topicTerms.forEach { t -> writer.addBookTitleTerm(book.id, book.categoryId, book.title, t) }
                        }
                        // Lookup: acronyms + topics
                        runCatching {
                            val acronyms = runCatching { localRepo.getAcronymsForBook(book.id) }.getOrDefault(emptyList())
                            val topicTerms = book.topics.asSequence()
                                .map { sanitizeAcronymTerm(it.name) }
                                .map { it.trim() }
                                .filter { it.isNotEmpty() }
                                .distinct()
                                .toList()
                            val terms = buildList {
                                add(book.title)
                                if (titleSan.isNotBlank()) add(titleSan)
                                addAll(acronyms)
                                addAll(topicTerms)
                            }.filter { it.isNotBlank() }
                            lookup.addBook(book.id, book.categoryId, book.title, terms)
                        }
                        // Lines in parallel chunks (but per-book task already parallelized; here we keep single-thread to reduce contention)
                        val total = book.totalLines
                        if (total > 0) {
                            val lines = runCatching { localRepo.getLines(book.id, 0, total - 1) }.getOrDefault(emptyList())
                            var processed = 0
                            var nextLogPct = 10
                            for (ln in lines) {
                                val normalizedHeb = normalizeForIndex(ln.content)
                                writer.addLine(
                                    bookId = book.id,
                                    bookTitle = book.title,
                                    categoryId = book.categoryId,
                                    lineId = ln.id,
                                    lineIndex = ln.lineIndex,
                                    // Index HebMorph tokens as primary (FIELD_TEXT)
                                    normalizedText = normalizedHeb,
                                    rawPlainText = null,
                                    // Also index HebMorph-targeted field (FIELD_TEXT_HE)
                                    normalizedTextHebrew = normalizedHeb
                                )
                                processed += 1
                                val pct = (processed.toLong() * 100L / total).toInt().coerceIn(0, 100)
                                if (pct >= nextLogPct) {
                                    logger.i { "   Lines ${processed}/$total (${pct}%) for '${book.title}' (bookParallel)" }
                                    nextLogPct = ((pct / 10) + 1) * 10
                                }
                            }
                        }
                        // TOC into lookup
                        runCatching {
                            val tocs = localRepo.getBookToc(book.id)
                            for (t in tocs) {
                                val norm = normalizeForIndex(t.text)
                                lookup.addToc(
                                    tocId = t.id,
                                    bookId = t.bookId,
                                    categoryId = book.categoryId,
                                    bookTitle = book.title,
                                    text = norm,
                                    level = t.level
                                )
                            }
                        }
                        logger.i { "Completed '${book.title}' [$current/$totalBooks | ${globalPct}%]" }
                    } finally {
                        localRepo.close()
                    }
                }
            }.awaitAll()
            writer.commit()
            lookup.commit()
            logger.i { "Lucene text index built successfully at $indexDir (HebMorph analyzer)" }
            logger.i { "Lucene lookup index built successfully at $lookupDir" }
        }
    }

    // If built in tmpfs, copy back to final destinations and cleanup
    if (needCopyBack) {
        fun copyTree(src: Path, dst: Path) {
            if (Files.exists(dst)) {
                Files.walk(dst).sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
            Files.createDirectories(dst)
            Files.walk(src).forEach { p ->
                val rel = src.relativize(p)
                val target = dst.resolve(rel)
                if (Files.isDirectory(p)) Files.createDirectories(target)
                else Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)
            }
        }
        runCatching { copyTree(indexDir, finalIndexDir) }
            .onSuccess { logger.i { "[Index] Copied text index to $finalIndexDir" } }
            .onFailure { logger.e(it) { "[Index] Failed to copy text index to $finalIndexDir" } }
        runCatching { copyTree(lookupDir, finalLookupDir) }
            .onSuccess { logger.i { "[Index] Copied lookup index to $finalLookupDir" } }
            .onFailure { logger.e(it) { "[Index] Failed to copy lookup index to $finalLookupDir" } }

        // Cleanup work dir
        val workRoot = Paths.get(processDbPath).parent
        if (workRoot != null && workRoot.startsWith(tmpfsRoot)) {
            runCatching { Files.walk(workRoot).sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
            logger.i { "[Index] Cleaned tmpfs dir: $workRoot" }
        }
    }

    repo.close()
}

private fun normalizeForIndex(html: String): String {
    val cleaned = Jsoup.clean(html, Safelist.none())
        .trim()
        .replace("\\s+".toRegex(), " ")
    val withoutMaqaf = HebrewTextUtils.replaceMaqaf(cleaned, " ")
    // Keep nikud for the analyzer's NiqqudFilter; strip only taamim here
    return HebrewTextUtils.removeTeamim(withoutMaqaf)
}

private fun sanitizeAcronymTerm(raw: String): String {
    var s = raw.trim()
    if (s.isEmpty()) return ""
    s = HebrewTextUtils.removeAllDiacritics(s)
    s = HebrewTextUtils.replaceMaqaf(s, " ")
    s = s.replace("\u05F4", "") // gershayim
    s = s.replace("\u05F3", "") // geresh
    s = s.replace("\\s+".toRegex(), " ").trim()
    return s
}

/**
 * Resolve the hspell data path, mirroring the logic in HebMorph's TestBase.
 */
private fun resolveHSpellPath(logger: Logger, dbDir: File? = null): String {
    val fromProp = System.getProperty("hebmorph.hspell.path")
    if (!fromProp.isNullOrBlank()) return ensureSlash(File(fromProp))

    val fromEnv = System.getenv("HEBMORPH_HSPELL_PATH")
    if (!fromEnv.isNullOrBlank()) return ensureSlash(File(fromEnv))

    // Same directory as the DB (sibling folder)
    if (dbDir != null) {
        val sibling = File(dbDir, "hspell-data-files")
        if (sibling.exists() && sibling.isDirectory) return ensureSlash(sibling)
    }

    // Common relative locations when running from SeforimLibrary
    val candidates = listOf(
        "HebMorph/hspell-data-files",
        "../HebMorph/hspell-data-files",
        "./hspell-data-files",
        "../hspell-data-files",
        "../../hspell-data-files"
    )
    for (c in candidates) {
        val f = File(c)
        if (f.exists() && f.isDirectory) return ensureSlash(f)
    }
    throw IllegalStateException("Cannot locate hspell-data-files. Place it next to the DB or set -Phebmorph.hspell.path/HEBMORPH_HSPELL_PATH.")
}

private fun ensureSlash(f: File): String = (if (f.path.endsWith("/")) f.path else f.path + "/")
