package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.QueryResult
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import com.code972.hebmorph.datastructures.DictHebMorph
import com.code972.hebmorph.hspell.HSpellDictionaryLoader
import kotlinx.coroutines.runBlocking
import org.apache.lucene.analysis.hebrew.HebrewLegacyIndexingAnalyzer
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
    Logger.setMinSeverity(Severity.Info)
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

    // Prepare index output paths next to the DB
    val indexDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lucene") else Paths.get("$dbPath.luceneindex")
    val lookupDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lookup.lucene") else Paths.get("$dbPath.lookupindex")
    runCatching { Files.createDirectories(indexDir) }
    runCatching { Files.createDirectories(lookupDir) }

    // Open repository
    val useMemoryDb = (System.getProperty("inMemoryDb") ?: "true") != "false"
    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repo = SeforimRepository(dbPath, driver)

    // Optionally seed in-memory DB from disk DB for faster reads
    if (useMemoryDb) {
        val baseDb = dbPath
        val baseFile = File(baseDb)
        if (baseFile.exists()) {
            logger.i { "[Index] Seeding in-memory DB from $baseDb" }
            runCatching {
                val escaped = baseDb.replace("'", "''")
                // Disable FKs during bulk copy to avoid validation overhead
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
                    // Ensure target table exists (schema already created by repo init)
                    repo.executeRawQuery("DELETE FROM \"$t\"")
                    repo.executeRawQuery("INSERT INTO \"$t\" SELECT * FROM disk.\"$t\"")
                }
                repo.executeRawQuery("DETACH DATABASE disk")
                repo.executeRawQuery("PRAGMA foreign_keys=ON")
                logger.i { "[Index] Seed complete: ${'$'}{tables.size} tables copied" }
            }.onFailure { e ->
                logger.e(e) { "[Index] Failed to seed in-memory DB; falling back to slower disk reads" }
            }
        } else {
            logger.w { "[Index] Base DB not found at $baseDb; cannot seed in-memory DB" }
        }
    }

    // Build analyzers and writers
    val hebrewAnalyzer = HebrewLegacyIndexingAnalyzer(dict)

    LuceneTextIndexWriter(indexDir, analyzer = hebrewAnalyzer).use { writer ->
        LuceneLookupIndexWriter(lookupDir).use { lookup ->
            val books = repo.getAllBooks()
            val totalBooks = books.size
            var processedBooks = 0
            logger.i { "Indexing $totalBooks books into $indexDir using HebMorph" }
            for (book in books) {
                processedBooks += 1
                val globalPct = if (totalBooks > 0) (processedBooks * 100 / totalBooks) else 0
                logger.i { "[$processedBooks/$totalBooks | ${globalPct}%] Indexing book: '${book.title}' (id=${book.id})" }

                // Title terms (stored in text index for future use)
                writer.addBookTitleTerm(book.id, book.categoryId, book.title, book.title)
                val titleSan = sanitizeAcronymTerm(book.title)
                if (titleSan.isNotBlank() && !titleSan.equals(book.title, ignoreCase = true)) {
                    writer.addBookTitleTerm(book.id, book.categoryId, book.title, titleSan)
                }
                // Topics as additional terms for book lookup by subject
                runCatching {
                    val topicTerms = book.topics
                        .asSequence()
                        .map { sanitizeAcronymTerm(it.name) }
                        .map { it.trim() }
                        .filter { it.isNotEmpty() }
                        .distinct()
                        .toList()
                    topicTerms.forEach { t ->
                        writer.addBookTitleTerm(book.id, book.categoryId, book.title, t)
                    }
                }

                // Lookup index: include title, sanitized title, acronyms and topics (StandardAnalyzer-based) for prefix suggestions
                runCatching {
                    val acronyms = runCatching { repo.getAcronymsForBook(book.id) }.getOrDefault(emptyList())
                    val topicTerms = book.topics
                        .asSequence()
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

                // Lines: stream in batches and index with HebMorph analyzer
                val total = book.totalLines
                val allLines = if (total > 0) runCatching { repo.getLines(book.id, 0, total - 1) }.getOrDefault(emptyList()) else emptyList()
                var processed = 0
                var nextLogPct = 10
                for (ln in allLines) {
                    val normalized = normalizeForIndex(ln.content)
                    writer.addLine(
                        bookId = book.id,
                        bookTitle = book.title,
                        categoryId = book.categoryId,
                        lineId = ln.id,
                        lineIndex = ln.lineIndex,
                        normalizedText = normalized,
                        rawPlainText = normalized
                    )
                    processed += 1
                    if (total > 0) {
                        val pct = (processed.toLong() * 100L / total).toInt().coerceIn(0, 100)
                        if (pct >= nextLogPct) {
                            logger.i { "   Lines ${processed}/$total (${pct}%) for '${book.title}'" }
                            nextLogPct = ((pct / 10) + 1) * 10
                        }
                    }
                }
                // TOC into lookup index only
                runCatching {
                    val tocs = repo.getBookToc(book.id)
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
                logger.i { "Completed '${book.title}' [${processedBooks}/$totalBooks | ${globalPct}%]" }
            }
            writer.commit()
            lookup.commit()
            logger.i { "Lucene text index built successfully at $indexDir (HebMorph analyzer)" }
            logger.i { "Lucene lookup index built successfully at $lookupDir" }
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
