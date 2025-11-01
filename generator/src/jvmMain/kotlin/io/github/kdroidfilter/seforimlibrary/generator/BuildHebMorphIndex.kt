package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
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

    val dbPath =  "/Volumes/Data/Downloads/seforim_lucene.db"

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
    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repo = SeforimRepository(dbPath, driver)

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

                // Lookup index: keep StandardAnalyzer-based terms for prefix suggestions
                runCatching {
                    val acronyms = runCatching { repo.getAcronymsForBook(book.id) }.getOrDefault(emptyList())
                    val terms = buildList {
                        add(book.title)
                        if (titleSan.isNotBlank()) add(titleSan)
                        addAll(acronyms)
                    }.filter { it.isNotBlank() }
                    lookup.addBook(book.id, book.categoryId, book.title, terms)
                }

                // Lines: stream in batches and index with HebMorph analyzer
                val total = book.totalLines
                val batch = 5000
                var start = 0
                var nextLogPct = 10
                while (start < total) {
                    val end = min(total - 1, start + batch - 1)
                    val lines = runCatching { repo.getLines(book.id, start, end) }.getOrDefault(emptyList())
                    for (ln in lines) {
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
                    }
                    start = end + 1
                    if (total > 0) {
                        val pct = (start.toLong() * 100L / total).toInt().coerceIn(0, 100)
                        if (pct >= nextLogPct) {
                            logger.i { "   Lines ${start}/$total (${pct}%) for '${book.title}'" }
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
