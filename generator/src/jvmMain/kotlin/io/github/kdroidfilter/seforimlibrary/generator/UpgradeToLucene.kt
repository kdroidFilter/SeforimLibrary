package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneTextIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneLookupIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.utils.HebrewTextUtils
import kotlinx.coroutines.runBlocking
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.math.min

/**
 * Upgrades an existing SQLite DB by removing FTS artifacts and building a Lucene index.
 * Optionally drops the legacy FTS tables/views. By default this does NOT drop the 'plainText'
 * column to avoid breaking older app binaries. Pass --drop-plaintext to drop it.
 *
 * Usage:
 *   ./gradlew :generator:upgradeToLucene -PseforimDb=/path/to/seforim.db [ -PdropPlainText=true ]
 */
fun main() = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("UpgradeToLucene")

    val dbPathProp = "/Users/eliegambache/Library/Application Support/io.github.kdroidfilter.seforimapp/databases/seforim.db"
    val dbPath = dbPathProp ?: run {
        logger.e { "SEFORIM_DB not provided. Use -PseforimDb or set SEFORIM_DB." }
        return@runBlocking
    }
    val dropPlainText = (System.getProperty("DROP_PLAINTEXT") ?: System.getenv("DROP_PLAINTEXT") ?: "false")
        .equals("true", ignoreCase = true)

    val dbFile = File(dbPath)
    require(dbFile.exists()) { "Database not found at $dbPath" }

    // Backup
    val backup = File("$dbPath.pre-lucene.bak")
    try {
        Files.copy(dbFile.toPath(), backup.toPath())
        logger.i { "Backup created at ${backup.absolutePath}" }
    } catch (e: Exception) {
        logger.w(e) { "Could not create backup; continuing anyway" }
    }

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repo = SeforimRepository(dbPath, driver)

    // Drop legacy FTS objects
    runCatching {
        logger.i { "Dropping legacy FTS objects if present..." }
        repo.executeRawQuery("DROP VIEW IF EXISTS line_with_book_title")
        repo.executeRawQuery("DROP TABLE IF EXISTS line_search")
        repo.executeRawQuery("DROP TABLE IF EXISTS book_title_search")
        logger.i { "FTS objects dropped" }
    }.onFailure { e -> logger.w(e) { "Error dropping FTS objects; continuing" } }

    if (dropPlainText) {
        // NOTE: Only enable if the app code no longer references line.plainText.
        logger.i { "Dropping line.plainText column (schema rebuild)" }
        runCatching {
            repo.runInTransaction {
                // Recreate line table without plainText column
                repo.executeRawQuery(
                    """
                    CREATE TABLE IF NOT EXISTS line_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        bookId INTEGER NOT NULL,
                        lineIndex INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        tocEntryId INTEGER,
                        FOREIGN KEY (bookId) REFERENCES book(id) ON DELETE CASCADE,
                        FOREIGN KEY (tocEntryId) REFERENCES tocEntry(id) ON DELETE SET NULL
                    );
                    """.trimIndent()
                )
                repo.executeRawQuery(
                    "INSERT INTO line_new(id, bookId, lineIndex, content, tocEntryId) " +
                            "SELECT id, bookId, lineIndex, content, tocEntryId FROM line"
                )
                repo.executeRawQuery("DROP TABLE line")
                repo.executeRawQuery("ALTER TABLE line_new RENAME TO line")
                // Recreate indexes
                repo.executeRawQuery("CREATE INDEX IF NOT EXISTS idx_line_book_index ON line(bookId, lineIndex)")
                repo.executeRawQuery("CREATE INDEX IF NOT EXISTS idx_line_toc ON line(tocEntryId)")
            }
            logger.i { "Dropped plainText column from line" }
        }.onFailure { e -> logger.w(e) { "Failed to drop plainText; DB left unchanged" } }
    }

    // Build Lucene indexes next to the DB (text index and lookup index)
    val indexDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lucene") else Paths.get("$dbPath.luceneindex")
    runCatching { Files.createDirectories(indexDir) }
    val lookupDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lookup.lucene") else Paths.get("$dbPath.lookupindex")
    runCatching { Files.createDirectories(lookupDir) }
    LuceneTextIndexWriter(indexDir).use { writer ->
        LuceneLookupIndexWriter(lookupDir).use { lookup ->
        val books = repo.getAllBooks()
        val totalBooks = books.size
        var processedBooks = 0
        logger.i { "Indexing $totalBooks books into $indexDir" }
        for (book in books) {
            processedBooks += 1
            val globalPct = if (totalBooks > 0) (processedBooks * 100 / totalBooks) else 0
            logger.i { "[$processedBooks/$totalBooks | ${globalPct}%] Indexing book: '${book.title}' (id=${book.id})" }
            // Title terms: exact + sanitized + acronyms
            writer.addBookTitleTerm(book.id, book.categoryId, book.title, book.title)
            val titleSan = sanitizeAcronymTerm(book.title)
            if (titleSan.isNotBlank() && !titleSan.equals(book.title, ignoreCase = true)) {
                writer.addBookTitleTerm(book.id, book.categoryId, book.title, titleSan)
            }
            val acronyms = runCatching { repo.getAcronymsForBook(book.id) }.getOrDefault(emptyList())
            for (t in acronyms) writer.addBookTitleTerm(book.id, book.categoryId, book.title, t)

            // Lookup index: add BOOK document with title/acronym terms
            runCatching {
                val terms = buildList {
                    add(book.title)
                    if (titleSan.isNotBlank()) add(titleSan)
                    addAll(acronyms)
                }.filter { it.isNotBlank() }
                lookup.addBook(book.id, book.categoryId, book.title, terms)
            }.onFailure { e -> logger.w(e) { "Failed to add book lookup for '${book.title}'" } }

            // Lines: stream in batches
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
            // Index TOCs for lookup
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
            }.onFailure { e -> logger.w(e) { "Failed indexing TOCs for '${book.title}'" } }
            logger.i { "Completed '${book.title}' [${processedBooks}/$totalBooks | ${globalPct}%]" }
        }
        writer.commit()
        lookup.commit()
        logger.i { "Lucene text index built successfully at $indexDir" }
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
    return HebrewTextUtils.removeAllDiacritics(withoutMaqaf)
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
