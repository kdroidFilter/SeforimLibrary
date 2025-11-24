package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.QueryResult
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneTextIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneLookupIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.utils.HebrewTextUtils
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.Dispatchers
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.ngram.NGramTokenFilter
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private const val SNIPPET_NEIGHBOR_WINDOW = 4
private const val SNIPPET_MIN_LENGTH = 280
private const val SNIPPET_MAX_LENGTH = 1600

/**
 * Build Lucene indexes using Lucene's StandardAnalyzer and an extra 4-gram field for substring search.
 *
 * Input: path to an existing SQLite DB (seforim.db).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :generator:buildLuceneIndexDefault -PseforimDb=/path/to/seforim.db
 *   Optional: -PinMemoryDb=false to read directly from disk (RAM by default)
 */
fun main() = runBlocking {
    Logger.setMinSeverity(Severity.Warn)
    val logger = Logger.withTag("BuildLuceneIndexDefault")

    val dbPath: String =
        System.getProperty("seforimDb")
            ?: System.getenv("SEFORIM_DB")
            ?: Paths.get("build", "seforim.db").toString()

    // Magic lexicon (surface/variant/base) must be present for intelligent boosts.
    val lexicalPath: Path =
        System.getProperty("magicDict")?.let { Paths.get(it) }
            ?: System.getenv("SEFORIM_MAGIC_DICT")?.let { Paths.get(it) }
            ?: Paths.get("SeforimLibrary", "SeforimMagicIndexer", "magicindexer", "build", "db", "lexical.db")
    require(Files.exists(lexicalPath)) {
        "Lexical dictionary not found at $lexicalPath. Build SeforimMagicIndexer or pass -DmagicDict=/path/lexical.db"
    }

    val dbFile = File(dbPath)
    require(dbFile.exists()) { "Database not found at $dbPath" }

    // Prepare index output paths next to the DB
    // Use a distinct suffix for this index to avoid clashing with other variants
    val indexDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lucene") else Paths.get("$dbPath.lucene")
    val lookupDir: Path = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lookup.lucene") else Paths.get("$dbPath.lookup")
    runCatching { Files.createDirectories(indexDir) }
    runCatching { Files.createDirectories(lookupDir) }

    // Open repository (prefer in-memory for faster reads)
    val useMemoryDb = (System.getProperty("inMemoryDb") ?: "true") != "false"
    // Use a shared in-memory DB so multiple connections can read concurrently when multithreading
    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite:file:seforim_index_std?mode=memory&cache=shared" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repo = SeforimRepository(dbPath, driver)

    if (useMemoryDb) {
        // Seed in-memory DB from disk to avoid disk I/O during indexing
        val baseDb = dbPath
        if (File(baseDb).exists()) {
            logger.i { "[IndexDefault] Seeding in-memory DB from $baseDb" }
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
                logger.i { "[IndexDefault] Seed complete: ${'$'}{tables.size} tables copied" }
            }.onFailure { e ->
                logger.e(e) { "[IndexDefault] Failed to seed in-memory DB; falling back to disk reads" }
            }
        } else {
            logger.w { "[IndexDefault] Base DB not found at $baseDb; cannot seed in-memory DB" }
        }
    }

    // Use Lucene's StandardAnalyzer by default; add per-field 4-gram analyzer for substring search
    val defaultAnalyzer = StandardAnalyzer()
    val ngram4Analyzer = object : Analyzer() {
        override fun createComponents(fieldName: String): TokenStreamComponents {
            val src = org.apache.lucene.analysis.standard.StandardTokenizer()
            var ts: TokenStream = src
            ts = LowerCaseFilter(ts)
            ts = NGramTokenFilter(ts, 4, 4, false)
            return TokenStreamComponents(src, ts)
        }
    }
    val analyzer = PerFieldAnalyzerWrapper(
        defaultAnalyzer,
        mapOf(
            LuceneTextIndexWriter.FIELD_TEXT_NG4 to ngram4Analyzer
        )
    )

    LuceneTextIndexWriter(indexDir, analyzer = analyzer).use { writer ->
        LuceneLookupIndexWriter(lookupDir, analyzer = analyzer).use { lookup ->
            val books = repo.getAllBooks()
            val indexThreads = (System.getProperty("indexThreads") ?: Runtime.getRuntime().availableProcessors().toString()).toInt().coerceAtLeast(1)
            val workerDispatcher = Dispatchers.Default.limitedParallelism(indexThreads)
            val totalBooks = books.size
            logger.i { "Indexing $totalBooks books into $indexDir using StandardAnalyzer + 4-gram field" }
            val progress = java.util.concurrent.atomic.AtomicInteger(0)

            books.map { book ->
                async(workerDispatcher) {
                    val current = progress.incrementAndGet()
                    val globalPct = if (totalBooks > 0) (current * 100 / totalBooks) else 0
                    logger.i { "[$current/$totalBooks | ${globalPct}%] Indexing book: '${book.title}' (id=${book.id})" }

                    // Create a separate read-only connection per worker for concurrent reads
                    val localRepo = SeforimRepository(dbPath, JdbcSqliteDriver(url = jdbcUrl))
                    // Slightly increase boost for base books by nudging orderIndex closer to the front
                    val orderIndexForBoost = if (book.isBaseBook) {
                        (book.order.toInt() - 5).coerceAtLeast(1)
                    } else {
                        book.order.toInt()
                    }
                    try {
                        // Title terms (stored in text index for future use)
                        writer.addBookTitleTerm(book.id, book.categoryId, book.title, book.title)
                        val titleSan = sanitizeAcronymTerm(book.title)
                        if (titleSan.isNotBlank() && !titleSan.equals(book.title, ignoreCase = true)) {
                            writer.addBookTitleTerm(book.id, book.categoryId, book.title, titleSan)
                        }
                        // Topics → title terms for text index
                        runCatching {
                            val topicTerms = book.topics
                                .asSequence()
                                .map { sanitizeAcronymTerm(it.name) }
                                .map { it.trim() }
                                .filter { it.isNotEmpty() }
                                .distinct()
                                .toList()
                            topicTerms.forEach { t -> writer.addBookTitleTerm(book.id, book.categoryId, book.title, t) }
                        }

                        // Lookup: acronyms + topics + title variants
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

                        // Lines: process sequentially per book; workers run per-book in parallel
                        val total = book.totalLines
                        if (total > 0) {
                            val allLines = runCatching { localRepo.getLines(book.id, 0, total - 1) }.getOrDefault(emptyList())
                            val plainLines = allLines.map { ln ->
                                Jsoup.clean(ln.content, Safelist.none())
                                    .replace("\\s+".toRegex(), " ")
                                    .trim()
                            }
                            var processed = 0
                            var nextLogPct = 10
                            for (ln in allLines) {
                                val normalized = normalizeForIndexDefault(ln.content)
                                val idx = ln.lineIndex.coerceIn(0, plainLines.lastIndex)
                                val basePlain = plainLines.getOrNull(idx).orEmpty()
                                val snippetSource = if (basePlain.length >= SNIPPET_MIN_LENGTH) {
                                    basePlain
                                } else {
                                    val start = (idx - SNIPPET_NEIGHBOR_WINDOW).coerceAtLeast(0)
                                    val end = (idx + SNIPPET_NEIGHBOR_WINDOW).coerceAtMost(plainLines.lastIndex)
                                    plainLines.subList(start, end + 1).joinToString(" ")
                                }.let { snippet ->
                                    if (snippet.length <= SNIPPET_MAX_LENGTH) snippet else snippet.substring(0, SNIPPET_MAX_LENGTH)
                                }
                                writer.addLine(
                                    bookId = book.id,
                                    bookTitle = book.title,
                                    categoryId = book.categoryId,
                                    lineId = ln.id,
                                    lineIndex = ln.lineIndex,
                                    normalizedText = normalized,
                                    rawPlainText = snippetSource,
                                    orderIndex = orderIndexForBoost,
                                    isBaseBook = book.isBaseBook
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
                                val norm = normalizeForIndexDefault(t.text)
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
            logger.i { "Lucene text index built successfully at $indexDir (StandardAnalyzer + 4-gram)" }
            logger.i { "Lucene lookup index built successfully at $lookupDir" }
        }
    }

    repo.close()
}

private fun normalizeForIndexDefault(html: String): String {
    val cleaned = Jsoup.clean(html, Safelist.none())
        .trim()
        .replace("\\s+".toRegex(), " ")
    val withoutMaqaf = HebrewTextUtils.replaceMaqaf(cleaned, " ")
    // Drop gershayim (U+05F4) and geresh (U+05F3)
    val withoutGeresh = withoutMaqaf
        .replace("\u05F4", "")
        .replace("\u05F3", "")
    // Normalize final forms (ך/ם/ן/ף/ץ) -> base letters (כ/מ/נ/פ/צ)
    val noFinals = normalizeFinalLetters(withoutGeresh)
    // Drop diacritics entirely to index unpointed text
    return HebrewTextUtils.removeAllDiacritics(noFinals)
}

private fun sanitizeAcronymTerm(raw: String): String {
    var s = raw.trim()
    if (s.isEmpty()) return ""
    s = HebrewTextUtils.removeAllDiacritics(s)
    s = HebrewTextUtils.replaceMaqaf(s, " ")
    s = s.replace("\u05F4", "") // gershayim
    s = s.replace("\u05F3", "") // geresh
    s = normalizeFinalLetters(s)
    s = s.replace("\\s+".toRegex(), " ").trim()
    return s
}

/** Replace Hebrew final letters (sofit) by their base forms. */
private fun normalizeFinalLetters(text: String): String = text
    .replace('\u05DA', '\u05DB') // ך -> כ
    .replace('\u05DD', '\u05DE') // ם -> מ
    .replace('\u05DF', '\u05E0') // ן -> נ
    .replace('\u05E3', '\u05E4') // ף -> פ
    .replace('\u05E5', '\u05E6') // ץ -> צ
