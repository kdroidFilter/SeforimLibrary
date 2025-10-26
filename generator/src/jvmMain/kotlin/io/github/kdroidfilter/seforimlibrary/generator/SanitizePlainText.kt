package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.generator.utils.HebrewTextUtils
import kotlinx.coroutines.runBlocking
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist

/**
 * Utility that sanitizes line.plainText for an existing DB by removing
 * Hebrew diacritics (nikud + teamim) and replacing maqaf with space, then
 * rebuilds the FTS index.
 *
 * Usage:
 *   ./gradlew :generator:sanitizePlainText -PseforimDb=/path/to/seforim.db
 * or set env var SEFORIM_DB.
 */
fun main() = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SanitizePlainText")

    val dbPath = "/home/elie-gambache/.local/share/io.github.kdroidfilter.seforimapp/databases/seforim_25-10-25.db"

    logger.i { "Using database: $dbPath" }
    val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath, driver)

    try {
        val books = repository.getAllBooks()
        logger.i { "Found ${books.size} books. Starting plainText sanitization..." }

        var totalUpdated = 0
        val batchSize = 1000

        // Per-book explicit transaction, tolerant to failures
        for ((bIdx, book) in books.withIndex()) {
            runCatching { repository.executeRawQuery("BEGIN IMMEDIATE") }
            try {
                var start = 0
                while (true) {
                    val end = start + batchSize - 1
                    val lines = repository.getLines(book.id, start, end)
                    if (lines.isEmpty()) break

                    for (line in lines) {
                        val cleaned = sanitizeHtml(line.content)
                        repository.updateLinePlainText(line.id, cleaned)
                        totalUpdated++
                    }
                    start += batchSize
                    if (totalUpdated % 10_000 == 0) {
                        logger.i { "Updated $totalUpdated lines so far..." }
                    }
                }
                runCatching { repository.executeRawQuery("COMMIT") }
            } catch (t: Throwable) {
                logger.w(t) { "Error sanitizing book ${book.id}. Rolling back its changes and continuing." }
                runCatching { repository.executeRawQuery("ROLLBACK") }
            }
            if ((bIdx + 1) % 50 == 0) {
                logger.i { "Processed ${bIdx + 1}/${books.size} books..." }
            }
        }

        logger.i { "Sanitization complete. Total updated lines: $totalUpdated" }
        logger.i { "Rebuilding FTS5 index (external content 'rebuild')..." }
        repository.executeRawQuery("INSERT INTO line_search(line_search) VALUES('rebuild');")
        logger.i { "FTS rebuild completed." }
    } finally {
        repository.close()
    }
}

private fun sanitizeHtml(html: String): String {
    val cleaned = Jsoup.clean(html, Safelist.none())
        .trim()
        .replace("\\s+".toRegex(), " ")
    val withoutMaqaf = HebrewTextUtils.replaceMaqaf(cleaned, " ")
    return HebrewTextUtils.removeAllDiacritics(withoutMaqaf)
}
