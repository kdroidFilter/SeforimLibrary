package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.generator.utils.HebrewTextUtils
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.system.exitProcess

object AcronymMigration {
    private val logger = Logger.withTag("AcronymMigration")

    fun sanitize(raw: String): String {
        var s = raw.trim()
        if (s.isEmpty()) return ""
        s = HebrewTextUtils.removeAllDiacritics(s)
        s = HebrewTextUtils.replaceMaqaf(s, " ")
        s = s.replace("\u05F4", "") // remove Hebrew gershayim (״)
        s = s.replace("\u05F3", "") // remove Hebrew geresh (׳)
        s = s.replace("\\s+".toRegex(), " ").trim()
        return s
    }

    suspend fun migrate(repository: SeforimRepository, acronymDbPath: String) {
        logger.i { "Starting acronym migration from: $acronymDbPath" }

        // Open acronymizer DB via JDBC
        val conn = DriverManager.getConnection("jdbc:sqlite:$acronymDbPath")
        conn.use { sql ->
            val books = repository.getAllBooks()
            logger.i { "Found ${books.size} books in main DB" }

            var processed = 0
            // Clear FTS index for titles, we will fully rebuild it
            repository.clearBookTitleFts()

            for (book in books) {
                try {
                    sql.prepareStatement(
                        "SELECT terms FROM AcronymResults WHERE book_title = ? ORDER BY id"
                    ).use { ps ->
                        ps.setString(1, book.title)
                        ps.executeQuery().use { rs ->
                            val collected = LinkedHashSet<String>()
                            while (rs.next()) {
                                val terms = rs.getString(1) ?: continue
                                terms.split(',').forEach { raw ->
                                    val t = sanitize(raw)
                                    if (t.isNotEmpty()) collected.add(t)
                                }
                            }

                            if (collected.isNotEmpty()) {
                                val titleNorm = sanitize(book.title)
                                val finalTerms = collected
                                    .filter { it.isNotEmpty() }
                                    .filter { !it.equals(book.title, ignoreCase = true) }
                                    .filter { !it.equals(titleNorm, ignoreCase = true) }

                                repository.bulkInsertBookAcronyms(book.id, finalTerms)
                                logger.d { "Book ${book.id} '${book.title}': inserted ${finalTerms.size} acronyms" }

                                // Insert acronym terms into FTS
                                finalTerms.forEach { t ->
                                    repository.insertBookTitleFtsTerm(book.id, t, book.title, book.categoryId)
                                }
                            }

                            // Always insert title (raw + sanitized if different) into FTS
                            repository.insertBookTitleFtsTerm(book.id, book.title, book.title, book.categoryId)
                            val titleSan = sanitize(book.title)
                            if (titleSan.isNotBlank() && !titleSan.equals(book.title, ignoreCase = true)) {
                                repository.insertBookTitleFtsTerm(book.id, titleSan, book.title, book.categoryId)
                            }
                        }
                    }
                } catch (e: Exception) {
                    logger.w(e) { "Failed processing acronyms for book '${book.title}'" }
                } finally {
                    processed++
                    if (processed % 100 == 0) logger.i { "Progress: $processed/${books.size}" }
                }
            }

            logger.i { "Acronym migration completed for $processed/${books.size} books" }
        }
    }
}

// Standalone entry point
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val log = Logger.withTag("AcronymMigrationMain")

    val dbPath = "/home/elie-gambache/.local/share/io.github.kdroidfilter.seforimapp/databases/seforim_25-10-25.db"

    val acronymDb = "/home/elie-gambache/IdeaProjects/SeforimApp/SeforimAcronymizer/acronymizer/src/jvmMain/acronymizer.db"

    log.i { "=== Acronym Migration ===" }
    log.i { "Main DB: $dbPath" }
    log.i { "Acronym DB: $acronymDb" }

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repo = SeforimRepository(dbPath, driver)
    try {
        AcronymMigration.migrate(repo, acronymDb)
        log.i { "Done." }
    } catch (e: Exception) {
        log.e(e) { "Error during migration" }
        exitProcess(1)
    } finally {
        repo.close()
    }
}
