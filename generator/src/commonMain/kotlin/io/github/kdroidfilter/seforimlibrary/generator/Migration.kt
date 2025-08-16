package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.system.exitProcess

/**
 * Utilities to migrate an existing Otzaria database to include the new
 * per-connection-type flags on the `book` table and populate them based
 * on existing links.
 */
object DatabaseMigration {
    private val logger = Logger.withTag("DatabaseMigration")

    /**
     * Runs the migration on an existing database file opened by the provided repository.
     * Steps:
     * 1) Add new columns to book (if they don't already exist) with default 0.
     * 2) For every book, compute whether it has links of type TARGUM, REFERENCE,
     *    COMMENTARY, OTHER (as source or target) and update the flags.
     */
    suspend fun migrateBookConnectionFlags(repository: SeforimRepository) = withContext(Dispatchers.IO) {
        logger.i { "Starting migration: add book connection flag columns and backfill values" }

        // 1) Add columns (attempt; ignore error if column already exists)
        addColumnIfMissing(repository, "hasTargumConnection")
        addColumnIfMissing(repository, "hasReferenceConnection")
        addColumnIfMissing(repository, "hasCommentaryConnection")
        addColumnIfMissing(repository, "hasOtherConnection")

        // 2) Backfill flags per book based on existing links
        val books = repository.getAllBooks()
        logger.i { "Found ${books.size} books to migrate" }

        var processed = 0
        for (book in books) {
            logger.d { "Backfilling flags for book ${book.id} - ${book.title}" }
            try {
                val targumCount = repository.countLinksBySourceBookAndType(book.id, "TARGUM") +
                        repository.countLinksByTargetBookAndType(book.id, "TARGUM")
                val referenceCount = repository.countLinksBySourceBookAndType(book.id, "REFERENCE") +
                        repository.countLinksByTargetBookAndType(book.id, "REFERENCE")
                val commentaryCount = repository.countLinksBySourceBookAndType(book.id, "COMMENTARY") +
                        repository.countLinksByTargetBookAndType(book.id, "COMMENTARY")
                val otherCount = repository.countLinksBySourceBookAndType(book.id, "OTHER") +
                        repository.countLinksByTargetBookAndType(book.id, "OTHER")

                val hasTargum = targumCount > 0
                val hasReference = referenceCount > 0
                val hasCommentary = commentaryCount > 0
                val hasOther = otherCount > 0

                repository.updateBookConnectionFlags(
                    bookId = book.id,
                    hasTargum = hasTargum,
                    hasReference = hasReference,
                    hasCommentary = hasCommentary,
                    hasOther = hasOther
                )
            } catch (e: Exception) {
                logger.w(e) { "Failed to backfill flags for book ${book.id} - ${book.title}" }
            } finally {
                processed++
                // Log progress after each book processed (attempted)
                logger.i { "Migration progress: $processed/${books.size} books" }
                if (processed % 100 == 0) {
                    logger.d { "Backfilled flags for $processed/${books.size} books" }
                }
            }
        }

        logger.i { "Migration completed: backfilled flags for $processed/${books.size} books" }
    }

    private suspend fun addColumnIfMissing(repository: SeforimRepository, column: String) {
        // SQLite doesn't universally support IF NOT EXISTS for ADD COLUMN; just try/catch
        val sql = "ALTER TABLE book ADD COLUMN $column INTEGER NOT NULL DEFAULT 0"
        try {
            repository.executeRawQuery(sql)
            logger.i { "Added column '$column' to book" }
        } catch (e: Exception) {
            // Column probably exists; log at debug level
            logger.d { "Column '$column' likely exists already; skipping (error: ${e.message})" }
        }
    }
}

/**
 * Standalone entry point to run ONLY the migration on an existing database.
 *
 * Usage:
 *   export SEFORIM_DB=/path/to/seforim.db
 *   (then run this main from your IDE or Gradle run configuration targeting MigrationMain)
 */
fun main() = runBlocking {
    // Enable verbose logs for live monitoring during migration
    Logger.setMinSeverity(Severity.Debug)
    val logger = Logger.withTag("MigrationMain")

    val dbPathEnv = System.getenv("SEFORIM_DB")
    if (dbPathEnv.isNullOrBlank()) {
        logger.e { "Missing required environment variable SEFORIM_DB" }
        logger.e { "Example: export SEFORIM_DB=/path/to/seforim.db" }
        exitProcess(1)
    }

    val dbPath = dbPathEnv
    logger.i { "=== SEFORIM Database Migration (existing DB) ===" }
    logger.i { "Database: $dbPath" }

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath, driver)
    try {
        DatabaseMigration.migrateBookConnectionFlags(repository)
        logger.i { "Migration completed successfully!" }
    } catch (e: Exception) {
        logger.e(e) { "Error during migration" }
        exitProcess(1)
    } finally {
        repository.close()
    }
}
