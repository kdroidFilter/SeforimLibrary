package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking

/**
 * Utility entrypoint to rebuild the category_closure table and the FTS5 index
 * for an existing database file, without re-importing all sources.
 *
 * Usage:
 *   ./gradlew :generator:rebuildClosureAndFts -PseforimDb=/path/to/seforim.db
 * or set env var SEFORIM_DB and run the task without -P.
 */
fun main() = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("RebuildClosureAndFts")

    val dbPath = "/home/elie-gambache/.local/share/io.github.kdroidfilter.seforimapp/databases/seforim_25-10-25.db"

    logger.i { "Using database: $dbPath" }

    val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath, driver)
    try {
        // Ensure view and FTS table have the latest schema (adds categoryId to FTS external content)
        logger.i { "Dropping and recreating line_with_book_title view and line_search FTS table..." }
        repository.executeRawQuery("DROP VIEW IF EXISTS line_with_book_title;")
        repository.executeRawQuery(
            """
            CREATE VIEW IF NOT EXISTS line_with_book_title AS
            SELECT 
                l.id,
                l.bookId,
                l.lineIndex,
                b.title AS bookTitle,
                b.categoryId AS categoryId,
                l.plainText
            FROM line l
            JOIN book b ON l.bookId = b.id;
            """.trimIndent()
        )
        repository.executeRawQuery("DROP TABLE IF EXISTS line_search;")
        repository.executeRawQuery(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS line_search USING fts5(
                bookId UNINDEXED,
                id UNINDEXED,
                lineIndex UNINDEXED,
                bookTitle UNINDEXED,
                categoryId UNINDEXED,
                plainText,
                content='line_with_book_title',
                content_rowid='id'
            );
            """.trimIndent()
        )

        // Build/refresh closure table
        logger.i { "Rebuilding category_closure (ancestor-descendant) table..." }
        repository.rebuildCategoryClosure()
        logger.i { "category_closure rebuilt successfully" }

        // Populate FTS from external content
        logger.i { "Rebuilding FTS5 index for line_search (external content 'rebuild')..." }
        repository.executeRawQuery("INSERT INTO line_search(line_search) VALUES('rebuild');")
        logger.i { "FTS5 index rebuilt successfully" }
    } finally {
        repository.close()
    }
}
