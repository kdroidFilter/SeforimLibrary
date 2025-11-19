package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.generator.DatabaseGenerator
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.io.File

/**
 * Main entry point for generating SQLite database from Sefaria data
 */
fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger("SefariaGeneratorMain")

    logger.info("Starting Sefaria database generation...")

    // Parse arguments
    val sefariaDir = args.getOrNull(0)?.let(::File) ?: File("build/sefaria")

    // Default DB path under build/
    val defaultDiskDbPath = File("build/sefaria.db").absolutePath
    val cliDbPath = args.getOrNull(1) ?: defaultDiskDbPath

    // In-memory DB generation enabled by default (override with -DinMemoryDb=false or use a concrete path)
    val inMemoryFlag = System.getProperty("inMemoryDb") ?: "true"
    val useMemoryDb = (inMemoryFlag != "false") || cliDbPath == ":memory:"

    // When using the special :memory: CLI sentinel, still log and persist to a concrete disk path
    val dbPath = if (cliDbPath == ":memory:") defaultDiskDbPath else cliDbPath
    val outputDb = File(dbPath)

    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: dbPath

    logger.info("Sefaria data directory: ${sefariaDir.absolutePath}")
    logger.info("Output database: $dbPath")
    if (useMemoryDb) {
        logger.info("Using in-memory SQLite database; will persist to: $persistDbPath")
    }
    logger.info("Log files:")
    logger.info("  - All logs: build/sefaria-conversion-all.log")
    logger.info("  - Errors only: build/sefaria-conversion-errors.log")
    logger.info("  - Warnings/Errors: build/sefaria-conversion-warnings.log")

    // Validate inputs
    if (!sefariaDir.exists()) {
        logger.error("Sefaria directory does not exist: ${sefariaDir.absolutePath}")
        return
    }

    // Prepare output location
    if (!useMemoryDb) {
        // Create output directory if needed and delete existing file for on-disk DB
        outputDb.parentFile?.mkdirs()
        if (outputDb.exists()) {
            logger.info("Deleting existing database at $dbPath...")
            outputDb.delete()
        }
    } else {
        // Ensure parent directory for persisted DB exists (VACUUM INTO target)
        File(persistDbPath).parentFile?.mkdirs()
    }

    // Create database driver
    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(jdbcUrl)

    // Create schema
    logger.info("Ensuring database schema exists...")
    runCatching { SeforimDb.Schema.create(driver) }

    // Create repository
    val repository = SeforimRepository(dbPath, driver)

    // Create converter and run conversion
    runBlocking {
        try {
            val converter = SefariaToSQLiteConverter(repository, sefariaDir)

            // Performance PRAGMAs similar to Otzaria generator
            repository.setSynchronousOff()
            repository.setJournalModeOff()

            // Initialize connection types and sources
            converter.initialize()

            // Run conversion
            converter.convert()

            // Rebuild category closure for fast descendant queries
            logger.info("Rebuilding category_closure table...")
            repository.rebuildCategoryClosure()

            // Build precomputed catalog (catalog.pb) next to the database
            logger.info("Building precomputed catalog (catalog.pb)...")
            val catalogGenerator = DatabaseGenerator(
                sourceDirectory = File(dbPath).parentFile.toPath(),
                repository = repository,
                acronymDbPath = null,
                textIndex = null,
                lookupIndex = null
            )
            catalogGenerator.buildAndSaveCatalog()

            // Restore PRAGMAs after bulk generation
            repository.setSynchronousNormal()
            repository.setJournalModeWal()

            // If using in-memory DB, persist to disk using VACUUM INTO
            if (useMemoryDb) {
                runCatching {
                    val outFile = File(persistDbPath)
                    outFile.parentFile?.mkdirs()
                    if (outFile.exists()) {
                        val backup = File(persistDbPath + ".bak")
                        if (backup.exists()) backup.delete()
                        if (!outFile.renameTo(backup)) {
                            // If rename fails, delete to allow VACUUM INTO
                            outFile.delete()
                        }
                        logger.info("Existing DB moved to ${backup.absolutePath}")
                    }
                    val escaped = persistDbPath.replace("'", "''")
                    logger.info("Persisting in-memory DB to $persistDbPath via VACUUM INTO...")
                    repository.executeRawQuery("VACUUM INTO '$escaped'")
                    logger.info("In-memory DB persisted to $persistDbPath")
                }.onFailure { e ->
                    logger.error("Failed to persist in-memory DB to $persistDbPath", e)
                    throw e
                }
            }

            val finalPath = if (useMemoryDb) persistDbPath else dbPath
            val finalFile = File(finalPath)

            logger.info("Database generation completed successfully!")
            logger.info("Database file: $finalPath")
            if (finalFile.exists()) {
                logger.info("Database size: ${finalFile.length() / 1024 / 1024} MB")
            }

            // Log summary
            logger.info("============================================================")
            logger.info("Conversion complete! Check log files for details:")
            logger.info("  - build/sefaria-conversion-all.log (all logs)")
            logger.info("  - build/sefaria-conversion-errors.log (errors only)")
            logger.info("  - build/sefaria-conversion-warnings.log (warnings + errors)")
            logger.info("============================================================")
        } catch (e: Exception) {
            logger.error("Error during generation", e)
            logger.error("Check build/sefaria-conversion-errors.log for details")
            throw e
        } finally {
            repository.close()
        }
    }
}
