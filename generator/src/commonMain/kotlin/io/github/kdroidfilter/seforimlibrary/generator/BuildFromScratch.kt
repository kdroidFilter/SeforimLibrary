package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.io.path.Path
import kotlin.system.exitProcess
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity

/**
 * Main entry point for the Otzaria database generator.
 * This function initializes the database, sets up the repository,
 * and runs the generation process.
 */
fun main() = runBlocking {
    // Configure Kermit to show all logs for live monitoring
    Logger.setMinSeverity(Severity.Debug)

    val logger = Logger.withTag("Main")

    // Resolve required environment variables for generation mode
    val dbPathEnv = System.getenv("SEFORIM_DB")
    if (dbPathEnv.isNullOrBlank()) {
        logger.e { "Missing required environment variable SEFORIM_DB" }
        logger.e { "Example: export SEFORIM_DB=/path/to/seforim.db" }
        exitProcess(1)
    }
    val dbPath = dbPathEnv

    val sourceDirEnv = System.getenv("OTZARIA_SOURCE_DIR")

    if (sourceDirEnv.isNullOrBlank()) {
        logger.e { "Missing required environment variable OTZARIA_SOURCE_DIR for generation mode" }
        logger.e { "Example: export OTZARIA_SOURCE_DIR=/path/to/otzaria_latest" }
        exitProcess(1)
    }

    val sourcePath = Path(sourceDirEnv)

    val dbFile = File(dbPath)
    val dbExists = dbFile.exists()
    logger.d { "Database file exists: $dbExists at $dbPath" }

    // If the database file exists, rename it to make sure we're creating a new one
    if (dbExists) {
        val backupFile = File("$dbPath.bak")
        if (backupFile.exists()) {
            backupFile.delete()
        }
        dbFile.renameTo(backupFile)
        logger.d { "Renamed existing database to ${backupFile.path}" }
    }

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")

    if (!sourcePath.toFile().exists()) {
        logger.e { "The source directory does not exist: $sourcePath" }
        exitProcess(1)
    }

    logger.i { "=== Otzaria Database Generator ===" }
    logger.i { "Source: $sourcePath" }
    logger.i { "Database: $dbPath" }

    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(sourcePath, repository)
        generator.generate()

        logger.i { "Generation completed successfully!" }
        logger.i { "Database created: $dbPath" }
    } catch (e: Exception) {
        logger.e(e) { "Error during generation" }
        exitProcess(1)
    } finally {
        repository.close()
    }
}
