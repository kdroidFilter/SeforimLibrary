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
fun main(args: Array<String>) = runBlocking {
    // Configure Kermit to show all logs for live monitoring
    Logger.setMinSeverity(Severity.Warn)

    val logger = Logger.withTag("Main")

    // Resolve required inputs: seforim DB, source dir, and optional acronymizer DB
    val dbPath = "/home/elie-gambache/Documents/SeforimDB/271025/seforim.db"

    val sourcePath = Path("/home/elie-gambache/Documents/SeforimDB/271025/otzaria_latest (2)")
    val acronymDbPath: String? = "/home/elie-gambache/IdeaProjects/SeforimApp/SeforimAcronymizer/acronymizer/src/jvmMain/acronymizer.db"

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
    if (acronymDbPath != null) logger.i { "Acronymizer DB: $acronymDbPath" } else logger.i { "Acronymizer DB: (none)" }

    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(sourcePath, repository, acronymDbPath)
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
