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
    val dbPath = when {
        args.size >= 1 -> args[0]
        !System.getenv("SEFORIM_DB").isNullOrBlank() -> System.getenv("SEFORIM_DB")
        else -> {
            logger.e { "Missing required SEFORIM_DB (env or arg[0])" }
            exitProcess(1)
        }
    }

    val sourcePath = when {
        args.size >= 2 -> Path(args[1])
        !System.getenv("OTZARIA_SOURCE_DIR").isNullOrBlank() -> Path(System.getenv("OTZARIA_SOURCE_DIR"))
        else -> {
            logger.e { "Missing required OTZARIA_SOURCE_DIR (env or arg[1])" }
            exitProcess(1)
        }
    }

    val acronymDbPath: String? = when {
        args.size >= 3 -> args[2]
        !System.getenv("ACRONYM_DB").isNullOrBlank() -> System.getenv("ACRONYM_DB")
        else -> null
    }

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
