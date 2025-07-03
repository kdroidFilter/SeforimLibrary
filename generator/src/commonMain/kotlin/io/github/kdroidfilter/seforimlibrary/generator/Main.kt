package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Paths
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
    // Configure Kermit to only log warnings and errors
    Logger.setMinSeverity(Severity.Error)

    val logger = Logger.withTag("Main")

    val dbFile = File("otzaria.db")
    val dbExists = dbFile.exists()
    logger.d{"Database file exists: $dbExists"}

    // If the database file exists, rename it to make sure we're creating a new one
    if (dbExists) {
        val backupFile = File("otzaria.db.bak")
        if (backupFile.exists()) {
            backupFile.delete()
        }
        dbFile.renameTo(backupFile)
        logger.d{"Renamed existing database to otzaria.db.bak"}
    }

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:otzaria.db")

//    val sourcePath = Path("/Users/elie/Downloads/otzaria_latest")
    val sourcePath = Path("/Users/elie/IdeaProjects/SeforimLibrary/otzaria_latest")
    val dbPath = Paths.get("otzaria.db").toFile().path

    if (!sourcePath.toFile().exists()) {
        logger.e{"The source directory does not exist: $sourcePath"}
        exitProcess(1)
    }

    logger.i{"=== Otzaria Database Generator ==="}
    logger.i{"Source: $sourcePath"}
    logger.i{"Database: $dbPath"}

    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(sourcePath, repository)
        generator.generate()

        logger.i{"Generation completed successfully!"}
        logger.i{"Database created: $dbPath"}
    } catch (e: Exception) {
        logger.e(e){"Error during generation"}
        exitProcess(1)
    } finally {
        repository.close()
    }
}
