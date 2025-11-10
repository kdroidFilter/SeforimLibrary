package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Paths

/**
 * Entry point for building only the precomputed catalog (catalog.pb) from an existing database.
 *
 * Usage examples:
 *   ./gradlew :generator:buildCatalog -PseforimDb=/path/to/seforim.db
 *   ./gradlew :generator:buildCatalog  # Uses default build/seforim.db
 *
 * The catalog.pb file will be created in the same directory as the database.
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildCatalog")

    // Get database path from arguments or system properties
    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()

    // Verify database exists
    val dbFile = File(dbPath)
    if (!dbFile.exists() || !dbFile.isFile) {
        logger.e { "Database file not found: $dbPath" }
        logger.e { "Please provide a valid database path." }
        throw IllegalArgumentException("Database file not found: $dbPath")
    }

    logger.i { "Building precomputed catalog from database: $dbPath" }

    val jdbcUrl = "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(
            sourceDirectory = dbFile.parentFile.toPath(),
            repository = repository,
            acronymDbPath = null,
            textIndex = null,
            lookupIndex = null
        )

        // Build and save the catalog
        generator.buildAndSaveCatalog()

        // Verify the catalog was created
        val catalogFile = File(dbFile.parentFile, "catalog.pb")
        if (catalogFile.exists()) {
            val sizeKB = catalogFile.length() / 1024
            logger.i { "✓ Catalog successfully created: ${catalogFile.absolutePath}" }
            logger.i { "✓ File size: $sizeKB KB" }
        } else {
            logger.e { "✗ Catalog file was not created" }
            throw IllegalStateException("Catalog file was not created")
        }
    } catch (e: Exception) {
        logger.e(e) { "Error building catalog" }
        throw e
    } finally {
        repository.close()
    }
}
