package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Paths

/**
 * Phase 2 entry point: process links only (requires that books/lines already exist).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :generator:generateLinks -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateLinks")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val sourceDir = args.getOrNull(1)
        ?: System.getProperty("sourceDir")
        ?: System.getenv("OTZARIA_SOURCE_DIR")
        ?: OtzariaFetcher.ensureLocalSource(logger).toString()

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(
            sourceDirectory = Paths.get(sourceDir),
            repository = repository,
            acronymDbPath = null,
            textIndex = null,
            lookupIndex = null
        )
        generator.generateLinksOnly()
        logger.i { "Phase 2 completed successfully. Links processed." }
    } catch (e: Exception) {
        logger.e(e) { "Error during phase 2 (links)" }
        throw e
    } finally {
        repository.close()
    }
}
