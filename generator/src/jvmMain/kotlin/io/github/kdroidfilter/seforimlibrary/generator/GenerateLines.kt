package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Paths

/**
 * Phase 1 entry point: generate categories, books, TOCs and lines only.
 *
 * Usage examples:
 *   ./gradlew -p SeforimLibrary :generator:generateLines -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria [-PacronymDb=/path/acronym.db]
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateLines")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val useMemoryDb = (System.getProperty("inMemoryDb") == "true") || dbPath == ":memory:"
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: Paths.get("build", "seforim.db").toString()
    val sourceDir = args.getOrNull(1)
        ?: System.getProperty("sourceDir")
        ?: System.getenv("OTZARIA_SOURCE_DIR")
        ?: OtzariaFetcher.ensureLocalSource(logger).toString()
    val acronymDbPath = args.getOrNull(2)
        ?: System.getProperty("acronymDb")
        ?: System.getenv("ACRONYM_DB")
        ?: run {
            // Prefer an already-downloaded DB under build/; otherwise fetch latest
            val defaultPath = Paths.get("build", "acronymizer", "acronymizer.db").toFile()
            if (defaultPath.exists() && defaultPath.isFile) defaultPath.absolutePath
            else AcronymizerFetcher.ensureLocalDb(logger).toAbsolutePath().toString()
        }

    // If writing directly to disk, rotate existing DB; for in-memory we will persist at the end
    if (!useMemoryDb) {
        val dbFile = File(dbPath)
        if (dbFile.exists()) {
            val backupFile = File("$dbPath.bak")
            if (backupFile.exists()) backupFile.delete()
            dbFile.renameTo(backupFile)
            logger.i { "Existing DB moved to ${backupFile.absolutePath}" }
        }
    }

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    // Ensure schema exists on a brand-new DB before repository init (idempotent)
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(
            sourceDirectory = Paths.get(sourceDir),
            repository = repository,
            acronymDbPath = acronymDbPath,
            textIndex = null,
            lookupIndex = null
        )
        generator.generateLinesOnly()
        if (useMemoryDb) {
            // Persist in-memory DB to disk using VACUUM INTO (target must not exist)
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
                    logger.i { "Existing DB moved to ${backup.absolutePath}" }
                }
                val escaped = persistDbPath.replace("'", "''")
                logger.i { "Persisting in-memory DB to $persistDbPath via VACUUM INTO..." }
                repository.executeRawQuery("VACUUM INTO '$escaped'")
                logger.i { "In-memory DB persisted to $persistDbPath" }
            }.onFailure { e ->
                logger.e(e) { "Failed to persist in-memory DB to $persistDbPath" }
                throw e
            }
        }
        logger.i { "Phase 1 completed successfully. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during phase 1 generation" }
        throw e
    } finally {
        repository.close()
    }
}
