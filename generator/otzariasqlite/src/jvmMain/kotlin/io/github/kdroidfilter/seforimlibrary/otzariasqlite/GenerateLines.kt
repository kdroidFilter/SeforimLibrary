package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
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
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLines -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria [-PacronymDb=/path/acronym.db]
 *   # To append to an existing DB instead of rotating it:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLines -PappendExistingDb=true
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateLines")

    val seforimDbPropOrEnv = System.getProperty("seforimDb") ?: System.getenv("SEFORIM_DB")
    val dbPath = args.getOrNull(0)
        ?: seforimDbPropOrEnv
        ?: Paths.get("build", "seforim.db").toString()
    val useMemoryDb = (System.getProperty("inMemoryDb") == "true") || dbPath == ":memory:"
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
    val appendExistingDb = listOf(
        System.getProperty("appendExistingDb"),
        System.getenv("APPEND_EXISTING_DB")
    ).firstOrNull { !it.isNullOrBlank() }
        ?.let { it.equals("true", ignoreCase = true) || it == "1" }
        ?: false
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: if (appendExistingDb) seforimDbPropOrEnv else null
        ?: Paths.get("build", "seforim.db").toString()

    // If writing directly to disk, rotate existing DB; for in-memory we will persist at the end
    if (!useMemoryDb && !appendExistingDb) {
        val dbFile = File(dbPath)
        if (dbFile.exists()) {
            val backupFile = File("$dbPath.bak")
            if (backupFile.exists()) backupFile.delete()
            dbFile.renameTo(backupFile)
            logger.i { "Existing DB moved to ${backupFile.absolutePath}" }
        }
    } else if (!useMemoryDb && appendExistingDb) {
        val dbFile = File(dbPath)
        if (dbFile.exists()) {
            logger.i { "Appending to existing DB at ${dbFile.absolutePath}" }
        } else {
            logger.i { "appendExistingDb enabled but no DB found at $dbPath; a new DB will be created" }
        }
    }

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    // Ensure schema exists on a brand-new DB before repository init (idempotent)
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)

    if (useMemoryDb && appendExistingDb) {
        val baseDbPath = System.getProperty("baseDb")
            ?: System.getenv("SEFORIM_DB_BASE")
            ?: seforimDbPropOrEnv
            ?: persistDbPath
        if (baseDbPath != null && baseDbPath != ":memory:") {
            val baseFile = File(baseDbPath)
            if (baseFile.exists()) {
                logger.i { "Seeding in-memory DB from base file: ${baseFile.absolutePath}" }
                runCatching {
                    repository.executeRawQuery("PRAGMA foreign_keys=OFF")
                    val escaped = baseFile.absolutePath.replace("'", "''")
                    repository.executeRawQuery("ATTACH DATABASE '$escaped' AS disk")
                    val tables = driver.executeQuery(
                        null,
                        "SELECT name FROM disk.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                        { c: SqlCursor ->
                            val list = mutableListOf<String>()
                            while (c.next().value) {
                                c.getString(0)?.let { list.add(it) }
                            }
                            QueryResult.Value(list)
                        },
                        0
                    ).value
                    for (t in tables) {
                        repository.executeRawQuery("DELETE FROM \"$t\"")
                        repository.executeRawQuery("INSERT INTO \"$t\" SELECT * FROM disk.\"$t\"")
                    }
                    repository.executeRawQuery("DETACH DATABASE disk")
                    repository.executeRawQuery("PRAGMA foreign_keys=ON")
                    logger.i { "Seeding completed. Imported ${tables.size} tables." }
                }.onFailure { e ->
                    logger.e(e) { "Failed to seed in-memory DB from $baseDbPath; continuing with empty DB." }
                }
            } else {
                logger.w { "appendExistingDb enabled but base DB not found at $baseDbPath; starting from empty in-memory DB" }
            }
        } else {
            logger.w { "appendExistingDb enabled in-memory but no base DB path provided; starting from empty DB" }
        }
    }

    try {
        val generator = DatabaseGenerator(
            sourceDirectory = Paths.get(sourceDir),
            repository = repository,
            acronymDbPath = acronymDbPath
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
