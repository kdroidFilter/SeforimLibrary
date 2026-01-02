package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.QueryResult
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Paths

/**
 * Phase 2 entry point: process links only (requires that books/lines already exist).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateLinks -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateLinks")

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

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repository = SeforimRepository(dbPath, driver)

    try {
        // If using in-memory DB, seed it from base DB on disk if provided
        if (useMemoryDb) {
            val baseDb = System.getProperty("baseDb")
                ?: System.getenv("SEFORIM_DB_BASE")
                ?: Paths.get("build", "seforim.db").toString()
            val baseFile = java.io.File(baseDb)
            if (baseFile.exists()) {
                logger.i { "Seeding in-memory DB from base file: $baseDb" }
                runCatching {
                    repository.executeRawQuery("PRAGMA foreign_keys=OFF")
                    val escaped = baseDb.replace("'", "''")
                    repository.executeRawQuery("ATTACH DATABASE '$escaped' AS disk")
                    // Load all table names from attached DB
                    val tables = driver.executeQuery(null,
                        "SELECT name FROM disk.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                        { c: SqlCursor ->
                            val list = mutableListOf<String>()
                            while (c.next().value) {
                                c.getString(0)?.let { list.add(it) }
                            }
                            QueryResult.Value(list)
                        }, 0
                    ).value
                    // Copy data for each table into main
                    for (t in tables) {
                        val tn = t
                        repository.executeRawQuery("DELETE FROM \"$tn\"")
                        repository.executeRawQuery("INSERT INTO \"$tn\" SELECT * FROM disk.\"$tn\"")
                    }
                    repository.executeRawQuery("DETACH DATABASE disk")
                    repository.executeRawQuery("PRAGMA foreign_keys=ON")
                    logger.i { "Seeding completed. Imported ${'$'}{tables.size} tables." }
                }.onFailure { e ->
                    logger.e(e) { "Failed to seed in-memory DB from $baseDb. Links may not be processed." }
                }
            } else {
                logger.w { "Base DB not found at $baseDb; running with empty in-memory DB" }
            }
        }

        val generator = DatabaseGenerator(
            sourceDirectory = Paths.get(sourceDir),
            repository = repository,
            acronymDbPath = null,
            filterSourcesForLinks = false
        )
        generator.generateLinksOnly()
        if (useMemoryDb) {
            // Persist in-memory DB to disk using VACUUM INTO (target must not exist)
            runCatching {
                val outFile = java.io.File(persistDbPath)
                outFile.parentFile?.mkdirs()
                if (outFile.exists()) {
                    // No backup required: remove existing file to allow VACUUM INTO
                    val deleted = runCatching { java.nio.file.Files.deleteIfExists(outFile.toPath()) }.getOrDefault(false)
                    if (!deleted) {
                        throw IllegalStateException("Cannot remove existing DB at ${outFile.absolutePath} before persisting")
                    }
                    logger.i { "Removed existing DB at ${outFile.absolutePath}" }
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
        logger.i { "Phase 2 completed successfully. Links processed. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during phase 2 (links)" }
        throw e
    } finally {
        repository.close()
    }
}
