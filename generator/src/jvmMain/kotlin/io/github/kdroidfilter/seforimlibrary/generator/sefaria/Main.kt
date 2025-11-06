package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.generator.DatabaseGenerator
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * End-to-end entry point: build an Otzaria-shaped source tree from Sefaria
 * exports (json + schemas + toc + links), then import it into the SQLite DB
 * through the existing DatabaseGenerator.
 *
 * Usage (via Gradle task `generateSefaria`):
 *   ./gradlew :generator:generateSefaria \
 *     -PseforimDb=/path/to/seforim.db \
 *     -PsefariaRoot=/path/to/generator/build/Sefaria
 */
fun main() = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SefariaMain")

    // Resolve Sefaria root robustly across different working directories (IDE vs Gradle task)
    val explicitRoot = System.getProperty("sefariaRoot")?.let { Paths.get(it) }
    val candidates = listOfNotNull(
        explicitRoot,
        // Common when running from the generator module
        Paths.get("build", "Sefaria").toAbsolutePath(),
        // Common when running from repo root
        Paths.get("generator", "build", "Sefaria").toAbsolutePath(),
        // Parent fallbacks
        Paths.get("..", "build", "Sefaria").toAbsolutePath(),
        Paths.get("..", "generator", "build", "Sefaria").toAbsolutePath(),
    )
    val sefariaRoot = candidates.firstOrNull { Files.isDirectory(it) }
        ?: run {
            val msg = buildString {
                append("Sefaria root not found. Tried:\n")
                candidates.forEach { append(" - ${it}\n") }
            }
            error(msg)
        }
    val outRoot = Paths.get("build", "otzaria", "from-sefaria").toAbsolutePath()

    // Prepare DB
    val dbPath = System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val useMemoryDb = (System.getProperty("inMemoryDb") == "true") || dbPath == ":memory:"
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: Paths.get("build", "seforim.db").toString()

    if (!Files.isDirectory(sefariaRoot)) {
        error("Sefaria root not found at: ${sefariaRoot.toAbsolutePath()}")
    }
    Files.createDirectories(outRoot)

    // Build Otzaria-like source tree from Sefaria exports
    val builder = SefariaToOtzariaBuilder(logger)
    val builtRoot = builder.build(sefariaRoot = sefariaRoot, outRoot = outRoot)
    logger.i { "Sefaria → Otzaria source built at: ${builtRoot.toAbsolutePath()}" }

    // Rotate DB if writing directly to disk
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
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(
            sourceDirectory = builtRoot,
            repository = repository,
            acronymDbPath = System.getProperty("acronymDb"),
            textIndex = null,
            lookupIndex = null
        )
        // Full generation (categories/books/lines + links)
        generator.generate()
        if (useMemoryDb) {
            // Persist in-memory DB to disk using VACUUM INTO (target must not exist)
            runCatching {
                val outFile = File(persistDbPath)
                outFile.parentFile?.mkdirs()
                if (outFile.exists()) {
                    val backup = File(persistDbPath + ".bak")
                    if (backup.exists()) backup.delete()
                    if (!outFile.renameTo(backup)) outFile.delete()
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
        logger.i { "Sefaria generation completed. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } finally {
        repository.close()
    }
}
