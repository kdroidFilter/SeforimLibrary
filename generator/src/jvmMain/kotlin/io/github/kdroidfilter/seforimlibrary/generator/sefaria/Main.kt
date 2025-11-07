package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.generator.DatabaseGenerator
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
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
    logger.i { "Resolved Sefaria root at ${sefariaRoot.toAbsolutePath()}" }
    val outRoot = Paths.get("build", "otzaria", "from-sefaria").toAbsolutePath()
    logger.i { "Will flush Otzaria source to ${outRoot.toAbsolutePath()}" }

    // Prepare DB
    val dbPath = System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    // Default to in-memory DB for fully RAM-based generation unless explicitly disabled
    val useMemoryDb = if (System.getProperty("inMemoryDb") == "false") false else true
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: (if (dbPath == ":memory:") Paths.get("build", "seforim.db").toString() else dbPath)
    logger.i { "DB target: ${if (useMemoryDb) ":memory:" else dbPath} (persist to: $persistDbPath)" }

    if (!Files.isDirectory(sefariaRoot)) {
        error("Sefaria root not found at: ${sefariaRoot.toAbsolutePath()}")
    }
    // Build Otzaria-like source tree fully in RAM (Jimfs), then import to DB
    val builder = SefariaToOtzariaBuilder(logger)
    val tBuildStart = System.nanoTime()
    val buildMem = builder.buildToMemoryFs(sefariaRoot = sefariaRoot)
    val tBuildEnd = System.nanoTime()
    val builtRoot = buildMem.fsRoot
    logger.i {
        val durMs = (tBuildEnd - tBuildStart) / 1_000_000
        "Sefaria → Otzaria source built in RAM: files=${buildMem.filesCount}, metadata=${buildMem.metadataCount}, duration=${durMs}ms"
    }

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
        val tGenStart = System.nanoTime()
        logger.i { "Initializing DatabaseGenerator and starting import…" }
        val generator = DatabaseGenerator(
            sourceDirectory = builtRoot,
            repository = repository,
            acronymDbPath = System.getProperty("acronymDb"),
            textIndex = null,
            lookupIndex = null
        )
        // Full generation (categories/books/lines + links)
        generator.generate()
        val tGenEnd = System.nanoTime()
        logger.i { "DB generation completed in ${(tGenEnd - tGenStart) / 1_000_000}ms" }
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
                val tVacStart = System.nanoTime()
                logger.i { "Persisting in-memory DB to $persistDbPath via VACUUM INTO…" }
                repository.executeRawQuery("VACUUM INTO '$escaped'")
                val tVacEnd = System.nanoTime()
                logger.i { "In-memory DB persisted to $persistDbPath in ${(tVacEnd - tVacStart) / 1_000_000}ms" }
            }.onFailure { e ->
                logger.e(e) { "Failed to persist in-memory DB to $persistDbPath" }
                throw e
            }
        }
        // After DB is done, flush the RAM-built Otzaria tree to disk (outRoot) in parallel
        Files.createDirectories(outRoot)
        val parallelism = System.getProperty("sefariaParallelism")?.toIntOrNull()
            ?: System.getenv("SEFARIA_PARALLELISM")?.toIntOrNull()
            ?: Runtime.getRuntime().availableProcessors()
        val dispatcher = kotlinx.coroutines.Dispatchers.IO.limitedParallelism(parallelism)
        kotlinx.coroutines.runBlocking {
            val tFlushStart = System.nanoTime()
            val paths = mutableListOf<Path>()
            Files.walk(builtRoot).use { s -> s.forEach { paths.add(it) } }
            // Create directories first
            for (p in paths) if (Files.isDirectory(p)) Files.createDirectories(outRoot.resolve(builtRoot.relativize(p).toString()))
            // Copy files in parallel
            kotlinx.coroutines.coroutineScope {
                val files = paths.filter { Files.isRegularFile(it) }
                logger.i { "Copying ${files.size} files to ${outRoot.toAbsolutePath()} using parallelism=$parallelism" }
                files.map { src ->
                    async(dispatcher) {
                        val rel = builtRoot.relativize(src)
                        val dst = outRoot.resolve(rel.toString())
                        Files.createDirectories(dst.parent)
                        Files.copy(src, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                    }
                }.forEach { it.await() }
            }
            val tFlushEnd = System.nanoTime()
            logger.i { "Flushed source to disk in ${(tFlushEnd - tFlushStart) / 1_000_000}ms" }
        }
        logger.i { "Sefaria generation completed. Source flushed to ${outRoot.toAbsolutePath()} | DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } finally {
        repository.close()
    }
}
