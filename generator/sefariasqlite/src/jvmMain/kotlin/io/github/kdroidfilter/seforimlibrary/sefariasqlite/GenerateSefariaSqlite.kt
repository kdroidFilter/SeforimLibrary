package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

/**
 * One-step conversion: Sefaria export -> SQLite (direct import, sans Otzaria intermédiaire).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :sefariasqlite:generateSefariaSqlite -PseforimDb=/path/to.db [-PexportDir=/path/to/database_export]
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SefariaSqlite")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val useMemoryDb = when {
        System.getProperty("inMemoryDb") != null -> System.getProperty("inMemoryDb") != "false"
        System.getenv("IN_MEMORY_DB") != null -> System.getenv("IN_MEMORY_DB") != "false"
        dbPath == ":memory:" -> true
        else -> true // default to in-memory for perf
    }
    val persistDbPath = System.getProperty("persistDb")
        ?: System.getenv("SEFORIM_DB_OUT")
        ?: dbPath

    val exportDirArg = args.getOrNull(1)
        ?: System.getProperty("exportDir")
        ?: System.getenv("SEFARIA_EXPORT_DIR")
    val exportRoot: Path = exportDirArg?.let { Paths.get(it) } ?: SefariaExportFetcher.ensureLocalExport(logger)

    // Prepare DB (optionally in-memory)
    if (!useMemoryDb) {
        val dbFile = File(dbPath)
        if (dbFile.exists()) {
            val backup = File("$dbPath.bak")
            if (backup.exists()) backup.delete()
            if (dbFile.renameTo(backup)) {
                logger.i { "Existing DB moved to ${backup.absolutePath}" }
            } else {
                logger.w { "Failed to move existing DB; it will be overwritten." }
            }
        }
    }

    val jdbcUrl = if (useMemoryDb) "jdbc:sqlite::memory:" else "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    runCatching { SeforimDb.Schema.create(driver) }
    val repository = SeforimRepository(dbPath, driver)

    // ─── IdAllocator wiring (delta-update support, DELTA_UPDATE_PLAN.md §3.5) ──
    // Load the previous build_state.db so primary keys remain stable across
    // builds. Path defaults to <dbPath>.buildstate; override via -PbuildStatePath
    // or BUILD_STATE_PATH env var.
    val buildStatePath: Path = run {
        val explicit = System.getProperty("buildStatePath")
            ?: System.getenv("BUILD_STATE_PATH")
        if (explicit != null) Paths.get(explicit)
        else Paths.get("$persistDbPath.buildstate")
    }
    val prevBuildState: Path? = buildStatePath.takeIf { java.nio.file.Files.exists(it) }
    val allocator = InMemoryIdAllocator.load(
        path = prevBuildState,
        logger = Logger.withTag("IdAllocator"),
    )
    if (prevBuildState != null) {
        logger.i { "Loaded previous build_state from $prevBuildState" }
    } else {
        logger.i { "No previous build_state at $buildStatePath — starting fresh." }
    }

    try {
        val importer = SefariaDirectImporter(
            exportRoot = exportRoot,
            repository = repository,
            allocator = allocator,
            logger = Logger.withTag("SefariaDirect")
        )
        importer.import()

        if (useMemoryDb) {
            // Persist in-memory DB to disk using VACUUM INTO (target must not exist)
            val outFile = File(persistDbPath)
            outFile.parentFile?.mkdirs()
            if (outFile.exists()) {
                val backup = File(persistDbPath + ".bak")
                if (backup.exists()) backup.delete()
                if (!outFile.renameTo(backup)) {
                    outFile.delete()
                }
                logger.i { "Existing DB moved to ${backup.absolutePath}" }
            }
            val escaped = persistDbPath.replace("'", "''")
            logger.i { "Persisting in-memory DB to $persistDbPath via VACUUM INTO..." }
            repository.executeRawQuery("VACUUM INTO '$escaped'")
            logger.i { "In-memory DB persisted to $persistDbPath" }
        }

        // Persist build_state.db so the next build re-uses the same primary keys.
        runCatching {
            allocator.snapshotTo(
                target = buildStatePath,
                extraMeta = mapOf(
                    "generator" to "sefariasqlite",
                    "generated_at" to java.time.Instant.now().toString(),
                ),
            )
        }.onFailure { logger.w(it) { "Failed to write build_state to $buildStatePath" } }
        Unit

        logger.i { "Sefaria -> SQLite completed. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during Sefaria->SQLite generation" }
        throw e
    } finally {
        repository.close()
    }
}
