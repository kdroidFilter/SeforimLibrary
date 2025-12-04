package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneLookupIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneTextIndexWriter
import io.github.kdroidfilter.seforimlibrary.sefaria.SefariaFetcher
import kotlinx.coroutines.runBlocking
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.ngram.NGramTokenFilter
import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.io.File
import java.nio.file.Files
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
    val exportRoot: Path = exportDirArg?.let { Paths.get(it) } ?: SefariaFetcher.ensureLocalExport(logger)
    val catalogOut = Paths.get(
        System.getProperty("catalogOut")
            ?: System.getenv("CATALOG_OUT")
            ?: persistDbPath
    ).resolveSibling("catalog.pb")
    val textIndexDir = if (persistDbPath.endsWith(".db")) Paths.get("$persistDbPath.lucene") else Paths.get("$persistDbPath.luceneindex")
    val lookupIndexDir = if (persistDbPath.endsWith(".db")) Paths.get("$persistDbPath.lookup.lucene") else Paths.get("$persistDbPath.lookupindex")
    runCatching { Files.createDirectories(textIndexDir) }
    runCatching { Files.createDirectories(lookupIndexDir) }

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
    var analyzer: Analyzer? = null
    var textIndexWriter: LuceneTextIndexWriter? = null
    var lookupIndexWriter: LuceneLookupIndexWriter? = null

    try {
        val localAnalyzer = buildDefaultAnalyzer()
        analyzer = localAnalyzer
        textIndexWriter = LuceneTextIndexWriter(textIndexDir, analyzer = localAnalyzer)
        lookupIndexWriter = LuceneLookupIndexWriter(lookupIndexDir, analyzer = localAnalyzer)
        val importer = SefariaDirectImporter(
            exportRoot = exportRoot,
            repository = repository,
            catalogOutput = catalogOut,
            textIndex = textIndexWriter,
            lookupIndex = lookupIndexWriter,
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

        logger.i { "Sefaria -> SQLite completed. DB at ${if (useMemoryDb) persistDbPath else dbPath}" }
    } catch (e: Exception) {
        logger.e(e) { "Error during Sefaria->SQLite generation" }
        throw e
    } finally {
        runCatching { textIndexWriter?.close() }
        runCatching { lookupIndexWriter?.close() }
        runCatching { analyzer?.close() }
        repository.close()
    }
}

private fun buildDefaultAnalyzer(): Analyzer {
    val defaultAnalyzer = StandardAnalyzer()
    val ngram4Analyzer = object : Analyzer() {
        override fun createComponents(fieldName: String): TokenStreamComponents {
            val src = org.apache.lucene.analysis.standard.StandardTokenizer()
            var ts: TokenStream = src
            ts = LowerCaseFilter(ts)
            ts = NGramTokenFilter(ts, 4, 4, false)
            return TokenStreamComponents(src, ts)
        }
    }
    return PerFieldAnalyzerWrapper(
        defaultAnalyzer,
        mapOf(
            LuceneTextIndexWriter.FIELD_TEXT_NG4 to ngram4Analyzer
        )
    )
}
