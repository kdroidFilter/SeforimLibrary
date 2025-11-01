package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneTextIndexWriter
import io.github.kdroidfilter.seforimlibrary.generator.lucene.LuceneLookupIndexWriter
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.Path as KPath
import kotlin.system.exitProcess

/**
 * JVM entry point for the Otzaria database generator with Lucene indexing.
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Warn)
    val logger = Logger.withTag("Main")

    // Resolve inputs (adapt as needed)
    val dbPath = "/Volumes/Data/Downloads/seforim_lucene.db"
    val sourcePath = KPath("/Volumes/Data/Downloads/otzaria_latest")
    val acronymDbPath: String? = "/Volumes/Data/Downloads/acronymizer.db"

    val dbFile = File(dbPath)
    val dbExists = dbFile.exists()
    if (dbExists) {
        val backupFile = File("$dbPath.bak")
        if (backupFile.exists()) backupFile.delete()
        dbFile.renameTo(backupFile)
    }

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath, driver)

    // Index dir beside the DB
    val indexDir = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lucene") else Paths.get("$dbPath.luceneindex")
    val luceneWriter = LuceneTextIndexWriter(indexDir)
    val lookupIndexDir = if (dbPath.endsWith(".db")) Paths.get("$dbPath.lookup.lucene") else Paths.get("$dbPath.lookupindex")
    val lookupWriter = LuceneLookupIndexWriter(lookupIndexDir)

    try {
        val generator = DatabaseGenerator(sourcePath, repository, acronymDbPath, luceneWriter, lookupWriter)
        generator.generate()
        logger.i { "Generation completed successfully! DB: $dbPath Index: $indexDir" }
    } catch (e: Exception) {
        logger.e(e) { "Error during generation" }
        exitProcess(1)
    } finally {
        runCatching { luceneWriter.close() }
        runCatching { lookupWriter.close() }
        repository.close()
    }
}
