package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Paths
import java.nio.file.Files

/**
 * Entry point to generate a SQLite DB from Sefaria exports located under generator/build/Sefaria
 * and import links. Usage examples:
 *
 * - Default paths (DB at generator/build/seforim.db, input at generator/build/Sefaria):
 *   ./gradlew :generator:jvmRun -PmainClass=io.github.kdroidfilter.seforimlibrary.generator.sefaria.MainKt
 *
 * - Custom paths via system properties:
 *   ./gradlew :generator:jvmRun -PmainClass=io.github.kdroidfilter.seforimlibrary.generator.sefaria.MainKt \
 *     -DseforimDb=/path/to/seforim.db -DsefariaRoot=/path/to/Sefaria
 */
fun main(args: Array<String>) = runBlocking {
    val logger = Logger.withTag("SefariaMain")

    val targetDbPath = System.getProperty("seforimDb")
        ?: (if (args.isNotEmpty()) args[0] else "generator/build/seforim.db")
    val sefariaRootPath = System.getProperty("sefariaRoot")
        ?: (if (args.size >= 2) args[1] else "generator/build/Sefaria")

    logger.i { "Target DB path: $targetDbPath" }
    logger.i { "Sefaria root: $sefariaRootPath" }

    // Generate entirely in memory, then persist to disk at the end
    val driver = JdbcSqliteDriver("jdbc:sqlite::memory:")
    val repo = SeforimRepository(":memory:", driver)

    try {
        val generator = SefariaGenerator(Paths.get(sefariaRootPath), repo, setOf("Hebrew"))
        generator.generate()
        generator.processLinks()

        // Persist the in-memory DB to disk using VACUUM INTO
        val out = Paths.get(targetDbPath).toAbsolutePath()
        val parent = out.parent
        if (parent != null) Files.createDirectories(parent)
        val escaped = out.toString().replace("'", "''")
        repo.executeRawQuery("VACUUM INTO '$escaped'")
        logger.i { "Sefaria in-memory DB persisted to: $out" }
    } finally {
        repo.close()
    }
}
