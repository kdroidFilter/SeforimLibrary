package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Paths

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

    val dbPath = System.getProperty("seforimDb")
        ?: (if (args.isNotEmpty()) args[0] else "generator/build/seforim.db")
    val sefariaRootPath = System.getProperty("sefariaRoot")
        ?: (if (args.size >= 2) args[1] else "generator/build/Sefaria")

    logger.i { "DB path: $dbPath" }
    logger.i { "Sefaria root: $sefariaRootPath" }

    val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    val repo = SeforimRepository(dbPath, driver)

    try {
        val generator = SefariaGenerator(Paths.get(sefariaRootPath), repo, setOf("Hebrew"))
        generator.generate()
        generator.processLinks()
        logger.i { "Sefaria generation done" }
    } finally {
        repo.close()
    }
}

