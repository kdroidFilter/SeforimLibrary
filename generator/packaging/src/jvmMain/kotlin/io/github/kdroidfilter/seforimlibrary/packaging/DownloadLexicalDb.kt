package io.github.kdroidfilter.seforimlibrary.packaging

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private const val LATEST_API = "https://api.github.com/repos/kdroidFilter/SeforimMagicIndexer/releases/latest"
private const val USER_AGENT = "SeforimLibrary-DownloadLexicalDb/1.0"

/**
 * Download the latest `lexical.db` from SeforimMagicIndexer GitHub releases and place it next to `seforim.db`.
 *
 * Usage:
 *   ./gradlew :packaging:downloadLexicalDb
 *   ./gradlew :packaging:downloadLexicalDb -PseforimDb=/path/to/seforim.db
 *
 * Output:
 *   Writes `lexical.db` next to the DB file (same directory as `seforim.db`).
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DownloadLexicalDb")

    val dbPath = resolveDbPath(args)
    val outDb = dbPath.resolveSibling("lexical.db")
    if (Files.exists(outDb) && Files.isRegularFile(outDb) && Files.size(outDb) > 0) {
        logger.i { "Using existing lexical.db at ${outDb.toAbsolutePath()}" }
        println(outDb.toAbsolutePath().toString())
        return
    }

    Files.createDirectories(outDb.parent)
    downloadLatestLexicalDb(outDb, logger)
    println(outDb.toAbsolutePath().toString())
}

private fun resolveDbPath(args: Array<String>): Path {
    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    return Paths.get(dbPathStr)
}

private fun downloadLatestLexicalDb(outDb: Path, logger: Logger) {
    // Fetch release info from GitHub API
    val body = OptimizedHttpClient.fetchJson(LATEST_API, USER_AGENT, logger)

    val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+/lexical\.db)"""")
    val dbUrl = regex.findAll(body).map { it.groupValues[1] }.firstOrNull()
        ?: throw IllegalStateException("No lexical.db asset found in latest SeforimMagicIndexer release")

    logger.i { "Downloading lexical.db from $dbUrl" }

    val tmp = outDb.resolveSibling(outDb.fileName.toString() + ".part")
    Files.deleteIfExists(tmp)

    // Download with optimized client
    OptimizedHttpClient.downloadFile(
        url = dbUrl,
        destination = tmp,
        userAgent = USER_AGENT,
        logger = logger,
        progressPrefix = "Downloading lexical.db"
    )

    // Atomic-ish replace
    if (Files.exists(outDb)) {
        val backup = outDb.resolveSibling(outDb.fileName.toString() + ".bak")
        Files.deleteIfExists(backup)
        Files.move(outDb, backup)
        logger.i { "Existing lexical.db moved to ${backup.toAbsolutePath()}" }
    }
    Files.move(tmp, outDb)
}

