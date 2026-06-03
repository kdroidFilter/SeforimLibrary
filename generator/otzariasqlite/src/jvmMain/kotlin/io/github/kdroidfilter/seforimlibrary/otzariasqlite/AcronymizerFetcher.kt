package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

object AcronymizerFetcher {
    private const val LATEST_API = "https://api.github.com/repos/kdroidFilter/SeforimAcronymizer/releases/latest"
    private const val USER_AGENT = "SeforimLibrary-AcronymizerFetcher/1.0"

    /** Ensure acronymizer DB is available locally under build/acronymizer/acronymizer.db (relative to CWD). */
    fun ensureLocalDb(logger: Logger): Path {
        val destRoot = Paths.get("build", "acronymizer")
        val dbPath = destRoot.resolve("acronymizer.db")
        if (Files.exists(dbPath) && Files.isRegularFile(dbPath) && Files.size(dbPath) > 0) {
            logger.i { "Using existing acronymizer DB at ${dbPath.toAbsolutePath()}" }
            return dbPath
        }
        Files.createDirectories(destRoot)
        downloadLatestDb(dbPath, logger)
        return dbPath
    }

    private fun downloadLatestDb(outDb: Path, logger: Logger) {
        // Fetch release info from GitHub API
        val body = OptimizedHttpClient.fetchJson(LATEST_API, USER_AGENT, logger)

        // Find a .db asset browser_download_url
        val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+\.db)"""")
        val dbUrl = regex.findAll(body).map { it.groupValues[1] }.firstOrNull()
            ?: throw IllegalStateException("No .db asset found in latest SeforimAcronymizer release")
        logger.i { "Downloading acronymizer DB from $dbUrl" }

        // Download the DB with optimized client
        OptimizedHttpClient.downloadFile(
            url = dbUrl,
            destination = outDb,
            userAgent = USER_AGENT,
            logger = logger,
            progressPrefix = "Downloading acronymizer DB"
        )
    }
}
