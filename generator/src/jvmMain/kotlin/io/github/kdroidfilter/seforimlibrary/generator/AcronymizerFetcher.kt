package io.github.kdroidfilter.seforimlibrary.generator

import co.touchlab.kermit.Logger
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

object AcronymizerFetcher {
    private const val LATEST_API = "https://api.github.com/repos/kdroidFilter/SeforimAcronymizer/releases/latest"

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
        val client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()
        val token = System.getenv("GITHUB_TOKEN") ?: System.getenv("GH_TOKEN")
        val req = HttpRequest.newBuilder(URI(LATEST_API))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "SeforimLibrary-AcronymizerFetcher/1.0")
            .apply { if (!token.isNullOrBlank()) header("Authorization", "Bearer $token") }
            .build()
        val res = client.send(req, HttpResponse.BodyHandlers.ofString())
        if (res.statusCode() !in 200..299) {
            throw IllegalStateException("GitHub API error: HTTP ${res.statusCode()}\n${res.body()}")
        }
        val body = res.body()
        // Find a .db asset browser_download_url
        val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+\.db)"""")
        val dbUrl = regex.findAll(body).map { it.groupValues[1] }.firstOrNull()
            ?: throw IllegalStateException("No .db asset found in latest SeforimAcronymizer release")
        logger.i { "Downloading acronymizer DB from $dbUrl" }

        val dbReq = HttpRequest.newBuilder(URI(dbUrl))
            .header("Accept", "application/octet-stream")
            .header("User-Agent", "SeforimLibrary-AcronymizerFetcher/1.0")
            .apply { if (!token.isNullOrBlank()) header("Authorization", "Bearer $token") }
            .build()
        val dbRes = client.send(dbReq, HttpResponse.BodyHandlers.ofInputStream())
        if (dbRes.statusCode() !in 200..299) {
            throw IllegalStateException("Failed to download acronymizer DB: HTTP ${dbRes.statusCode()}")
        }
        Files.createDirectories(outDb.parent)
        dbRes.body().use { input ->
            Files.newOutputStream(outDb).use { output ->
                input.copyTo(output, 1 shl 20) // 1 MiB buffer
            }
        }
        logger.i { "Saved acronymizer DB to ${outDb.toAbsolutePath()}" }
    }
}

