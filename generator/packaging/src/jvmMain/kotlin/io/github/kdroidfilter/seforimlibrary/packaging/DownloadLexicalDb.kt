package io.github.kdroidfilter.seforimlibrary.packaging

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private const val LATEST_API = "https://api.github.com/repos/kdroidFilter/SeforimMagicIndexer/releases/latest"

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
    val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build()
    val token = System.getenv("GITHUB_TOKEN") ?: System.getenv("GH_TOKEN")

    val req = HttpRequest.newBuilder(URI(LATEST_API))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "SeforimLibrary-DownloadLexicalDb/1.0")
        .apply { if (!token.isNullOrBlank()) header("Authorization", "Bearer $token") }
        .build()
    val res = client.send(req, HttpResponse.BodyHandlers.ofString())
    if (res.statusCode() !in 200..299) {
        throw IllegalStateException("GitHub API error: HTTP ${res.statusCode()}\n${res.body()}")
    }

    val body = res.body()
    val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+/lexical\.db)"""")
    val dbUrl = regex.findAll(body).map { it.groupValues[1] }.firstOrNull()
        ?: throw IllegalStateException("No lexical.db asset found in latest SeforimMagicIndexer release")

    logger.i { "Downloading lexical.db from $dbUrl" }
    val dbReq = HttpRequest.newBuilder(URI(dbUrl))
        .header("Accept", "application/octet-stream")
        .header("User-Agent", "SeforimLibrary-DownloadLexicalDb/1.0")
        .apply { if (!token.isNullOrBlank()) header("Authorization", "Bearer $token") }
        .build()
    val dbRes = client.send(dbReq, HttpResponse.BodyHandlers.ofInputStream())
    if (dbRes.statusCode() !in 200..299) {
        throw IllegalStateException("Failed to download lexical.db: HTTP ${dbRes.statusCode()}")
    }

    val tmp = outDb.resolveSibling(outDb.fileName.toString() + ".part")
    Files.deleteIfExists(tmp)

    dbRes.body().use { input ->
        Files.newOutputStream(tmp).use { output ->
            input.copyTo(output, 1 shl 20)
        }
    }

    // Atomic-ish replace
    if (Files.exists(outDb)) {
        val backup = outDb.resolveSibling(outDb.fileName.toString() + ".bak")
        Files.deleteIfExists(backup)
        Files.move(outDb, backup)
        logger.i { "Existing lexical.db moved to ${backup.toAbsolutePath()}" }
    }
    Files.move(tmp, outDb)
    logger.i { "Saved lexical.db to ${outDb.toAbsolutePath()}" }
}

