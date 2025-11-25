package io.github.kdroidfilter.seforimlibrary.sefaria

import co.touchlab.kermit.Logger
import com.github.luben.zstd.ZstdInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import java.io.BufferedInputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.DEFAULT_BUFFER_SIZE
import kotlin.io.path.exists
import kotlin.io.path.isDirectory

/**
 * Downloads the latest Sefaria export (.tar.zst) and extracts it locally.
 * The archive is fetched from the latest GitHub release of kdroidFilter/SefariaExport.
 */
object SefariaFetcher {
    private const val LATEST_API = "https://api.github.com/repos/kdroidFilter/SefariaExport/releases/latest"

    /**
    * Ensure a Sefaria export is available locally under build/sefaria/export (relative to CWD).
    * If the directory already exists and contains data, it is reused.
    *
    * @return Path to the directory that contains the `database_export` folder.
    */
    fun ensureLocalExport(logger: Logger): Path {
        val destRoot = Paths.get("build", "sefaria", "export")
        if (destRoot.isDirectory() && Files.list(destRoot).use { it.findAny().isPresent }) {
            val root = findExportRoot(destRoot)
            logger.i { "Using existing Sefaria export at ${root.toAbsolutePath()}" }
            return root
        }
        Files.createDirectories(destRoot)
        val archivePath = destRoot.parent.resolve("sefaria-export.tar.zst")
        downloadLatestArchive(archivePath, logger)
        extractTarZst(archivePath, destRoot, logger)
        return findExportRoot(destRoot)
    }

    private fun downloadLatestArchive(out: Path, logger: Logger) {
        val client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()
        val token = System.getenv("GITHUB_TOKEN") ?: System.getenv("GH_TOKEN")
        val req = HttpRequest.newBuilder(URI(LATEST_API))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "SeforimLibrary-SefariaFetcher/1.0")
            .apply { if (!token.isNullOrBlank()) header("Authorization", "Bearer $token") }
            .build()
        val res = client.send(req, HttpResponse.BodyHandlers.ofString())
        if (res.statusCode() !in 200..299) {
            throw IllegalStateException("GitHub API error: HTTP ${res.statusCode()}\n${res.body()}")
        }
        val body = res.body()
        val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+\.tar\.zst)"""")
        val archiveUrl = regex.findAll(body).map { it.groupValues[1] }.firstOrNull()
            ?: throw IllegalStateException("No .tar.zst asset found in latest SefariaExport release")
        logger.i { "Downloading Sefaria export from $archiveUrl" }

        val dlReq = HttpRequest.newBuilder(URI(archiveUrl))
            .header("Accept", "application/octet-stream")
            .header("User-Agent", "SeforimLibrary-SefariaFetcher/1.0")
            .apply { if (!token.isNullOrBlank()) header("Authorization", "Bearer $token") }
            .build()
        val dlRes = client.send(dlReq, HttpResponse.BodyHandlers.ofInputStream())
        if (dlRes.statusCode() !in 200..299) {
            throw IllegalStateException("Failed to download Sefaria export: HTTP ${dlRes.statusCode()}")
        }
        val contentLength = dlRes.headers().firstValue("Content-Length").orElse(null)?.toLongOrNull() ?: -1L
        if (contentLength > 0) {
            val totalMb = contentLength.toDouble() / (1024 * 1024)
            logger.i { "Download size: ${"%.1f".format(totalMb)} MB" }
        }
        Files.createDirectories(out.parent)
        dlRes.body().use { input ->
            Files.newOutputStream(out).use { output ->
                val buffer = ByteArray(1 shl 20) // 1 MiB
                var downloaded = 0L
                var lastLog = System.nanoTime()
                var nextPct = 0
                val start = lastLog
                while (true) {
                    val read = input.read(buffer)
                    if (read <= 0) break
                    output.write(buffer, 0, read)
                    downloaded += read
                    val now = System.nanoTime()
                    val elapsed = (now - lastLog) / 1_000_000_000.0
                    val pct = if (contentLength > 0) ((downloaded * 100.0) / contentLength).toInt().coerceIn(0, 100) else -1
                    val shouldLog = (contentLength > 0 && pct >= nextPct) || elapsed >= 1.0
                    if (shouldLog) {
                        val totalElapsed = (now - start) / 1_000_000_000.0
                        val speedMb = if (totalElapsed > 0) (downloaded / totalElapsed) / (1024 * 1024) else 0.0
                        val downloadedMb = downloaded.toDouble() / (1024 * 1024)
                        val totalMb = if (contentLength > 0) contentLength.toDouble() / (1024 * 1024) else -1.0
                        val msg = buildString {
                            append("Downloading Sefaria export: ")
                            if (pct >= 0) append("$pct% ")
                            append("@ ${"%.2f".format(speedMb)} MB/s ")
                            append("(${"%.1f".format(downloadedMb)}")
                            if (totalMb > 0) append("/${"%.1f".format(totalMb)}")
                            append(" MB)")
                        }
                        logger.i { msg }
                        lastLog = now
                        if (pct >= 0) nextPct = ((pct / 5) + 1) * 5
                    }
                }
            }
        }
        logger.i { "Saved Sefaria export to ${out.toAbsolutePath()}" }
    }

    private fun extractTarZst(archive: Path, destinationDir: Path, logger: Logger) {
        logger.i { "Extracting ${archive.fileName} to ${destinationDir.toAbsolutePath()}" }
        Files.createDirectories(destinationDir)
        BufferedInputStream(Files.newInputStream(archive)).use { fileStream ->
            ZstdInputStream(fileStream).use { zstd ->
                TarArchiveInputStream(zstd).use { tar ->
                    var entry = tar.nextTarEntry
                    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                    while (entry != null) {
                        val newPath = destinationDir.resolve(entry.name).normalize()
                        if (!newPath.startsWith(destinationDir)) {
                            throw IllegalStateException("Blocked suspicious path while extracting: ${entry.name}")
                        }
                        if (entry.isDirectory) {
                            Files.createDirectories(newPath)
                        } else {
                            Files.createDirectories(newPath.parent)
                            Files.newOutputStream(newPath).use { out ->
                                var n = tar.read(buffer)
                                while (n > 0) {
                                    out.write(buffer, 0, n)
                                    n = tar.read(buffer)
                                }
                            }
                        }
                        entry = tar.nextTarEntry
                    }
                }
            }
        }
        logger.i { "Extraction complete." }
    }

    private fun findExportRoot(extractRoot: Path): Path {
        val direct = extractRoot.resolve("database_export")
        if (direct.exists()) return extractRoot
        Files.newDirectoryStream(extractRoot).use { ds ->
            for (p in ds) {
                if (p.isDirectory()) {
                    val candidate = p.resolve("database_export")
                    if (candidate.exists()) return p
                }
            }
        }
        return extractRoot
    }
}
