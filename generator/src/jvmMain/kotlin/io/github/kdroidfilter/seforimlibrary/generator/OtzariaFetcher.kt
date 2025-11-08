package io.github.kdroidfilter.seforimlibrary.generator

import co.touchlab.kermit.Logger
import java.io.BufferedInputStream
import java.io.FileOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Comparator
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

object OtzariaFetcher {
    private const val LATEST_API = "https://api.github.com/repos/Y-PLONI/otzaria-library/releases/latest"

    /** Ensure otzaria source is available locally under build/otzaria/source (relative to CWD). */
    fun ensureLocalSource(logger: Logger): Path {
        val destRoot = Paths.get("build", "otzaria", "source")
        if (Files.isDirectory(destRoot) && Files.list(destRoot).use { it.findAny().isPresent }) {
            val root = findSourceRoot(destRoot)
            removeUnwantedFolder(root, logger)
            logger.i { "Using existing otzaria source at ${root.toAbsolutePath()}" }
            return root
        }
        Files.createDirectories(destRoot)
        val zipPath = destRoot.parent.resolve("otzaria.zip")
        downloadLatestZip(zipPath, logger)
        extractZip(zipPath, destRoot, logger)
        val root = findSourceRoot(destRoot)
        removeUnwantedFolder(root, logger)
        return root
    }

    private fun findSourceRoot(extractRoot: Path): Path {
        // If this folder already looks like the source (contains metadata.json and אוצריא), return it
        val meta = extractRoot.resolve("metadata.json")
        val libDir = extractRoot.resolve("אוצריא")
        if (Files.exists(meta) && Files.isRegularFile(meta) && Files.isDirectory(libDir)) return extractRoot

        // Otherwise, check first-level subdirectories for the expected layout
        Files.newDirectoryStream(extractRoot).use { ds ->
            for (p in ds) {
                if (Files.isDirectory(p)) {
                    val m = p.resolve("metadata.json")
                    val l = p.resolve("אוצריא")
                    if (Files.exists(m) && Files.isRegularFile(m) && Files.isDirectory(l)) return p
                }
            }
        }
        return extractRoot
    }

    private fun downloadLatestZip(outZip: Path, logger: Logger) {
        val client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()
        val token = System.getenv("GITHUB_TOKEN") ?: System.getenv("GH_TOKEN")
        val req = HttpRequest.newBuilder(URI(LATEST_API))
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "SeforimLibrary-OtzariaFetcher/1.0")
            .apply {
                if (!token.isNullOrBlank()) header("Authorization", "Bearer $token")
            }
            .build()
        val res = client.send(req, HttpResponse.BodyHandlers.ofString())
        if (res.statusCode() !in 200..299) {
            throw IllegalStateException("GitHub API error: HTTP ${res.statusCode()}\n${res.body()}")
        }
        val body = res.body()
        val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+\.zip)"""")
        val zipUrl = regex.findAll(body).map { it.groupValues[1] }.firstOrNull()
            ?: throw IllegalStateException("No .zip asset found in latest otzaria-library release")
        logger.i { "Downloading otzaria from $zipUrl" }

        val zipReq = HttpRequest.newBuilder(URI(zipUrl))
            .header("Accept", "application/octet-stream")
            .header("User-Agent", "SeforimLibrary-OtzariaFetcher/1.0")
            .apply {
                if (!token.isNullOrBlank()) header("Authorization", "Bearer $token")
            }
            .build()
        val zipRes = client.send(zipReq, HttpResponse.BodyHandlers.ofInputStream())
        if (zipRes.statusCode() !in 200..299) {
            throw IllegalStateException("Failed to download otzaria zip: HTTP ${zipRes.statusCode()}")
        }
        Files.createDirectories(outZip.parent)
        // Progress-aware copy with percentage and speed
        val contentLength = zipRes.headers().firstValue("Content-Length").orElse(null)?.toLongOrNull() ?: -1L
        if (contentLength > 0) {
            val totalMb = contentLength.toDouble() / (1024 * 1024)
            logger.i { "Download size: ${"%.1f".format(totalMb)} MB" }
        }

        zipRes.body().use { input ->
            Files.newOutputStream(outZip).use { output ->
                val buffer = ByteArray(1 shl 20) // 1 MiB
                var downloaded = 0L
                var lastLoggedBytes = 0L
                var lastLogTime = System.nanoTime()
                var nextPctLog = 0
                val startTime = lastLogTime

                while (true) {
                    val read = input.read(buffer)
                    if (read <= 0) break
                    output.write(buffer, 0, read)
                    downloaded += read

                    val now = System.nanoTime()
                    val elapsedSince = (now - lastLogTime) / 1_000_000_000.0
                    val overallSec = (now - startTime) / 1_000_000_000.0

                    val pct = if (contentLength > 0) ((downloaded * 100.0) / contentLength).toInt().coerceIn(0, 100) else -1
                    val shouldLogPct = contentLength > 0 && pct >= nextPctLog
                    val shouldLogTime = elapsedSince >= 1.0
                    if (shouldLogPct || shouldLogTime) {
                        val deltaBytes = downloaded - lastLoggedBytes
                        val speed = if (elapsedSince > 0) deltaBytes / elapsedSince else 0.0 // bytes/sec
                        val speedMb = speed / (1024 * 1024)
                        val downloadedMb = downloaded.toDouble() / (1024 * 1024)
                        val totalMb = if (contentLength > 0) contentLength.toDouble() / (1024 * 1024) else -1.0

                        val msg = buildString {
                            append("Downloading otzaria: ")
                            if (pct >= 0) append("$pct% ")
                            append("@ ${"%.2f".format(speedMb)} MB/s ")
                            append("(${"%.1f".format(downloadedMb)}")
                            if (totalMb > 0) append("/${"%.1f".format(totalMb)}")
                            append(" MB)")
                        }
                        logger.i { msg }
                        lastLoggedBytes = downloaded
                        lastLogTime = now
                        if (contentLength > 0) nextPctLog = ((pct / 5) + 1) * 5 // log every 5%
                    }
                }
            }
        }
        logger.i { "Saved otzaria zip to ${outZip.toAbsolutePath()}" }
    }

    private fun extractZip(zipFile: Path, destinationDir: Path, logger: Logger) {
        logger.i { "Extracting otzaria to ${destinationDir.toAbsolutePath()}" }
        ZipInputStream(BufferedInputStream(Files.newInputStream(zipFile))).use { zis ->
            var entry: ZipEntry? = zis.nextEntry
            while (entry != null) {
                val newPath = destinationDir.resolve(entry.name).normalize()
                if (entry.isDirectory) {
                    Files.createDirectories(newPath)
                } else {
                    Files.createDirectories(newPath.parent)
                    FileOutputStream(newPath.toFile()).use { fos ->
                        // Increase buffer to speed up extraction of large entries
                        zis.copyTo(fos, 1 shl 20) // 1 MiB buffer
                    }
                }
                zis.closeEntry()
                entry = zis.nextEntry
            }
        }
        logger.i { "Extraction complete." }
    }

    private fun removeUnwantedFolder(root: Path, logger: Logger) {
        val base = root.resolve("אוצריא")
        val namesToRemove = loadOtzariaFoldersToRemove(logger)
        for (name in namesToRemove) {
            val trimmed = name.trim()
            if (trimmed.isEmpty()) continue
            val dir = base.resolve(trimmed)
            if (Files.exists(dir)) {
                runCatching {
                    Files.walk(dir)
                        .sorted(Comparator.reverseOrder())
                        .forEach { Files.deleteIfExists(it) }
                    logger.i { "Removed unwanted folder: ${dir.toAbsolutePath()}" }
                }.onFailure { e ->
                    logger.w(e) { "Failed removing unwanted folder at ${dir.toAbsolutePath()}" }
                }
            }
        }
    }

    private fun loadOtzariaFoldersToRemove(logger: Logger): List<String> {
        // Use only the resource file (no hardcoded fallback)
        return try {
            val resourceNames = listOf("otzaria-folder-to-remove.txt", "/otzaria-folder-to-remove.txt")
            val cl = Thread.currentThread().contextClassLoader
            val stream = resourceNames.asSequence()
                .mapNotNull { name -> cl?.getResourceAsStream(name) ?: this::class.java.getResourceAsStream(name) }
                .firstOrNull()
            if (stream == null) {
                logger.i { "No otzaria-folder-to-remove.txt resource found; no folders will be removed" }
                return emptyList()
            }
            stream.bufferedReader(Charsets.UTF_8).use { br ->
                br.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .toList()
            }
        } catch (e: Exception) {
            logger.w(e) { "Failed to load otzaria-folder-to-remove.txt; no folders will be removed" }
            emptyList()
        }
    }
}
