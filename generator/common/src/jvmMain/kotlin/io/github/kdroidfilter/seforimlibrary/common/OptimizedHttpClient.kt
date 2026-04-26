package io.github.kdroidfilter.seforimlibrary.common

import co.touchlab.kermit.Logger
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.Executors

/**
 * Optimized HTTP client for fast downloads.
 *
 * Key optimizations:
 * - HTTP/1.1 for large file downloads (avoids HTTP/2 multiplexing overhead)
 * - Large buffers (8 MiB) for high throughput
 * - Dedicated thread pool for async I/O
 * - Automatic retry with exponential backoff
 */
object OptimizedHttpClient {

    private const val BUFFER_SIZE = 8 shl 20 // 8 MiB - larger buffer for better throughput

    // HTTP/1.1 client for large downloads - avoids HTTP/2 overhead
    private val downloadClient: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(30))
            .executor(Executors.newCachedThreadPool())
            .build()
    }

    // HTTP/2 client for API calls (smaller requests benefit from multiplexing)
    private val apiClient: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(30))
            .build()
    }

    private val token: String? by lazy {
        System.getenv("GITHUB_TOKEN") ?: System.getenv("GH_TOKEN")
    }

    /**
     * Fetch JSON from a URL (e.g., GitHub API).
     */
    fun fetchJson(url: String, userAgent: String, logger: Logger): String {
        return executeWithRetry(logger, "fetch $url") {
            val request = HttpRequest.newBuilder(URI(url))
                .timeout(Duration.ofMinutes(2))
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", userAgent)
                .apply { token?.let { header("Authorization", "Bearer $it") } }
                .build()

            val response = apiClient.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() !in 200..299) {
                throw HttpException(response.statusCode(), response.body())
            }
            response.body()
        }
    }

    /**
     * Download a file with progress reporting.
     *
     * @param url The URL to download from
     * @param destination The local file path to save to
     * @param userAgent User-Agent header value
     * @param logger Logger for progress output
     * @param progressPrefix Prefix for progress messages (e.g., "Downloading Otzaria")
     */
    fun downloadFile(
        url: String,
        destination: Path,
        userAgent: String,
        logger: Logger,
        progressPrefix: String = "Downloading"
    ) {
        executeWithRetry(logger, "download $url") {
            val request = HttpRequest.newBuilder(URI(url))
                .timeout(Duration.ofMinutes(30)) // Long timeout for large files
                .header("Accept", "application/octet-stream")
                .header("User-Agent", userAgent)
                .header("Accept-Encoding", "identity") // Don't compress already-compressed files
                .apply { token?.let { header("Authorization", "Bearer $it") } }
                .build()

            // Use HTTP/1.1 client for large downloads
            val response = downloadClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
            if (response.statusCode() !in 200..299) {
                throw HttpException(response.statusCode(), "Failed to download")
            }

            Files.createDirectories(destination.parent)
            val contentLength = response.headers()
                .firstValue("Content-Length")
                .orElse(null)
                ?.toLongOrNull() ?: -1L

            if (contentLength > 0) {
                logger.i { "$progressPrefix: ${"%.1f".format(contentLength / 1_048_576.0)} MB" }
            }

            // Use buffered streams with large buffers for maximum throughput
            BufferedInputStream(response.body(), BUFFER_SIZE).use { input ->
                BufferedOutputStream(Files.newOutputStream(destination), BUFFER_SIZE).use { output ->
                    copyWithProgress(input, output, contentLength, logger, progressPrefix)
                }
            }
        }
    }

    /**
     * Download and return an InputStream for streaming extraction.
     * The caller is responsible for closing the stream.
     */
    fun downloadStream(
        url: String,
        userAgent: String,
        logger: Logger
    ): StreamDownload {
        return executeWithRetry(logger, "download stream $url") {
            val request = HttpRequest.newBuilder(URI(url))
                .timeout(Duration.ofMinutes(30))
                .header("Accept", "application/octet-stream")
                .header("User-Agent", userAgent)
                .header("Accept-Encoding", "identity")
                .apply { token?.let { header("Authorization", "Bearer $it") } }
                .build()

            // Use HTTP/1.1 client for large downloads
            val response = downloadClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
            if (response.statusCode() !in 200..299) {
                throw HttpException(response.statusCode(), "Failed to download")
            }

            val contentLength = response.headers()
                .firstValue("Content-Length")
                .orElse(null)
                ?.toLongOrNull() ?: -1L

            StreamDownload(BufferedInputStream(response.body(), BUFFER_SIZE), contentLength)
        }
    }

    private fun copyWithProgress(
        input: InputStream,
        output: java.io.OutputStream,
        totalBytes: Long,
        logger: Logger,
        prefix: String
    ) {
        val buffer = ByteArray(BUFFER_SIZE)
        var downloaded = 0L
        var lastLogTime = System.nanoTime()
        var lastLoggedBytes = 0L
        var nextPctLog = 5
        val startTime = lastLogTime

        while (true) {
            val read = input.read(buffer)
            if (read <= 0) break
            output.write(buffer, 0, read)
            downloaded += read

            val now = System.nanoTime()
            val elapsedSinceLog = (now - lastLogTime) / 1_000_000_000.0
            val pct = if (totalBytes > 0) ((downloaded * 100.0) / totalBytes).toInt().coerceIn(0, 100) else -1
            val shouldLogPct = totalBytes > 0 && pct >= nextPctLog
            val shouldLogTime = elapsedSinceLog >= 1.0

            if (shouldLogPct || shouldLogTime) {
                val deltaBytes = downloaded - lastLoggedBytes
                val speed = if (elapsedSinceLog > 0) deltaBytes / elapsedSinceLog else 0.0
                val speedMb = speed / 1_048_576.0
                val downloadedMb = downloaded / 1_048_576.0
                val totalMb = if (totalBytes > 0) totalBytes / 1_048_576.0 else -1.0

                val msg = buildString {
                    append(prefix)
                    append(": ")
                    if (pct >= 0) append("$pct% ")
                    append("@ ${"%.2f".format(speedMb)} MB/s ")
                    append("(${"%.1f".format(downloadedMb)}")
                    if (totalMb > 0) append("/${"%.1f".format(totalMb)}")
                    append(" MB)")
                }
                logger.i { msg }
                lastLoggedBytes = downloaded
                lastLogTime = now
                if (totalBytes > 0) nextPctLog = ((pct / 5) + 1) * 5
            }
        }

        // Final log
        val totalTime = (System.nanoTime() - startTime) / 1_000_000_000.0
        val avgSpeed = if (totalTime > 0) (downloaded / totalTime) / 1_048_576.0 else 0.0
        logger.i { "$prefix complete: ${"%.1f".format(downloaded / 1_048_576.0)} MB in ${"%.1f".format(totalTime)}s (avg ${"%.2f".format(avgSpeed)} MB/s)" }
    }

    private fun <T> executeWithRetry(
        logger: Logger,
        operation: String,
        maxRetries: Int = 3,
        initialDelayMs: Long = 1000,
        block: () -> T
    ): T {
        var lastException: Exception? = null
        var delay = initialDelayMs

        repeat(maxRetries) { attempt ->
            try {
                return block()
            } catch (e: Exception) {
                lastException = e
                val isRetryable = e is java.net.SocketTimeoutException ||
                    e is java.net.ConnectException ||
                    e is java.io.IOException ||
                    (e is HttpException && e.statusCode in listOf(429, 500, 502, 503, 504))

                if (!isRetryable || attempt == maxRetries - 1) {
                    throw e
                }

                logger.w { "Retry ${attempt + 1}/$maxRetries for $operation after ${delay}ms: ${e.message}" }
                Thread.sleep(delay)
                delay *= 2 // Exponential backoff
            }
        }

        throw lastException ?: IllegalStateException("Retry failed without exception")
    }

    class HttpException(val statusCode: Int, message: String) : Exception("HTTP $statusCode: $message")

    data class StreamDownload(val stream: InputStream, val contentLength: Long)
}
