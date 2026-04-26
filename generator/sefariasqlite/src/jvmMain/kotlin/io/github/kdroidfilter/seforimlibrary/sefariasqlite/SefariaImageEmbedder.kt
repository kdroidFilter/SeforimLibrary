package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.time.Duration
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile
import kotlin.io.path.readBytes

/**
 * Downloads `textimages.sefaria.org` images referenced from book content and
 * keeps an in-memory + on-disk cache of the corresponding `data:image/(mime);base64,...`
 * URIs.
 *
 * Once populated, [cleanSefariaLine] can use [substituteImages] to inline the
 * base64 payloads so the resulting SQLite DB is self-contained (no runtime
 * network dependency on textimages.sefaria.org, whose images render as broken
 * ❌ placeholders in offline use — see issue 392).
 *
 * Workflow:
 *   1. [prefetch] — scan all merged.json paths once, collect unique image URLs,
 *      download anything not yet cached, populate the in-memory map.
 *   2. [substituteImages] — pure function, rewrites `<img src="…">` occurrences
 *      to `<img src="data:…">`; called from [cleanSefariaLine].
 */
object SefariaImageEmbedder {
    private const val URL_PREFIX = "https://textimages.sefaria.org/"
    private const val USER_AGENT = "SeforimLibrary-SefariaImageEmbedder/1.0"
    private const val DOWNLOAD_PARALLELISM = 16
    private const val MAX_IMAGE_BYTES = 5 * 1024 * 1024 // 5 MiB ceiling per image

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(20))
            .build()
    }

    // Regex used both to detect URLs up-front and to rewrite them inline.
    // Captures: (1) the full URL, (2) nothing — we replace the URL inside the
    // matched `<img ...>` tag so surrounding attributes are preserved.
    private val IMG_TAG_REGEX = Regex(
        "<img\\s+[^>]*src=[\"'](https://textimages\\.sefaria\\.org/[^\"']+)[\"'][^>]*/?>",
        RegexOption.IGNORE_CASE
    )

    private val dataUriByUrl = ConcurrentHashMap<String, String>()

    /**
     * Set by the importer at startup. When null, [substituteImages] is a no-op
     * and the original URLs are kept (so tests/debug runs don't require network).
     */
    @Volatile
    var enabled: Boolean = false
        private set

    /**
     * Scan [mergedJsonPaths], extract unique Sefaria image URLs, download any
     * that aren't already in [cacheDir], and populate the in-memory data-URI map.
     * Safe to call multiple times — already-cached entries are reused.
     */
    suspend fun prefetch(
        mergedJsonPaths: Collection<Path>,
        cacheDir: Path = Paths.get("build", "sefaria", "image-cache"),
        logger: Logger = Logger.withTag("SefariaImageEmbedder")
    ) = coroutineScope {
        Files.createDirectories(cacheDir)

        val urls = collectUrls(mergedJsonPaths)
        if (urls.isEmpty()) {
            enabled = true
            return@coroutineScope
        }
        logger.i { "Embedder: ${urls.size} unique Sefaria image URLs to process" }

        val semaphore = Semaphore(DOWNLOAD_PARALLELISM)
        var newlyDownloaded = 0
        var failed = 0

        urls.map { url ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    val cached = ensureCachedBytes(url, cacheDir, logger)
                    if (cached != null) {
                        if (cached.fromNetwork) newlyDownloaded++
                        dataUriByUrl[url] = toDataUri(url, cached.bytes)
                    } else {
                        failed++
                    }
                }
            }
        }.awaitAll()

        enabled = true
        logger.i {
            "Embedder: cached=${dataUriByUrl.size} (downloaded=$newlyDownloaded, failed=$failed)"
        }
    }

    /**
     * Substitute every `<img src="https://textimages.sefaria.org/…">` with a
     * matching `data:` URI, assuming [prefetch] populated the cache.
     * Preserves any other attributes on the tag.
     */
    fun substituteImages(content: String): String {
        if (!enabled || !content.contains(URL_PREFIX)) return content
        return IMG_TAG_REGEX.replace(content) { match ->
            val url = match.groupValues[1]
            val dataUri = dataUriByUrl[url] ?: return@replace match.value
            match.value.replace(url, dataUri)
        }
    }

    /**
     * Drop all in-memory cache state (does not touch disk cache). Mostly for tests.
     */
    internal fun resetForTest() {
        dataUriByUrl.clear()
        enabled = false
    }

    /**
     * Seed the in-memory cache directly (tests only — lets us skip the network).
     */
    internal fun seedForTest(url: String, dataUri: String) {
        dataUriByUrl[url] = dataUri
        enabled = true
    }

    private fun collectUrls(mergedJsonPaths: Collection<Path>): Set<String> {
        val out = HashSet<String>()
        val bytePattern = URL_PREFIX.toByteArray(Charsets.UTF_8)
        val textRegex = Regex("https://textimages\\.sefaria\\.org/[^\"'<>\\s\\\\]+")
        for (p in mergedJsonPaths) {
            val bytes = runCatching { Files.readAllBytes(p) }.getOrNull() ?: continue
            if (indexOfSubSequence(bytes, bytePattern) < 0) continue
            val text = bytes.toString(Charsets.UTF_8)
            for (m in textRegex.findAll(text)) {
                out += stripTrailingJsonArtifacts(m.value)
            }
        }
        return out
    }

    private fun stripTrailingJsonArtifacts(url: String): String {
        // Sefaria JSON sometimes embeds URLs ending with a stray backslash or
        // punctuation from the surrounding content. Keep only characters that
        // are legal in a URL path.
        return url.trimEnd('\\', '\'', '"', ',', '.', ';', ')', ']', '}')
    }

    private fun indexOfSubSequence(haystack: ByteArray, needle: ByteArray): Int {
        if (needle.isEmpty() || haystack.size < needle.size) return -1
        outer@ for (i in 0..(haystack.size - needle.size)) {
            for (j in needle.indices) {
                if (haystack[i + j] != needle[j]) continue@outer
            }
            return i
        }
        return -1
    }

    private data class CachedBytes(val bytes: ByteArray, val fromNetwork: Boolean)

    private fun ensureCachedBytes(url: String, cacheDir: Path, logger: Logger): CachedBytes? {
        val cachePath = cacheDir.resolve(cacheFileName(url))
        if (cachePath.exists() && cachePath.isRegularFile()) {
            return runCatching { CachedBytes(cachePath.readBytes(), fromNetwork = false) }
                .getOrElse {
                    logger.w(it) { "Corrupted cached image at $cachePath; redownloading" }
                    runCatching { Files.delete(cachePath) }
                    null
                }
                ?: return ensureCachedBytes(url, cacheDir, logger)
        }

        val bytes = runCatching { downloadBytes(url) }.getOrElse {
            logger.w(it) { "Failed to download image: $url" }
            return null
        }
        if (bytes.size > MAX_IMAGE_BYTES) {
            logger.w { "Skipping oversized image (${bytes.size} bytes): $url" }
            return null
        }
        val tmp = cachePath.resolveSibling(cachePath.fileName.toString() + ".part")
        Files.createDirectories(cachePath.parent)
        Files.write(tmp, bytes)
        Files.move(tmp, cachePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        return CachedBytes(bytes, fromNetwork = true)
    }

    private fun downloadBytes(url: String): ByteArray {
        val request = HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .header("User-Agent", USER_AGENT)
            .header("Accept", "image/*")
            .build()
        val response = client.send(request, HttpResponse.BodyHandlers.ofByteArray())
        if (response.statusCode() !in 200..299) {
            throw IllegalStateException("HTTP ${response.statusCode()} for $url")
        }
        return response.body()
    }

    private fun cacheFileName(url: String): String {
        val keyPart = url.removePrefix(URL_PREFIX)
        return keyPart.replace('/', '_').replace(':', '_').replace('?', '_')
    }

    private fun toDataUri(url: String, bytes: ByteArray): String {
        val mime = when {
            url.endsWith(".png", ignoreCase = true) -> "image/png"
            url.endsWith(".jpg", ignoreCase = true) || url.endsWith(".jpeg", ignoreCase = true) -> "image/jpeg"
            url.endsWith(".gif", ignoreCase = true) -> "image/gif"
            url.endsWith(".svg", ignoreCase = true) -> "image/svg+xml"
            url.endsWith(".webp", ignoreCase = true) -> "image/webp"
            else -> "image/png"
        }
        val b64 = Base64.getEncoder().encodeToString(bytes)
        return "data:$mime;base64,$b64"
    }
}
