package io.github.kdroidfilter.seforimlibrary.deltaupdater

import co.touchlab.kermit.Logger
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

/**
 * Resumable downloader for patch artefacts.
 *
 *  - Each file is downloaded next to its target as `<file>.part`. If
 *    `<file>` already exists with the right sha256, it's reused.
 *  - The HTTP request includes a `Range: bytes=N-` header when resuming.
 *  - The transferred bytes are sha256'd on the fly; a mismatch aborts and
 *    deletes the partial file.
 *
 * No external HTTP client dep (kept on stdlib so the client module stays
 * small). See `DELTA_UPDATE_PLAN.md` §7.2.
 */
class DeltaDownloader(
    private val logger: Logger = Logger.withTag("DeltaDownloader"),
    private val connectTimeoutMs: Int = DEFAULT_CONNECT_TIMEOUT_MS,
    private val readTimeoutMs: Int = DEFAULT_READ_TIMEOUT_MS,
) {

    fun download(url: String, dest: Path, expectedSha256: String, expectedSize: Long?): Path {
        Files.createDirectories(dest.toAbsolutePath().parent)
        if (Files.exists(dest) && sha256(dest) == expectedSha256.lowercase()) {
            logger.i { "Skipping ${dest.fileName} — sha256 already matches" }
            return dest
        }
        val partial = dest.resolveSibling("${dest.fileName}.part")
        var existing = if (Files.exists(partial)) Files.size(partial) else 0L
        val conn = URI(url).toURL().openConnection() as HttpURLConnection
        // Default JVM timeouts are 0 = "infinite" — a hung proxy or stalled
        // peer would block forever. The read timeout applies per read(), so
        // 60 s is the inter-packet stall budget, not the whole-transfer cap.
        conn.connectTimeout = connectTimeoutMs
        conn.readTimeout = readTimeoutMs
        if (existing > 0) {
            conn.setRequestProperty("Range", "bytes=$existing-")
            logger.i { "Resuming ${dest.fileName} from byte $existing" }
        }
        conn.connect()
        // If we requested a Range and the server replied 200 instead of 206,
        // it ignored the header and is streaming the whole file from byte 0.
        // Appending those bytes onto our prefix would corrupt the result —
        // truncate the partial and restart cleanly.
        if (existing > 0 && conn.responseCode == HttpURLConnection.HTTP_OK) {
            logger.w { "Server ignored Range header on ${dest.fileName}; restarting from byte 0" }
            Files.deleteIfExists(partial)
            existing = 0L
        }
        val total = expectedSize ?: (conn.contentLengthLong + existing)
        val md = MessageDigest.getInstance("SHA-256")
        if (existing > 0) {
            // We need to feed the already-downloaded bytes into the digest.
            Files.newInputStream(partial).use { it.copyTo(SinkOutputStream { b, o, l -> md.update(b, o, l) }) }
        }
        Files.newOutputStream(partial, *appendFlags(existing > 0)).use { out ->
            conn.getInputStream().use { input ->
                val buf = ByteArray(BUFFER_SIZE)
                var read: Int
                var written = existing
                while (input.read(buf).also { read = it } > 0) {
                    out.write(buf, 0, read)
                    md.update(buf, 0, read)
                    written += read
                    if (total > 0 && written % (1L shl 23) < read) {
                        logger.d { "${dest.fileName}: $written / $total bytes" }
                    }
                }
            }
        }
        val actual = md.digest().joinToString("") { "%02x".format(it) }
        if (actual.lowercase() != expectedSha256.lowercase()) {
            Files.deleteIfExists(partial)
            throw IOException(
                "Sha256 mismatch on $url: expected $expectedSha256, got $actual",
            )
        }
        Files.move(partial, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        logger.i { "Downloaded $url → $dest (${Files.size(dest)} bytes)" }
        return dest
    }

    private fun sha256(path: Path): String {
        val md = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buf = ByteArray(BUFFER_SIZE)
            var read: Int
            while (input.read(buf).also { read = it } > 0) md.update(buf, 0, read)
        }
        return md.digest().joinToString("") { "%02x".format(it) }
    }

    private fun appendFlags(append: Boolean) = if (append) {
        arrayOf(java.nio.file.StandardOpenOption.APPEND, java.nio.file.StandardOpenOption.WRITE)
    } else {
        arrayOf(java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.TRUNCATE_EXISTING, java.nio.file.StandardOpenOption.WRITE)
    }

    companion object {
        private const val BUFFER_SIZE: Int = 1 shl 16
        const val DEFAULT_CONNECT_TIMEOUT_MS: Int = 30_000
        const val DEFAULT_READ_TIMEOUT_MS: Int = 60_000
    }
}

/** Adapter so we can stream bytes through [MessageDigest.update] via an OutputStream. */
private class SinkOutputStream(val sink: (ByteArray, Int, Int) -> Unit) : java.io.OutputStream() {
    override fun write(b: Int) { sink(byteArrayOf(b.toByte()), 0, 1) }
    override fun write(b: ByteArray, off: Int, len: Int) { sink(b, off, len) }
}
