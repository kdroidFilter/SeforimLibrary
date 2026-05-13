package io.github.kdroidfilter.seforimlibrary.deltaupdater

import com.sun.net.httpserver.HttpServer
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.net.InetSocketAddress
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DeltaDownloaderTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun sha256Hex(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes)
            .joinToString("") { "%02x".format(it) }

    @Test
    fun `restarts from byte 0 when server ignores Range header and returns 200`() {
        // 1 MiB of deterministic content.
        val payload = ByteArray(1024 * 1024) { (it and 0xFF).toByte() }
        val sha = sha256Hex(payload)

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/file") { ex ->
            // Always reply with 200 + the full payload, even if Range header is set.
            ex.sendResponseHeaders(200, payload.size.toLong())
            ex.responseBody.use { it.write(payload) }
        }
        server.start()
        try {
            val dest = tmp.newFolder().toPath().resolve("file.bin")
            // Pre-seed a partial .part file with WRONG bytes so the test would
            // fail with a sha256 mismatch if the downloader appended onto it.
            val partial = dest.resolveSibling("file.bin.part")
            Files.write(partial, ByteArray(1024) { 0x7F })

            val url = "http://127.0.0.1:${server.address.port}/file"
            val result = DeltaDownloader().download(
                url = url,
                dest = dest,
                expectedSha256 = sha,
                expectedSize = payload.size.toLong(),
            )

            assertTrue(Files.exists(result))
            val downloadedSha = sha256Hex(Files.readAllBytes(result))
            assertEquals(sha, downloadedSha, "downloader must end with correct content")
            assertEquals(payload.size.toLong(), Files.size(result))
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `resumes cleanly when server honors Range header with 206`() {
        val payload = ByteArray(1024 * 1024) { (it and 0xFF).toByte() }
        val sha = sha256Hex(payload)
        val prefixLen = 1024

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/file") { ex ->
            val range = ex.requestHeaders.getFirst("Range")
            if (range == null) {
                ex.sendResponseHeaders(200, payload.size.toLong())
                ex.responseBody.use { it.write(payload) }
            } else {
                val start = Regex("bytes=(\\d+)-").find(range)!!.groupValues[1].toInt()
                val slice = payload.copyOfRange(start, payload.size)
                ex.responseHeaders.set("Content-Range", "bytes $start-${payload.size - 1}/${payload.size}")
                ex.sendResponseHeaders(206, slice.size.toLong())
                ex.responseBody.use { it.write(slice) }
            }
        }
        server.start()
        try {
            val dest = tmp.newFolder().toPath().resolve("file.bin")
            // Pre-seed a CORRECT prefix to simulate an interrupted earlier download.
            val partial = dest.resolveSibling("file.bin.part")
            Files.write(partial, payload.copyOfRange(0, prefixLen))

            val url = "http://127.0.0.1:${server.address.port}/file"
            val result = DeltaDownloader().download(
                url = url,
                dest = dest,
                expectedSha256 = sha,
                expectedSize = payload.size.toLong(),
            )
            assertEquals(sha, sha256Hex(Files.readAllBytes(result)))
        } finally {
            server.stop(0)
        }
    }
}
