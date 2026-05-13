package io.github.kdroidfilter.seforimlibrary.deltaupdater

import com.sun.net.httpserver.HttpServer
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
    fun `read timeout fires fast on a stalled server`() {
        // Server accepts the request, sends headers promising bytes, then
        // never writes a body — simulates a hung CDN / proxy mid-transfer.
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/file") { ex ->
            // Promise a big body but never send it; the request handler hangs
            // until the client closes the socket, which the read timeout
            // forces from the client side.
            ex.sendResponseHeaders(200, 1_000_000)
            Thread.sleep(10_000) // longer than the test's read timeout
            ex.close()
        }
        server.start()
        try {
            val dest = tmp.newFolder().toPath().resolve("file.bin")
            val downloader = DeltaDownloader(readTimeoutMs = 300)
            val started = System.nanoTime()
            assertFailsWith<IOException> {
                downloader.download(
                    url = "http://127.0.0.1:${server.address.port}/file",
                    dest = dest,
                    expectedSha256 = "0".repeat(64),
                    expectedSize = 1_000_000L,
                )
            }
            val elapsedMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(elapsedMs < 5_000, "timeout should fire in < 5s (was ${elapsedMs}ms)")
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
