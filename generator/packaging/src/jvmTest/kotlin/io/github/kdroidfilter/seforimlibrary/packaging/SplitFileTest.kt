package io.github.kdroidfilter.seforimlibrary.packaging

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import kotlin.io.path.exists
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SplitFileTest {
    private val dir = Files.createTempDirectory("split-file")
    private val bundle: Path = dir.resolve("seforim_bundle.tar.zst")
    private val logger = Logger.withTag("SplitFileTest")

    @AfterTest
    fun cleanup() {
        dir.toFile().deleteRecursively()
    }

    private fun sha256(bytes: ByteArray) = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

    @Test
    fun `splits into parts whose concatenation is the bundle`() {
        val content = ByteArray(10) { it.toByte() }
        Files.write(bundle, content)

        val manifest = splitFile(bundle, partSizeBytes = 4, logger = logger)

        assertEquals(listOf("part01", "part02", "part03"), manifest.parts.map { it.file.substringAfterLast('.') })
        assertEquals(listOf(4L, 4L, 2L), manifest.parts.map { it.size })
        val joined = manifest.parts.map { Files.readAllBytes(dir.resolve(it.file)) }.reduce { a, b -> a + b }
        assertContentEquals(content, joined)
        manifest.parts.forEach { assertEquals(sha256(Files.readAllBytes(dir.resolve(it.file))), it.sha256) }
        assertEquals(sha256(content), manifest.sha256)
        assertEquals(10L, manifest.size)
    }

    @Test
    fun `exact multiple still ends with a strictly smaller part`() {
        Files.write(bundle, ByteArray(8))

        val manifest = splitFile(bundle, partSizeBytes = 4, logger = logger)

        assertEquals(listOf(3L, 3L, 2L), manifest.parts.map { it.size })
        assertFalse(dir.resolve("seforim_bundle.tar.zst.part04").exists())
    }

    @Test
    fun `small bundle is published as is`() {
        Files.write(bundle, ByteArray(3) { 7 })

        val manifest = splitFile(bundle, partSizeBytes = 4, logger = logger)

        assertEquals(listOf(BundleManifest.Part(bundle.fileName.toString(), 3, manifest.sha256)), manifest.parts)
    }

    @Test
    fun `removes stale parts from a previous run`() {
        Files.write(dir.resolve("seforim_bundle.tar.zst.part05"), ByteArray(1))
        Files.write(bundle, ByteArray(6))

        splitFile(bundle, partSizeBytes = 4, logger = logger)

        assertFalse(dir.resolve("seforim_bundle.tar.zst.part05").exists())
        assertTrue(dir.resolve("seforim_bundle.tar.zst.part02").exists())
    }

    @Test
    fun `manifest is written next to the bundle`() {
        Files.write(bundle, ByteArray(6))

        val path = splitFile(bundle, partSizeBytes = 4, logger = logger).writeNextTo(bundle)

        assertEquals("seforim_bundle.tar.zst.manifest.json", path.fileName.toString())
        val json = Files.readString(path)
        assertTrue("\"formatVersion\": 1" in json)
        assertTrue("\"file\": \"seforim_bundle.tar.zst.part02\", \"size\": 2" in json)
    }
}
