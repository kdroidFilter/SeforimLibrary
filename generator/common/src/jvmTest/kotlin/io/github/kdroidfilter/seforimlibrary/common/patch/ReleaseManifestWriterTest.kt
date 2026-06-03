package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReleaseManifestWriterTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `writeManifest emits a parseable manifest with compressed + uncompressed sha256 + size`() {
        val patch = tmp.newFile("patch.db").toPath()
        Files.writeString(patch, "hello world")
        val compressed = PatchCompressor.compress(patch, level = 3, workers = 1)
        val target = ReleaseManifestWriter().writeManifest(
            patchFile = patch,
            fromVersion = 1,
            toVersion = 2,
            fromSchemaVersion = 3,
            toSchemaVersion = 3,
            fromContentHash = "aaaa",
            toContentHash = "bbbb",
            compressed = ReleaseManifestWriter.CompressedPatchSpec(
                file = compressed.compressedFile,
                sha256 = compressed.compressedSha256,
                size = compressed.compressedSize,
                compression = "zstd",
            ),
        )
        val body = Files.readString(target)
        assertTrue(body.contains("\"fromVersion\": 1"))
        assertTrue(body.contains("\"toVersion\": 2"))
        assertTrue(body.contains("\"fromSchemaVersion\": 3"))
        assertTrue(body.contains("\"fromContentHash\": \"aaaa\""))
        assertTrue(body.contains("\"toContentHash\": \"bbbb\""))
        assertTrue(body.contains("\"compression\": \"zstd\""))
        // sha256 of "hello world" — should appear as uncompressedSha256.
        assertTrue(body.contains("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"), body)
        assertTrue(body.contains("\"uncompressedSize\": 11"))
        assertTrue(body.contains("\"sha256\": \"${compressed.compressedSha256}\""))
        assertTrue(body.contains("\"size\": ${compressed.compressedSize}"))
        assertTrue(body.contains("\"catalogBlobName\": \"catalog.pb\""))
    }

    @Test
    fun `upsertReleaseMeta creates and merges incrementally`() {
        val metaPath = tmp.newFolder().toPath().resolve("release_meta.json")
        val writer = ReleaseManifestWriter()
        val bundle = ReleaseManifestWriter.FullBundleSpec(
            version = 3, url = "https://x/full", sha256 = "deadbeef", size = 1_000_000_000L,
        )

        // First entry.
        writer.upsertReleaseMeta(
            releaseMetaPath = metaPath,
            latestVersion = 2,
            fullBundle = bundle,
            newEntry = ReleaseManifestWriter.DeltaEntrySpec(
                fromVersion = 1, toVersion = 2,
                manifestUrl = "https://x/1-2.manifest.json", totalSize = 100,
            ),
        )
        // Second entry.
        writer.upsertReleaseMeta(
            releaseMetaPath = metaPath,
            latestVersion = 3,
            fullBundle = bundle,
            newEntry = ReleaseManifestWriter.DeltaEntrySpec(
                fromVersion = 2, toVersion = 3,
                manifestUrl = "https://x/2-3.manifest.json", totalSize = 200,
            ),
        )
        // Re-publish the first entry with a different size → should replace (not duplicate).
        writer.upsertReleaseMeta(
            releaseMetaPath = metaPath,
            latestVersion = 3,
            fullBundle = bundle,
            newEntry = ReleaseManifestWriter.DeltaEntrySpec(
                fromVersion = 1, toVersion = 2,
                manifestUrl = "https://x/1-2.manifest.json", totalSize = 150,
            ),
        )

        val body = Files.readString(metaPath)
        assertTrue(body.contains("\"latestVersion\": 3"), body)
        // Both entries present, in (fromVersion ASC) order.
        val deltas = Regex(""""fromVersion"\s*:\s*(\d+)\s*,\s*"toVersion"\s*:\s*(\d+)""")
            .findAll(body).map { it.groupValues[1].toInt() to it.groupValues[2].toInt() }.toList()
        assertEquals(listOf(1 to 2, 2 to 3), deltas)
        // Updated size took effect (the rewritten v1→v2 entry uses size=150).
        assertTrue(body.contains("\"totalSize\": 150"))
        assertTrue(body.contains("\"totalSize\": 200"))
    }

    @Test
    fun `manifest writes are atomic — no tmp lingers on success`() {
        val patchFile = tmp.newFile("patch.db").toPath()
        Files.writeString(patchFile, "fake patch bytes")
        val compressed = PatchCompressor.compress(patchFile, level = 3, workers = 1)
        val writer = ReleaseManifestWriter()
        val manifest = writer.writeManifest(
            patchFile = patchFile,
            fromVersion = 1, toVersion = 2,
            fromSchemaVersion = 1, toSchemaVersion = 1,
            fromContentHash = "from", toContentHash = "to",
            compressed = ReleaseManifestWriter.CompressedPatchSpec(
                file = compressed.compressedFile,
                sha256 = compressed.compressedSha256,
                size = compressed.compressedSize,
                compression = "zstd",
            ),
        )
        assertTrue(Files.exists(manifest), "manifest must exist")
        assertTrue(
            !Files.exists(manifest.resolveSibling("${manifest.fileName}.tmp")),
            ".tmp companion must be moved away, not left on disk",
        )
        val metaPath = tmp.newFolder().toPath().resolve("release_meta.json")
        writer.upsertReleaseMeta(
            releaseMetaPath = metaPath,
            latestVersion = 2,
            fullBundle = ReleaseManifestWriter.FullBundleSpec(2, "u", "s", 1L),
            newEntry = ReleaseManifestWriter.DeltaEntrySpec(1, 2, "m", 1L),
        )
        assertTrue(Files.exists(metaPath))
        assertTrue(
            !Files.exists(metaPath.resolveSibling("${metaPath.fileName}.tmp")),
            "release_meta.json.tmp companion must not linger",
        )
    }

    @Test
    fun `release_meta survives quotes and slashes in URLs`() {
        val metaPath = tmp.newFolder().toPath().resolve("release_meta.json")
        ReleaseManifestWriter().upsertReleaseMeta(
            releaseMetaPath = metaPath,
            latestVersion = 5,
            fullBundle = ReleaseManifestWriter.FullBundleSpec(5, "https://x/full?q=1", "abc", 1L),
            newEntry = ReleaseManifestWriter.DeltaEntrySpec(
                4, 5, "https://x/path/with%20space/manifest.json", 42L,
            ),
        )
        val body = Files.readString(metaPath)
        assertTrue("https://x/path/with%20space/manifest.json" in body, body)
        assertTrue("https://x/full?q=1" in body, body)
    }
}
