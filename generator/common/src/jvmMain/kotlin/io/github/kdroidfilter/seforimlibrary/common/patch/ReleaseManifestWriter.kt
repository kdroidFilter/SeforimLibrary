package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

/**
 * Server-side companion to `:delta-updater`'s `DeltaManifest` /
 * `ReleaseMeta`.
 *
 * Emits two JSON files alongside a freshly-produced `patch.db`:
 *
 *  - `<patchFile>.manifest.json` — the per-delta manifest. Pinned to
 *    `from_version`, `to_version`, `from_content_hash`, `to_content_hash`,
 *    plus the patch file's sha256 + size.
 *  - `release_meta.json` (optional, written if [releaseMetaPath] is set) —
 *    the release-level index that the client polls. Carries the
 *    `latestVersion`, the `fullBundle` info, and the list of available
 *    deltas. Existing entries are merged in so multiple `produce` calls
 *    can incrementally extend the same release_meta.json over time.
 *
 * JSON is emitted by hand (no kotlinx-serialization dep needed on the
 * generator side); the schemas mirror `:delta-updater/Manifest.kt`.
 */
class ReleaseManifestWriter(
    private val logger: Logger = Logger.withTag("ReleaseManifestWriter"),
) {

    fun writeManifest(
        patchFile: Path,
        fromVersion: Int,
        toVersion: Int,
        fromSchemaVersion: Int,
        toSchemaVersion: Int,
        fromContentHash: String,
        toContentHash: String,
        catalogBlobName: String? = "catalog.pb",
    ): Path {
        require(Files.isRegularFile(patchFile)) { "patch file not found: $patchFile" }
        val sha256 = sha256(patchFile)
        val size = Files.size(patchFile)
        val target = patchFile.resolveSibling("${patchFile.fileName}.manifest.json")
        val body = buildString {
            append("{\n")
            append("  \"fromVersion\": ").append(fromVersion).append(",\n")
            append("  \"toVersion\": ").append(toVersion).append(",\n")
            append("  \"fromSchemaVersion\": ").append(fromSchemaVersion).append(",\n")
            append("  \"toSchemaVersion\": ").append(toSchemaVersion).append(",\n")
            append("  \"fromContentHash\": ").appendString(fromContentHash).append(",\n")
            append("  \"toContentHash\": ").appendString(toContentHash).append(",\n")
            append("  \"patchFiles\": [\n")
            append("    {\n")
            append("      \"file\": ").appendString(patchFile.fileName.toString()).append(",\n")
            append("      \"sha256\": ").appendString(sha256).append(",\n")
            append("      \"size\": ").append(size).append("\n")
            append("    }\n")
            append("  ]")
            if (catalogBlobName != null) {
                append(",\n  \"catalogBlobName\": ").appendString(catalogBlobName)
            }
            append("\n}\n")
        }
        Files.writeString(target, body)
        logger.i { "Manifest written: $target" }
        return target
    }

    /**
     * Inserts or replaces this delta's entry in [releaseMetaPath]. The file
     * is rewritten whole; entries for the same (fromVersion, toVersion) are
     * deduped.
     *
     * @param manifestUrl absolute URL where the client will fetch the per-delta
     *   manifest (typically a github-pages or S3 URL).
     * @param fullBundle metadata for the fall-back full bundle download.
     */
    fun upsertReleaseMeta(
        releaseMetaPath: Path,
        latestVersion: Int,
        retentionWindow: Int = 30,
        fullBundle: FullBundleSpec,
        newEntry: DeltaEntrySpec,
    ): Path {
        val existingEntries = if (Files.exists(releaseMetaPath)) {
            parseExistingDeltas(Files.readString(releaseMetaPath))
        } else emptyList()
        val merged = (existingEntries.filterNot {
            it.fromVersion == newEntry.fromVersion && it.toVersion == newEntry.toVersion
        } + newEntry).sortedBy { it.fromVersion }

        val body = buildString {
            append("{\n")
            append("  \"latestVersion\": ").append(latestVersion).append(",\n")
            append("  \"retentionWindow\": ").append(retentionWindow).append(",\n")
            append("  \"fullBundle\": {\n")
            append("    \"version\": ").append(fullBundle.version).append(",\n")
            append("    \"url\": ").appendString(fullBundle.url).append(",\n")
            append("    \"sha256\": ").appendString(fullBundle.sha256).append(",\n")
            append("    \"size\": ").append(fullBundle.size).append("\n")
            append("  },\n")
            append("  \"deltas\": [\n")
            merged.forEachIndexed { i, d ->
                append("    {")
                append("\"fromVersion\": ").append(d.fromVersion).append(", ")
                append("\"toVersion\": ").append(d.toVersion).append(", ")
                append("\"manifestUrl\": ").appendString(d.manifestUrl).append(", ")
                append("\"totalSize\": ").append(d.totalSize)
                append("}")
                if (i < merged.size - 1) append(",")
                append("\n")
            }
            append("  ]\n")
            append("}\n")
        }
        Files.writeString(releaseMetaPath, body)
        logger.i { "release_meta.json updated: $releaseMetaPath (${merged.size} delta entries)" }
        return releaseMetaPath
    }

    data class FullBundleSpec(
        val version: Int,
        val url: String,
        val sha256: String,
        val size: Long,
    )

    data class DeltaEntrySpec(
        val fromVersion: Int,
        val toVersion: Int,
        val manifestUrl: String,
        val totalSize: Long,
    )

    // ─── Minimal JSON helpers (kept dep-free) ──────────────────────────────────

    private fun StringBuilder.appendString(s: String): StringBuilder {
        append('"')
        for (c in s) when (c) {
            '"' -> append("\\\"")
            '\\' -> append("\\\\")
            '\n' -> append("\\n")
            '\r' -> append("\\r")
            '\t' -> append("\\t")
            else -> append(c)
        }
        append('"')
        return this
    }

    /**
     * Extremely small "regex over JSON" extractor for the `deltas` array.
     * Sufficient because we control the producer (our own
     * [upsertReleaseMeta] emits a stable per-entry shape).
     */
    private fun parseExistingDeltas(json: String): List<DeltaEntrySpec> {
        val deltaObj = Regex(
            """\{\s*"fromVersion"\s*:\s*(\d+)\s*,\s*"toVersion"\s*:\s*(\d+)\s*,\s*"manifestUrl"\s*:\s*"((?:\\.|[^"\\])*)"\s*,\s*"totalSize"\s*:\s*(\d+)\s*\}"""
        )
        return deltaObj.findAll(json).map { m ->
            DeltaEntrySpec(
                fromVersion = m.groupValues[1].toInt(),
                toVersion = m.groupValues[2].toInt(),
                manifestUrl = m.groupValues[3].replace("\\\"", "\"").replace("\\\\", "\\"),
                totalSize = m.groupValues[4].toLong(),
            )
        }.toList()
    }

    private fun sha256(path: Path): String {
        val md = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buf = ByteArray(1 shl 16)
            var read: Int
            while (input.read(buf).also { read = it } > 0) md.update(buf, 0, read)
        }
        return md.digest().joinToString("") { "%02x".format(it) }
    }
}
