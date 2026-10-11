package io.github.kdroidfilter.seforimlibrary.packaging

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/**
 * Describes the published bundle: the files to download, in concatenation order, and the
 * checksums of each part and of the reassembled `.tar.zst`. Lets clients know how many
 * parts exist (a missing last part is otherwise undetectable) and verify each download.
 *
 * Written as `<bundle>.manifest.json` next to the bundle. JSON is emitted by hand, like
 * `ReleaseManifestWriter` (no kotlinx-serialization on the generator side).
 */
data class BundleManifest(
    val file: String,
    val size: Long,
    val sha256: String,
    val parts: List<Part>,
) {
    data class Part(
        val file: String,
        val size: Long,
        val sha256: String,
    )

    fun toJson(): String =
        buildString {
            append("{\n")
            append("  \"formatVersion\": ").append(FORMAT_VERSION).append(",\n")
            append("  \"file\": \"").append(file).append("\",\n")
            append("  \"size\": ").append(size).append(",\n")
            append("  \"sha256\": \"").append(sha256).append("\",\n")
            append("  \"parts\": [\n")
            parts.forEachIndexed { index, part ->
                append("    { \"file\": \"").append(part.file).append("\", ")
                append("\"size\": ").append(part.size).append(", ")
                append("\"sha256\": \"").append(part.sha256).append("\" }")
                append(if (index < parts.lastIndex) ",\n" else "\n")
            }
            append("  ]\n")
            append("}\n")
        }

    /** Writes the manifest next to [bundle] atomically and returns its path. */
    fun writeNextTo(bundle: Path): Path {
        val target = bundle.resolveSibling("${bundle.fileName}.manifest.json")
        val tmp = target.resolveSibling("${target.fileName}.tmp")
        Files.writeString(tmp, toJson())
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        return target
    }

    companion object {
        const val FORMAT_VERSION = 1
    }
}
