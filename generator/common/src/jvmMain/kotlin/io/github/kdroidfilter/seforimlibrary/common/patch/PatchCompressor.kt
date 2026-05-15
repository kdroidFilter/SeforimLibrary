package io.github.kdroidfilter.seforimlibrary.common.patch

import com.github.luben.zstd.ZstdOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

/**
 * Compresses a freshly-produced `patch.db` with zstd and returns the
 * metadata both the manifest writer and the client need for verification.
 *
 * Output sits next to the input (`<patch>.zst`). Caller decides whether
 * to keep or delete the original `.db`.
 */
object PatchCompressor {

    data class Result(
        /** Path of the produced `<patch>.zst`. */
        val compressedFile: Path,
        /** sha256 of the `.zst` (= what the client downloads). */
        val compressedSha256: String,
        /** Size in bytes of the `.zst`. */
        val compressedSize: Long,
        /** sha256 of the original uncompressed patch.db (post-decompress verification). */
        val uncompressedSha256: String,
        /** Size in bytes of the uncompressed patch.db. */
        val uncompressedSize: Long,
    )

    /**
     * Compresses [patchDb] to `<patchDb>.zst` at level [level].
     *
     * Default level 22 (ultra) matches the full bundle's compression
     * setting in `PackageArtifacts.kt` for end-to-end consistency.
     * Trade-off vs L19 in our measurements: a few percent smaller output
     * for ~5-10× CPU cost. Use a lower level (e.g. 19, 15) when CI
     * wall-time matters more than the marginal size win.
     */
    fun compress(patchDb: Path, level: Int = 22, workers: Int = Runtime.getRuntime().availableProcessors()): Result {
        require(Files.isRegularFile(patchDb)) { "patch.db not found: $patchDb" }
        val target = patchDb.resolveSibling("${patchDb.fileName}.zst")
        val uncompressedSha = MessageDigest.getInstance("SHA-256")
        val uncompressedSize = Files.size(patchDb)

        Files.newInputStream(patchDb).use { input ->
            Files.newOutputStream(
                target,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
                java.nio.file.StandardOpenOption.WRITE,
            ).use { rawOut ->
                ZstdOutputStream(rawOut, level).use { zstd ->
                    // Enable multithreaded compression when workers > 1.
                    if (workers > 1) zstd.setWorkers(workers)
                    val buf = ByteArray(1 shl 20) // 1 MiB
                    while (true) {
                        val n = input.read(buf)
                        if (n <= 0) break
                        uncompressedSha.update(buf, 0, n)
                        zstd.write(buf, 0, n)
                    }
                }
            }
        }

        val compressedSha = sha256(target)
        return Result(
            compressedFile = target,
            compressedSha256 = hex(compressedSha),
            compressedSize = Files.size(target),
            uncompressedSha256 = hex(uncompressedSha.digest()),
            uncompressedSize = uncompressedSize,
        )
    }

    private fun sha256(path: Path): ByteArray {
        val md = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { stream ->
            val buf = ByteArray(1 shl 20)
            while (true) {
                val n = stream.read(buf)
                if (n <= 0) break
                md.update(buf, 0, n)
            }
        }
        return md.digest()
    }

    private fun hex(bytes: ByteArray): String =
        bytes.joinToString("") { b -> "%02x".format(b) }

    /** Replace [source] with [destination] atomically. Used after a successful copy. */
    fun replaceAtomic(source: Path, destination: Path) {
        Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }
}
