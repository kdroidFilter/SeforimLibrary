package io.github.kdroidfilter.seforimlibrary.packaging

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import com.github.luben.zstd.ZstdOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.io.BufferedOutputStream
import java.io.BufferedInputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.DigestOutputStream
import java.security.MessageDigest
import java.time.Instant
import java.util.Date
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Package artifacts with Zstandard (zstd):
 *  - SQLite DB (seforim.db)
 *  - Precomputed catalog (catalog.pb)
 *  - Lucene indexes (text and lookup)
 *  - Lexical DB (lexical.db)
 *  All packaged into a single .tar.zst bundle
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :packaging:packageArtifacts \
 *     -PseforimDb=/path/to/seforim.db \
 *     [-PbundleOutput=/path/to/seforim_bundle.tar.zst] \
 *     [-PzstdLevel=22] \
 *     [-PzstdWorkers=0] \
 *     [-PsplitPartBytes=2040109465]
 *
 * Env alternatives:
 *   SEFORIM_DB, OUTPUT_BUNDLE_TAR_ZST, ZSTD_LEVEL, ZSTD_WORKERS, SPLIT_PART_BYTES
 *   (legacy: OUTPUT_TAR_ZST or -Poutput maps to bundleOutput for compatibility)
 *
 * Defaults:
 *   DB path from -PseforimDb, env SEFORIM_DB, or build/seforim.db
 *   Bundle output to build/package/seforim_bundle.tar.zst
 *   Catalog expected at same location as DB (catalog.pb)
 *   Indexes expected next to DB (.lucene, .lookup.lucene)
 *   Release info expected next to DB (release_info.txt)
 *   Lexical DB expected next to DB (lexical.db)
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("PackageArtifacts")

    // Resolve DB path
    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val dbPath = Paths.get(dbPathStr)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }

    // Resolve index directories next to the DB
    val textIndexDir: Path = if (dbPathStr.endsWith(".db")) Paths.get("$dbPathStr.lucene") else Paths.get("$dbPathStr.luceneindex")
    val lookupIndexDir: Path = if (dbPathStr.endsWith(".db")) Paths.get("$dbPathStr.lookup.lucene") else Paths.get("$dbPathStr.lookupindex")

    // Resolve precomputed catalog next to the DB
    val catalogPath: Path = dbPath.resolveSibling("catalog.pb")

    // Resolve release info file next to the DB
    val releaseInfoPath: Path = dbPath.resolveSibling("release_info.txt")

    // Resolve lexical DB next to the DB
    val lexicalDbPath: Path = dbPath.resolveSibling("lexical.db")

    if (!textIndexDir.toFile().isDirectory) {
        logger.w { "Lucene text index directory missing: $textIndexDir (will skip)" }
    }
    if (!lookupIndexDir.toFile().isDirectory) {
        logger.w { "Lucene lookup index directory missing: $lookupIndexDir (will skip)" }
    }
    if (!catalogPath.exists()) {
        logger.w { "Precomputed catalog missing: $catalogPath (will skip)" }
    }
    if (!releaseInfoPath.exists()) {
        logger.w { "Release info file missing: $releaseInfoPath (will skip)" }
    }
    if (!lexicalDbPath.exists()) {
        logger.w { "Lexical DB missing: $lexicalDbPath (will skip)" }
    }

    // Output: single bundle tar.zst
    val legacyOutput = System.getProperty("output") ?: System.getenv("OUTPUT_TAR_ZST")
    val bundleOutputStr = System.getProperty("bundleOutput")
        ?: System.getenv("OUTPUT_BUNDLE_TAR_ZST")
        ?: legacyOutput
        ?: Paths.get("build", "package", "seforim_bundle.tar.zst").toString()
    val bundleOutputPath = Paths.get(bundleOutputStr)
    Files.createDirectories(bundleOutputPath.parent)

    // Compression level (default ultra 22)
    val zstdLevel = (
        System.getProperty("zstdLevel")
            ?: System.getenv("ZSTD_LEVEL")
            ?: "22"
        ).toIntOrNull()?.coerceIn(1, 22) ?: 22

    // Workers (threads). Default: all available processors (like zstd -T0)
    val workers = (
        System.getProperty("zstdWorkers")
            ?: System.getenv("ZSTD_WORKERS")
            ?: "0"
    ).toIntOrNull()?.let { if (it <= 0) Runtime.getRuntime().availableProcessors() else it }
        ?: Runtime.getRuntime().availableProcessors()

    // Split part size (~1.9 GiB by default)
    val defaultSplitSize = (1.9 * 1024.0 * 1024.0 * 1024.0).toLong()
    val splitPartBytes = (
        System.getProperty("splitPartBytes")
            ?: System.getenv("SPLIT_PART_BYTES")
            ?: defaultSplitSize.toString()
        ).toLongOrNull()?.takeIf { it > 0 } ?: defaultSplitSize

    logger.i {
        "Packaging into single bundle:\n" +
            " - DB: $dbPath\n" +
            " - Catalog: $catalogPath\n" +
            " - Release info: $releaseInfoPath\n" +
            " - Lexical DB: $lexicalDbPath\n" +
            " - Text index: $textIndexDir\n" +
            " - Lookup index: $lookupIndexDir\n" +
            " -> Bundle .tar.zst: $bundleOutputPath\n" +
            " (zstd level $zstdLevel, workers $workers, split ${humanSize(splitPartBytes)})"
    }

    try {
        // Tar + zstd the three artifacts into a single bundle
        Files.newOutputStream(bundleOutputPath).use { fos ->
            BufferedOutputStream(fos, 1 shl 20).use { bos ->
                ZstdOutputStream(bos, zstdLevel).use { zstd ->
                    runCatching { zstd.setWorkers(workers) }
                        .onFailure { logger.w(it) { "zstd setWorkers($workers) failed; continuing single-threaded" } }
                    TarArchiveOutputStream(zstd).use { tar ->
                        tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
                        tar.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX)

                        val haveText = textIndexDir.toFile().isDirectory
                        val haveLookup = lookupIndexDir.toFile().isDirectory
                        val haveCatalog = catalogPath.exists()
                        val haveReleaseInfo = releaseInfoPath.exists()
                        val haveLexicalDb = lexicalDbPath.exists()

                        if (haveLookup) {
                            addDirectoryToTar(tar, lookupIndexDir, lookupIndexDir.fileName.toString(), logger)
                        } else {
                            logger.w { "Lucene lookup index directory missing: $lookupIndexDir (skipped)" }
                        }
                        if (haveText) {
                            addDirectoryToTar(tar, textIndexDir, textIndexDir.fileName.toString(), logger)
                        } else {
                            logger.w { "Lucene text index directory missing: $textIndexDir (skipped)" }
                        }

                        // Add the database file itself
                        addFileToTar(tar, dbPath, dbPath.fileName.toString(), logger)

                        // Add lexical DB if available
                        if (haveLexicalDb) {
                            addFileToTar(tar, lexicalDbPath, lexicalDbPath.fileName.toString(), logger)
                            logger.i { "Added lexical DB to bundle" }
                        } else {
                            logger.w { "Lexical DB missing: $lexicalDbPath (skipped)" }
                        }

                        // Add the precomputed catalog if available
                        if (haveCatalog) {
                            addFileToTar(tar, catalogPath, catalogPath.fileName.toString(), logger)
                            logger.i { "Added precomputed catalog to bundle" }
                        } else {
                            logger.w { "Precomputed catalog missing: $catalogPath (skipped)" }
                        }

                        // Add the release info file if available
                        if (haveReleaseInfo) {
                            addFileToTar(tar, releaseInfoPath, releaseInfoPath.fileName.toString(), logger)
                            logger.i { "Added release info to bundle" }
                        } else {
                            logger.w { "Release info file missing: $releaseInfoPath (skipped)" }
                        }

                        tar.finish()
                    }
                }
            }
        }

        val bundleSizeMb = Files.size(bundleOutputPath).toDouble() / (1024 * 1024)
        logger.i { "Bundle written: $bundleOutputPath (${"%.2f".format(bundleSizeMb)} MB)" }
        println(bundleOutputPath.toAbsolutePath().toString())

        // Split the bundle into parts of ~1.9 GiB (or configured size) and describe them
        val manifest = splitFile(bundleOutputPath, splitPartBytes, logger)
        if (manifest.parts.size > 1) {
            logger.i { "Bundle split into ${manifest.parts.size} parts of up to ${humanSize(splitPartBytes)} each" }
        } else {
            logger.i { "Bundle size below split threshold; no split files created" }
        }
        val manifestPath = manifest.writeNextTo(bundleOutputPath)
        logger.i { "Bundle manifest written: $manifestPath" }
    } catch (e: Exception) {
        logger.e(e) { "Failed to package artifacts" }
        exitProcess(1)
    }
}

private fun addDirectoryToTar(tar: TarArchiveOutputStream, dir: Path, entryName: String, logger: Logger) {
    val normalized = entryName.trimEnd('/')
    val dirEntry = TarArchiveEntry("$normalized/")
    dirEntry.modTime = Date.from(Instant.now())
    try {
        tar.putArchiveEntry(dirEntry)
        tar.closeArchiveEntry()
    } catch (e: IOException) {
        logger.w(e) { "Skipping directory entry $normalized/ due to error" }
        return
    }

    Files.list(dir).use { stream ->
        stream.forEach { child ->
            val childName = "$normalized/${child.fileName}"
            if (Files.isDirectory(child)) {
                addDirectoryToTar(tar, child, childName, logger)
            } else {
                addFileToTar(tar, child, childName, logger)
            }
        }
    }
}

private fun addFileToTar(tar: TarArchiveOutputStream, file: Path, entryName: String, logger: Logger) {
    val f = file.toFile()
    if (!f.exists() || !f.isFile) {
        logger.w { "Skipping missing file: $file" }
        return
    }
    val entry = TarArchiveEntry(f, entryName)
    // Ensure size is set for some JVMs
    entry.size = f.length()
    entry.modTime = Date(f.lastModified())
    tar.putArchiveEntry(entry)
    val declared = entry.size
    Files.newInputStream(file).use { input ->
        val buffer = ByteArray(1 shl 20)
        var remaining = declared
        while (remaining > 0) {
            val toRead = if (remaining > buffer.size) buffer.size else remaining.toInt()
            val read = input.read(buffer, 0, toRead)
            if (read <= 0) break
            tar.write(buffer, 0, read)
            remaining -= read
        }
    }
    tar.closeArchiveEntry()
}

/**
 * Splits [source] into `.partNN` files of at most [partSizeBytes] (no split when it fits in one),
 * all of the same size except a strictly smaller last one, hashing the bundle and each part in the same pass. Returns the manifest of the files to publish.
 */
internal fun splitFile(source: Path, partSizeBytes: Long, logger: Logger): BundleManifest {
    require(partSizeBytes > 1) { "splitPartBytes must be greater than 1" }
    val parent = source.toAbsolutePath().parent
    val baseName = source.fileName.toString()
    val size = Files.size(source)

    // Parts left by a previous run (possibly more of them) must not be published with the new ones
    Files.list(parent).use { files ->
        files
            .filter { file ->
                val name = file.fileName.toString()
                name.startsWith(baseName) && PART_NAME.matches(name.removePrefix(baseName))
            }.forEach(Files::delete)
    }

    val bundleDigest = MessageDigest.getInstance("SHA-256")
    if (size <= partSizeBytes) {
        Files.newInputStream(source).use { input -> input.copyTo(DigestOutputStream(OutputStream.nullOutputStream(), bundleDigest)) }
        val sha = bundleDigest.digest().toHex()
        return BundleManifest(baseName, size, sha, listOf(BundleManifest.Part(baseName, size, sha)))
    }

    // Clients without the manifest (offline installs) detect a missing last part because it is the
    // only one shorter than the others, so never let the last part be full-size.
    val partSize = if (size % partSizeBytes == 0L) partSizeBytes - 1 else partSizeBytes
    val parts = mutableListOf<BundleManifest.Part>()
    val buffer = ByteArray(8 * 1024 * 1024) // 8 MiB
    BufferedInputStream(Files.newInputStream(source), buffer.size).use { input ->
        var remaining = size
        while (remaining > 0) {
            val partName = baseName + String.format(".part%02d", parts.size + 1)
            val outPath = parent.resolve(partName)
            val partDigest = MessageDigest.getInstance("SHA-256")
            val bytes = minOf(partSize, remaining)
            logger.i { "Writing part ${parts.size + 1} -> $outPath" }
            BufferedOutputStream(Files.newOutputStream(outPath), buffer.size).use { out ->
                var left = bytes
                while (left > 0) {
                    val read = input.read(buffer, 0, minOf(buffer.size.toLong(), left).toInt())
                    if (read <= 0) throw IOException("Unexpected end of $source")
                    out.write(buffer, 0, read)
                    partDigest.update(buffer, 0, read)
                    bundleDigest.update(buffer, 0, read)
                    left -= read
                }
            }
            parts += BundleManifest.Part(partName, bytes, partDigest.digest().toHex())
            remaining -= bytes
        }
    }
    return BundleManifest(baseName, size, bundleDigest.digest().toHex(), parts)
}

private val PART_NAME = Regex("""\.part\d+""")

private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

private fun humanSize(bytes: Long): String {
    val kb = 1024.0
    val mb = kb * 1024
    val gb = mb * 1024
    return when {
        bytes >= gb -> String.format("%.2f GiB", bytes / gb)
        bytes >= mb -> String.format("%.2f MiB", bytes / mb)
        bytes >= kb -> String.format("%.2f KiB", bytes / kb)
        else -> "$bytes B"
    }
}
