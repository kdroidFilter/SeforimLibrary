package io.github.kdroidfilter.seforimlibrary.generator

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import com.github.luben.zstd.ZstdOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.io.BufferedOutputStream
import java.io.BufferedInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.Date
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Package artifacts with Zstandard (zstd):
 *  - Compress the SQLite DB (seforim.db) directly to a single .zst file (no tar)
 *  - Archive Lucene indexes into a .tar and compress it with zstd (.tar.zst)
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :generator:packageArtifacts \
 *     -PseforimDb=/path/to/seforim.db \
 *     [-PdbOutput=/path/to/seforim.db.zst] \
 *     [-PindexesOutput=/path/to/lucene_indexes.tar.zst] \
 *     [-PzstdLevel=19]
 *
 * Env alternatives:
 *   SEFORIM_DB, OUTPUT_DB_ZST, OUTPUT_INDEXES_TAR_ZST, ZSTD_LEVEL, ZSTD_WORKERS
 *   (legacy: OUTPUT_TAR_ZST or -Poutput maps to indexesOutput for compatibility)
 *
 * Defaults:
 *   DB path from -PseforimDb, env SEFORIM_DB, or generator/build/seforim.db
 *   DB .zst to generator/build/package/seforim.db.zst
 *   Indexes .tar.zst to generator/build/package/lucene_indexes.tar.zst
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

    if (!textIndexDir.toFile().isDirectory) {
        logger.w { "Lucene text index directory missing: $textIndexDir (will skip)" }
    }
    if (!lookupIndexDir.toFile().isDirectory) {
        logger.w { "Lucene lookup index directory missing: $lookupIndexDir (will skip)" }
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
                        tar.finish()
                    }
                }
            }
        }

        val bundleSizeMb = Files.size(bundleOutputPath).toDouble() / (1024 * 1024)
        logger.i { "Bundle written: $bundleOutputPath (${"%.2f".format(bundleSizeMb)} MB)" }
        println(bundleOutputPath.toAbsolutePath().toString())

        // Split the bundle into parts of ~1.9 GiB (or configured size)
        val partCount = splitFile(bundleOutputPath, splitPartBytes, logger)
        if (partCount > 1) {
            logger.i { "Bundle split into $partCount parts of up to ${humanSize(splitPartBytes)} each" }
        } else {
            logger.i { "Bundle size below split threshold; no split files created" }
        }
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

private fun splitFile(source: Path, partSizeBytes: Long, logger: Logger): Int {
    val size = try { Files.size(source) } catch (e: IOException) { 0L }
    if (size <= partSizeBytes || size <= 0L) return 1

    val parent = source.parent ?: Paths.get(".")
    val baseName = source.fileName.toString()

    val buffer = ByteArray(8 * 1024 * 1024) // 8 MiB
    var index = 1
    var bytesInPart = 0L
    var currentOut: BufferedOutputStream? = null

    fun openNext(): BufferedOutputStream {
        val suffix = String.format(".part%02d", index)
        val outPath = parent.resolve(baseName + suffix)
        val bos = BufferedOutputStream(Files.newOutputStream(outPath), buffer.size)
        logger.i { "Writing part $index -> ${outPath.toAbsolutePath()}" }
        return bos
    }

    BufferedInputStream(Files.newInputStream(source), buffer.size).use { input ->
        currentOut = openNext()
        while (true) {
            val remainingInPart = partSizeBytes - bytesInPart
            val toRead = if (remainingInPart < buffer.size) remainingInPart.toInt() else buffer.size
            val read = input.read(buffer, 0, toRead)
            if (read <= 0) break
            currentOut!!.write(buffer, 0, read)
            bytesInPart += read
            if (bytesInPart >= partSizeBytes) {
                currentOut!!.flush()
                currentOut!!.close()
                index += 1
                bytesInPart = 0
                currentOut = openNext()
            }
        }
    }
    currentOut?.flush()
    currentOut?.close()
    return index
}

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
