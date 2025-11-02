package io.github.kdroidfilter.seforimlibrary.generator

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import com.github.luben.zstd.ZstdOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import java.io.BufferedOutputStream
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

    // Outputs
    val legacyOutput = System.getProperty("output") ?: System.getenv("OUTPUT_TAR_ZST")
    val dbOutputStr = args.getOrNull(1)
        ?: System.getProperty("dbOutput")
        ?: System.getProperty("outputDbZst")
        ?: System.getenv("OUTPUT_DB_ZST")
        ?: Paths.get("build", "package", "seforim.db.zst").toString()
    val indexesOutputStr = args.getOrNull(2)
        ?: System.getProperty("indexesOutput")
        ?: System.getProperty("outputIndexesTarZst")
        ?: System.getenv("OUTPUT_INDEXES_TAR_ZST")
        ?: legacyOutput
        ?: Paths.get("build", "package", "lucene_indexes.tar.zst").toString()

    val dbOutputPath = Paths.get(dbOutputStr)
    val indexesOutputPath = Paths.get(indexesOutputStr)
    Files.createDirectories(dbOutputPath.parent)
    Files.createDirectories(indexesOutputPath.parent)

    // Compression level (default ultra 22)
    val zstdLevel = (args.getOrNull(2)
        ?: System.getProperty("zstdLevel")
        ?: System.getenv("ZSTD_LEVEL")
        ?: "19").toIntOrNull()?.coerceIn(1, 22) ?: 22

    // Workers (threads). Default: all available processors (like zstd -T0)
    val workers = (
        System.getProperty("zstdWorkers")
            ?: System.getenv("ZSTD_WORKERS")
            ?: "0"
    ).toIntOrNull()?.let { if (it <= 0) Runtime.getRuntime().availableProcessors() else it }
        ?: Runtime.getRuntime().availableProcessors()

    logger.i { "Packaging:\n - DB: $dbPath\n - Text index: $textIndexDir\n - Lookup index: $lookupIndexDir\n -> DB .zst: $dbOutputPath\n -> Indexes .tar.zst: $indexesOutputPath\n (zstd level $zstdLevel, workers $workers)" }

    try {
        // 1) Compress the DB file directly to .zst (no tar)
        Files.newOutputStream(dbOutputPath).use { fos ->
            BufferedOutputStream(fos, 1 shl 20).use { bos ->
                ZstdOutputStream(bos, zstdLevel).use { zstd ->
                    runCatching { zstd.setWorkers(workers) }
                        .onFailure { logger.w(it) { "zstd setWorkers($workers) failed; continuing single-threaded" } }
                    Files.newInputStream(dbPath).use { input ->
                        val buffer = ByteArray(1 shl 20)
                        while (true) {
                            val read = input.read(buffer)
                            if (read <= 0) break
                            zstd.write(buffer, 0, read)
                        }
                    }
                    zstd.flush()
                }
            }
        }
        val dbSizeMb = Files.size(dbOutputPath).toDouble() / (1024 * 1024)
        logger.i { "DB compressed: $dbOutputPath (${"%.2f".format(dbSizeMb)} MB)" }
        println(dbOutputPath.toAbsolutePath().toString())

        // 2) Tar + zst the Lucene indexes (if present)
        val haveText = textIndexDir.toFile().isDirectory
        val haveLookup = lookupIndexDir.toFile().isDirectory
        if (!haveText && !haveLookup) {
            logger.w { "No Lucene indexes found. Skipping indexes archive." }
        } else {
            Files.newOutputStream(indexesOutputPath).use { fos ->
                BufferedOutputStream(fos, 1 shl 20).use { bos ->
                    ZstdOutputStream(bos, zstdLevel).use { zstd ->
                        runCatching { zstd.setWorkers(workers) }
                            .onFailure { logger.w(it) { "zstd setWorkers($workers) failed; continuing single-threaded" } }
                        TarArchiveOutputStream(zstd).use { tar ->
                            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
                            tar.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX)

                            if (haveText) {
                                addDirectoryToTar(tar, textIndexDir, textIndexDir.fileName.toString(), logger)
                            }
                            if (haveLookup) {
                                addDirectoryToTar(tar, lookupIndexDir, lookupIndexDir.fileName.toString(), logger)
                            }
                            tar.finish()
                        }
                    }
                }
            }
            val idxSizeMb = Files.size(indexesOutputPath).toDouble() / (1024 * 1024)
            logger.i { "Indexes archive written: $indexesOutputPath (${"%.2f".format(idxSizeMb)} MB)" }
            println(indexesOutputPath.toAbsolutePath().toString())
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
