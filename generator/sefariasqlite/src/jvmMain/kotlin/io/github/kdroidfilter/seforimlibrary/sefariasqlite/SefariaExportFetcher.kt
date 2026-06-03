package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import com.github.luben.zstd.ZstdInputStream
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import java.io.BufferedInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.isDirectory

/**
 * Downloads the latest Sefaria export (.tar.zst) and extracts it locally.
 *
 * The archive is fetched from the latest GitHub release of `kdroidFilter/SefariaExport`.
 */
object SefariaExportFetcher {
    private const val LATEST_API = "https://api.github.com/repos/kdroidFilter/SefariaExport/releases/latest"
    private const val USER_AGENT = "SeforimLibrary-SefariaExportFetcher/1.0"

    /**
     * Ensure a Sefaria export is available locally under `build/sefaria/export` (relative to CWD).
     * If the directory already exists and contains data, it is reused.
     *
     * @return Path to the directory that contains the `database_export` folder.
     */
    fun ensureLocalExport(logger: Logger): Path {
        val destRoot = Paths.get("build", "sefaria", "export")
        if (destRoot.isDirectory() && Files.list(destRoot).use { it.findAny().isPresent }) {
            val root = findExportRoot(destRoot)
            logger.i { "Using existing Sefaria export at ${root.toAbsolutePath()}" }
            return root
        }
        Files.createDirectories(destRoot)
        val archivePath = destRoot.parent.resolve("sefaria-export.tar.zst")
        downloadLatestArchive(archivePath, logger)
        extractTarZst(archivePath, destRoot, logger)
        return findExportRoot(destRoot)
    }

    private fun downloadLatestArchive(out: Path, logger: Logger) {
        // Fetch release info from GitHub API
        val body = OptimizedHttpClient.fetchJson(LATEST_API, USER_AGENT, logger)

        val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+\.tar\.zst)"""")
        val archiveUrl = regex.findAll(body).map { it.groupValues[1] }.firstOrNull()
            ?: throw IllegalStateException("No .tar.zst asset found in latest SefariaExport release")
        logger.i { "Downloading Sefaria export from $archiveUrl" }

        // Download the archive with optimized client
        OptimizedHttpClient.downloadFile(
            url = archiveUrl,
            destination = out,
            userAgent = USER_AGENT,
            logger = logger,
            progressPrefix = "Downloading Sefaria export"
        )
    }

    private fun extractTarZst(archive: Path, destinationDir: Path, logger: Logger) {
        logger.i { "Extracting ${archive.fileName} to ${destinationDir.toAbsolutePath()}" }
        Files.createDirectories(destinationDir)
        BufferedInputStream(Files.newInputStream(archive)).use { fileStream ->
            ZstdInputStream(fileStream).use { zstd ->
                TarArchiveInputStream(zstd).use { tar ->
                    var entry = tar.nextTarEntry
                    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                    while (entry != null) {
                        val newPath = destinationDir.resolve(entry.name).normalize()
                        if (!newPath.startsWith(destinationDir)) {
                            throw IllegalStateException("Blocked suspicious path while extracting: ${entry.name}")
                        }
                        if (entry.isDirectory) {
                            Files.createDirectories(newPath)
                        } else {
                            Files.createDirectories(newPath.parent)
                            Files.newOutputStream(newPath).use { out ->
                                var n = tar.read(buffer)
                                while (n > 0) {
                                    out.write(buffer, 0, n)
                                    n = tar.read(buffer)
                                }
                            }
                        }
                        entry = tar.nextTarEntry
                    }
                }
            }
        }
        logger.i { "Extraction complete." }
    }

    private fun findExportRoot(extractRoot: Path): Path {
        val direct = extractRoot.resolve("database_export")
        if (direct.exists()) return extractRoot
        Files.newDirectoryStream(extractRoot).use { ds ->
            for (p in ds) {
                if (p.isDirectory()) {
                    val candidate = p.resolve("database_export")
                    if (candidate.exists()) return p
                }
            }
        }
        return extractRoot
    }
}
