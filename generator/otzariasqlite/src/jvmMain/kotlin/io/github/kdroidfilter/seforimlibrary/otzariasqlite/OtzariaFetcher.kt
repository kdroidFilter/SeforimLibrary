package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import java.io.BufferedInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Comparator
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

object OtzariaFetcher {
    private const val LATEST_API = "https://api.github.com/repos/otzaria/otzaria-library/releases/latest"
    private const val USER_AGENT = "SeforimLibrary-OtzariaFetcher/1.0"

    /** Ensure otzaria source is available locally under build/otzaria/source (relative to CWD). */
    fun ensureLocalSource(logger: Logger): Path {
        val destRoot = Paths.get("build", "otzaria", "source")
        if (Files.isDirectory(destRoot) && Files.list(destRoot).use { it.findAny().isPresent }) {
            val root = findSourceRoot(destRoot)
            removeUnwantedFolder(root, logger)
            logger.i { "Using existing otzaria source at ${root.toAbsolutePath()}" }
            return root
        }
        Files.createDirectories(destRoot)

        // Download and extract all zip files from the release
        val zipFiles = downloadAllZips(destRoot.parent, logger)
        for (zipFile in zipFiles) {
            extractZip(zipFile, destRoot, logger)
        }

        val root = findSourceRoot(destRoot)
        removeUnwantedFolder(root, logger)
        return root
    }

    private fun findSourceRoot(extractRoot: Path): Path {
        // If this folder already looks like the source (contains metadata.json and אוצריא), return it
        val meta = extractRoot.resolve("metadata.json")
        val libDir = extractRoot.resolve("אוצריא")
        if (Files.exists(meta) && Files.isRegularFile(meta) && Files.isDirectory(libDir)) return extractRoot

        // Otherwise, check first-level subdirectories for the expected layout
        Files.newDirectoryStream(extractRoot).use { ds ->
            for (p in ds) {
                if (Files.isDirectory(p)) {
                    val m = p.resolve("metadata.json")
                    val l = p.resolve("אוצריא")
                    if (Files.exists(m) && Files.isRegularFile(m) && Files.isDirectory(l)) return p
                }
            }
        }
        return extractRoot
    }

    /**
     * Download all zip files from the latest release.
     * Returns list of downloaded zip file paths.
     */
    private fun downloadAllZips(downloadDir: Path, logger: Logger): List<Path> {
        // Fetch release info from GitHub API
        val body = OptimizedHttpClient.fetchJson(LATEST_API, USER_AGENT, logger)

        // Find all .zip asset URLs
        val regex = Regex(""""browser_download_url"\s*:\s*"([^"]+\.zip)"""")
        val zipUrls = regex.findAll(body).map { it.groupValues[1] }.toList()

        if (zipUrls.isEmpty()) {
            throw IllegalStateException("No .zip asset found in latest otzaria-library release")
        }

        logger.i { "Found ${zipUrls.size} zip file(s) to download" }

        val downloadedFiles = mutableListOf<Path>()
        for ((index, zipUrl) in zipUrls.withIndex()) {
            // Extract filename from URL
            val fileName = zipUrl.substringAfterLast('/')
            val outZip = downloadDir.resolve(fileName)

            logger.i { "Downloading [${index + 1}/${zipUrls.size}]: $fileName" }

            OptimizedHttpClient.downloadFile(
                url = zipUrl,
                destination = outZip,
                userAgent = USER_AGENT,
                logger = logger,
                progressPrefix = "Downloading $fileName"
            )

            downloadedFiles.add(outZip)
        }

        return downloadedFiles
    }

    private fun extractZip(zipFile: Path, destinationDir: Path, logger: Logger) {
        logger.i { "Extracting otzaria to ${destinationDir.toAbsolutePath()}" }
        ZipInputStream(BufferedInputStream(Files.newInputStream(zipFile))).use { zis ->
            var entry: ZipEntry? = zis.nextEntry
            while (entry != null) {
                val newPath = destinationDir.resolve(entry.name).normalize()
                if (entry.isDirectory) {
                    Files.createDirectories(newPath)
                } else {
                    Files.createDirectories(newPath.parent)
                    FileOutputStream(newPath.toFile()).use { fos ->
                        // Increase buffer to speed up extraction of large entries
                        zis.copyTo(fos, 1 shl 20) // 1 MiB buffer
                    }
                }
                zis.closeEntry()
                entry = zis.nextEntry
            }
        }
        logger.i { "Extraction complete." }
    }

    private fun removeUnwantedFolder(root: Path, logger: Logger) {
        val base = root.resolve("אוצריא")
        val namesToRemove = loadOtzariaFoldersToRemove(logger)
        for (name in namesToRemove) {
            val trimmed = name.trim()
            if (trimmed.isEmpty()) continue
            val dir = base.resolve(trimmed)
            if (Files.exists(dir)) {
                runCatching {
                    Files.walk(dir)
                        .sorted(Comparator.reverseOrder())
                        .forEach { Files.deleteIfExists(it) }
                    logger.i { "Removed unwanted folder: ${dir.toAbsolutePath()}" }
                }.onFailure { e ->
                    logger.w(e) { "Failed removing unwanted folder at ${dir.toAbsolutePath()}" }
                }
            }
        }
    }

    private fun loadOtzariaFoldersToRemove(logger: Logger): List<String> {
        // Use only the resource file (no hardcoded fallback)
        return try {
            val resourceNames = listOf("otzaria-folder-to-remove.txt", "/otzaria-folder-to-remove.txt")
            val cl = Thread.currentThread().contextClassLoader
            val stream = resourceNames.asSequence()
                .mapNotNull { name -> cl?.getResourceAsStream(name) ?: this::class.java.getResourceAsStream(name) }
                .firstOrNull()
            if (stream == null) {
                logger.i { "No otzaria-folder-to-remove.txt resource found; no folders will be removed" }
                return emptyList()
            }
            stream.bufferedReader(Charsets.UTF_8).use { br ->
                br.lineSequence()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
                    .toList()
            }
        } catch (e: Exception) {
            logger.w(e) { "Failed to load otzaria-folder-to-remove.txt; no folders will be removed" }
            emptyList()
        }
    }
}
