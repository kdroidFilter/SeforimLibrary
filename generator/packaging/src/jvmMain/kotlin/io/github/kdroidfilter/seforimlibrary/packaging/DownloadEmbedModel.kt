package io.github.kdroidfilter.seforimlibrary.packaging

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

private const val RELEASE_API =
    "https://api.github.com/repos/kdroidFilter/SeforimEmbedding/releases/tags/v5-int8"
private const val USER_AGENT = "SeforimLibrary-DownloadEmbedModel/1.0"

// Runtime dense-search artifacts pulled from the private SeforimEmbedding release.
private val ASSETS = listOf("seforim-embed-v5-int8.onnx", "tokenizer.json")

/**
 * Download the int8 embedding model + tokenizer from the private `v5-int8` release
 * and place them next to `seforim.db` so [PackageArtifacts] bundles them.
 *
 * Requires a token with read access to the private repo via `GITHUB_TOKEN` / `GH_TOKEN`
 * (consumed by [OptimizedHttpClient]). On any failure (no token, network, missing
 * asset) it logs a warning and exits 0 so packaging proceeds WITHOUT the model
 * (the app then degrades to lexical-only search).
 *
 * Usage:
 *   ./gradlew :packaging:downloadEmbedModel
 *   ./gradlew :packaging:downloadEmbedModel -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DownloadEmbedModel")

    val dbPath = resolveDbPath(args)

    val present = ASSETS.all { name ->
        val p = dbPath.resolveSibling(name)
        Files.exists(p) && Files.isRegularFile(p) && Files.size(p) > 0
    }
    if (present) {
        logger.i { "Embedding model already present next to ${dbPath.fileName}; skipping download" }
        return
    }

    runCatching {
        val json = OptimizedHttpClient.fetchJson(RELEASE_API, USER_AGENT, logger)
        for (name in ASSETS) {
            val out = dbPath.resolveSibling(name)
            if (Files.exists(out) && Files.size(out) > 0) {
                logger.i { "Using existing $name" }
                continue
            }
            val url = assetApiUrl(json, name)
                ?: throw IllegalStateException("Asset '$name' not found in v4-int8 release")
            Files.createDirectories(out.parent)
            val tmp = out.resolveSibling("${out.fileName}.part")
            Files.deleteIfExists(tmp)
            // Asset API url + Accept: octet-stream + token -> works for private repos.
            OptimizedHttpClient.downloadFile(url, tmp, USER_AGENT, logger, "Downloading $name")
            Files.deleteIfExists(out)
            Files.move(tmp, out)
            logger.i { "Downloaded $name -> ${out.toAbsolutePath()}" }
        }
    }.onFailure {
        logger.w(it) { "Could not download the embedding model; bundle will omit it (dense search disabled)" }
    }
}

private fun resolveDbPath(args: Array<String>): Path {
    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    return Paths.get(dbPathStr)
}

/** Extract the GitHub *asset API* url for a given asset name from the release JSON. */
private fun assetApiUrl(json: String, name: String): String? {
    val re = Regex(
        "\\{\"url\":\"(https://api\\.github\\.com/[^\"]+?/assets/\\d+)\"[^{}]*?\"name\":\"" +
            Regex.escape(name) + "\"",
    )
    return re.find(json)?.groupValues?.get(1)
}
