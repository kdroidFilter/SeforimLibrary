package io.github.kdroidfilter.seforimlibrary.deltaupdater

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import java.io.FileNotFoundException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path

/**
 * Top-level facade for the client app to consume. Hides the wiring of
 * [DeltaApplierClient], [DeltaDownloader], [LuceneUpdater], [CatalogUpdater]
 * and [UpdateOrchestrator] behind a tiny surface area:
 *
 *   val client = DeltaUpdaterClient(
 *       seforimDb = Path.of("/.../seforim.db"),
 *       catalogPb = Path.of("/.../catalog.pb"),
 *       workDir   = Path.of("/.../delta-cache"),
 *       releaseMetaUrl = "https://example.org/release_meta.json",
 *       indexSinks = { lineIndexAdapter.deleteAndUpsert() },
 *   )
 *   client.recoverIfNeeded()                     // call at app start
 *   val report = client.checkForUpdate()         // poll periodically
 *   if (report is UpdatePath.Chain) client.applyChain(report.deltas)
 *
 * Network IO is done through [HttpClient] (java.net) — no Ktor dep — so
 * the runtime footprint stays tiny.
 */
class DeltaUpdaterClient(
    private val seforimDb: Path,
    private val catalogPb: Path,
    private val workDir: Path,
    private val releaseMetaUrl: String,
    private val indexSinks: () -> LuceneUpdater.SinkSession,
    private val localVersionProvider: () -> Int,
    private val httpGet: (String) -> String = ::defaultHttpGet,
    private val urlForPatchFile: (DeltaEntry, String) -> String = { entry, file ->
        // Default: manifestUrl is "<base>/<from>-<to>.json"; resolve siblings.
        val base = entry.manifestUrl.substringBeforeLast('/')
        "$base/$file"
    },
    private val logger: Logger = Logger.withTag("DeltaUpdaterClient"),
) {
    private val json = Json { ignoreUnknownKeys = true; isLenient = true }
    private val applier = DeltaApplierClient(logger)
    private val downloader = DeltaDownloader(logger)
    private val luceneUpdater = LuceneUpdater(logger)
    private val catalogUpdater = CatalogUpdater(logger)
    private val orchestrator = UpdateOrchestrator(
        applier, downloader, luceneUpdater, catalogUpdater, workDir, logger,
    )

    init {
        Files.createDirectories(workDir)
    }

    /** Must be called at app startup so a half-applied chain can be unwound. */
    fun recoverIfNeeded(): Boolean = applier.recoverIfNeeded(seforimDb)

    /**
     * Polls the server's `release_meta.json` and picks an update path.
     *
     * A 404 is treated as [UpdatePath.UpToDate]: it means the server has
     * never published a release yet (common in early deployments), or has
     * temporarily un-published the manifest — neither is a user-facing
     * error condition.
     */
    fun checkForUpdate(): UpdatePath {
        val body = try {
            httpGet(releaseMetaUrl)
        } catch (e: FileNotFoundException) {
            // HttpURLConnection.getInputStream() throws FileNotFoundException
            // on HTTP 404. Treat as "nothing on the server yet".
            logger.i { "release_meta.json not found at $releaseMetaUrl — treating as up-to-date" }
            return UpdatePath.UpToDate
        }
        val meta = json.decodeFromString<ReleaseMeta>(body)
        return chooseUpdatePath(localVersionProvider(), meta)
    }

    /** Applies a chain of deltas onto the live `seforim.db`. */
    fun applyChain(
        chain: List<DeltaEntry>,
        progress: (current: Int, total: Int, status: String) -> Unit = { _, _, _ -> },
    ) {
        orchestrator.applyChain(
            seforimDb = seforimDb,
            catalogPb = catalogPb,
            chain = chain,
            fetchManifest = { entry -> json.decodeFromString(httpGet(entry.manifestUrl)) },
            baseUrlForEntry = urlForPatchFile,
            luceneSinks = indexSinks,
            progress = progress,
        )
    }

    companion object {
        fun defaultHttpGet(url: String): String =
            URI(url).toURL().openConnection().run {
                connectTimeout = 30_000
                readTimeout = 60_000
                getInputStream().use { it.readBytes().toString(Charsets.UTF_8) }
            }
    }
}
