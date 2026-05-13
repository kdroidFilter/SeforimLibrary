package io.github.kdroidfilter.seforimlibrary.deltaupdater

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path

/**
 * Top-level coordinator that turns a [UpdatePath.Chain] into a fully-applied
 * sequence of patches.
 *
 *  For each delta in the chain:
 *   1. Download all patch files into a temp dir, verifying sha256.
 *   2. Apply the SQLite patch via [DeltaApplierClient].
 *   3. Update Lucene via [LuceneUpdater] + caller-supplied sinks.
 *   4. Update `catalog.pb` via [CatalogUpdater].
 *   5. Call [DeltaApplierClient.finalizeApply] to remove the marker/backup.
 *
 * A crash anywhere up to step 5 triggers [DeltaApplierClient.recoverIfNeeded]
 * on the next launch, restoring `seforim.db.backup`. The orchestrator is
 * idempotent: re-running on the same chain entry is safe because the
 * SQLite apply is `ON CONFLICT DO UPDATE` and the Lucene sinks idempotently
 * delete-then-add per line id.
 *
 * The chain is resumable at the **delta granularity** (DELTA_UPDATE_PLAN.md
 * §5.4): if delta 3/5 fails, delta 1 and 2 stay committed and the next
 * attempt continues from delta 3.
 */
class UpdateOrchestrator(
    private val applier: DeltaApplierClient,
    private val downloader: DeltaDownloader,
    private val luceneUpdater: LuceneUpdater,
    private val catalogUpdater: CatalogUpdater,
    private val workDir: Path,
    private val logger: Logger = Logger.withTag("UpdateOrchestrator"),
) {

    /**
     * Applies [chain] in order onto [seforimDb] / [catalogPb] / [luceneSinks].
     *
     * [fetchManifest] is the caller's hook to load a [DeltaManifest] from
     * [DeltaEntry.manifestUrl] (HTTP GET + JSON parse).
     */
    fun applyChain(
        seforimDb: Path,
        catalogPb: Path,
        chain: List<DeltaEntry>,
        fetchManifest: (DeltaEntry) -> DeltaManifest,
        baseUrlForEntry: (DeltaEntry, String) -> String,
        luceneSinks: () -> Pair<LuceneUpdater.DeleteSink, LuceneUpdater.UpsertSink>,
        progress: (current: Int, total: Int, status: String) -> Unit = { _, _, _ -> },
    ) {
        // First: recover from any half-applied previous run.
        applier.recoverIfNeeded(seforimDb)

        for ((idx, entry) in chain.withIndex()) {
            val step = idx + 1
            progress(step, chain.size, "downloading manifest")
            val manifest = fetchManifest(entry)
            val deltaDir = Files.createTempDirectory(workDir, "delta-v${entry.fromVersion}-")
            try {
                progress(step, chain.size, "downloading patch files")
                val patchFiles = manifest.patchFiles.map { f ->
                    downloader.download(
                        url = baseUrlForEntry(entry, f.file),
                        dest = deltaDir.resolve(f.file),
                        expectedSha256 = f.sha256,
                        expectedSize = f.size,
                    )
                }
                val mainPatch = patchFiles.firstOrNull { it.fileName.toString().contains("global") }
                    ?: patchFiles.first()

                progress(step, chain.size, "applying sqlite delta")
                applier.apply(seforimDb = seforimDb, patchDb = mainPatch, manifest = manifest)

                progress(step, chain.size, "updating lucene")
                val (delSink, upSink) = luceneSinks()
                luceneUpdater.applyTo(mainPatch, delSink, upSink)

                progress(step, chain.size, "updating catalog")
                manifest.catalogBlobName?.let { catalogUpdater.update(mainPatch, catalogPb, it) }

                applier.finalizeApply(seforimDb)
                logger.i { "Delta ${entry.fromVersion} → ${entry.toVersion} applied" }
            } finally {
                runCatching { deltaDir.toFile().deleteRecursively() }
            }
        }
        progress(chain.size, chain.size, "done")
    }

    companion object {
        val DEFAULT_JSON: Json = Json {
            ignoreUnknownKeys = true
            isLenient = true
        }
    }
}
