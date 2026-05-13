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
 * Failure handling has two layers:
 *
 *  - **In-process** — if any post-commit step (lucene, catalog, finalize)
 *    throws inside [applyChain], the orchestrator immediately calls
 *    [DeltaApplierClient.recoverIfNeeded] to roll the SQLite delta back
 *    before re-throwing. The caller observes a single all-or-nothing
 *    outcome per delta.
 *  - **Across process crashes** — if the JVM dies between SQLite COMMIT
 *    and [DeltaApplierClient.finalizeApply], the marker + backup persist
 *    on disk and the next launch's [DeltaApplierClient.recoverIfNeeded]
 *    restores `seforim.db` to its pre-apply state.
 *
 * The orchestrator is idempotent: re-running on the same chain entry is
 * safe because the SQLite apply is `ON CONFLICT DO UPDATE` and the
 * Lucene sinks idempotently delete-then-add per line id.
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
        luceneSinks: () -> LuceneUpdater.SinkSession,
        progress: (current: Int, total: Int, status: String) -> Unit = { _, _, _ -> },
    ) {
        // First: recover from any half-applied previous run.
        applier.recoverIfNeeded(seforimDb)

        for ((idx, entry) in chain.withIndex()) {
            val step = idx + 1
            progress(step, chain.size, "downloading manifest")
            val manifest = fetchManifest(entry)
            // Stable per-delta directory (no random suffix) so a failed
            // download's `.part` survives across orchestrator runs and the
            // downloader can resume from where it left off. Cleaned up
            // only on success — see `succeeded` flag below.
            val deltaDir = workDir.resolve("delta-v${entry.fromVersion}-v${entry.toVersion}")
            Files.createDirectories(deltaDir)
            var succeeded = false
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

                // From here SQLite has committed but marker + backup are
                // still on disk: if any post-commit step throws, roll back
                // the SQLite delta in-process via recoverIfNeeded() so the
                // process doesn't carry inconsistent state forward into a
                // retry. (Without this, the rollback would only happen on
                // the next app launch.)
                try {
                    progress(step, chain.size, "updating lucene")
                    luceneSinks().use { session ->
                        luceneUpdater.applyTo(mainPatch, session.delete, session.upsert)
                    }

                    progress(step, chain.size, "updating catalog")
                    manifest.catalogBlobName?.let { catalogUpdater.update(mainPatch, catalogPb, it) }

                    applier.finalizeApply(seforimDb)
                } catch (t: Throwable) {
                    runCatching { applier.recoverIfNeeded(seforimDb) }
                    logger.e(t) {
                        "Post-SQLite step failed for delta ${entry.fromVersion} → " +
                            "${entry.toVersion}; rolled back in-process"
                    }
                    throw t
                }
                logger.i { "Delta ${entry.fromVersion} → ${entry.toVersion} applied" }
                succeeded = true
            } finally {
                if (succeeded) {
                    runCatching { deltaDir.toFile().deleteRecursively() }
                }
                // else: leave the partial dir so the next retry's downloader
                // can resume from its `.part` files instead of restarting at
                // byte 0 — this is the contract DeltaDownloader.download
                // assumes when the same `dest` path is reused.
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
