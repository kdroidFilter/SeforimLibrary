package io.github.kdroidfilter.seforimlibrary.deltaupdater

import kotlinx.serialization.Serializable

/**
 * Manifest of one delta: tells the client what versions the patch bridges,
 * which patch files to download, what their sha256 are, and what the
 * expected logical content hash is before and after apply.
 *
 * See `DELTA_UPDATE_PLAN.md` §5.2.
 */
@Serializable
data class DeltaManifest(
    val fromVersion: Int,
    val toVersion: Int,
    val fromSchemaVersion: Int,
    val toSchemaVersion: Int,
    val fromContentHash: String,
    val toContentHash: String,
    /**
     * Books touched / added / removed / renamed between fromVersion and toVersion.
     * Empty when the diff has no per-book scope (e.g. lookup-only changes).
     */
    val booksTouched: List<Long> = emptyList(),
    val booksRenamed: List<RenameEntry> = emptyList(),
    val patchFiles: List<PatchFileEntry>,
    /** Pointer into `patch_global.db.blobs` for the new `catalog.pb`. */
    val catalogBlobName: String? = null,
)

@Serializable
data class RenameEntry(
    val oldKey: String,
    val newKey: String,
    val bookId: Long,
)

@Serializable
data class PatchFileEntry(
    val file: String,
    val sha256: String,
    val size: Long,
)

/**
 * Release-level metadata served by the server. Lists every published delta
 * so the client can pick a chain (or fall back to the full bundle).
 *
 * See `DELTA_UPDATE_PLAN.md` §5.4.
 */
@Serializable
data class ReleaseMeta(
    val latestVersion: Int,
    val fullBundle: FullBundleEntry,
    val deltas: List<DeltaEntry>,
    val retentionWindow: Int = 30,
)

@Serializable
data class FullBundleEntry(
    val version: Int,
    val url: String,
    val sha256: String,
    val size: Long,
)

@Serializable
data class DeltaEntry(
    val fromVersion: Int,
    val toVersion: Int,
    val manifestUrl: String,
    /** Total bytes of all patch files in this delta — used by [chooseUpdatePath]. */
    val totalSize: Long,
)
