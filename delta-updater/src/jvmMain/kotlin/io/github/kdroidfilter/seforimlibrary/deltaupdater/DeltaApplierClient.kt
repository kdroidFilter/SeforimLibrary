package io.github.kdroidfilter.seforimlibrary.deltaupdater

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.patch.PatchApplier
import io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.DriverManager

/**
 * Client-side wrapper around [PatchApplier] that adds:
 *
 *  - **File backup** of the live `seforim.db` before any write, with a
 *    marker file. If the JVM crashes between the SQLite COMMIT and the end
 *    of post-apply work (Lucene / catalog), [recoverIfNeeded] on the next
 *    launch restores the backup and we re-apply the delta cleanly.
 *  - **Content-hash check** against the manifest (both `fromContentHash` and
 *    `toContentHash`).
 *  - Deterministic ordering: any failure rolls back the SQLite transaction
 *    AND restores the backup before re-raising.
 *
 * Lucene + catalog updates are NOT done here — they are the orchestrator's
 * job, sequenced after a successful SQLite apply.
 *
 * See `DELTA_UPDATE_PLAN.md` §7.3 and §7.5.
 */
class DeltaApplierClient(
    private val logger: Logger = Logger.withTag("DeltaApplierClient"),
) {

    data class Result(
        val applied: PatchApplier.Result,
        val backupPath: Path,
        val markerPath: Path,
    )

    fun apply(
        seforimDb: Path,
        patchDb: Path,
        manifest: DeltaManifest,
    ): Result {
        // 1. Read current logical content hash and require it matches manifest.from.
        val currentHash = LogicalContentHasher().compute(openConn(seforimDb))
        if (currentHash != manifest.fromContentHash) {
            throw IllegalStateException(
                "Local DB content_hash ($currentHash) does not match manifest.fromContentHash " +
                    "(${manifest.fromContentHash}). Refusing to apply.",
            )
        }

        // 2. File backup + marker so recoverIfNeeded() can roll back after a crash.
        val backup = seforimDb.resolveSibling("${seforimDb.fileName}.backup")
        val marker = seforimDb.resolveSibling("${seforimDb.fileName}.applying")
        Files.copy(seforimDb, backup, StandardCopyOption.REPLACE_EXISTING)
        Files.writeString(marker, "from=${manifest.fromVersion} to=${manifest.toVersion}")
        logger.i { "Backup written to $backup, marker at $marker" }

        try {
            // 3. Apply the patch in a single transaction, verifying to-hash.
            openConn(seforimDb).use { conn ->
                conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
                val applied = PatchApplier(logger).apply(
                    conn = conn,
                    patchDb = patchDb,
                    expectedToContentHash = manifest.toContentHash,
                )
                return Result(applied = applied, backupPath = backup, markerPath = marker)
            }
        } catch (t: Throwable) {
            // Restore from backup so the file is in its pre-apply state,
            // then clear the marker + backup pair: the in-process catch has
            // fully reverted us, so a later recoverIfNeeded() must not log a
            // misleading "recovered from interrupted apply" warning.
            Files.copy(backup, seforimDb, StandardCopyOption.REPLACE_EXISTING)
            runCatching { Files.deleteIfExists(marker) }
            runCatching { Files.deleteIfExists(backup) }
            logger.e(t) { "Apply failed — backup restored, marker + backup cleared" }
            throw t
        }
    }

    /**
     * Removes the marker + backup pair after the orchestrator has finished
     * everything (Lucene, catalog, etc.). Until this is called, a subsequent
     * launch will see the marker and roll back.
     */
    fun finalizeApply(seforimDb: Path) {
        val backup = seforimDb.resolveSibling("${seforimDb.fileName}.backup")
        val marker = seforimDb.resolveSibling("${seforimDb.fileName}.applying")
        runCatching { Files.deleteIfExists(marker) }
        runCatching { Files.deleteIfExists(backup) }
    }

    /**
     * Called at app startup. If a marker file is present, the previous run
     * crashed between SQLite COMMIT and the orchestrator's cleanup → restore
     * the backup so the chain can be retried cleanly.
     */
    fun recoverIfNeeded(seforimDb: Path): Boolean {
        val backup = seforimDb.resolveSibling("${seforimDb.fileName}.backup")
        val marker = seforimDb.resolveSibling("${seforimDb.fileName}.applying")
        if (!Files.exists(marker) || !Files.exists(backup)) return false
        Files.copy(backup, seforimDb, StandardCopyOption.REPLACE_EXISTING)
        Files.deleteIfExists(backup)
        Files.deleteIfExists(marker)
        logger.w { "Recovered from interrupted delta apply: restored $seforimDb from backup" }
        return true
    }

    private fun openConn(path: Path) =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}")
}
