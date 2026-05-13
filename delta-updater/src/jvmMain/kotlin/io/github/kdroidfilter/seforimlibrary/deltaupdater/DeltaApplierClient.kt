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
        // 1. Schema-version gate: the manifest declares the seforim.db
        //    schema this delta was produced against. If the local DB
        //    carries a different version, applying would produce a
        //    mixed-schema DB. Refuse and ask the user to take the full
        //    bundle instead (which the orchestrator's path chooser would
        //    have selected if the operator had set retentionWindow
        //    correctly — this guard is the last line of defence).
        openConn(seforimDb).use { conn ->
            val localSchemaVersion = readSchemaMetaInt(conn, "db_schema_version")
            if (localSchemaVersion != null && localSchemaVersion != manifest.fromSchemaVersion) {
                throw IllegalStateException(
                    "Local DB schema_meta.db_schema_version=$localSchemaVersion does not match " +
                        "manifest.fromSchemaVersion=${manifest.fromSchemaVersion}. " +
                        "Refusing to apply — request a full bundle download instead.",
                )
            }
        }

        // 2. Read current logical content hash and require it matches manifest.from.
        val currentHash = LogicalContentHasher().compute(openConn(seforimDb))
        if (currentHash != manifest.fromContentHash) {
            throw IllegalStateException(
                "Local DB content_hash ($currentHash) does not match manifest.fromContentHash " +
                    "(${manifest.fromContentHash}). Refusing to apply.",
            )
        }

        // 2. Disk-space pre-flight: the apply needs room for a full backup
        //    copy of seforim.db plus growth headroom for the patch itself.
        //    Fail fast with a clear message rather than ENOSPC-ing mid-copy
        //    and leaving a truncated backup on disk.
        assertEnoughFreeSpace(seforimDb, patchDb)

        // 3. File backup + marker so recoverIfNeeded() can roll back after a crash.
        //    The marker doubles as a concurrency lock: if it already exists,
        //    either (a) a previous run crashed and recoverIfNeeded() wasn't
        //    called yet, or (b) another apply is in flight. Both cases mean
        //    "do not run a fresh apply right now". Callers must either invoke
        //    recoverIfNeeded() at boot or serialise their apply calls.
        val backup = seforimDb.resolveSibling("${seforimDb.fileName}.backup")
        val marker = seforimDb.resolveSibling("${seforimDb.fileName}.applying")
        if (Files.exists(marker)) {
            throw IllegalStateException(
                "Apply marker already present at $marker: another apply is in flight or " +
                    "a previous run crashed without recovery. Call recoverIfNeeded() at app " +
                    "startup, or wait for the in-flight apply to finish, before retrying.",
            )
        }
        Files.copy(seforimDb, backup, StandardCopyOption.REPLACE_EXISTING)
        // Use CREATE_NEW so two concurrent threads that both passed the
        // existence check above can't both end up holding the marker — the
        // second writeString call will throw FileAlreadyExistsException.
        Files.writeString(
            marker,
            "from=${manifest.fromVersion} to=${manifest.toVersion}",
            java.nio.file.StandardOpenOption.CREATE_NEW,
            java.nio.file.StandardOpenOption.WRITE,
        )
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

    /**
     * Reads a single integer-valued key from `seforim.db.schema_meta`.
     * Returns `null` when the key is absent — older DBs never wrote one,
     * so absence means "trust the operator" (we don't refuse to apply).
     */
    private fun readSchemaMetaInt(conn: java.sql.Connection, key: String): Int? = runCatching {
        conn.prepareStatement("SELECT value FROM schema_meta WHERE key = ?").use { ps ->
            ps.setString(1, key)
            ps.executeQuery().use { rs ->
                if (rs.next()) rs.getString(1)?.toIntOrNull() else null
            }
        }
    }.getOrNull()

    /**
     * Verifies the partition holding [seforimDb] has enough free space to
     * hold a full backup copy plus enough headroom for the patch to inflate
     * the live DB. Required ≈ size(seforimDb) + size(patchDb) + 64 MB
     * (covers WAL + temp files on commit).
     *
     * Visible for testing and for callers that want to surface a
     * disk-space prompt to the user before invoking [apply].
     */
    internal fun assertEnoughFreeSpace(seforimDb: Path, patchDb: Path) {
        val dbSize = Files.size(seforimDb)
        val patchSize = if (Files.exists(patchDb)) Files.size(patchDb) else 0L
        val headroom = 64L * 1024 * 1024
        val required = dbSize + patchSize + headroom
        val available = Files.getFileStore(seforimDb.toAbsolutePath().parent).usableSpace
        if (available < required) {
            throw IllegalStateException(
                "Not enough free space to apply delta: required ${humanBytes(required)} " +
                    "(backup ${humanBytes(dbSize)} + patch ${humanBytes(patchSize)} + " +
                    "${humanBytes(headroom)} headroom), available ${humanBytes(available)} " +
                    "on ${seforimDb.toAbsolutePath().parent}",
            )
        }
    }

    private fun humanBytes(n: Long): String {
        if (n < 1024) return "$n B"
        val units = arrayOf("KiB", "MiB", "GiB", "TiB")
        var d = n.toDouble() / 1024.0
        var i = 0
        while (d >= 1024.0 && i < units.size - 1) { d /= 1024.0; i++ }
        return "%.1f %s".format(d, units[i])
    }
}
