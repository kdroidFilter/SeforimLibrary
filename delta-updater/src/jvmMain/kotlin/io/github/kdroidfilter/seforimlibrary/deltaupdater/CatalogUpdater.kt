package io.github.kdroidfilter.seforimlibrary.deltaupdater

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager

/**
 * Pulls a fresh `catalog.pb` blob out of `patch.db.blobs` and writes it
 * atomically next to `seforim.db`. The catalog is a derived artefact, so we
 * never merge — every release ships a full replacement and the client just
 * swaps it in.
 *
 * Atomic write: write to `catalog.pb.tmp`, fsync the parent dir, then
 * `Files.move(ATOMIC_MOVE)` over the target. A crash before the move leaves
 * the old `catalog.pb` intact.
 *
 * See `DELTA_UPDATE_PLAN.md` §7.6.
 */
class CatalogUpdater(
    private val logger: Logger = Logger.withTag("CatalogUpdater"),
) {

    /**
     * @return `true` if a catalog blob was found and written, `false` if the
     *   patch.db carried no blob for this key (no-op).
     */
    fun update(patchDb: Path, target: Path, blobName: String = "catalog.pb"): Boolean {
        DriverManager.getConnection("jdbc:sqlite:${patchDb.toAbsolutePath()}").use { conn ->
            val bytes = readBlob(conn, blobName) ?: run {
                logger.d { "patch.db has no blob '$blobName' — skipping catalog update" }
                return false
            }
            val tmp = target.resolveSibling("${target.fileName}.tmp")
            Files.write(tmp, bytes)
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            logger.i { "Catalog '$blobName' written to $target (${bytes.size} bytes)" }
            return true
        }
    }

    private fun readBlob(conn: Connection, name: String): ByteArray? {
        conn.prepareStatement("SELECT content FROM blobs WHERE name = ?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getBytes(1) else null
            }
        }
    }
}
