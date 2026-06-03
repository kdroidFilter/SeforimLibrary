package io.github.kdroidfilter.seforimlibrary.deltaupdater

import co.touchlab.kermit.Logger
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Surface that applies a delta to a Lucene index.
 *
 * The actual Lucene-side implementation lives in the search module (or in
 * SeforimApp, where it has access to the `Document` builder used by the
 * client's [LuceneTextIndexWriter]). This class encapsulates the read side:
 *
 *  - Iterates `patch.delete_line.id` → forwards to [DeleteSink.deleteLineById].
 *  - Iterates `patch.upsert_line.*` → forwards to [UpsertSink.upsertLine].
 *
 * The two sinks are passed in as lambdas so the search-module dep doesn't
 * have to leak here.
 *
 * See `DELTA_UPDATE_PLAN.md` §7.6.
 */
class LuceneUpdater(
    private val logger: Logger = Logger.withTag("LuceneUpdater"),
) {

    fun interface DeleteSink {
        fun deleteLineById(lineId: Long)
    }

    fun interface UpsertSink {
        fun upsertLine(line: PatchLine)
    }

    /**
     * Bundles a [DeleteSink] + [UpsertSink] with a deterministic close hook.
     * The orchestrator obtains one of these per delta via the caller's
     * provider lambda and closes it after [applyTo] returns, so the caller
     * can commit + close a Lucene `IndexWriter` (or any other resource)
     * exactly when it needs to. Without this, naive providers leak open
     * writers — and silently drop updates that were never committed.
     */
    class SinkSession(
        val delete: DeleteSink,
        val upsert: UpsertSink,
        private val onClose: () -> Unit = {},
    ) : AutoCloseable {
        override fun close() = onClose()
    }

    data class PatchLine(
        val id: Long,
        val bookId: Long,
        val lineIndex: Int,
        val content: String,
        val heRef: String?,
    )

    fun applyTo(
        patchDb: Path,
        deletes: DeleteSink,
        upserts: UpsertSink,
        liveDbPath: Path? = null,
    ): Stats {
        DriverManager.getConnection("jdbc:sqlite:${patchDb.toAbsolutePath()}").use { conn ->
            val deleted = forEachDelete(conn, deletes)
            val upserted = forEachUpsert(conn, upserts)
            // Some patches change book-level metadata (title, categoryId,
            // orderIndex, isBaseBook, …) WITHOUT touching any of the book's
            // lines. The line-level loop above wouldn't see those, so the
            // Lucene Document's stored fields (book_title, category_id, …)
            // would stay at the pre-patch values. Re-index every line of
            // those books so the search-side metadata stays in sync.
            val metaReindexed = if (liveDbPath != null) {
                reindexBooksWithMetadataOnlyChange(conn, liveDbPath, upserts)
            } else 0
            logger.i { "Lucene delta — deletes=$deleted, upserts=$upserted, meta-reindexed lines=$metaReindexed" }
            return Stats(deleted, upserted + metaReindexed)
        }
    }

    private fun reindexBooksWithMetadataOnlyChange(
        patchConn: Connection,
        liveDbPath: Path,
        sink: UpsertSink,
    ): Int {
        if (!hasTable(patchConn, "upsert_book")) return 0
        // Collect book ids touched at metadata level but not via line ops.
        val booksTouched = mutableListOf<Long>()
        patchConn.prepareStatement(
            """
            SELECT b.id FROM upsert_book b
            WHERE NOT EXISTS (
                SELECT 1 FROM upsert_line ul WHERE ul.bookId = b.id
            )
            """.trimIndent()
        ).use { ps ->
            ps.executeQuery().use { rs -> while (rs.next()) booksTouched += rs.getLong(1) }
        }
        if (booksTouched.isEmpty()) return 0
        var n = 0
        DriverManager.getConnection("jdbc:sqlite:${liveDbPath.toAbsolutePath()}").use { live ->
            live.prepareStatement(
                "SELECT id, bookId, lineIndex, content, heRef FROM line WHERE bookId = ?",
            ).use { ps ->
                for (bookId in booksTouched) {
                    ps.setLong(1, bookId)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            sink.upsertLine(
                                PatchLine(
                                    id = rs.getLong(1),
                                    bookId = rs.getLong(2),
                                    lineIndex = rs.getInt(3),
                                    content = rs.getString(4),
                                    heRef = rs.getString(5),
                                ),
                            )
                            n++
                        }
                    }
                }
            }
        }
        return n
    }

    data class Stats(val deletes: Int, val upserts: Int)

    private fun forEachDelete(conn: Connection, sink: DeleteSink): Int {
        if (!hasTable(conn, "delete_line")) return 0
        var n = 0
        conn.createStatement().use { st ->
            st.executeQuery("SELECT id FROM delete_line").use { rs ->
                while (rs.next()) {
                    sink.deleteLineById(rs.getLong(1))
                    n++
                }
            }
        }
        return n
    }

    private fun forEachUpsert(conn: Connection, sink: UpsertSink): Int {
        if (!hasTable(conn, "upsert_line")) return 0
        var n = 0
        conn.createStatement().use { st ->
            st.executeQuery(
                "SELECT id, bookId, lineIndex, content, heRef FROM upsert_line",
            ).use { rs ->
                while (rs.next()) {
                    sink.upsertLine(
                        PatchLine(
                            id = rs.getLong(1),
                            bookId = rs.getLong(2),
                            lineIndex = rs.getInt(3),
                            content = rs.getString(4),
                            heRef = rs.getString(5),
                        ),
                    )
                    n++
                }
            }
        }
        return n
    }

    private fun hasTable(conn: Connection, name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }
}
