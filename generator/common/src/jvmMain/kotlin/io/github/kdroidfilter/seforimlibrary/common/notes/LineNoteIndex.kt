package io.github.kdroidfilter.seforimlibrary.common.notes

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentCompression
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import io.github.kdroidfilter.seforimlibrary.common.patch.SCHEMA_7_TABLE_DDL
import java.sql.Connection

data class LineNoteIndexReport(
    /** Lines the pre-filter selected (any case of "footnote"). */
    val linesScanned: Long,
    /** Selected lines in which the parser found no note body. */
    val linesWithoutNote: Long,
    val linesChanged: Long,
    val notes: Long,
    val unanchoredNotes: Long,
    /** Lines left untouched: a body without its `</i>`. */
    val unclosedLines: Long,
    /** Lines left untouched: a marker sup not followed by a note would break the k-th marker rule. */
    val orphanMarkerLines: Long,
    val anchorsRemapped: Long,
    /** Range anchors with one end inside a removed body, cut back to the body's edge. */
    val anchorsClipped: Long,
    /** Point anchors inside a removed body, and ranges left empty by it. */
    val anchorsDroppedInBody: Long,
    /** Anchors whose shifted position collides with another anchor of the same link and side. */
    val anchorsDroppedOnCollision: Long,
    /** NULL version_line rows of changed lines given the old text (split shape only), so they keep it. */
    val versionRowsMaterialized: Long,
)

/**
 * Moves the inline footnote bodies of every base `line` into `line_note`, in one transaction.
 *
 * Per changed line: the bodies leave the text (marker sups stay), `charCount` is recounted,
 * and its `link_anchor` rows are shifted by the visible length of the bodies removed before
 * them. Line ids are kept. `version_line` is left as it was. A line that already has
 * `line_note` rows fails the run, so a second run over a built DB changes nothing.
 */
fun rebuildLineNoteIndex(conn: Connection, logger: Logger, batchSize: Int = 2_000): LineNoteIndexReport {
    check(conn.autoCommit) { "rebuildLineNoteIndex requires an unowned JDBC connection" }
    check(!LineContentCompression.isCompressed(conn)) {
        "line_note must run before compressLineContent (line text is zstd frames)"
    }
    val split = LineContentShape.isSplit(conn)
    conn.autoCommit = false
    return try {
        conn.createStatement().use { it.execute(SCHEMA_7_TABLE_DDL.getValue("line_note")) }
        LineNotePass(conn, logger, split, batchSize).use { it.run() }.also { conn.commit() }
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

private class LineNotePass(
    private val conn: Connection,
    private val logger: Logger,
    private val split: Boolean,
    private val batchSize: Int,
) : AutoCloseable {
    private var scanned = 0L
    private var withoutNote = 0L
    private var changed = 0L
    private var notes = 0L
    private var unanchored = 0L
    private var unclosed = 0L
    private var orphanMarkers = 0L
    private var remapped = 0L
    private var clipped = 0L
    private var droppedInBody = 0L
    private var droppedOnCollision = 0L
    private var materialized = 0L
    private var batches = 0
    private val opened = ArrayList<java.sql.PreparedStatement>()

    private fun prepare(sql: String) = conn.prepareStatement(sql).also { opened += it }

    private val select = prepare(
        if (split) {
            "SELECT l.id, c.content FROM line l JOIN line_content c ON c.id = l.id " +
                "WHERE l.id > ? AND instr(lower(c.content), 'footnote') > 0 ORDER BY l.id LIMIT ?"
        } else {
            "SELECT id, content FROM line WHERE id > ? AND instr(lower(content), 'footnote') > 0 ORDER BY id LIMIT ?"
        },
    )
    private val existing = prepare("SELECT 1 FROM line_note WHERE lineId = ? LIMIT 1")
    private val insertNote = prepare("INSERT INTO line_note (lineId, noteIndex, marker, body) VALUES (?, ?, ?, ?)")
    private val updateContent = prepare(
        if (split) "UPDATE line_content SET content = ? WHERE id = ?" else "UPDATE line SET content = ? WHERE id = ?",
    )
    private val updateCharCount = prepare("UPDATE line SET charCount = ? WHERE id = ?")
    private val materializeVersions by lazy {
        prepare("UPDATE version_line SET content = ? WHERE lineId = ? AND content IS NULL")
    }
    private val selectAnchors by lazy {
        prepare(
            "SELECT a.linkId, a.side, a.charStart, a.charEnd, a.label FROM link l " +
                "JOIN link_anchor a ON a.linkId = l.id AND a.side = 0 WHERE l.sourceLineId = ? " +
                "UNION ALL SELECT a.linkId, a.side, a.charStart, a.charEnd, a.label FROM link l " +
                "JOIN link_anchor a ON a.linkId = l.id AND a.side = 1 WHERE l.targetLineId = ?",
        )
    }
    private val deleteAnchor by lazy { prepare("DELETE FROM link_anchor WHERE linkId = ? AND side = ? AND charStart = ?") }
    private val insertAnchor by lazy {
        prepare("INSERT OR IGNORE INTO link_anchor (linkId, side, charStart, charEnd, label) VALUES (?, ?, ?, ?, ?)")
    }
    private val hasAnchors = tableExists("link_anchor") && tableExists("link")
    private val hasNullableVersions = split && tableExists("version_line")

    private class Anchor(
        val linkId: Long, val side: Int, val start: Int, val end: Int?, val label: String?, val clipped: Boolean = false,
    )

    fun run(): LineNoteIndexReport {
        var lastId = Long.MIN_VALUE
        while (true) {
            select.setLong(1, lastId)
            select.setInt(2, batchSize)
            val batch = select.executeQuery().use { rs ->
                buildList { while (rs.next()) add(rs.getLong(1) to rs.getString(2)) }
            }
            if (batch.isEmpty()) break
            for ((id, content) in batch) processLine(id, content)
            lastId = batch.last().first
            if (++batches % 50 == 0) logger.i { "line_note: $scanned lines with notes scanned, $changed changed" }
        }
        val report = LineNoteIndexReport(
            scanned, withoutNote, changed, notes, unanchored, unclosed, orphanMarkers,
            remapped, clipped, droppedInBody, droppedOnCollision, materialized,
        )
        logger.i { "line_note: $report" }
        return report
    }

    private fun processLine(id: Long, content: String) {
        scanned++
        val found = when (val parsed = InlineNotes.parse(content)) {
            is InlineNotes.Parsed.Unclosed -> {
                if (unclosed++ < SAMPLE) logger.w { "line_note: line $id has an unclosed note body at ${parsed.at}; left inline" }
                return
            }
            is InlineNotes.Parsed.Notes -> parsed.notes
        }
        if (found.isEmpty()) {
            withoutNote++
            return
        }
        val stripped = InlineNotes.strip(content, found)
        if (InlineNotes.countMarkers(stripped) != found.count { it.marker != null }) {
            if (orphanMarkers++ < SAMPLE) logger.w { "line_note: line $id has a marker sup without a note; left inline" }
            return
        }
        existing.setLong(1, id)
        check(existing.executeQuery().use { !it.next() }) { "line $id still carries note bodies but already has line_note rows" }

        found.forEachIndexed { index, note ->
            insertNote.setLong(1, id)
            insertNote.setInt(2, index)
            insertNote.setString(3, note.marker)
            insertNote.setString(4, note.body)
            insertNote.addBatch()
        }
        insertNote.executeBatch()
        notes += found.size
        unanchored += found.count { it.marker == null }

        if (hasNullableVersions) {
            materializeVersions.setString(1, content)
            materializeVersions.setLong(2, id)
            materialized += materializeVersions.executeUpdate()
        }
        updateContent.setString(1, stripped)
        updateContent.setLong(2, id)
        check(updateContent.executeUpdate() == 1) { "line $id: content row not updated" }
        updateCharCount.setInt(1, countVisibleChars(stripped))
        updateCharCount.setLong(2, id)
        updateCharCount.executeUpdate()
        if (hasAnchors) remapAnchors(id, found.map { countVisibleChars(content, it.start) to countVisibleChars(content, it.end) })
        changed++
    }

    /** [removed]: visible [start, end) of each removed body, in order. */
    private fun remapAnchors(lineId: Long, removed: List<Pair<Int, Int>>) {
        selectAnchors.setLong(1, lineId)
        selectAnchors.setLong(2, lineId)
        val anchors = selectAnchors.executeQuery().use { rs ->
            buildList {
                while (rs.next()) {
                    add(Anchor(rs.getLong(1), rs.getInt(2), rs.getInt(3), rs.getObject(4)?.let { rs.getInt(4) }, rs.getString(5)))
                }
            }
        }
        val moves = ArrayList<Pair<Anchor, Anchor?>>()
        for (a in anchors) {
            val start = shift(a.start, removed)
            val new = if (a.end == null) {
                start.takeUnless { start.inside }?.let { Anchor(a.linkId, a.side, it.value, null, a.label) }
            } else {
                val end = shift(a.end, removed)
                if (end.value <= start.value) null
                else Anchor(a.linkId, a.side, start.value, end.value, a.label, clipped = start.inside || end.inside)
            }
            when {
                new == null -> moves += a to null
                new.start != a.start || new.end != a.end -> moves += a to new
            }
        }
        if (moves.isEmpty()) return
        for ((old, _) in moves) {
            deleteAnchor.setLong(1, old.linkId)
            deleteAnchor.setInt(2, old.side)
            deleteAnchor.setInt(3, old.start)
            deleteAnchor.executeUpdate()
        }
        for ((_, new) in moves) {
            if (new == null) { droppedInBody++; continue }
            insertAnchor.setLong(1, new.linkId)
            insertAnchor.setInt(2, new.side)
            insertAnchor.setInt(3, new.start)
            if (new.end == null) insertAnchor.setNull(4, java.sql.Types.INTEGER) else insertAnchor.setInt(4, new.end)
            insertAnchor.setString(5, new.label)
            if (insertAnchor.executeUpdate() == 1) {
                remapped++
                if (new.clipped) clipped++
            } else {
                droppedOnCollision++
            }
        }
    }

    /** [value]: position after the removal; one strictly inside a removed body moves to that body's start. */
    private class Shifted(val value: Int, val inside: Boolean)

    private fun shift(x: Int, removed: List<Pair<Int, Int>>): Shifted {
        var delta = 0
        for ((start, end) in removed) {
            when {
                x >= end -> delta += end - start
                x > start -> return Shifted(start - delta, inside = true)
            }
        }
        return Shifted(x - delta, inside = false)
    }

    override fun close() {
        opened.forEach { it.close() }
    }

    private fun tableExists(name: String): Boolean =
        conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { it.next() }
        }

    private companion object {
        const val SAMPLE = 20
    }
}
