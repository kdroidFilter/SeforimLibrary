package io.github.kdroidfilter.seforimlibrary.common.notes

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentCompression
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import io.github.kdroidfilter.seforimlibrary.common.patch.SCHEMA_7_TABLE_DDL
import io.github.kdroidfilter.seforimlibrary.common.visibleUnits
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
    /** Lines left untouched: without their bodies a visible offset would no longer map back exactly. */
    val unmappableLines: Long,
    /** NULL version_line rows of changed lines given the old text (split shape only), so they keep it. */
    val versionRowsMaterialized: Long,
)

/**
 * Moves the inline footnote bodies of every base `line` into `line_note`, in one transaction.
 * `charPos` is the visible offset in the stripped line where the body stood.
 *
 * Per changed line: the bodies leave the text (marker sups stay) and `charCount` is recounted.
 * Line ids are kept. `version_line` and `link_anchor` are left as they were: anchor offsets keep
 * addressing the full line, i.e. the stored line with each body's visible chars at its `charPos`
 * (in noteIndex order), which is also what [LineNoteRehydration] gives back. A line for which
 * either reading differs from the original stays inline. A line that already has `line_note`
 * rows fails the run, so a second run over a built DB changes nothing.
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
    private var unmappable = 0L
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
    private val insertNote = prepare("INSERT INTO line_note (lineId, noteIndex, charPos, marker, body) VALUES (?, ?, ?, ?, ?)")
    private val updateContent = prepare(
        if (split) "UPDATE line_content SET content = ? WHERE id = ?" else "UPDATE line SET content = ? WHERE id = ?",
    )
    private val updateCharCount = prepare("UPDATE line SET charCount = ? WHERE id = ?")
    private val materializeVersions by lazy {
        prepare("UPDATE version_line SET content = ? WHERE lineId = ? AND content IS NULL")
    }
    private val hasNullableVersions = split && tableExists("version_line")

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
            scanned, withoutNote, changed, notes, unanchored, unclosed, orphanMarkers, unmappable, materialized,
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

        val charPos = ArrayList<Int>(found.size)
        var removedBefore = 0
        for (note in found) {
            val start = countVisibleChars(content, note.start)
            charPos += start - removedBefore
            removedBefore += countVisibleChars(content, note.end) - start
        }
        val moved = found.mapIndexed { index, note -> LineNoteRehydration.NoteAt(charPos[index], note.body, note.marker != null) }
        if (!keepsOffsets(content, stripped, moved)) {
            if (unmappable++ < SAMPLE) logger.w { "line_note: line $id would not map its visible offsets back; left inline" }
            return
        }
        found.forEachIndexed { index, note ->
            insertNote.setLong(1, id)
            insertNote.setInt(2, index)
            insertNote.setInt(3, charPos[index])
            insertNote.setString(4, note.marker)
            insertNote.setString(5, note.body)
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
        changed++
    }

    /** The original's visible chars equal both the stored line with the bodies spliced in and its rehydration. */
    private fun keepsOffsets(content: String, stripped: String, notes: List<LineNoteRehydration.NoteAt>): Boolean {
        val original = visibleUnits(content)
        val stored = visibleUnits(stripped)
        val spliced = ArrayList<String>(original.size)
        var cursor = 0
        for (note in notes) {
            if (note.charPos > stored.size) return false
            spliced.addAll(stored.subList(cursor, note.charPos))
            cursor = note.charPos
            spliced.addAll(visibleUnits(note.body))
        }
        spliced.addAll(stored.subList(cursor, stored.size))
        return spliced == original && visibleUnits(LineNoteRehydration.rehydrate(stripped, notes)) == original
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
