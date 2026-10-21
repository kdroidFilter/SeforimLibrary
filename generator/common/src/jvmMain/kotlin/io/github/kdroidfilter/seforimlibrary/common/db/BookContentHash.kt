package io.github.kdroidfilter.seforimlibrary.common.db

import com.github.luben.zstd.Zstd
import io.github.kdroidfilter.seforimlibrary.common.notes.InlineNotes
import io.github.kdroidfilter.seforimlibrary.common.notes.LineNoteRehydration
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.security.MessageDigest
import java.sql.Connection

/**
 * Schema 7 `book.contentHash`: an opaque 64-bit fingerprint of everything that becomes a
 * book's indexed text, so a client can tell which books to re-index without reading them.
 *
 * Inputs, per book, in this order (SHA-256 over a length-prefixed big-endian stream):
 *  1. the tag [FORMAT_TAG];
 *  2. every line by `lineIndex`: `'L'`, lineIndex (i64), UTF-8 length (i32), the UTF-8 text —
 *     the decoded text when `line_content` holds a zstd frame, so compression never changes it;
 *  3. every `line_note` of the book by (lineIndex, noteIndex): `'N'`, lineIndex (i64),
 *     noteIndex (i64), charPos (i64), marker (0, or 1 + i32 length + UTF-8), body (i32 length + UTF-8,
 *     decoded like the line text). A row with `hasMarker` = 1 takes the inner HTML of the next
 *     marker sup of its line ([InlineNotes.markers]); a line's sups and such rows must pair up exactly.
 * The value is the first 8 digest bytes as a signed big-endian i64; 0 is reserved for
 * "unknown" and maps to 1. Row ids, heRef, tocEntryId and charCount are not inputs.
 */
object BookContentHash {
    const val COLUMN: String = "contentHash"
    const val UNKNOWN: Long = 0L

    /** Bumping it changes every book's value; do it only when the inputs above change. */
    const val FORMAT_TAG: String = "seforim-book-content-v1"

    /** The column exactly as Database.sq declares it. */
    fun hasColumn(conn: Connection): Boolean =
        conn.prepareStatement(
            "SELECT 1 FROM pragma_table_info('book') WHERE name = ? AND upper(type) = 'INTEGER' " +
                "AND \"notnull\" = 1 AND dflt_value = '0'",
        ).use { ps ->
            ps.setString(1, COLUMN)
            ps.executeQuery().use { it.next() }
        }

    /** Recomputes the column for every book; the caller owns the transaction. Returns the book count. */
    fun updateAll(conn: Connection): Int {
        check(hasColumn(conn)) { "book.$COLUMN INTEGER NOT NULL DEFAULT 0 is missing" }
        val split = LineContentShape.isSplit(conn)
        val bookIds = conn.createStatement().use { st ->
            st.executeQuery("SELECT id FROM book ORDER BY id").use { rs ->
                buildList { while (rs.next()) add(rs.getLong(1)) }
            }
        }
        Hasher(conn, split).use { hasher ->
            conn.prepareStatement("UPDATE book SET $COLUMN = ? WHERE id = ?").use { update ->
                for (bookId in bookIds) {
                    update.setLong(1, hasher.hash(bookId))
                    update.setLong(2, bookId)
                    update.addBatch()
                }
                update.executeBatch()
            }
        }
        return bookIds.size
    }

    /** [bookId]'s value as [updateAll] would store it. */
    fun compute(conn: Connection, bookId: Long): Long =
        Hasher(conn, LineContentShape.isSplit(conn)).use { it.hash(bookId) }

    private fun hasLineNote(conn: Connection): Boolean =
        conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'line_note'").use { ps ->
            ps.executeQuery().use { it.next() }
        }

    private class Hasher(conn: Connection, split: Boolean) : AutoCloseable {
        private val lines = conn.prepareStatement(
            "SELECT l.lineIndex, typeof(${LineContentShape.contentExpr(split)}), " +
                "CAST(${LineContentShape.contentExpr(split)} AS BLOB), l.id FROM line l " +
                "${LineContentShape.contentJoin(split)} WHERE l.bookId = ? ORDER BY l.lineIndex, l.id",
        )
        // Absent before schema 7; a book then simply has no note rows.
        private val notes = if (!hasLineNote(conn)) {
            null
        } else {
            conn.prepareStatement(
                "SELECT l.lineIndex, n.noteIndex, n.charPos, n.hasMarker, typeof(n.body), CAST(n.body AS BLOB), l.id " +
                    "FROM line l JOIN line_note n ON n.lineId = l.id WHERE l.bookId = ? " +
                    "ORDER BY l.lineIndex, l.id, n.noteIndex",
            )
        }
        private val markedLines = notes?.let {
            conn.prepareStatement(
                "SELECT n.lineId, sum(n.hasMarker) FROM line l JOIN line_note n ON n.lineId = l.id " +
                    "WHERE l.bookId = ? GROUP BY n.lineId",
            )
        }
        private val dictionaries = LineContentCompression.storedDictionaries(conn)
        private val decompressors = HashMap<Long, LineContentCompression.Decompressor>()

        fun hash(bookId: Long): Long {
            val digest = MessageDigest.getInstance("SHA-256")
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)
            fun flush() {
                out.flush()
                digest.update(buffer.toByteArray())
                buffer.reset()
            }
            out.write(FORMAT_TAG.toByteArray(Charsets.UTF_8))
            // Lines with notes, by id: how many of their notes have a marker.
            val anchored = HashMap<Long, Long>()
            markedLines?.setLong(1, bookId)
            markedLines?.executeQuery()?.use { rs -> while (rs.next()) anchored[rs.getLong(1)] = rs.getLong(2) }
            val markers = HashMap<Long, ArrayDeque<ByteArray>>()
            lines.setLong(1, bookId)
            lines.executeQuery().use { rs ->
                while (rs.next()) {
                    val text = rs.getBytes(3) ?: ByteArray(0)
                    val utf8 = if (rs.getString(2) == "blob") decode(text) else text
                    val lineId = rs.getLong(4)
                    anchored[lineId]?.let { expected ->
                        val found = InlineNotes.markers(utf8.toString(Charsets.UTF_8))
                        check(found.size.toLong() == expected) {
                            "line $lineId: $expected line_note rows with a marker, ${found.size} marker sups in its text"
                        }
                        markers[lineId] = ArrayDeque(found.map { it.toByteArray(Charsets.UTF_8) })
                    }
                    out.writeByte('L'.code)
                    out.writeLong(rs.getLong(1))
                    out.writeInt(utf8.size)
                    out.write(utf8)
                    flush()
                }
            }
            if (notes != null) {
                notes.setLong(1, bookId)
                notes.executeQuery().use { rs ->
                    while (rs.next()) {
                        out.writeByte('N'.code)
                        out.writeLong(rs.getLong(1))
                        out.writeLong(rs.getLong(2))
                        out.writeLong(rs.getLong(3))
                        if (!LineNoteRehydration.hasMarker(rs.getObject(4))) {
                            out.writeByte(0)
                        } else {
                            val marker = markers.getValue(rs.getLong(7)).removeFirst()
                            out.writeByte(1)
                            out.writeInt(marker.size)
                            out.write(marker)
                        }
                        val stored = rs.getBytes(6) ?: ByteArray(0)
                        val body = if (rs.getString(5) == "blob") decode(stored) else stored
                        out.writeInt(body.size)
                        out.write(body)
                        flush()
                    }
                }
            }
            flush()
            val bytes = digest.digest()
            var value = 0L
            for (i in 0 until 8) value = (value shl 8) or (bytes[i].toLong() and 0xFF)
            return if (value == UNKNOWN) 1L else value
        }

        private fun decode(frame: ByteArray): ByteArray {
            val dictId = Zstd.getDictIdFromFrame(frame)
            val decompressor = decompressors.getOrPut(dictId) {
                val dict = checkNotNull(dictionaries[dictId]) { "zstd frame names dictionary $dictId, not in zstd_dict" }
                LineContentCompression.Decompressor(dict)
            }
            return decompressor.decompress(frame)
        }

        override fun close() {
            lines.close()
            notes?.close()
            markedLines?.close()
            decompressors.values.forEach { it.close() }
        }
    }
}
