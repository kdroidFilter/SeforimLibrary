package io.github.kdroidfilter.seforimlibrary.common.buildstate

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Persists an in-RAM [BuildStateSnapshot] to disk.
 *
 * The write is done atomically: we write to `<target>.tmp` and rename at the
 * very end so a crash mid-write never leaves a partial file in place.
 */
class BuildStateWriter(private val logger: Logger = Logger.withTag("BuildStateWriter")) {

    fun write(snapshot: BuildStateSnapshot, target: Path) {
        Files.createDirectories(target.toAbsolutePath().parent)
        val tmp = target.resolveSibling("${target.fileName}.tmp")
        if (Files.exists(tmp)) Files.delete(tmp)

        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${tmp.toAbsolutePath()}").use { conn ->
            conn.autoCommit = false
            applyDdl(conn)
            writeAll(conn, snapshot)
            conn.commit()
            // Compact the file so the published artifact is small.
            conn.autoCommit = true
            conn.createStatement().use { it.execute("VACUUM") }
        }

        Files.move(
            tmp,
            target,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
            java.nio.file.StandardCopyOption.ATOMIC_MOVE,
        )
        logger.i {
            "build_state.db snapshot written to $target (" +
                "books=${snapshot.books.size}, lines=${snapshot.lines.size}, " +
                "tocEntries=${snapshot.tocEntries.size}, links=${snapshot.links.size})"
        }
    }

    private fun applyDdl(conn: Connection) {
        conn.createStatement().use { st ->
            BuildStateSchema.statements.forEach { st.executeUpdate(it) }
        }
    }

    private fun writeAll(conn: Connection, snapshot: BuildStateSnapshot) {
        writeMeta(conn, snapshot)
        writeCounters(conn, snapshot.counters)
        writeLookups(conn, snapshot.lookups)
        writeBooks(conn, snapshot.books)
        writeLines(conn, snapshot.lines)
        writeTocEntries(conn, snapshot.tocEntries)
        writeAltTocStructures(conn, snapshot.altTocStructures)
        writeAltTocEntries(conn, snapshot.altTocEntries)
        writeLinks(conn, snapshot.links)
        writeAliases(conn, snapshot.bookAliases)
        writeSourceHashes(conn, snapshot.sourceHashes)
    }

    private fun writeSourceHashes(conn: Connection, hashes: Map<BookKey, BookSourceHash>) {
        if (hashes.isEmpty()) return
        conn.prepareStatement(
            """
            INSERT INTO book_source_hashes(
                source_name, canonical_he_title, source_hash, last_seen_version
            ) VALUES (?, ?, ?, ?)
            """.trimIndent(),
        ).use { ps ->
            for ((key, hash) in hashes) {
                ps.setString(1, key.sourceName)
                ps.setString(2, key.canonicalHeTitle)
                ps.setBytes(3, hash.hash)
                ps.setInt(4, hash.lastSeenVersion)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeMeta(conn: Connection, snapshot: BuildStateSnapshot) {
        conn.prepareStatement("INSERT INTO meta(key, value) VALUES (?, ?)").use { ps ->
            ps.setString(1, "schema_version")
            ps.setString(2, snapshot.schemaVersion.toString())
            ps.executeUpdate()
            for ((k, v) in snapshot.meta) {
                if (k == "schema_version") continue
                ps.setString(1, k)
                ps.setString(2, v)
                ps.executeUpdate()
            }
        }
    }

    private fun writeCounters(conn: Connection, counters: Map<IdTable, Long>) {
        conn.prepareStatement(
            "INSERT INTO id_counters(table_name, next_id) VALUES (?, ?)",
        ).use { ps ->
            for ((table, next) in counters) {
                ps.setString(1, table.tableName)
                ps.setLong(2, next)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeLookups(conn: Connection, lookups: Map<IdTable, Map<String, Long>>) {
        conn.prepareStatement(
            "INSERT INTO id_lookup(kind, natural_key, id) VALUES (?, ?, ?)",
        ).use { ps ->
            for ((table, map) in lookups) {
                val kind = table.lookupKind ?: continue
                for ((key, id) in map) {
                    ps.setString(1, kind)
                    ps.setString(2, key)
                    ps.setLong(3, id)
                    ps.addBatch()
                }
            }
            ps.executeBatch()
        }
    }

    private fun writeBooks(conn: Connection, books: Map<BookKey, Long>) {
        conn.prepareStatement(
            "INSERT INTO id_book(source_name, canonical_he_title, id) VALUES (?, ?, ?)",
        ).use { ps ->
            for ((key, id) in books) {
                ps.setString(1, key.sourceName)
                ps.setString(2, key.canonicalHeTitle)
                ps.setLong(3, id)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeLines(conn: Connection, lines: Map<LineKey, Long>) {
        conn.prepareStatement(
            "INSERT INTO id_line(book_id, content_hash, occurrence_idx, id) VALUES (?, ?, ?, ?)",
        ).use { ps ->
            var batched = 0
            for ((key, id) in lines) {
                ps.setLong(1, key.bookId)
                ps.setBytes(2, key.contentHash)
                ps.setInt(3, key.occurrenceIdx)
                ps.setLong(4, id)
                ps.addBatch()
                if (++batched % 10_000 == 0) ps.executeBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeTocEntries(conn: Connection, entries: Map<TocEntryKey, Long>) {
        conn.prepareStatement(
            "INSERT INTO id_toc_entry(book_id, ancestor_path, id) VALUES (?, ?, ?)",
        ).use { ps ->
            for ((key, id) in entries) {
                ps.setLong(1, key.bookId)
                ps.setString(2, key.ancestorPath)
                ps.setLong(3, id)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeAltTocStructures(conn: Connection, m: Map<AltTocStructureKey, Long>) {
        conn.prepareStatement(
            "INSERT INTO id_alt_toc_structure(book_id, key, id) VALUES (?, ?, ?)",
        ).use { ps ->
            for ((key, id) in m) {
                ps.setLong(1, key.bookId)
                ps.setString(2, key.key)
                ps.setLong(3, id)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeAltTocEntries(conn: Connection, m: Map<AltTocEntryKey, Long>) {
        conn.prepareStatement(
            "INSERT INTO id_alt_toc_entry(structure_id, ancestor_path, id) VALUES (?, ?, ?)",
        ).use { ps ->
            for ((key, id) in m) {
                ps.setLong(1, key.structureId)
                ps.setString(2, key.ancestorPath)
                ps.setLong(3, id)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeLinks(conn: Connection, links: Map<LinkKey, Long>) {
        conn.prepareStatement(
            "INSERT INTO id_link(src_line_id, tgt_line_id, connection_type_id, id) VALUES (?, ?, ?, ?)",
        ).use { ps ->
            var batched = 0
            for ((key, id) in links) {
                ps.setLong(1, key.srcLineId)
                ps.setLong(2, key.tgtLineId)
                ps.setLong(3, key.connectionTypeId)
                ps.setLong(4, id)
                ps.addBatch()
                if (++batched % 10_000 == 0) ps.executeBatch()
            }
            ps.executeBatch()
        }
    }

    private fun writeAliases(conn: Connection, aliases: List<BookAlias>) {
        if (aliases.isEmpty()) return
        conn.prepareStatement(
            """
            INSERT INTO book_aliases(
                old_source_name, old_canonical_he_title,
                new_source_name, new_canonical_he_title,
                detected_at_version
            ) VALUES (?, ?, ?, ?, ?)
            """.trimIndent(),
        ).use { ps ->
            for (a in aliases) {
                ps.setString(1, a.oldKey.sourceName)
                ps.setString(2, a.oldKey.canonicalHeTitle)
                ps.setString(3, a.newKey.sourceName)
                ps.setString(4, a.newKey.canonicalHeTitle)
                ps.setInt(5, a.detectedAtVersion)
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }
}
