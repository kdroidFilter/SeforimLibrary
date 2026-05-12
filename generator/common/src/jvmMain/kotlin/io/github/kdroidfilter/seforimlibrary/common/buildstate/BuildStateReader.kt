package io.github.kdroidfilter.seforimlibrary.common.buildstate

import co.touchlab.kermit.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Reads a `build_state.db` artifact from disk into a [BuildStateSnapshot] held in RAM.
 *
 * If the file does not exist, returns [BuildStateSnapshot.empty] — this is the
 * normal case for the first build that adopts stable ids.
 */
class BuildStateReader(private val logger: Logger = Logger.withTag("BuildStateReader")) {

    fun read(path: Path): BuildStateSnapshot {
        if (!Files.exists(path)) {
            logger.i { "No previous build_state at $path — starting with empty snapshot." }
            return BuildStateSnapshot.empty()
        }
        Class.forName("org.sqlite.JDBC")
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.autoCommit = true
            return readFrom(conn)
        }
    }

    internal fun readFrom(conn: Connection): BuildStateSnapshot {
        val rawMeta = readMeta(conn)
        val schemaVersion = rawMeta["schema_version"]?.toIntOrNull() ?: BuildStateSchema.CURRENT_VERSION
        val meta = rawMeta - "schema_version"
        check(schemaVersion <= BuildStateSchema.CURRENT_VERSION) {
            "build_state.db schema_version=$schemaVersion is newer than supported ${BuildStateSchema.CURRENT_VERSION}"
        }

        val counters = readCounters(conn)
        val lookups = readLookups(conn)
        val books = readBooks(conn)
        val lines = readLines(conn)
        val tocEntries = readTocEntries(conn)
        val altStructures = readAltTocStructures(conn)
        val altEntries = readAltTocEntries(conn)
        val links = readLinks(conn)
        val aliases = readBookAliases(conn)

        logger.i {
            "Loaded build_state: ${books.size} books, ${lines.size} lines, " +
                "${tocEntries.size} tocEntries, ${links.size} links, ${aliases.size} aliases"
        }
        return BuildStateSnapshot(
            schemaVersion = schemaVersion,
            meta = meta,
            counters = counters,
            lookups = lookups,
            books = books,
            lines = lines,
            tocEntries = tocEntries,
            altTocStructures = altStructures,
            altTocEntries = altEntries,
            links = links,
            bookAliases = aliases,
        )
    }

    private fun readMeta(conn: Connection): Map<String, String> {
        if (!tableExists(conn, "meta")) return emptyMap()
        val out = HashMap<String, String>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT key, value FROM meta").use { rs ->
                while (rs.next()) out[rs.getString(1)] = rs.getString(2)
            }
        }
        return out
    }

    private fun readCounters(conn: Connection): Map<IdTable, Long> {
        if (!tableExists(conn, "id_counters")) return emptyMap()
        val out = HashMap<IdTable, Long>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT table_name, next_id FROM id_counters").use { rs ->
                while (rs.next()) {
                    val table = IdTable.fromTableName(rs.getString(1)) ?: continue
                    out[table] = rs.getLong(2)
                }
            }
        }
        return out
    }

    private fun readLookups(conn: Connection): Map<IdTable, Map<String, Long>> {
        if (!tableExists(conn, "id_lookup")) return emptyMap()
        val byKind = HashMap<String, HashMap<String, Long>>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT kind, natural_key, id FROM id_lookup").use { rs ->
                while (rs.next()) {
                    val kind = rs.getString(1)
                    val key = rs.getString(2)
                    val id = rs.getLong(3)
                    byKind.getOrPut(kind) { HashMap() }[key] = id
                }
            }
        }
        return IdTable.values()
            .filter { it.lookupKind != null }
            .mapNotNull { table -> byKind[table.lookupKind]?.let { table to it.toMap() } }
            .toMap()
    }

    private fun readBooks(conn: Connection): Map<BookKey, Long> {
        if (!tableExists(conn, "id_book")) return emptyMap()
        val out = HashMap<BookKey, Long>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT source_name, canonical_he_title, id FROM id_book").use { rs ->
                while (rs.next()) {
                    out[BookKey(rs.getString(1), rs.getString(2))] = rs.getLong(3)
                }
            }
        }
        return out
    }

    private fun readLines(conn: Connection): Map<LineKey, Long> {
        if (!tableExists(conn, "id_line")) return emptyMap()
        val out = HashMap<LineKey, Long>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT book_id, content_hash, occurrence_idx, id FROM id_line").use { rs ->
                while (rs.next()) {
                    out[LineKey(rs.getLong(1), rs.getBytes(2), rs.getInt(3))] = rs.getLong(4)
                }
            }
        }
        return out
    }

    private fun readTocEntries(conn: Connection): Map<TocEntryKey, Long> {
        if (!tableExists(conn, "id_toc_entry")) return emptyMap()
        val out = HashMap<TocEntryKey, Long>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT book_id, ancestor_path, id FROM id_toc_entry").use { rs ->
                while (rs.next()) {
                    out[TocEntryKey(rs.getLong(1), rs.getString(2))] = rs.getLong(3)
                }
            }
        }
        return out
    }

    private fun readAltTocStructures(conn: Connection): Map<AltTocStructureKey, Long> {
        if (!tableExists(conn, "id_alt_toc_structure")) return emptyMap()
        val out = HashMap<AltTocStructureKey, Long>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT book_id, key, id FROM id_alt_toc_structure").use { rs ->
                while (rs.next()) {
                    out[AltTocStructureKey(rs.getLong(1), rs.getString(2))] = rs.getLong(3)
                }
            }
        }
        return out
    }

    private fun readAltTocEntries(conn: Connection): Map<AltTocEntryKey, Long> {
        if (!tableExists(conn, "id_alt_toc_entry")) return emptyMap()
        val out = HashMap<AltTocEntryKey, Long>()
        conn.createStatement().use { st ->
            st.executeQuery("SELECT structure_id, ancestor_path, id FROM id_alt_toc_entry").use { rs ->
                while (rs.next()) {
                    out[AltTocEntryKey(rs.getLong(1), rs.getString(2))] = rs.getLong(3)
                }
            }
        }
        return out
    }

    private fun readLinks(conn: Connection): Map<LinkKey, Long> {
        if (!tableExists(conn, "id_link")) return emptyMap()
        val out = HashMap<LinkKey, Long>()
        conn.createStatement().use { st ->
            st.executeQuery(
                "SELECT src_line_id, tgt_line_id, connection_type_id, id FROM id_link",
            ).use { rs ->
                while (rs.next()) {
                    out[LinkKey(rs.getLong(1), rs.getLong(2), rs.getLong(3))] = rs.getLong(4)
                }
            }
        }
        return out
    }

    private fun readBookAliases(conn: Connection): List<BookAlias> {
        if (!tableExists(conn, "book_aliases")) return emptyList()
        val out = ArrayList<BookAlias>()
        conn.createStatement().use { st ->
            st.executeQuery(
                """
                SELECT old_source_name, old_canonical_he_title,
                       new_source_name, new_canonical_he_title,
                       detected_at_version
                FROM book_aliases
                """.trimIndent(),
            ).use { rs ->
                while (rs.next()) {
                    out += BookAlias(
                        oldKey = BookKey(rs.getString(1), rs.getString(2)),
                        newKey = BookKey(rs.getString(3), rs.getString(4)),
                        detectedAtVersion = rs.getInt(5),
                    )
                }
            }
        }
        return out
    }

    private fun tableExists(conn: Connection, name: String): Boolean {
        conn.prepareStatement(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        ).use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }
}
