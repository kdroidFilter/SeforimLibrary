package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals

/**
 * Synthetic produce→apply roundtrip for the link/version satellite tables
 * (link_anchor, link_range, link_coverage, book_version, version_line), which the
 * real-diff CI runs don't always touch. Each table gets an upsert AND a delete so
 * both diff directions of its PatchTables spec stay covered.
 */
class PatchLinkAndVersionTablesTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `produce and apply cover upserts and deletes on all five satellite tables`() {
        val prev = tmp.newFolder().toPath().resolve("prev.db")
        val next = tmp.newFolder().toPath().resolve("next.db")
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        val target = tmp.newFolder().toPath().resolve("target.db")

        buildDb(prev, version = 1) { st ->
            st.executeUpdate("INSERT INTO link_anchor(linkId, side, charStart, charEnd, label) VALUES (1000, 0, 5, NULL, 'א'), (1000, 0, 9, 12, NULL)")
            st.executeUpdate("INSERT INTO link_range(linkId, side, endLineId, endLineIndex) VALUES (1000, 0, 101, 1), (1001, 1, 101, 1)")
            st.executeUpdate("INSERT INTO link_coverage(lineId, linkId, side) VALUES (101, 1000, 0), (102, 1000, 0)")
            st.executeUpdate("INSERT INTO book_version(id, bookId, versionTitle, heVersionTitle, hasContent) VALUES (500, 10, 'A', NULL, 1), (501, 10, 'B', NULL, 0)")
            st.executeUpdate("INSERT INTO version_line(versionId, lineId, content, charCount) VALUES (500, 100, 'x', 1), (500, 101, 'y', 1)")
        }
        // Per table vs prev: one row modified (upsert), one deleted, one added (upsert) —
        // except link_coverage (updatable=false pure junction): add + delete only.
        buildDb(next, version = 2) { st ->
            st.executeUpdate("INSERT INTO link_anchor(linkId, side, charStart, charEnd, label) VALUES (1000, 0, 5, NULL, 'ב'), (1001, 0, 2, NULL, 'ג')")
            st.executeUpdate("INSERT INTO link_range(linkId, side, endLineId, endLineIndex) VALUES (1000, 0, 102, 2), (1001, 0, 102, 2)")
            st.executeUpdate("INSERT INTO link_coverage(lineId, linkId, side) VALUES (101, 1000, 0), (102, 1001, 1)")
            st.executeUpdate("INSERT INTO book_version(id, bookId, versionTitle, heVersionTitle, hasContent) VALUES (500, 10, 'A', 'א׳', 1), (502, 10, 'C', NULL, 0)")
            st.executeUpdate("INSERT INTO version_line(versionId, lineId, content, charCount) VALUES (500, 100, 'x2', 2), (500, 102, 'z', 1)")
        }

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        for (table in listOf("link_anchor", "link_range", "book_version", "version_line")) {
            assertEquals(2, produced.upsertCounts.getValue(table), "upserts for $table")
            assertEquals(1, produced.deleteCounts.getValue(table), "deletes for $table")
        }
        assertEquals(1, produced.upsertCounts.getValue("link_coverage"), "upserts for link_coverage")
        assertEquals(1, produced.deleteCounts.getValue("link_coverage"), "deletes for link_coverage")

        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(conn, patch)
            assertEquals(logicalHash(next), LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).compute(conn))
        }
    }

    /** Minimal-shape schema: parents shared by both builds, satellite rows via [seed]. */
    private fun buildDb(path: Path, version: Int, seed: (java.sql.Statement) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO schema_meta(key, value) VALUES ('db_version', '$version')")
                st.executeUpdate("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.executeUpdate("INSERT INTO book(id, title) VALUES (10, 'Genesis')")
                st.executeUpdate("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL, lineIndex INTEGER NOT NULL, content TEXT NOT NULL, FOREIGN KEY (bookId) REFERENCES book(id) ON DELETE CASCADE)")
                st.executeUpdate("INSERT INTO line(id, bookId, lineIndex, content) VALUES (100, 10, 0, 'l0'), (101, 10, 1, 'l1'), (102, 10, 2, 'l2')")
                st.executeUpdate("CREATE TABLE connection_type (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
                st.executeUpdate("INSERT INTO connection_type(id, name) VALUES (1, 'COMMENTARY')")
                st.executeUpdate("""
                    CREATE TABLE link (
                        id INTEGER PRIMARY KEY NOT NULL,
                        sourceBookId INTEGER NOT NULL, targetBookId INTEGER NOT NULL,
                        sourceLineId INTEGER NOT NULL, targetLineId INTEGER NOT NULL,
                        connectionTypeId INTEGER NOT NULL,
                        FOREIGN KEY (sourceLineId) REFERENCES line(id) ON DELETE CASCADE,
                        FOREIGN KEY (targetLineId) REFERENCES line(id) ON DELETE CASCADE,
                        FOREIGN KEY (connectionTypeId) REFERENCES connection_type(id) ON DELETE CASCADE
                    )
                """.trimIndent())
                st.executeUpdate("INSERT INTO link(id, sourceBookId, targetBookId, sourceLineId, targetLineId, connectionTypeId) VALUES (1000, 10, 10, 100, 101, 1), (1001, 10, 10, 100, 102, 1)")
                st.executeUpdate("""
                    CREATE TABLE link_anchor (
                        linkId INTEGER NOT NULL, side INTEGER NOT NULL DEFAULT 0,
                        charStart INTEGER NOT NULL, charEnd INTEGER, label TEXT,
                        PRIMARY KEY (linkId, side, charStart),
                        FOREIGN KEY (linkId) REFERENCES link(id) ON DELETE CASCADE
                    )
                """.trimIndent())
                st.executeUpdate("""
                    CREATE TABLE link_range (
                        linkId INTEGER NOT NULL, side INTEGER NOT NULL,
                        endLineId INTEGER NOT NULL, endLineIndex INTEGER NOT NULL,
                        PRIMARY KEY (linkId, side),
                        FOREIGN KEY (linkId) REFERENCES link(id) ON DELETE CASCADE,
                        FOREIGN KEY (endLineId) REFERENCES line(id) ON DELETE CASCADE
                    )
                """.trimIndent())
                st.executeUpdate("""
                    CREATE TABLE link_coverage (
                        lineId INTEGER NOT NULL, linkId INTEGER NOT NULL, side INTEGER NOT NULL,
                        PRIMARY KEY (lineId, linkId, side),
                        FOREIGN KEY (lineId) REFERENCES line(id) ON DELETE CASCADE,
                        FOREIGN KEY (linkId) REFERENCES link(id) ON DELETE CASCADE
                    )
                """.trimIndent())
                st.executeUpdate("""
                    CREATE TABLE book_version (
                        id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL,
                        versionTitle TEXT NOT NULL, heVersionTitle TEXT,
                        hasContent INTEGER NOT NULL DEFAULT 0,
                        UNIQUE (bookId, versionTitle),
                        FOREIGN KEY (bookId) REFERENCES book(id) ON DELETE CASCADE
                    )
                """.trimIndent())
                st.executeUpdate("""
                    CREATE TABLE version_line (
                        versionId INTEGER NOT NULL, lineId INTEGER NOT NULL,
                        content TEXT NOT NULL, charCount INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (versionId, lineId),
                        FOREIGN KEY (versionId) REFERENCES book_version(id) ON DELETE CASCADE,
                        FOREIGN KEY (lineId) REFERENCES line(id) ON DELETE CASCADE
                    )
                """.trimIndent())
                seed(st)
            }
        }
    }

    private fun logicalHash(path: Path): String =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
            LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).compute(it)
        }
}
