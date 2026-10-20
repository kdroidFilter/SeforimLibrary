package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** The 6 -> 7 transition: an ordinary delta whose migrations create the four schema-7 tables. */
class SchemaSevenPatchTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `a 6 to 7 delta creates the schema 7 tables by migration and reproduces the schema 7 hash`() {
        val prev = schemaSixDb("prev.db")
        val next = schemaSevenDb("next.db")
        val patch = path("patch.db")

        val produced = PatchDbProducer().produce(
            prev, next, patch, fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 7,
        )

        val migrations = query(patch, "SELECT sql FROM migrations ORDER BY version").map { it.single() as String }
        val expected = SCHEMA_7_TABLES_IN_ORDER.flatMap { table ->
            listOf("DROP TABLE IF EXISTS \"$table\"", SCHEMA_7_TABLE_DDL.getValue(table).replace("IF NOT EXISTS ", ""))
        }
        assertEquals(expected, migrations)
        assertEquals(
            mapOf(
                "book_banner" to 2, "book_protection" to 1, "line_divine_name" to 2, "line_note" to 3,
                "line_note_divine_name" to 1,
            ),
            produced.upsertCounts.filterKeys { it in SCHEMA_7_TABLES },
        )
        assertEquals(1, produced.upsertCounts.getValue("line"), "the stripped line is an ordinary upsert")
        assertEquals("4", query(patch, "SELECT value FROM patch_meta WHERE key = 'schema_version'").single().single())

        val target = copy(prev)
        assertEquals(hash(prev, 6), hash(target, 6))
        connect(target).use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            val result = PatchApplier().apply(conn, patch, expectedToContentHash = hash(next, 7), expectedToSchemaVersion = 7)
            assertEquals(10, result.migrationsApplied)
        }
        assertEquals(hash(next, 7), hash(target, 7))
        assertEquals(
            listOf(
                listOf<Any?>(10, 0, 2, "1", "גוף <i>פנימי</i>"),
                listOf<Any?>(10, 1, 6, null, "נספח"),
                listOf<Any?>(11, 0, 1, "*", "ב"),
            ),
            query(target, "SELECT lineId, noteIndex, charPos, marker, body FROM line_note ORDER BY lineId, noteIndex"),
        )
    }

    @Test
    fun `a 6 to 7 delta adds book contentHash by migration and carries every value`() {
        val prev = schemaSixDb("prev.db")
        val next = schemaSevenDb("next.db")
        connect(next).use { conn ->
            conn.createStatement().use { st ->
                st.execute("ALTER TABLE book ADD COLUMN contentHash INTEGER NOT NULL DEFAULT 0")
                st.execute("UPDATE book SET contentHash = CASE id WHEN 1 THEN -7 ELSE 9007199254740993 END")
            }
        }
        val patch = path("patch.db")
        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 7)

        val migrations = query(patch, "SELECT sql FROM migrations ORDER BY version").map { it.single() as String }
        assertTrue("ALTER TABLE \"book\" ADD COLUMN \"contentHash\" INTEGER NOT NULL DEFAULT 0" in migrations, migrations.toString())
        assertEquals(2, produced.upsertCounts.getValue("book"))

        val target = copy(prev)
        connect(target).use { PatchApplier().apply(it, patch, expectedToContentHash = hash(next, 7), expectedToSchemaVersion = 7) }
        assertEquals(hash(next, 7), hash(target, 7))
        assertEquals(
            listOf(listOf<Any?>(1, -7), listOf<Any?>(2, 9007199254740993L)),
            query(target, "SELECT id, contentHash FROM book ORDER BY id"),
        )
    }

    @Test
    fun `a 7 to 7 delta upserts only the books whose contentHash changed`() {
        val prev = schemaSevenDb("prev.db")
        val next = schemaSevenDb("next.db")
        for (db in listOf(prev, next)) {
            connect(db).use { conn ->
                conn.createStatement().use { it.execute("ALTER TABLE book ADD COLUMN contentHash INTEGER NOT NULL DEFAULT 0") }
            }
        }
        connect(prev).use { conn -> conn.createStatement().use { it.execute("UPDATE book SET contentHash = id * 11") } }
        connect(next).use { conn -> conn.createStatement().use { it.execute("UPDATE book SET contentHash = CASE id WHEN 2 THEN 5 ELSE id * 11 END") } }
        val patch = path("patch.db")
        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 31, toVersion = 32, fromSchemaVersion = 7, toSchemaVersion = 7)

        assertEquals(1, produced.upsertCounts.getValue("book"))
        val target = copy(prev)
        connect(target).use { PatchApplier().apply(it, patch, expectedToContentHash = hash(next, 7), expectedToSchemaVersion = 7) }
        assertEquals(listOf(listOf<Any?>(1, 11), listOf<Any?>(2, 5)), query(target, "SELECT id, contentHash FROM book ORDER BY id"))
    }

    @Test
    fun `stale rows of a schema 7 table that prev carried physically are reset`() {
        val prev = schemaSixDb("prev.db", staleNote = true)
        val next = schemaSevenDb("next.db")
        val patch = path("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 7)

        val target = copy(prev)
        connect(target).use { conn ->
            PatchApplier().apply(conn, patch, expectedToContentHash = hash(next, 7), expectedToSchemaVersion = 7)
        }
        assertEquals(hash(next, 7), hash(target, 7))
    }

    @Test
    fun `a 7 to 7 delta diffs line_note row by row`() {
        val prev = schemaSevenDb("prev.db")
        val next = schemaSevenDb("next.db")
        connect(next).use { conn ->
            conn.createStatement().use { st ->
                st.execute("UPDATE line_note SET body = 'נספח מתוקן' WHERE lineId = 10 AND noteIndex = 1")
                st.execute("DELETE FROM line_note WHERE lineId = 11")
            }
        }
        val patch = path("patch.db")
        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 31, toVersion = 32, fromSchemaVersion = 7, toSchemaVersion = 7)

        assertEquals(1, produced.upsertCounts.getValue("line_note"))
        assertEquals(1, produced.deleteCounts.getValue("line_note"))
        assertTrue(query(patch, "SELECT sql FROM migrations").isEmpty())
        val target = copy(prev)
        connect(target).use { PatchApplier().apply(it, patch, expectedToContentHash = hash(next, 7), expectedToSchemaVersion = 7) }
        assertEquals(hash(next, 7), hash(target, 7))
    }

    @Test
    fun `Database sq declares the schema 7 tables exactly as their DDL`() {
        val generated = path("generated.db")
        JdbcSqliteDriver("jdbc:sqlite:${generated.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        val fromSpec = path("spec.db")
        connect(fromSpec).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL)")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL)")
                SCHEMA_7_TABLE_DDL.values.forEach { st.execute(it) }
            }
        }
        assertEquals(SCHEMA_7_TABLES, SCHEMA_7_TABLE_DDL.keys)
        for (table in SCHEMA_7_TABLE_DDL.keys) {
            assertEquals(shape(fromSpec, table), shape(generated, table), "Database.sq drifted from the $table DDL")
        }
    }

    @Test
    fun `line_note hashes rows in primary-key order, not by body`() {
        val db = tablesDb("order.db")
        insertNotes(db, listOf(listOf(2, 0, 7, "1", "א"), listOf(1, 0, 3, null, "ב")))
        val actual = connect(db).use { LogicalContentHasher(listOf("line_note")).computeReport(it).tableHashes }

        val bytes = ByteArrayOutputStream()
        bytes.write(" table:line_note ".toByteArray())
        bytes.write("cols:body,charPos,lineId,marker,noteIndex".toByteArray())
        bytes.write(0x00)
        fun text(s: String) { bytes.write(3); bytes.write(s.toByteArray()); bytes.write(0x1F) }
        fun int(n: Int) { bytes.write(2); bytes.write(n.toString().toByteArray()); bytes.write(0x1F) }
        text("ב"); int(3); int(1); bytes.write(0); bytes.write(0x1F); int(0); bytes.write(0xFF)
        text("א"); int(7); int(2); text("1"); int(0); bytes.write(0xFF)
        val expected = MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()).joinToString("") { "%02x".format(it) }
        assertEquals(mapOf("line_note" to expected), actual)
    }

    @Test
    fun `cross-repo golden per-table hashes of the schema 7 tables`() {
        val full = tablesDb("golden.db")
        connect(full).use { conn ->
            conn.prepareStatement("INSERT INTO book_banner (bookId, text) VALUES (?, ?)").use { ps ->
                ps.setLong(1, 1); ps.setString(2, "ספר זה באדיבות המו\"ל\n[לאתר המו\"ל](https://example.com/books?id=1)"); ps.executeUpdate()
                ps.setLong(1, 2); ps.setString(2, "שורה ראשונה\nשורה שנייה"); ps.executeUpdate()
            }
            conn.createStatement().use { st ->
                st.execute("INSERT INTO book_protection (bookId, level) VALUES (1, 1), (2, 2)")
                st.execute(
                    "INSERT INTO line_divine_name (lineId, charStart, charEnd, kind, source) VALUES " +
                        "(2, 20, 24, 3, 4), (1, 0, 4, 1, 1), (2, 3, 6, 7, 3), (1, 12, 17, 5, 2)",
                )
            }
        }
        insertNotes(full, GOLDEN_NOTES)
        insertNoteNames(full, GOLDEN_NOTE_NAMES)
        assertEquals(
            mapOf(
                "book_banner" to "87026c32c4996eee6e5c75b65ce5e08be1a1a28e52864a57a696cbb6555a0c59",
                "book_protection" to "015c3c2291e1ebac6c68a6f33c8c01b42af0ce46dc3840d7e0a12ae370ae1319",
                "line_divine_name" to "ab37c80ae94f079eabde2b02cd108fc90f1c75f523f0f8fd1f9b87f5a8dc67b6",
                "line_note" to GOLDEN_LINE_NOTE_FULL,
                "line_note_divine_name" to GOLDEN_LINE_NOTE_DIVINE_NAME_FULL,
            ),
            tableHashes(full),
        )
        assertEquals(
            mapOf(
                "book_banner" to "473b121ac837ac17a9627ed583981faec4074ec3217224c18bb0a6bb62752240",
                "book_protection" to "260da62f074581043b5bf203c9790203a9ea052f9d1e14e27ecc780f66790a81",
                "line_divine_name" to "278213e576ad7f0865c73036789d82ec801bb65e594d9eee0ec6e6e799325b85",
                "line_note" to GOLDEN_LINE_NOTE_EMPTY,
                "line_note_divine_name" to GOLDEN_LINE_NOTE_DIVINE_NAME_EMPTY,
            ),
            tableHashes(tablesDb("empty.db")),
        )
    }

    @Test
    fun `cross-repo golden of book contentHash at the int64 edges`() {
        val db = path("book.db")
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, contentHash INTEGER NOT NULL DEFAULT 0)")
                st.execute(
                    "INSERT INTO book VALUES (1, -3371204337321344343), (2, 0), " +
                        "(3, 9223372036854775807), (4, -9223372036854775808)",
                )
            }
        }
        val actual = connect(db).use { LogicalContentHasher(listOf("book")).computeReport(it).tableHashes }
        assertEquals(mapOf("book" to GOLDEN_BOOK_CONTENT_HASH), actual)
    }

    @Test
    fun `the schema 6 hash ignores the schema 7 tables`() {
        val withNotes = schemaSevenDb("next.db")
        val without = schemaSevenDb("bare.db")
        connect(without).use { conn -> conn.createStatement().use { st -> SCHEMA_7_TABLES.forEach { st.execute("DELETE FROM $it") } } }
        assertEquals(hash(without, 6), hash(withNotes, 6))
        assertFalse(hash(without, 7) == hash(withNotes, 7))
    }

    private fun tableHashes(db: Path): Map<String, String> =
        connect(db).use { LogicalContentHasher(SCHEMA_7_TABLES_IN_ORDER).computeReport(it).tableHashes }

    /** book + line only, plus the four schema-7 tables, empty. */
    private fun tablesDb(name: String): Path {
        val p = path(name)
        connect(p).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL)")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL)")
                SCHEMA_7_TABLE_DDL.values.forEach { st.execute(it) }
            }
        }
        return p
    }

    private fun schemaSixDb(name: String, staleNote: Boolean = false): Path {
        val p = path(name)
        connect(p).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book VALUES (1, 'א'), (2, 'ב')")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL, charCount INTEGER NOT NULL)")
                st.execute("INSERT INTO line VALUES (10, 1, 30), (11, 1, 5), (12, 2, 7)")
                if (staleNote) {
                    st.execute(SCHEMA_7_TABLE_DDL.getValue("line_note"))
                    st.execute("INSERT INTO line_note VALUES (12, 0, 0, NULL, 'stale')")
                }
            }
        }
        return p
    }

    private fun schemaSevenDb(name: String): Path {
        val p = schemaSixDb(name)
        connect(p).use { conn ->
            conn.createStatement().use { st ->
                st.execute("UPDATE line SET charCount = 18 WHERE id = 10")
                SCHEMA_7_TABLE_DDL.values.forEach { st.execute(it) }
                st.execute("INSERT INTO book_banner VALUES (1, 'באדיבות'), (2, 'שורה\nשנייה')")
                st.execute("INSERT INTO book_protection VALUES (2, 1)")
                st.execute("INSERT INTO line_divine_name VALUES (10, 0, 4, 1, 1), (12, 2, 6, 5, 3)")
            }
        }
        insertNotes(p, listOf(listOf(10, 0, 2, "1", "גוף <i>פנימי</i>"), listOf(10, 1, 6, null, "נספח"), listOf(11, 0, 1, "*", "ב")))
        insertNoteNames(p, listOf(listOf(10, 0, 4, 9, 1, 1)))
        return p
    }

    private fun insertNotes(db: Path, rows: List<List<Any?>>) {
        connect(db).use { conn ->
            conn.prepareStatement("INSERT INTO line_note (lineId, noteIndex, charPos, marker, body) VALUES (?, ?, ?, ?, ?)").use { ps ->
                for (row in rows) {
                    row.forEachIndexed { i, v -> ps.setObject(i + 1, v) }
                    ps.executeUpdate()
                }
            }
        }
    }

    private fun insertNoteNames(db: Path, rows: List<List<Int>>) {
        connect(db).use { conn ->
            conn.prepareStatement(
                "INSERT INTO line_note_divine_name (lineId, noteIndex, charStart, charEnd, kind, source) VALUES (?, ?, ?, ?, ?, ?)",
            ).use { ps ->
                for (row in rows) {
                    row.forEachIndexed { i, v -> ps.setInt(i + 1, v) }
                    ps.executeUpdate()
                }
            }
        }
    }

    private fun shape(db: Path, table: String): Pair<List<List<Any?>>, String> {
        val cols = query(db, "SELECT name, type, \"notnull\", pk FROM pragma_table_info('$table')")
        val fks = query(db, "SELECT \"table\", \"from\", \"to\", on_delete FROM pragma_foreign_key_list('$table')")
        val withoutRowid = query(db, "SELECT sql FROM sqlite_master WHERE name = '$table'").single().single() as String
        return cols to (fks.toString() + withoutRowid.contains("WITHOUT ROWID"))
    }

    private fun hash(db: Path, schemaVersion: Int): String =
        connect(db).use { LogicalContentHasher.forSchemaVersion(schemaVersion).compute(it) }

    private fun path(name: String): Path = tmp.newFolder().toPath().resolve(name)

    private fun copy(source: Path): Path = path("target.db").also { Files.copy(source, it) }

    private fun connect(p: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${p.toAbsolutePath()}")

    private fun query(p: Path, sql: String): List<List<Any?>> = connect(p).use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                val n = rs.metaData.columnCount
                buildList { while (rs.next()) add((1..n).map { rs.getObject(it) }) }
            }
        }
    }

    private companion object {
        val SCHEMA_7_TABLES_IN_ORDER =
            listOf("book_banner", "book_protection", "line_divine_name", "line_note", "line_note_divine_name")

        // Same rows in the Dart golden test; NULL marker, nested <i>, newline and quotes on purpose.
        val GOLDEN_NOTES: List<List<Any?>> = listOf(
            listOf(2, 0, 5, "1", "הערה <i>פנימית</i> א"),
            listOf(1, 1, 12, null, "נספח בסוף השורה"),
            listOf(1, 0, 3, "*", "גוף \"ב\"\nשורה שנייה"),
            listOf(2, 1, 9, "<b>2</b>", "<b>ב</b> &amp; ג"),
        )

        // (lineId, noteIndex, charStart, charEnd, kind, source), on the notes above.
        val GOLDEN_NOTE_NAMES: List<List<Int>> = listOf(
            listOf(2, 0, 0, 4, 1, 1),
            listOf(1, 1, 6, 11, 5, 2),
            listOf(1, 0, 3, 6, 7, 3),
            listOf(2, 0, 10, 14, 3, 4),
        )

        // Shared with otzaria_library_updater: the Dart hasher must give the same hex.
        const val GOLDEN_LINE_NOTE_FULL = "b01db649c8a87765a89d0667849cad5389f9cc5b64137486e06d1bb631b6c487"
        const val GOLDEN_LINE_NOTE_EMPTY = "027c225d40b02cfc2025d0c279668dcd341af7ab68c725b3b187fac0dbdce8a8"
        const val GOLDEN_LINE_NOTE_DIVINE_NAME_FULL = "5c2aded4a4b3fa5a4146308c2bc07cbfe0926766ddcb2e6c4b13785db1b893cf"
        const val GOLDEN_LINE_NOTE_DIVINE_NAME_EMPTY = "4c7710872d55a8575723dac795a2de02e09085ec0643712cc4aa41014d5320aa"
        const val GOLDEN_BOOK_CONTENT_HASH = "eb5db532be575315bed061263dc4fce8edf21bc4edf27556f8aa689a3c29eb43"
    }
}
