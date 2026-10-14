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
            mapOf("book_banner" to 2, "book_protection" to 1, "line_divine_name" to 2, "line_note" to 3),
            produced.upsertCounts.filterKeys { it in SCHEMA_7_TABLES },
        )
        assertEquals(1, produced.upsertCounts.getValue("line"), "the stripped line is an ordinary upsert")
        assertEquals("4", query(patch, "SELECT value FROM patch_meta WHERE key = 'schema_version'").single().single())

        val target = copy(prev)
        assertEquals(hash(prev, 6), hash(target, 6))
        connect(target).use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            val result = PatchApplier().apply(conn, patch, expectedToContentHash = hash(next, 7), expectedToSchemaVersion = 7)
            assertEquals(8, result.migrationsApplied)
        }
        assertEquals(hash(next, 7), hash(target, 7))
        assertEquals(
            listOf(listOf<Any?>(10, 0, "1", "גוף <i>פנימי</i>"), listOf<Any?>(10, 1, null, "נספח"), listOf<Any?>(11, 0, "*", "ב")),
            query(target, "SELECT lineId, noteIndex, marker, body FROM line_note ORDER BY lineId, noteIndex"),
        )
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
    fun `line_note hashes rows ordered by every column alphabetically`() {
        val db = tablesDb("order.db")
        insertNotes(db, listOf(listOf(2, 0, "1", "ב"), listOf(1, 0, null, "א")))
        val actual = connect(db).use { LogicalContentHasher(listOf("line_note")).computeReport(it).tableHashes }

        val bytes = ByteArrayOutputStream()
        bytes.write(" table:line_note ".toByteArray())
        bytes.write("cols:body,lineId,marker,noteIndex".toByteArray())
        bytes.write(0x00)
        fun text(s: String) { bytes.write(3); bytes.write(s.toByteArray()); bytes.write(0x1F) }
        fun int(n: Int) { bytes.write(2); bytes.write(n.toString().toByteArray()); bytes.write(0x1F) }
        text("א"); int(1); bytes.write(0); bytes.write(0x1F); int(0); bytes.write(0xFF)
        text("ב"); int(2); text("1"); int(0); bytes.write(0xFF)
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
        assertEquals(
            mapOf(
                "book_banner" to "87026c32c4996eee6e5c75b65ce5e08be1a1a28e52864a57a696cbb6555a0c59",
                "book_protection" to "015c3c2291e1ebac6c68a6f33c8c01b42af0ce46dc3840d7e0a12ae370ae1319",
                "line_divine_name" to "e0b9cc2885495c4bf2ebdfe044b06948aa8cb0c859e777d45b880e59f3d4e799",
                "line_note" to GOLDEN_LINE_NOTE_FULL,
            ),
            tableHashes(full),
        )
        assertEquals(
            mapOf(
                "book_banner" to "473b121ac837ac17a9627ed583981faec4074ec3217224c18bb0a6bb62752240",
                "book_protection" to "260da62f074581043b5bf203c9790203a9ea052f9d1e14e27ecc780f66790a81",
                "line_divine_name" to "278213e576ad7f0865c73036789d82ec801bb65e594d9eee0ec6e6e799325b85",
                "line_note" to GOLDEN_LINE_NOTE_EMPTY,
            ),
            tableHashes(tablesDb("empty.db")),
        )
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
                    st.execute("INSERT INTO line_note VALUES (12, 0, NULL, 'stale')")
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
        insertNotes(p, listOf(listOf(10, 0, "1", "גוף <i>פנימי</i>"), listOf(10, 1, null, "נספח"), listOf(11, 0, "*", "ב")))
        return p
    }

    private fun insertNotes(db: Path, rows: List<List<Any?>>) {
        connect(db).use { conn ->
            conn.prepareStatement("INSERT INTO line_note (lineId, noteIndex, marker, body) VALUES (?, ?, ?, ?)").use { ps ->
                for (row in rows) {
                    row.forEachIndexed { i, v -> ps.setObject(i + 1, v) }
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
        val SCHEMA_7_TABLES_IN_ORDER = listOf("book_banner", "book_protection", "line_divine_name", "line_note")

        // Same rows in the Dart golden test; NULL marker, nested <i>, newline and quotes on purpose.
        val GOLDEN_NOTES: List<List<Any?>> = listOf(
            listOf(2, 0, "1", "הערה <i>פנימית</i> א"),
            listOf(1, 1, null, "נספח בסוף השורה"),
            listOf(1, 0, "*", "גוף \"ב\"\nשורה שנייה"),
            listOf(2, 1, "<b>2</b>", "<b>ב</b> &amp; ג"),
        )

        // Shared with otzaria_library_updater: the Dart hasher must give the same hex.
        const val GOLDEN_LINE_NOTE_FULL = "cc6c99229bb4f95d66a0c089330c57245b555b37cea99a7a87dc6826c5230484"
        const val GOLDEN_LINE_NOTE_EMPTY = "e484b003c6701873e28a91136af62ff0b41f6416aec35fee56749eabb85a35c2"
    }
}
