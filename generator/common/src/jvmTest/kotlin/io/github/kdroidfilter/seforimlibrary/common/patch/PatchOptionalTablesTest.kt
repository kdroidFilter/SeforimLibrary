package io.github.kdroidfilter.seforimlibrary.common.patch

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
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * book_banner / book_protection travel outside the schema contract: a full snapshot
 * per patch, replaced whole by the applier and verified by optionalTableContentHashes.
 */
class PatchOptionalTablesTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `the producer ships DDL and a full snapshot of each optional table the new DB has`() {
        val prev = db("prev.db", banners = emptyMap(), protections = null)
        val next = db("next.db", banners = mapOf(1L to "שורה א\nשורה ב", 2L to "[קישור](https://x.org)"), protections = mapOf(2L to 2))
        val patch = path("patch.db")

        val output = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        assertEquals(
            OPTIONAL_PATCH_TABLES.associate { it.name to it.ddl },
            query(patch, "SELECT name, sql FROM optional_table_ddl ORDER BY name").associate { it[0] as String to it[1] as String },
        )
        assertEquals(
            listOf(listOf<Any?>(1, "שורה א\nשורה ב"), listOf<Any?>(2, "[קישור](https://x.org)")),
            query(patch, "SELECT bookId, text FROM optional_book_banner ORDER BY bookId"),
        )
        assertEquals(listOf(listOf<Any?>(2, 2)), query(patch, "SELECT bookId, level FROM optional_book_protection"))
        assertFalse(hasTable(patch, "upsert_book_banner"))
        assertFalse("book_banner" in output.upsertCounts)
    }

    @Test
    fun `apply replaces the tables whole and the optional hashes match the new DB`() {
        // prev carries stale rows a client may hold after skipping an earlier patch.
        val prev = db("prev.db", banners = mapOf(1L to "ישן", 3L to "יימחק"), protections = mapOf(1L to 1))
        val next = db("next.db", banners = mapOf(1L to "חדש {title}"), protections = mapOf(2L to 3))
        val patch = path("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        val expected = connect(next).use { optionalTableContentHashes(it) }
        assertEquals(listOf("book_banner", "book_protection"), expected.keys.toList())

        val target = copy(prev)
        val result = connect(target).use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(
                conn, patch,
                expectedToContentHash = wholeHash(next),
                expectedToSchemaVersion = CURRENT_DB_SCHEMA_VERSION,
                expectedOptionalTableHashes = expected,
            )
        }

        assertEquals(mapOf("book_banner" to 1, "book_protection" to 1), result.optionalTablesReplaced)
        assertFalse("book_banner" in result.upsertCounts)
        assertEquals(query(next, "SELECT * FROM book_banner ORDER BY bookId"), query(target, "SELECT * FROM book_banner ORDER BY bookId"))
        assertEquals(query(next, "SELECT * FROM book_protection ORDER BY bookId"), query(target, "SELECT * FROM book_protection ORDER BY bookId"))
        assertEquals(expected, connect(target).use { optionalTableContentHashes(it) })
    }

    @Test
    fun `a client without the tables gets them created from the shipped DDL`() {
        val prev = db("prev.db", banners = null, protections = null)
        val next = db("next.db", banners = mapOf(2L to "x"), protections = mapOf(1L to 1))
        val patch = path("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        val target = copy(prev)
        connect(target).use { conn ->
            PatchApplier().apply(conn, patch, expectedOptionalTableHashes = connect(next).use { optionalTableContentHashes(it) })
        }
        assertEquals(listOf(listOf<Any?>(2, "x")), query(target, "SELECT bookId, text FROM book_banner"))
        assertEquals(listOf(listOf<Any?>(1, 1)), query(target, "SELECT bookId, level FROM book_protection"))
    }

    @Test
    fun `a new DB without the tables ships no side channel and the client's tables stay untouched`() {
        val prev = db("prev.db", banners = mapOf(1L to "נשאר"), protections = null)
        val next = db("next.db", banners = null, protections = null)
        val patch = path("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        assertFalse(hasTable(patch, OPTIONAL_TABLE_DDL_TABLE))
        assertTrue(connect(next).use { optionalTableContentHashes(it) }.isEmpty())
        val target = copy(prev)
        val result = connect(target).use { PatchApplier().apply(it, patch) }
        assertTrue(result.optionalTablesReplaced.isEmpty())
        assertEquals(listOf(listOf<Any?>(1, "נשאר")), query(target, "SELECT bookId, text FROM book_banner"))
    }

    @Test
    fun `a mismatched optional hash rolls the whole apply back`() {
        val prev = db("prev.db", banners = mapOf(1L to "ישן"), protections = null)
        val next = db("next.db", banners = mapOf(1L to "חדש"), protections = null)
        val patch = path("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        val target = copy(prev)
        connect(target).use { conn ->
            val error = assertFailsWith<IllegalStateException> {
                PatchApplier().apply(conn, patch, expectedOptionalTableHashes = mapOf("book_banner" to "0".repeat(64)))
            }
            assertTrue("book_banner" in error.message.orEmpty(), error.message)
        }
        assertEquals(listOf(listOf<Any?>(1, "ישן")), query(target, "SELECT bookId, text FROM book_banner"))
    }

    @Test
    fun `unknown optional snapshots are ignored`() {
        val prev = db("prev.db", banners = null, protections = null)
        val next = db("next.db", banners = null, protections = null)
        val patch = path("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        connect(patch).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE optional_table_ddl (name TEXT PRIMARY KEY NOT NULL, sql TEXT NOT NULL)")
                st.execute("INSERT INTO optional_table_ddl VALUES ('book_future', 'CREATE TABLE IF NOT EXISTS book_future (x INTEGER)')")
                st.execute("CREATE TABLE optional_book_future (x INTEGER)")
            }
        }
        val target = copy(prev)
        val result = connect(target).use { PatchApplier().apply(it, patch, expectedOptionalTableHashes = mapOf("book_future" to "x")) }
        assertTrue(result.optionalTablesReplaced.isEmpty())
        assertFalse(hasTable(target, "book_future"))
    }

    @Test
    fun `the hasher hashes a table outside every schema list with the per-table algorithm`() {
        val next = db("next.db", banners = mapOf(2L to "ב", 1L to "א"), protections = null)
        val actual = connect(next).use { LogicalContentHasher(listOf("book_banner")).computeReport(it).tableHashes }

        // No id column: rows ordered by every column, alphabetically.
        val bytes = ByteArrayOutputStream()
        fun put(b: ByteArray) = bytes.write(b)
        put(" table:book_banner ".toByteArray())
        put("cols:bookId,text".toByteArray())
        put(byteArrayOf(0x00))
        for ((id, text) in listOf(1 to "א", 2 to "ב")) {
            put(byteArrayOf(2)); put(id.toString().toByteArray()); put(byteArrayOf(0x1F))
            put(byteArrayOf(3)); put(text.toByteArray()); put(byteArrayOf(0x1F))
            put(byteArrayOf(0xFF.toByte()))
        }
        val expected = MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()).joinToString("") { "%02x".format(it) }
        assertEquals(mapOf("book_banner" to expected), actual)
    }

    @Test
    fun `cross-repo golden per-table hashes`() {
        val golden = db(
            "golden.db",
            banners = mapOf(
                1L to "ספר זה באדיבות המו\"ל\n[לאתר המו\"ל](https://example.com/books?id=1)",
                2L to "שורה ראשונה\nשורה שנייה",
            ),
            protections = mapOf(1L to 1, 2L to 2),
        )
        assertEquals(
            mapOf(
                "book_banner" to "87026c32c4996eee6e5c75b65ce5e08be1a1a28e52864a57a696cbb6555a0c59",
                "book_protection" to "015c3c2291e1ebac6c68a6f33c8c01b42af0ce46dc3840d7e0a12ae370ae1319",
            ),
            connect(golden).use { optionalTableContentHashes(it) },
        )
    }

    @Test
    fun `a DDL that is not a single CREATE TABLE IF NOT EXISTS is refused`() {
        val prev = db("prev.db", banners = null, protections = null)
        val next = db("next.db", banners = mapOf(1L to "x"), protections = null)
        val patch = path("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        connect(patch).use { conn ->
            conn.createStatement().use {
                it.execute("UPDATE optional_table_ddl SET sql = sql || '; DROP TABLE book' WHERE name = 'book_banner'")
            }
        }
        val target = copy(prev)
        connect(target).use { conn -> assertFailsWith<IllegalStateException> { PatchApplier().apply(conn, patch) } }
        assertTrue(hasTable(target, "book"))
        assertFalse(hasTable(target, "book_banner"))
    }

    @Test
    fun `the schema hash ignores the optional tables`() {
        val without = db("a.db", banners = null, protections = null)
        val with = db("b.db", banners = mapOf(1L to "x"), protections = mapOf(1L to 2))
        assertEquals(wholeHash(without), wholeHash(with))
    }

    private fun db(name: String, banners: Map<Long, String>?, protections: Map<Long, Int>?): Path {
        val p = path(name)
        connect(p).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book (id, title) VALUES (1, 'א'), (2, 'ב'), (3, 'ג')")
            }
            if (banners != null) insertAll(conn, "book_banner", banners)
            if (protections != null) insertAll(conn, "book_protection", protections)
        }
        return p
    }

    private fun insertAll(conn: Connection, table: String, rows: Map<Long, Any>) {
        val spec = OPTIONAL_PATCH_TABLES.single { it.name == table }
        conn.createStatement().use { it.execute(spec.ddl) }
        conn.prepareStatement("INSERT INTO $table (${spec.columns.joinToString()}) VALUES (?, ?)").use { ps ->
            for ((id, v) in rows) {
                ps.setLong(1, id); ps.setObject(2, v); ps.executeUpdate()
            }
        }
    }

    private fun path(name: String): Path = tmp.newFolder().toPath().resolve(name)

    private fun copy(source: Path): Path = path("target.db").also { Files.copy(source, it) }

    private fun connect(p: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${p.toAbsolutePath()}")

    private fun wholeHash(p: Path): String = connect(p).use { LogicalContentHasher().compute(it) }

    private fun query(p: Path, sql: String): List<List<Any?>> = connect(p).use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                val n = rs.metaData.columnCount
                buildList { while (rs.next()) add((1..n).map { rs.getObject(it) }) }
            }
        }
    }

    private fun hasTable(p: Path, name: String): Boolean = connect(p).use { conn ->
        conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { it.next() }
        }
    }
}
