package io.github.kdroidfilter.seforimlibrary.search

import java.nio.file.Files
import java.sql.DriverManager
import kotlin.concurrent.thread
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class SqliteReaderTest {
    private fun withDatabase(block: (String) -> Unit) {
        val file = Files.createTempFile("reader", ".db")
        try {
            DriverManager.getConnection("jdbc:sqlite:$file").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE word (value TEXT, base TEXT)")
                    stmt.execute("INSERT INTO word VALUES ('שלום', 'שלם'), ('שלם', 'שלם'), ('תורה', 'ירה')")
                }
            }
            block(file.toString())
        } finally {
            file.deleteIfExists()
        }
    }

    @Test
    fun `a statement run again binds its new arguments`() =
        withDatabase { path ->
            openReadOnlySqlite(path).use { db ->
                val sql = "SELECT value FROM word WHERE base = ? ORDER BY value"
                assertEquals(listOf("שלום", "שלם"), db.query(sql, listOf("שלם")).map { it["value"] })
                assertEquals(listOf("תורה"), db.query(sql, listOf("ירה")).map { it["value"] })
                assertEquals(emptyList(), db.query(sql, listOf("אין")))
            }
        }

    @Test
    fun `a closed reader refuses queries, from any thread`() =
        withDatabase { path ->
            val db = openReadOnlySqlite(path)
            db.query("SELECT 1 AS one")
            db.close()
            assertFailsWith<IllegalStateException> { db.query("SELECT 1 AS one") }
            // A thread that never queried doesn't open a connection either
            var failure: Throwable? = null
            thread { failure = runCatching { db.query("SELECT 1 AS one") }.exceptionOrNull() }.join()
            assertIs<IllegalStateException>(failure)
            db.close()
        }
}
