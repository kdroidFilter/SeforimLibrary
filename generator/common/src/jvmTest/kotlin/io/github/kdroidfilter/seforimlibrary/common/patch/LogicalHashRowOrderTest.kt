package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull

/**
 * From schema 7 the logical hash reads every table in its primary-key order, so
 * SQLite streams it straight from the PK index instead of sorting it in a temp B-tree.
 */
class LogicalHashRowOrderTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `the schema 7 row order is each table's physical primary key`() = freshDb { conn ->
        val rowOrder = requireNotNull(LogicalContentHasher.rowOrderForSchemaVersion(7))
        assertEquals(LogicalContentHasher.TABLES_SCHEMA_7.toSet(), rowOrder.keys)
        for ((table, order) in rowOrder) {
            val pk = query(conn, "SELECT name FROM pragma_table_info('$table') WHERE pk > 0 ORDER BY pk")
            assertEquals(pk, order, "primary key of '$table'")
        }
    }

    @Test
    fun `no schema 7 table is sorted in a temp b-tree`() = freshDb { conn ->
        val hasher = LogicalContentHasher.forSchemaVersion(7)
        for (table in LogicalContentHasher.TABLES_SCHEMA_7) {
            val cols = PatchDbSchema.readTableInfo(conn, "main", table).map { it.name }.sorted()
            val plan = query(conn, "EXPLAIN QUERY PLAN ${hasher.selectSql(table, cols)}").joinToString(" | ")
            assertFalse("TEMP B-TREE" in plan, "'$table' hashes through a sort: $plan")
        }
    }

    @Test
    fun `older schemas keep the frozen id-else-every-column order`() {
        for (schemaVersion in 1..6) assertNull(LogicalContentHasher.rowOrderForSchemaVersion(schemaVersion))
        val legacy = LogicalContentHasher(listOf("line_note"), rowOrder = null)
        assertEquals(
            "SELECT \"body\",\"lineId\" FROM \"line_note\" ORDER BY \"body\",\"lineId\"",
            legacy.selectSql("line_note", listOf("body", "lineId")),
        )
        assertEquals("SELECT \"id\",\"x\" FROM \"t\" ORDER BY id", legacy.selectSql("t", listOf("id", "x")))
    }

    @Test
    fun `a table without a declared row order fails loudly`() {
        assertFailsWith<IllegalArgumentException> {
            LogicalContentHasher(listOf("line_note", "unknown_table"), LogicalContentHasher.rowOrderForSchemaVersion(7))
        }
    }

    private fun freshDb(block: (Connection) -> Unit) {
        val db = tmp.newFolder().toPath().resolve("fresh-seforim.db")
        JdbcSqliteDriver("jdbc:sqlite:${db.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            splitLineContent(conn)
            compressLineContent(conn, threads = 1)
            block(conn)
        }
    }

    private fun query(conn: Connection, sql: String): List<String> = conn.createStatement().use { st ->
        st.executeQuery(sql).use { rs ->
            val col = rs.metaData.columnCount
            buildList { while (rs.next()) add(rs.getString(col)) }
        }
    }
}
