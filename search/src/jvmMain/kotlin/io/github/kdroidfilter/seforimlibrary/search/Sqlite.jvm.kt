package io.github.kdroidfilter.seforimlibrary.search

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ConcurrentLinkedQueue

/** Through JDBC (sqlite-jdbc), with a connection per thread: SQLite connections serialize their statements. */
actual fun openReadOnlySqlite(path: String): SqliteReader = JdbcSqliteReader(path).also { it.query("SELECT 1") }

private class JdbcSqliteReader(
    path: String,
) : SqliteReader {
    private val url = "jdbc:sqlite:${java.io.File(path).absolutePath}"
    private val opened = ConcurrentLinkedQueue<Connection>()
    private val connection =
        ThreadLocal.withInitial {
            DriverManager.getConnection(url).apply {
                autoCommit = false
                // Enforce read-only queries without altering connection flags post-open
                createStatement().use { it.execute("PRAGMA query_only=ON") }
                opened += this
            }
        }

    override fun query(
        sql: String,
        args: List<String>,
    ): List<Map<String, String?>> =
        connection.get().prepareStatement(sql).use { stmt ->
            args.forEachIndexed { i, arg -> stmt.setString(i + 1, arg) }
            stmt.executeQuery().use { rs ->
                val columns = (1..rs.metaData.columnCount).map { rs.metaData.getColumnLabel(it) }
                val rows = ArrayList<Map<String, String?>>()
                while (rs.next()) rows += columns.associateWith { rs.getString(it) }
                rows
            }
        }

    override fun close() {
        generateSequence { opened.poll() }.forEach { runCatching { it.close() } }
    }
}
