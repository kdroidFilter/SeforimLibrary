package io.github.kdroidfilter.seforimlibrary.search

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.util.concurrent.ConcurrentLinkedQueue

/** Through JDBC (sqlite-jdbc), with a connection per thread: SQLite connections serialize their statements. */
actual fun openReadOnlySqlite(path: String): SqliteReader = JdbcSqliteReader(path).also { it.query("SELECT 1") }

private class JdbcSqliteReader(
    path: String,
) : SqliteReader {
    // A thread's connection, with its statements prepared once: the dictionary runs the same lookup for every word
    private class ThreadConnection(
        val connection: Connection,
    ) {
        val statements = HashMap<String, PreparedStatement>()
    }

    private val url = "jdbc:sqlite:${java.io.File(path).absolutePath}"
    private val opened = ConcurrentLinkedQueue<Connection>()

    @Volatile
    private var closed = false
    private val connection =
        ThreadLocal.withInitial {
            val connection =
                DriverManager.getConnection(url).apply {
                    autoCommit = false
                    // Enforce read-only queries without altering connection flags post-open
                    createStatement().use { it.execute("PRAGMA query_only=ON") }
                }
            opened += connection
            // Opened by a thread racing close(): closed here, as close() may already have drained the queue
            if (closed) {
                connection.close()
                error("SQLite: the database is closed")
            }
            ThreadConnection(connection)
        }

    override fun query(
        sql: String,
        args: List<String>,
    ): List<Map<String, String?>> {
        // A thread that never queried would otherwise open a connection no one closes
        check(!closed) { "SQLite: the database is closed" }
        val thread = connection.get()
        val stmt = thread.statements.getOrPut(sql) { thread.connection.prepareStatement(sql) }
        args.forEachIndexed { i, arg -> stmt.setString(i + 1, arg) }
        return stmt.executeQuery().use { rs ->
            val columns = (1..rs.metaData.columnCount).map { rs.metaData.getColumnLabel(it) }
            val rows = ArrayList<Map<String, String?>>()
            while (rs.next()) rows += columns.associateWith { rs.getString(it) }
            rows
        }
    }

    // Closing a connection closes its statements
    override fun close() {
        closed = true
        generateSequence { opened.poll() }.forEach { runCatching { it.close() } }
    }
}
