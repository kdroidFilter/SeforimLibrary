package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager

/**
 * Per-table LogicalContentHasher comparison between two seforim.db files, each
 * hashed under its own `db_schema_version`. Reports which tables match and which diverge.
 *
 * Required system properties:
 *   - `leftDb`   absolute path to one DB
 *   - `rightDb`  absolute path to the other
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val left = Paths.get(System.getProperty("leftDb") ?: error("-PleftDb= missing"))
    val right = Paths.get(System.getProperty("rightDb") ?: error("-PrightDb= missing"))
    val l = perTableLogicalHashes(left, "leftSchemaVersion")
    val r = perTableLogicalHashes(right, "rightSchemaVersion")
    val tables = (l.keys + r.keys).distinct()
    println()
    println("Per-table comparison")
    println("====================")
    var diff = 0
    for (t in tables) {
        val a = l[t] ?: "MISSING"
        val b = r[t] ?: "MISSING"
        val mark = if (a == b) "✓" else "✗"
        if (a != b) diff++
        println("$mark $t  left=${a.take(12)}… right=${b.take(12)}…")
    }
    println()
    println("$diff table(s) diverge.")
    if (diff == 0) println("✅ The two databases are logically identical.")
}

/**
 * Per-table logical hashes of [db] under its own `db_schema_version` contract (the
 * values a manifest's table hashes carry), each with its row count.
 */
internal fun perTableLogicalHashes(db: Path, schemaKey: String): Map<String, String> {
    val schemaVersion = resolveSchemaVersion(db, schemaKey)
    return DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
        LogicalContentHasher.forSchemaVersion(schemaVersion).computeReport(conn).tableHashes
            .mapValues { (table, hash) -> "$hash (rows=${rowCount(conn, table)})" }
    }
}

private fun rowCount(conn: Connection, table: String): String {
    val exists = conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").use { ps ->
        ps.setString(1, table)
        ps.executeQuery().use { it.next() }
    }
    if (!exists) return "no_table"
    return conn.createStatement().use { st ->
        st.executeQuery("SELECT count(*) FROM \"$table\"").use { rs -> rs.next(); rs.getLong(1).toString() }
    }
}
