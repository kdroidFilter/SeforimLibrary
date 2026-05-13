package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.sql.Connection
import java.sql.DriverManager

/**
 * Diagnostic CLI: applies a patch.db on a copy of prevDb, then computes the
 * LogicalContentHasher per-table on both target-after-apply and newDb,
 * reporting which tables diverge.
 *
 * Usage:
 *   ./gradlew :generator-common:diagnoseHashMismatch \
 *       -PprevDb=… -PnewDb=… -Ppatch=…
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DiagnoseHashMismatchCli")

    val prev = Paths.get(System.getProperty("prevDb") ?: error("prevDb missing"))
    val new = Paths.get(System.getProperty("newDb") ?: error("newDb missing"))
    val patch = Paths.get(System.getProperty("patch") ?: error("patch missing"))

    val target = prev.resolveSibling("diag-target.db")
    Files.copy(prev, target, StandardCopyOption.REPLACE_EXISTING)

    DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
        conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
        PatchApplier(logger).apply(conn, patch)
    }

    val tables = LogicalContentHasher.DEFAULT_TABLES
    val targetHashes = perTableHash(target, tables)
    val newHashes = perTableHash(new, tables)

    println()
    println("Per-table hash comparison (target = v1 + patch, expected = new = v2)")
    println("====================================================================")
    var diffCount = 0
    for (table in tables) {
        val t = targetHashes[table] ?: "MISSING"
        val n = newHashes[table] ?: "MISSING"
        val mark = if (t == n) "✓" else "✗"
        if (t != n) diffCount++
        println("$mark $table: target=${t.take(12)}… new=${n.take(12)}…")
    }
    println()
    println("$diffCount table(s) diverge.")
}

private fun perTableHash(dbPath: Path, tables: List<String>): Map<String, String> {
    val out = HashMap<String, String>()
    DriverManager.getConnection("jdbc:sqlite:${dbPath.toAbsolutePath()}").use { conn ->
        for (table in tables) {
            out[table] = hashSingleTable(conn, table)
        }
    }
    return out
}

private fun hashSingleTable(conn: Connection, table: String): String {
    // Mirror LogicalContentHasher's encoding for a single table.
    val md = MessageDigest.getInstance("SHA-256")
    val cols = ArrayList<String>()
    conn.createStatement().use { st ->
        st.executeQuery("PRAGMA table_info(\"$table\")").use { rs ->
            while (rs.next()) cols += rs.getString("name")
        }
    }
    if (cols.isEmpty()) return "no_table"
    val sorted = cols.sorted()
    md.update(sorted.joinToString(",", prefix = "cols:").toByteArray())
    md.update(byteArrayOf(0x00))
    val colsCsv = sorted.joinToString(",") { "\"$it\"" }
    val pkOrder = if ("id" in sorted) "id" else colsCsv
    var rowCount = 0
    conn.createStatement().use { st ->
        st.executeQuery("SELECT $colsCsv FROM \"$table\" ORDER BY $pkOrder").use { rs ->
            while (rs.next()) {
                rowCount++
                for (i in 1..sorted.size) {
                    val obj = rs.getObject(i)
                    when {
                        obj == null || rs.wasNull() -> md.update(byteArrayOf(0))
                        obj is ByteArray -> { md.update(byteArrayOf(1)); md.update(obj) }
                        obj is Number -> { md.update(byteArrayOf(2)); md.update(obj.toString().toByteArray()) }
                        else -> { md.update(byteArrayOf(3)); md.update(obj.toString().toByteArray()) }
                    }
                    md.update(byteArrayOf(0x1F))
                }
                md.update(byteArrayOf(0xFF.toByte()))
            }
        }
    }
    val digest = md.digest().joinToString("") { "%02x".format(it) }
    return "$digest (rows=$rowCount)"
}
