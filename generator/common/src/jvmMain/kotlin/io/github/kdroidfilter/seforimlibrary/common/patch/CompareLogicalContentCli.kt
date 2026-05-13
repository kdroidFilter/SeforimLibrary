package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Paths
import java.security.MessageDigest
import java.sql.Connection
import java.sql.DriverManager

/**
 * Per-table LogicalContentHasher comparison between two seforim.db files.
 * Reports which tables hash-match and which diverge.
 *
 * Required system properties:
 *   - `leftDb`   absolute path to one DB
 *   - `rightDb`  absolute path to the other
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val left = Paths.get(System.getProperty("leftDb") ?: error("-PleftDb= missing"))
    val right = Paths.get(System.getProperty("rightDb") ?: error("-PrightDb= missing"))
    val tables = LogicalContentHasher.DEFAULT_TABLES
    val l = perTable(left.toString(), tables)
    val r = perTable(right.toString(), tables)
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

private fun perTable(dbPath: String, tables: List<String>): Map<String, String> {
    val out = HashMap<String, String>()
    DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
        for (t in tables) out[t] = hashSingle(conn, t)
    }
    return out
}

private fun hashSingle(conn: Connection, table: String): String {
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
    val csv = sorted.joinToString(",") { "\"$it\"" }
    val order = if ("id" in sorted) "id" else csv
    var rows = 0
    conn.createStatement().use { st ->
        st.executeQuery("SELECT $csv FROM \"$table\" ORDER BY $order").use { rs ->
            while (rs.next()) {
                rows++
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
    return "${md.digest().joinToString("") { "%02x".format(it) }} (rows=$rows)"
}
