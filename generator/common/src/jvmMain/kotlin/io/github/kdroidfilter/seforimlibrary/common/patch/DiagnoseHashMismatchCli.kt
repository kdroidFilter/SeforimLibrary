package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.sql.DriverManager

/**
 * Diagnostic CLI: applies a patch.db on a copy of prevDb, then computes the
 * LogicalContentHasher per-table (each DB under its own schema version) on both
 * target-after-apply and newDb,
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

    val targetHashes = perTableLogicalHashes(target, "targetSchemaVersion")
    val newHashes = perTableLogicalHashes(new, "newSchemaVersion")
    val tables = (newHashes.keys + targetHashes.keys).distinct()

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
