package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.sql.DriverManager
import kotlin.system.exitProcess

/**
 * CLI tool that produces a patch.db from (prev, new) seforim.db pair, then
 * applies it onto a fresh copy of prev and asserts that the resulting
 * logical content hash matches new's.
 *
 * Acts as the **Phase 4 acceptance test** in CI:
 *
 *   ./gradlew :generator-common:producePatchAndVerify \
 *       -PprevDb=build/seforim.db.runA  \
 *       -PnewDb=build/seforim.db.runB   \
 *       -Pout=build/patch.db
 *
 * Exits with code 0 on success and prints the produced patch path, or
 * fails loud with a clear hash-mismatch message.
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("PatchPipelineCli")

    val prev = args.getOrNull(0) ?: System.getProperty("prevDb") ?: error("prev db missing")
    val new = args.getOrNull(1) ?: System.getProperty("newDb") ?: error("new db missing")
    val out = args.getOrNull(2) ?: System.getProperty("out") ?: "build/patch.db"
    val from = (System.getProperty("fromVersion") ?: System.getenv("FROM_VERSION") ?: "1").toInt()
    val to = (System.getProperty("toVersion") ?: System.getenv("TO_VERSION") ?: (from + 1).toString()).toInt()

    val prevPath = Paths.get(prev)
    val newPath = Paths.get(new)
    val outPath = Paths.get(out)
    require(Files.exists(prevPath)) { "prev not found at $prev" }
    require(Files.exists(newPath)) { "new not found at $new" }

    logger.i { "Producing patch $prev → $new at $out (v$from → v$to)" }
    val output = PatchDbProducer(logger).produce(
        prevDb = prevPath,
        newDb = newPath,
        outputPath = outPath,
        fromVersion = from,
        toVersion = to,
    )
    val totalUpserts = output.upsertCounts.values.sum()
    val totalDeletes = output.deleteCounts.values.sum()
    logger.i { "patch.db produced — upserts=$totalUpserts, deletes=$totalDeletes" }
    logger.i { "  upserts by table: ${output.upsertCounts.filterValues { it > 0 }}" }
    logger.i { "  deletes by table: ${output.deleteCounts.filterValues { it > 0 }}" }

    // Verify apply: copy prev, apply patch, hash, compare with hash(new).
    val target = outPath.resolveSibling("verify-${outPath.fileName}")
    Files.copy(prevPath, target, StandardCopyOption.REPLACE_EXISTING)
    val newHash = DriverManager.getConnection("jdbc:sqlite:${newPath.toAbsolutePath()}").use {
        LogicalContentHasher().compute(it)
    }
    DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
        conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
        PatchApplier(logger).apply(conn = conn, patchDb = outPath, expectedToContentHash = newHash)
    }
    runCatching { Files.deleteIfExists(target) }
    logger.i { "✅ Patch apply verified: target hash matches new ($newHash)" }
}
