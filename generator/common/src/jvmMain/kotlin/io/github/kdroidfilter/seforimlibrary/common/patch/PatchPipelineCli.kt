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
 * Exit codes:
 *   - `0` patch produced and verified.
 *   - [UnpatchableAnchorException.EXIT_CODE] (3) this anchor must not ship a
 *     delta: either the pair cannot be expressed as one at all (missing PK
 *     column, or a column dropped without a `db_schema_version` bump), or the
 *     delta is oversized (see [PatchSizeGuard]). A `<out>.unpatchable` marker file
 *     carrying the reason is written next to the patch, the Gradle task lets
 *     the build succeed, and the release workflow skips just that anchor.
 *   - any other non-zero: a genuine failure (hash mismatch, IO, …) — the
 *     Gradle task rethrows it and the release fails.
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

    // Schema versions signed into the manifest must reflect the real DBs, not
    // a hard-coded "1". Precedence: explicit -P override, else schema_meta of
    // the DB itself; hard-fail if neither is available (no silent default).
    val fromSchemaVersion = resolveSchemaVersion(prevPath, "fromSchemaVersion")
    val toSchemaVersion = resolveSchemaVersion(newPath, "toSchemaVersion")

    logger.i { "Producing patch $prev → $new at $out (v$from → v$to, schema $fromSchemaVersion → $toSchemaVersion)" }
    // Marker consumed by the Gradle task / release workflow — clear any
    // leftover from an earlier anchor before this run can write its own.
    val unpatchableMarker = outPath.resolveSibling(
        "${outPath.fileName}${UnpatchableAnchorException.MARKER_SUFFIX}",
    )
    runCatching { Files.deleteIfExists(unpatchableMarker) }
    val output = try {
        PatchDbProducer(logger).produce(
            prevDb = prevPath,
            newDb = newPath,
            outputPath = outPath,
            fromVersion = from,
            toVersion = to,
            fromSchemaVersion = fromSchemaVersion,
            toSchemaVersion = toSchemaVersion,
        )
    } catch (unpatchable: UnpatchableAnchorException) {
        // Not a build failure: this (prev → new) pair simply cannot be
        // expressed as a delta. Record why, then exit with the reserved code
        // so the caller can skip this anchor and keep the rest of the fan.
        Files.createDirectories(unpatchableMarker.toAbsolutePath().parent)
        Files.write(
            unpatchableMarker,
            "${unpatchable.message}\n".toByteArray(Charsets.UTF_8),
        )
        // The producer aborts with its half-built "<out>.tmp" still on disk.
        runCatching { Files.deleteIfExists(outPath.resolveSibling("${outPath.fileName}.tmp")) }
        logger.w {
            "Anchor v$from → v$to is NOT patchable (${unpatchable.table}: " +
                "${unpatchable.columns.joinToString(", ")}): ${unpatchable.message} — " +
                "wrote $unpatchableMarker and exiting ${UnpatchableAnchorException.EXIT_CODE}"
        }
        exitProcess(UnpatchableAnchorException.EXIT_CODE)
    }

    val totalUpserts = output.upsertCounts.values.sum()
    val totalDeletes = output.deleteCounts.values.sum()
    // Logged before the size guard: on a skip these counts are the only clue to
    // WHY the delta blew up (id churn shows as an upsert per line).
    logger.i { "patch.db produced — upserts=$totalUpserts, deletes=$totalDeletes" }
    logger.i { "  upserts by table: ${output.upsertCounts.filterValues { it > 0 }}" }
    logger.i { "  deletes by table: ${output.deleteCounts.filterValues { it > 0 }}" }

    // Oversized-delta guard. Runs before verify/compress: an anchor we will not
    // publish should not burn the apply-and-hash pass either.
    val sizeDecision = PatchSizeGuard.decide(
        patchUncompressedSize = Files.size(outPath),
        newDbSize = Files.size(newPath),
        maxRatio = PatchSizeGuard.configuredMaxRatio(),
    )
    if (!sizeDecision.publish) {
        // Leading token, not prose: the release workflow tells a size-guard skip
        // (still a publishable full-only release) from a structural one by it.
        val reason = "${PatchSizeGuard.MARKER_REASON_TOKEN}: anchor v$from → v$to produced an oversized delta: " +
            "${sizeDecision.describe()}, upserts=$totalUpserts deletes=$totalDeletes — " +
            "a full-bundle download is faster for the client than applying it"
        Files.createDirectories(unpatchableMarker.toAbsolutePath().parent)
        Files.write(unpatchableMarker, "$reason\n".toByteArray(Charsets.UTF_8))
        runCatching { Files.deleteIfExists(outPath) }
        logger.w { "$reason — wrote $unpatchableMarker and exiting ${UnpatchableAnchorException.EXIT_CODE}" }
        exitProcess(UnpatchableAnchorException.EXIT_CODE)
    }
    logger.i { "Patch size within budget: ${sizeDecision.describe()}" }

    // Verify apply: copy prev, apply patch, hash, compare with hash(new).
    val target = outPath.resolveSibling("verify-${outPath.fileName}")
    Files.copy(prevPath, target, StandardCopyOption.REPLACE_EXISTING)
    val newReport = DriverManager.getConnection("jdbc:sqlite:${newPath.toAbsolutePath()}").use {
        LogicalContentHasher.forSchemaVersion(toSchemaVersion).computeReport(it)
    }
    val newHash = newReport.wholeHash
    DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
        conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
        // The producer ships upserts/deletes for every table in
        // PATCH_TABLES_IN_FK_ORDER (including the book_* junctions,
        // book_generation and link_range) — all confirmed to round-trip
        // exactly across the v13→v14/v14→v15 verifications. HARD gate: a
        // patch that does not reproduce the target byte-for-logical-byte is
        // a broken distribution artifact and must never ship with a warning.
        PatchApplier(logger).apply(
            conn = conn,
            patchDb = outPath,
            expectedToContentHash = newHash,
            expectedToSchemaVersion = toSchemaVersion,
        )
        val appliedReport = LogicalContentHasher.forSchemaVersion(toSchemaVersion).computeReport(conn)
        val appliedHash = appliedReport.wholeHash
        check(appliedHash == newHash) {
            "Patch verification FAILED: applied=$appliedHash expected=$newHash — " +
                "inspect with diagnoseHashMismatch; refusing to publish this patch."
        }
        // The per-table hashes ship in the manifest and drive the client's
        // partial verification, so they are gated as hard as the whole hash.
        val divergent = newReport.tableHashes.keys.filter {
            appliedReport.tableHashes[it] != newReport.tableHashes[it]
        }
        check(divergent.isEmpty()) {
            "Patch verification FAILED: per-table hashes diverge for ${divergent.joinToString(", ")} — " +
                "refusing to publish this patch."
        }
        logger.i { "✅ Patch apply verified: target hash matches new ($newHash)" }
    }
    runCatching { Files.deleteIfExists(target) }

    // Stuff the new catalog.pb into patch.blobs so CatalogUpdater can pull
    // it out client-side. Without this the manifest claims a catalogBlobName
    // but the patch.db ships with an empty blobs table — caught by the real
    // e2e on Zayit (catalog.pb timestamp stayed at v1).
    //
    // On THIS branch catalog.pb is no longer produced at all (see BRANCH-STACK
    // "ביטול catalog.pb"; the release workflow even `rm -f build/catalog.pb`s any
    // stale copy a dirty self-hosted workspace might smuggle in), and the
    // manifest below correctly omits catalogBlobName when nothing was embedded —
    // so a consumer never looks for a blob that is not there. The default,
    // nobody-asked-for-a-catalog case is therefore expected and is reported as
    // INFO; it was a WARN, which meant every cycle shipped 5 standing warnings
    // (one per anchor) that no one could ever act on. Only an EXPLICIT request
    // (-DcatalogPb / CATALOG_PB_PATH) pointing at a missing file is a real gap
    // and keeps its warning.
    val requestedCatalogPath = System.getProperty("catalogPb") ?: System.getenv("CATALOG_PB_PATH")
    val catalogPath = requestedCatalogPath
        ?: outPath.resolveSibling("catalog.pb").toAbsolutePath().toString()
    val catalogFile = Paths.get(catalogPath)
    val catalogEmbedded = Files.isRegularFile(catalogFile)
    if (catalogEmbedded) {
        DriverManager.getConnection("jdbc:sqlite:${outPath.toAbsolutePath()}").use { conn ->
            conn.prepareStatement("INSERT OR REPLACE INTO blobs(name, content) VALUES (?, ?)").use { ps ->
                ps.setString(1, "catalog.pb")
                ps.setBytes(2, Files.readAllBytes(catalogFile))
                ps.executeUpdate()
            }
        }
        logger.i { "Embedded catalog.pb (${Files.size(catalogFile)} bytes) into patch.blobs" }
    } else if (requestedCatalogPath != null) {
        logger.w {
            "No catalog.pb at the explicitly requested $catalogPath — patch ships without a " +
                "catalog blob and its manifest omits catalogBlobName"
        }
    } else {
        logger.i {
            "no catalog.pb produced by this pipeline — patch ships without a catalog blob and " +
                "its manifest omits catalogBlobName, which is the supported shape"
        }
    }

    // Compress the patch with zstd. The .db file remains around so
    // producePatchAndVerify's strict invariant can re-hash it locally if
    // needed; releases ship only the .zst (~6× smaller).
    // Default matches PackageArtifacts (full bundle): level 22 (ultra).
    // Slower than 19 but consistent end-to-end and squeezes a few extra %
    // off each patch. Override via -PzstdLevel for ad-hoc faster CI runs.
    val zstdLevel = (System.getProperty("zstdLevel") ?: System.getenv("ZSTD_LEVEL") ?: "22").toInt()
    val compressed = PatchCompressor.compress(outPath, level = zstdLevel)
    logger.i {
        "Compressed patch.db (zstd L$zstdLevel): ${Files.size(outPath)} → ${compressed.compressedSize} bytes " +
            "(${"%.1f".format(compressed.compressedSize * 100.0 / Files.size(outPath))}%)"
    }

    val prevReport = DriverManager.getConnection("jdbc:sqlite:${prevPath.toAbsolutePath()}").use {
        LogicalContentHasher.forSchemaVersion(fromSchemaVersion).computeReport(it)
    }

    // Emit a per-delta manifest.json next to the .zst.
    ReleaseManifestWriter(logger).writeManifest(
        patchFile = outPath,
        fromVersion = from,
        toVersion = to,
        fromSchemaVersion = fromSchemaVersion,
        toSchemaVersion = toSchemaVersion,
        fromContentHash = prevReport.wholeHash,
        toContentHash = newHash,
        fromTableContentHashes = prevReport.tableHashes,
        toTableContentHashes = newReport.tableHashes,
        compressed = ReleaseManifestWriter.CompressedPatchSpec(
            file = compressed.compressedFile,
            sha256 = compressed.compressedSha256,
            size = compressed.compressedSize,
            compression = "zstd",
        ),
        // Only claim a catalog blob in the manifest when one was actually
        // embedded, so the manifest can't advertise a blob the patch lacks.
        catalogBlobName = if (catalogEmbedded) "catalog.pb" else null,
    )

    val releaseMeta = System.getProperty("releaseMeta")
        ?: System.getenv("RELEASE_META_PATH")
    val fullBundleUrl = System.getProperty("fullBundleUrl") ?: System.getenv("FULL_BUNDLE_URL")
    val fullBundleSha = System.getProperty("fullBundleSha") ?: System.getenv("FULL_BUNDLE_SHA")
    val fullBundleSize = (System.getProperty("fullBundleSize") ?: System.getenv("FULL_BUNDLE_SIZE"))?.toLongOrNull()
    val manifestBaseUrl = System.getProperty("manifestBaseUrl") ?: System.getenv("MANIFEST_BASE_URL")
    if (releaseMeta != null && fullBundleUrl != null && fullBundleSha != null && fullBundleSize != null && manifestBaseUrl != null) {
        ReleaseManifestWriter(logger).upsertReleaseMeta(
            releaseMetaPath = Paths.get(releaseMeta),
            latestVersion = to,
            fullBundle = ReleaseManifestWriter.FullBundleSpec(
                version = to, url = fullBundleUrl, sha256 = fullBundleSha, size = fullBundleSize,
            ),
            newEntry = ReleaseManifestWriter.DeltaEntrySpec(
                fromVersion = from, toVersion = to,
                manifestUrl = "$manifestBaseUrl/${outPath.fileName}.manifest.json",
                totalSize = Files.size(outPath),
            ),
        )
    }
}

/**
 * Resolves a DB's schema version for the manifest. Explicit `-P<propKey>`
 * wins; otherwise reads `schema_meta.db_schema_version` from the DB. Hard
 * fails when neither is available — no silent default (no-fallbacks policy).
 */
internal fun resolveSchemaVersion(dbPath: Path, propKey: String): Int {
    System.getProperty(propKey)?.let { explicit ->
        return explicit.toIntOrNull() ?: error("-P$propKey='$explicit' is not an integer")
    }
    // Guard before JDBC: sqlite would create an empty file at a bad path.
    check(Files.exists(dbPath)) { "DB not found at $dbPath — cannot read schema_meta.db_schema_version" }
    val stamped = DriverManager.getConnection("jdbc:sqlite:${dbPath.toAbsolutePath()}").use { conn ->
        conn.prepareStatement("SELECT value FROM schema_meta WHERE key = 'db_schema_version'").use { ps ->
            ps.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
        }
    }
    return stamped?.toIntOrNull() ?: error(
        "schema_meta.db_schema_version missing (or non-integer) in $dbPath and no -P$propKey supplied — " +
            "stamp the DB via StampSchemaVersionCli or pass -P$propKey explicitly; refusing to guess a schema version",
    )
}
