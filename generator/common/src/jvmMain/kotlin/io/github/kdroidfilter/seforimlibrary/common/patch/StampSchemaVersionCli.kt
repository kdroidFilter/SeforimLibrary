package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
 * Stamps `schema_meta.db_version` (and `schema_meta.db_schema_version`) into
 * a freshly-built `seforim.db` so the delta-update client can read its
 * current version off the live DB.
 *
 * Required system properties (forwarded via Gradle `-P` flags):
 *
 *   - `dbPath`         absolute path to the seforim.db to stamp
 *   - `dbVersion`      integer release version (matches release_meta.json
 *                      `latestVersion` and `deltas[].toVersion`)
 *   - `dbSchemaVersion` integer SQLDelight schema version (optional, defaults
 *                      to [PatchDbSchema.CURRENT_VERSION]); matches the
 *                      manifest's `toSchemaVersion`
 *
 * Idempotent: re-running with the same values is a no-op
 * (`INSERT OR REPLACE`).
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("StampSchemaVersionCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val dbVersion = System.getProperty("dbVersion")?.toIntOrNull()
        ?: error("-PdbVersion= missing or not an integer")
    val dbSchemaVersion = System.getProperty("dbSchemaVersion")?.toIntOrNull()
        ?: PatchDbSchema.CURRENT_VERSION

    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        conn.prepareStatement(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES (?, ?)",
        ).use { ps ->
            ps.setString(1, "db_version"); ps.setString(2, dbVersion.toString()); ps.executeUpdate()
            ps.setString(1, "db_schema_version"); ps.setString(2, dbSchemaVersion.toString()); ps.executeUpdate()
        }
    }
    logger.i {
        "Stamped $dbPath: db_version=$dbVersion, db_schema_version=$dbSchemaVersion"
    }
}
