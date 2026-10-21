package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.divine.DivineNameOverrides
import io.github.kdroidfilter.seforimlibrary.common.divine.openReadOnly
import io.github.kdroidfilter.seforimlibrary.common.divine.rebuildLineDivineNameIndex
import io.github.kdroidfilter.seforimlibrary.common.divine.unmatchedOverrideRefs
import io.github.kdroidfilter.seforimlibrary.common.divine.writeDivineNameReport
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Builds `line_divine_name` (see `LineDivineNameIndex.kt`) with the curated verse list
 * from otzaria-library `ForDB/divine_name_overrides.csv`.
 *
 * The list is required: only it can suppress the profane Tanach verses (Pesel Micha,
 * the golden calf, the judges…), so a missing file or any row it cannot read fails the build.
 *
 * System properties: `seforimDb`; `divineNameOverridesCsv` (a local CSV instead of
 * ForDB); `reportDir` (dry run: the DB is opened read-only, nothing is written to it).
 *
 * Usage:
 *   ./gradlew :sefariasqlite:buildLineDivineNameIndex -PseforimDb=/path/to/seforim.db
 *   ./gradlew :sefariasqlite:reportLineDivineNames -PseforimDb=… -PreportDir=…
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildLineDivineNames")
    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    try {
        val overrides = loadDivineNameOverrides(logger)
        val reportDir = System.getProperty("reportDir")?.takeIf { it.isNotBlank() }
        if (reportDir != null) {
            openReadOnly(dbPath).use { conn ->
                val report = writeDivineNameReport(conn, Paths.get(reportDir), logger, overrides)
                logger.i { "line_divine_name report: ${report.total} spans over ${report.lines} lines -> $reportDir" }
            }
            return
        }
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            val report = rebuildLineDivineNameIndex(conn, logger, overrides)
            logger.i {
                "line_divine_name: ${report.total} spans over ${report.lines} lines " +
                    "(${report.countsByKindSource.entries.joinToString { "${it.key}=${it.value}" }}); " +
                    "HVYH gate: ${report.appHvyh} app matches, ${report.hvyhNotEmitted} inside markup; " +
                    "line_note_divine_name: ${report.noteSpans} spans in ${report.noteBodies} note bodies"
            }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to build line_divine_name; aborting" }
        exitProcess(1)
    }
}

/** CSV records of the list: the local override file, else the required ForDB file. */
internal fun loadDivineNameOverrideRecords(
    logger: Logger,
    forDbFile: (String) -> List<String> = { downloadRequiredForDbFile(it, logger) },
): List<List<String>> {
    val local = System.getProperty("divineNameOverridesCsv")?.takeIf { it.isNotBlank() }
    val text = if (local != null) {
        logger.i { "Reading the divine-name override list from $local" }
        check(Files.isRegularFile(Paths.get(local))) { "${DivineNameOverrides.FILE_NAME} not found at $local" }
        Files.readString(Paths.get(local)).removePrefix("﻿")
    } else {
        forDbText(forDbFile(DivineNameOverrides.FILE_NAME))
    }
    return parseForDbCsvRecords(text)
}

/** The list; fails, naming every problem, when it is absent or any row is not understood. */
internal fun loadDivineNameOverrides(
    logger: Logger,
    forDbFile: (String) -> List<String> = { downloadRequiredForDbFile(it, logger) },
): DivineNameOverrides {
    val parsed = DivineNameOverrides.parse(loadDivineNameOverrideRecords(logger, forDbFile))
    parsed.problems.forEach { problem -> logger.e { "${DivineNameOverrides.FILE_NAME}: $problem" } }
    return checkNotNull(parsed.overrides) {
        "${DivineNameOverrides.FILE_NAME} has ${parsed.problems.size} problem(s): ${parsed.problems.joinToString("; ")}"
    }
}

/**
 * The ForDB gate: fails like the build on a missing file or a row it cannot read;
 * refs that match no verse in [conn] are warnings only.
 */
internal fun validateDivineNameOverrides(conn: Connection, logger: Logger): Int {
    val overrides = loadDivineNameOverrides(logger)
    unmatchedOverrideRefs(conn, overrides).forEach { ref ->
        logger.w { "ForDB ${DivineNameOverrides.FILE_NAME}: $ref matches no verse in the DB" }
    }
    return overrides.entries.size
}
