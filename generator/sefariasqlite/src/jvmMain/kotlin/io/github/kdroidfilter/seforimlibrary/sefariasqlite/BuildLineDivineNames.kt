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
 * The list is optional. Without it — or when any row of it is not understood — every
 * Tanach base-book line is limited to the HVYH rule, because only the list can
 * suppress the profane verses there (Pesel Micha, the golden calf, the judges…).
 * Every other book keeps all deterministic rules: the list never applied to them.
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

/** CSV records of the list: the local override file, else the optional ForDB file, else null. */
internal fun loadDivineNameOverrideRecords(logger: Logger): List<List<String>>? {
    val local = System.getProperty("divineNameOverridesCsv")?.takeIf { it.isNotBlank() }
    val text = if (local != null) {
        logger.i { "Reading the divine-name override list from $local" }
        Files.readString(Paths.get(local)).removePrefix("﻿")
    } else {
        downloadOptionalForDbFile(DivineNameOverrides.FILE_NAME, logger)?.let(::forDbText) ?: return null
    }
    return parseForDbCsvRecords(text)
}

/** The usable list, or null (absent or not fully understood) with every problem logged. */
internal fun loadDivineNameOverrides(logger: Logger): DivineNameOverrides? {
    val records = loadDivineNameOverrideRecords(logger)
    if (records == null) {
        logger.w { "${DivineNameOverrides.FILE_NAME} is absent: Tanach base lines get HVYH only" }
        return null
    }
    val parsed = DivineNameOverrides.parse(records)
    parsed.problems.forEach { problem -> logger.w { problem } }
    if (parsed.overrides == null) {
        logger.w { "${DivineNameOverrides.FILE_NAME} has ${parsed.problems.size} problem(s): ignored whole, Tanach base lines get HVYH only" }
    }
    return parsed.overrides
}

/**
 * The ForDB gate: warnings only. Reports rows the build would not understand (which
 * would make it ignore the whole list) and refs that match no verse in [conn].
 */
internal fun validateDivineNameOverrides(conn: Connection, logger: Logger): Int {
    val records = loadDivineNameOverrideRecords(logger) ?: return 0
    val parsed = DivineNameOverrides.parse(records)
    parsed.problems.forEach { problem -> logger.w { "ForDB ${DivineNameOverrides.FILE_NAME}: $problem" } }
    val overrides = parsed.overrides ?: return 0
    unmatchedOverrideRefs(conn, overrides).forEach { ref ->
        logger.w { "ForDB ${DivineNameOverrides.FILE_NAME}: $ref matches no verse in the DB" }
    }
    return overrides.entries.size
}
