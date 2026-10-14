package io.github.kdroidfilter.seforimlibrary.common.notes

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
 * Fills `line_note` (see [rebuildLineNoteIndex]) — `:generator-common:buildLineNoteIndex`.
 *
 * Runs after every stage that writes line text and before every stage that stores
 * visible-char offsets into it or reads it as text: the alt-TOC writers, line_dh,
 * line_divine_name, the catalog, splitLineContent and the linker's line dump.
 *
 * Required system property: `dbPath`.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildLineNoteIndexCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        val report = rebuildLineNoteIndex(conn, logger)
        logger.i {
            "line_note: ${report.notes} notes (${report.unanchoredNotes} without a marker) moved out of " +
                "${report.linesChanged} lines; ${report.anchorsRemapped} anchors shifted, ${report.anchorsDropped} dropped; " +
                "left inline: ${report.unclosedLines} unclosed, ${report.orphanMarkerLines} with a marker but no note"
        }
    }
}
