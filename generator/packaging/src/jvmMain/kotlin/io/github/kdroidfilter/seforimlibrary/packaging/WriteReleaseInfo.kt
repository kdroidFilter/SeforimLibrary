package io.github.kdroidfilter.seforimlibrary.packaging

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import kotlin.system.exitProcess

/**
 * Write release information to a release_info.txt file.
 * The release name follows the format used in CI: yyyyMMddHHmmss (UTC).
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :packaging:writeReleaseInfo
 *   ./gradlew -p SeforimLibrary :packaging:writeReleaseInfo -PseforimDb=/path/to/seforim.db
 *   or with custom release name:
 *   ./gradlew -p SeforimLibrary :packaging:writeReleaseInfo -PreleaseName=20251108195010
 *
 * Output (by default):
 *   Creates release_info.txt next to the DB file (same directory as seforim.db).
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("WriteReleaseInfo")

    // Get release name from property or generate current UTC timestamp
    val releaseName = System.getProperty("releaseName")
        ?: ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    val dbPath = resolveDbPath(args)
    val outputPath = dbPath.resolveSibling("release_info.txt")
    
    try {
        Files.createDirectories(outputPath.parent)
        
        // Write release name to file
        Files.writeString(outputPath, releaseName)
        
        logger.i { "Release info written to $outputPath: $releaseName" }
        println("Release: $releaseName")
        
    } catch (e: Exception) {
        logger.e(e) { "Failed to write release info to $outputPath" }
        exitProcess(1)
    }
}

private fun resolveDbPath(args: Array<String>): Path {
    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    return Paths.get(dbPathStr)
}
