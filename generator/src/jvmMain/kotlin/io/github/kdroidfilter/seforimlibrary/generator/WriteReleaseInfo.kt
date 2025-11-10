package io.github.kdroidfilter.seforimlibrary.generator

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Files
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
 *   ./gradlew -p SeforimLibrary :generator:writeReleaseInfo
 *   or with custom release name:
 *   ./gradlew -p SeforimLibrary :generator:writeReleaseInfo -PreleaseName=20251108195010
 *
 * Output:
 *   Creates release_info.txt in generator/build/ directory containing the release name
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("WriteReleaseInfo")

    // Get release name from property or generate current UTC timestamp
    val releaseName = System.getProperty("releaseName")
        ?: ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    // Output path: generator/build/release_info.txt
    val outputPath = Paths.get("build", "release_info.txt")
    
    try {
        // Ensure build directory exists
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