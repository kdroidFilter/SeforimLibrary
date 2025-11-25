package io.github.kdroidfilter.seforimlibrary.sefaria

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Paths

/**
 * JVM entry point: download the latest Sefaria export, convert it to Otzaria format,
 * and print the output directory.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DownloadSefaria")

    val exportRoot = SefariaFetcher.ensureLocalExport(logger)
    val outputRoot = Paths.get("build", "sefaria", "otzaria")

    val converter = SefariaToOtzariaConverter(
        exportRoot = exportRoot,
        outputRoot = outputRoot,
        logger = logger
    )
    val result = converter.convert(generateRefs = true, copyMetadata = true)

    logger.i { "Sefaria converted. Library at ${result.libraryRoot.toAbsolutePath()}" }
    result.refsCsv?.let { logger.i { "refs.csv written to ${it.toAbsolutePath()}" } }
    result.linksRoot?.let { logger.i { "links JSON written under ${it.toAbsolutePath()}" } }
    logger.i { "files_manifest.json at ${result.manifestPath?.toAbsolutePath()}" }
    println(outputRoot.toAbsolutePath().toString())
}
