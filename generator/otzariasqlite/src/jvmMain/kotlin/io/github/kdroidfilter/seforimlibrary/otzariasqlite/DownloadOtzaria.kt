package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity

/**
 * Standalone entrypoint that downloads the latest otzaria-library release (.zip)
 * and extracts it under build/otzaria/source. Prints the path when done.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DownloadOtzaria")
    val path = OtzariaFetcher.ensureLocalSource(logger)
    logger.i { "Otzaria ready at: ${path.toAbsolutePath()}" }
    println(path.toAbsolutePath().toString())
}
