package io.github.kdroidfilter.seforimlibrary.generator

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity

/**
 * Standalone entrypoint that downloads the latest SeforimAcronymizer .db
 * and saves it under build/acronymizer/acronymizer.db. Prints the path.
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("DownloadAcronymizer")
    val path = AcronymizerFetcher.ensureLocalDb(logger)
    logger.i { "Acronymizer DB ready at: ${path.toAbsolutePath()}" }
    println(path.toAbsolutePath().toString())
}

