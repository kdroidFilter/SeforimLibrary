package io.github.kdroidfilter.seforimlibrary.bookimporter

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlin.io.path.absolutePathString
import kotlin.io.path.exists

class ImportCoordinator(
    private val dbPath: Path,
    private val roots: List<Path>,
    private val includeCatalog: Boolean,
    private val includeIndexes: Boolean,
    private val duplicatePolicy: DuplicatePolicy,
    private val progress: (Float, String) -> Unit,
    private val log: (String) -> Unit
) {
    suspend fun execute() {
        currentCoroutineContext().ensureActive()
        val backup = createBackup(dbPath)
        log("[INFO] Backup created: ${backup.absolutePathString()}")
        log("[INFO] Duplicate policy selected: ${duplicatePolicy.label}")

        runCatching {
            var stepIndex = 0
            val totalSteps = roots.size * 2 + (if (includeCatalog) 1 else 0) + (if (includeIndexes) 1 else 0)

            roots.forEach { root ->
                currentCoroutineContext().ensureActive()
                val sourceRoot = resolveOtzariaSourceRoot(root)
                val sourceDir = sourceRoot.absolutePathString()

                progress(stepIndex.toFloat() / totalSteps, "Importing lines from $sourceDir")
                log("[INFO] Running GenerateLines for root: $sourceDir")
                runWithProperties(
                    mapOf(
                        "appendExistingDb" to "true",
                        "baseDb" to dbPath.absolutePathString(),
                        "persistDb" to dbPath.absolutePathString(),
                        "sourceDir" to sourceDir
                    )
                ) {
                    invokeArrayMain("io.github.kdroidfilter.seforimlibrary.otzariasqlite.GenerateLinesKt", arrayOf(":memory:"))
                }
                stepIndex++

                currentCoroutineContext().ensureActive()
                progress(stepIndex.toFloat() / totalSteps, "Importing links from $sourceDir")
                log("[INFO] Running GenerateLinks for root: $sourceDir")
                runWithProperties(
                    mapOf(
                        "baseDb" to dbPath.absolutePathString(),
                        "persistDb" to dbPath.absolutePathString(),
                        "sourceDir" to sourceDir
                    )
                ) {
                    invokeArrayMain("io.github.kdroidfilter.seforimlibrary.otzariasqlite.GenerateLinksKt", arrayOf(":memory:"))
                }
                stepIndex++
            }

            if (includeCatalog) {
                currentCoroutineContext().ensureActive()
                progress(stepIndex.toFloat() / totalSteps, "Updating catalog.pb")
                log("[INFO] Building catalog.pb")
                invokeArrayMain("io.github.kdroidfilter.seforimlibrary.catalog.BuildCatalogKt", arrayOf(dbPath.absolutePathString()))
                stepIndex++
            }

            if (includeIndexes) {
                currentCoroutineContext().ensureActive()
                progress(stepIndex.toFloat() / totalSteps, "Building Lucene indexes")
                log("[INFO] Building Lucene indexes")
                runWithProperties(mapOf("seforimDb" to dbPath.absolutePathString())) {
                    invokeNoArgMain("io.github.kdroidfilter.seforimlibrary.searchindex.BuildLuceneIndexKt")
                }
                stepIndex++
            }

            if (totalSteps > 0) {
                progress(stepIndex.toFloat() / totalSteps, "Import completed")
            }
        }.onFailure { error ->
            restoreBackup(backup, dbPath)
            log("[ERROR] Import failed, DB restored from backup: ${error.message}")
            throw error
        }
    }

    private fun createBackup(db: Path): Path {
        require(db.exists()) { "Database does not exist: ${db.absolutePathString()}" }
        val backup = db.resolveSibling("${db.fileName}.before-import.bak")
        Files.copy(db, backup, StandardCopyOption.REPLACE_EXISTING)
        return backup
    }

    private fun restoreBackup(backup: Path, db: Path) {
        Files.copy(backup, db, StandardCopyOption.REPLACE_EXISTING)
    }
}


private fun resolveOtzariaSourceRoot(root: Path): Path {
    val normalized = root.toAbsolutePath().normalize()
    val dirName = normalized.fileName?.toString()
    return if (dirName == "אוצריא") normalized.parent ?: normalized else normalized
}

private fun invokeArrayMain(className: String, args: Array<String>) {
    val clazz = Class.forName(className)
    val main = clazz.getMethod("main", Array<String>::class.java)
    main.invoke(null, args)
}

private fun invokeNoArgMain(className: String) {
    val clazz = Class.forName(className)
    val main = clazz.getMethod("main")
    main.invoke(null)
}

private inline fun runWithProperties(properties: Map<String, String>, block: () -> Unit) {
    val previous = mutableMapOf<String, String?>()
    properties.forEach { (key, value) ->
        previous[key] = System.getProperty(key)
        System.setProperty(key, value)
    }
    try {
        block()
    } finally {
        previous.forEach { (key, oldValue) ->
            if (oldValue == null) System.clearProperty(key) else System.setProperty(key, oldValue)
        }
    }
}
