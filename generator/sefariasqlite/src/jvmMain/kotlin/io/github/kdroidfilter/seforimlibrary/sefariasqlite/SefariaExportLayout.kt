package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory

internal fun findDatabaseExportRoot(base: Path): Path {
    if (isDatabaseExportCandidate(base)) return base
    val direct = base.resolve("database_export")
    if (isDatabaseExportCandidate(direct)) return direct

    Files.newDirectoryStream(base).use { ds ->
        for (entry in ds) {
            if (entry.isDirectory() && isDatabaseExportCandidate(entry.resolve("database_export"))) {
                return entry.resolve("database_export")
            }
        }
    }
    throw IllegalStateException("database_export folder not found under $base")
}

private fun isDatabaseExportCandidate(path: Path): Boolean {
    if (!path.isDirectory()) return false
    val jsonDir = path.resolve("json")
    val schemaDir = path.resolve("schemas")
    return jsonDir.isDirectory() && schemaDir.isDirectory()
}

