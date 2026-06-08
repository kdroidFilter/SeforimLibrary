package io.github.kdroidfilter.seforimlibrary.cli

import io.github.vinceglb.filekit.FileKit
import io.github.vinceglb.filekit.databasesDir
import io.github.vinceglb.filekit.path
import java.nio.file.Path
import java.nio.file.Paths

/** App ID matching the Compose app so the CLI resolves the same data location. */
private const val APP_ID = "io.github.kdroidfilter.seforimapp"
private const val DEFAULT_DB_NAME = "seforim.db"

/** Returns the default database path using FileKit (same as the Compose app). */
internal fun getDefaultDatabasePath(): Path {
    FileKit.init(APP_ID)
    return Paths.get(FileKit.databasesDir.path, DEFAULT_DB_NAME)
}

/** Parsed command line for a single CLI invocation. */
data class CliConfig(
    val command: String,
    val query: String = "",
    val dbPath: Path = getDefaultDatabasePath(),
    val indexPath: Path? = null,
    val dictPath: Path? = null,
    val limit: Int = 25,
    val near: Int = 5,
    val bookFilter: Long? = null,
    val categoryFilter: Long? = null,
    val baseOnly: Boolean = false,
    val jsonOutput: Boolean = false,
    val noSnippets: Boolean = false,
    val fetchAll: Boolean = false,
    // DB-navigation options (line/lines/toc/links/book commands)
    val fromIndex: Int? = null,
    val toIndex: Int? = null,
    val tocId: Long? = null,
    val tocText: String? = null,
    val stripHtml: Boolean = false,
    val forwardOnly: Boolean = false,
)

/** Minimal hand-rolled argument parser: first non-flag token is the command, second is the query. */
fun parseArgs(args: Array<String>): CliConfig {
    var command = ""
    var query = ""
    var dbPath = getDefaultDatabasePath()
    var indexPath: Path? = null
    var dictPath: Path? = null
    var limit = 25
    var near = 5
    var bookFilter: Long? = null
    var categoryFilter: Long? = null
    var baseOnly = false
    var jsonOutput = false
    var noSnippets = false
    var fetchAll = false
    var fromIndex: Int? = null
    var toIndex: Int? = null
    var tocId: Long? = null
    var tocText: String? = null
    var stripHtml = false
    var forwardOnly = false

    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--from" -> {
                fromIndex = args.getOrNull(++i)?.toIntOrNull() ?: error("--from requires a number")
            }
            "--to" -> {
                toIndex = args.getOrNull(++i)?.toIntOrNull() ?: error("--to requires a number")
            }
            "--toc" -> {
                tocId = args.getOrNull(++i)?.toLongOrNull() ?: error("--toc requires a TOC entry ID")
            }
            "--text" -> {
                tocText = args.getOrNull(++i) ?: error("--text requires a value")
            }
            "--strip" -> stripHtml = true
            "--forward-only" -> forwardOnly = true
            "--db" -> {
                dbPath = Paths.get(args.getOrNull(++i) ?: error("--db requires a path"))
            }
            "--index" -> {
                indexPath = Paths.get(args.getOrNull(++i) ?: error("--index requires a path"))
            }
            "--dict" -> {
                dictPath = Paths.get(args.getOrNull(++i) ?: error("--dict requires a path"))
            }
            "--limit" -> {
                limit = args.getOrNull(++i)?.toIntOrNull() ?: error("--limit requires a number")
            }
            "--near" -> {
                near = args.getOrNull(++i)?.toIntOrNull() ?: error("--near requires a number")
            }
            "--book" -> {
                bookFilter = args.getOrNull(++i)?.toLongOrNull() ?: error("--book requires an ID")
            }
            "--category" -> {
                categoryFilter = args.getOrNull(++i)?.toLongOrNull() ?: error("--category requires an ID")
            }
            "--base-only" -> baseOnly = true
            "--json" -> jsonOutput = true
            "--no-snippets" -> noSnippets = true
            "--all" -> fetchAll = true
            else -> {
                if (command.isEmpty()) {
                    command = args[i]
                } else if (query.isEmpty()) {
                    query = args[i]
                } else {
                    query += " " + args[i]
                }
            }
        }
        i++
    }

    return CliConfig(
        command = command,
        query = query.trim(),
        dbPath = dbPath,
        indexPath = indexPath,
        dictPath = dictPath,
        limit = limit,
        near = near,
        bookFilter = bookFilter,
        categoryFilter = categoryFilter,
        baseOnly = baseOnly,
        jsonOutput = jsonOutput,
        noSnippets = noSnippets,
        fetchAll = fetchAll,
        fromIndex = fromIndex,
        toIndex = toIndex,
        tocId = tocId,
        tocText = tocText,
        stripHtml = stripHtml,
        forwardOnly = forwardOnly,
    )
}
