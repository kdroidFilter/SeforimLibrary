package io.github.kdroidfilter.seforimlibrary.cli

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.search.LuceneSearchEngine
import io.github.kdroidfilter.seforimlibrary.search.SearchEngine
import io.github.vinceglb.filekit.FileKit
import io.github.vinceglb.filekit.databasesDir
import io.github.vinceglb.filekit.path
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess

private fun suppressLuceneWarnings() {
    Logger.getLogger("org.apache.lucene").level = Level.SEVERE
}

private val jsonPretty = Json { prettyPrint = true }

/** App ID matching the Compose app for consistent data location */
private const val APP_ID = "io.github.kdroidfilter.seforimapp"
private const val DEFAULT_DB_NAME = "seforim.db"

/**
 * Returns the default database path using FileKit (same as Compose app).
 */
private fun getDefaultDatabasePath(): Path {
    FileKit.init(APP_ID)
    return Paths.get(FileKit.databasesDir.path, DEFAULT_DB_NAME)
}

/**
 * CLI for searching the Seforim database.
 *
 * Usage:
 *   seforim-cli search <query> [options]
 *   seforim-cli books <prefix>
 *   seforim-cli facets <query>
 *
 * Options:
 *   --db <path>         Path to seforim.db (default: same location as SeforimApp)
 *   --index <path>      Path to Lucene index directory (default: <db>.lucene)
 *   --dict <path>       Path to lexical.db dictionary (default: <db>/../lexical.db)
 *   --limit <n>         Number of results per page (default: 25)
 *   --near <n>          Proximity slop for phrase matching (default: 5)
 *   --book <id>         Filter by book ID
 *   --category <id>     Filter by category ID
 *   --base-only         Only search base books
 *   --json              Output results as JSON
 *   --no-snippets       Skip snippet generation (faster)
 *   --all               Fetch all results (not just first page)
 */
fun main(args: Array<String>) {
    suppressLuceneWarnings()

    if (args.isEmpty()) {
        printUsage()
        exitProcess(1)
    }

    val config = parseArgs(args)

    when (config.command) {
        "search" -> runSearch(config)
        "books" -> runBookSearch(config)
        "facets" -> runFacets(config)
        "help", "--help", "-h" -> printUsage()
        else -> {
            System.err.println("Unknown command: ${config.command}")
            printUsage()
            exitProcess(1)
        }
    }
}

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
)

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

    var i = 0
    while (i < args.size) {
        when (args[i]) {
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
    )
}

fun createSearchEngine(config: CliConfig): Pair<SearchEngine, SeforimRepository> {
    val dbPath = config.dbPath.toAbsolutePath()
    require(Files.exists(dbPath)) { "Database not found: $dbPath" }

    val indexPath = config.indexPath?.toAbsolutePath()
        ?: Paths.get("$dbPath.lucene")
    require(Files.exists(indexPath)) { "Lucene index not found: $indexPath" }

    // Dictionary path resolution (same logic as Compose app):
    // 1. Explicit --dict argument if provided
    // 2. Otherwise, deduce from db path: sibling of the database file
    val dictPath = config.dictPath?.toAbsolutePath()
        ?: dbPath.resolveSibling("lexical.db")

    if (!Files.exists(dictPath)) {
        System.err.println("Warning: Dictionary not found at $dictPath - search expansion disabled")
    }

    val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath.toString(), driver)

    val snippetProvider = if (config.noSnippets) null else CliSnippetProvider(repository)

    val engine = LuceneSearchEngine(
        indexDir = indexPath,
        snippetProvider = snippetProvider,
        dictionaryPath = dictPath,
    )

    return engine to repository
}

fun runSearch(config: CliConfig) {
    if (config.query.isBlank()) {
        System.err.println("Error: Search query required")
        exitProcess(1)
    }

    val (engine, _) = createSearchEngine(config)
    engine.use {
        val session = engine.openSession(
            query = config.query,
            near = config.near,
            bookFilter = config.bookFilter,
            categoryFilter = config.categoryFilter,
            baseBookOnly = config.baseOnly,
        )

        if (session == null) {
            if (config.jsonOutput) {
                println(Json.encodeToString(SearchOutput(query = config.query, totalHits = 0, results = emptyList())))
            } else {
                println("No results found for: ${config.query}")
            }
            return
        }

        session.use { s ->
            val allResults = mutableListOf<SearchResultOutput>()
            var totalHits = 0L

            runBlocking {
                do {
                    val page = s.nextPage(config.limit) ?: break
                    totalHits = page.totalHits

                    for (hit in page.hits) {
                        allResults.add(
                            SearchResultOutput(
                                bookId = hit.bookId,
                                bookTitle = hit.bookTitle,
                                lineId = hit.lineId,
                                lineIndex = hit.lineIndex,
                                snippet = stripHtml(hit.snippet),
                                score = hit.score,
                            )
                        )
                    }

                    if (!config.fetchAll || page.isLastPage) break
                } while (true)
            }

            if (config.jsonOutput) {
                val output = SearchOutput(
                    query = config.query,
                    totalHits = totalHits,
                    results = allResults,
                )
                println(jsonPretty.encodeToString(output))
            } else {
                println("Query: ${config.query}")
                println("Total hits: $totalHits")
                println("Showing: ${allResults.size} results")
                println("-".repeat(60))

                for ((idx, result) in allResults.withIndex()) {
                    println("\n[${idx + 1}] ${result.bookTitle} (line ${result.lineIndex})")
                    println("    Score: ${result.score}")
                    println("    ${result.snippet}")
                }
            }
        }
    }
}

fun runBookSearch(config: CliConfig) {
    if (config.query.isBlank()) {
        System.err.println("Error: Book prefix required")
        exitProcess(1)
    }

    val (engine, _) = createSearchEngine(config)
    engine.use {
        val bookIds = engine.searchBooksByTitlePrefix(config.query, config.limit)

        if (config.jsonOutput) {
            println(jsonPretty.encodeToString(BookSearchOutput(config.query, bookIds)))
        } else {
            println("Books matching '${config.query}':")
            for (id in bookIds) {
                println("  - Book ID: $id")
            }
        }
    }
}

fun runFacets(config: CliConfig) {
    if (config.query.isBlank()) {
        System.err.println("Error: Search query required")
        exitProcess(1)
    }

    val (engine, _) = createSearchEngine(config)
    engine.use {
        val facets = engine.computeFacets(
            query = config.query,
            near = config.near,
            bookFilter = config.bookFilter,
            categoryFilter = config.categoryFilter,
            baseBookOnly = config.baseOnly,
        )

        if (facets == null) {
            if (config.jsonOutput) {
                println(Json.encodeToString(FacetsOutput(config.query, 0, emptyMap(), emptyMap())))
            } else {
                println("No facets for: ${config.query}")
            }
            return
        }

        if (config.jsonOutput) {
            val output = FacetsOutput(
                query = config.query,
                totalHits = facets.totalHits,
                bookCounts = facets.bookCounts.mapKeys { it.key.toString() },
                categoryCounts = facets.categoryCounts.mapKeys { it.key.toString() },
            )
            println(jsonPretty.encodeToString(output))
        } else {
            println("Query: ${config.query}")
            println("Total hits: ${facets.totalHits}")
            println("\nBooks (${facets.bookCounts.size}):")
            facets.bookCounts.entries
                .sortedByDescending { it.value }
                .take(20)
                .forEach { (bookId, count) ->
                    println("  Book $bookId: $count hits")
                }
            println("\nCategories (${facets.categoryCounts.size}):")
            facets.categoryCounts.entries
                .sortedByDescending { it.value }
                .take(20)
                .forEach { (catId, count) ->
                    println("  Category $catId: $count hits")
                }
        }
    }
}

fun stripHtml(html: String): String =
    html.replace(Regex("<[^>]+>"), "")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("...", "...")
        .trim()

fun printUsage() {
    println(
        """
        Seforim Search CLI

        Usage:
          seforim-cli search <query> [options]    Search for text in the database
          seforim-cli books <prefix>              Search books by title prefix
          seforim-cli facets <query>              Get hit counts by book/category
          seforim-cli help                        Show this help

        Options:
          --db <path>         Path to seforim.db (default: same as SeforimApp)
          --index <path>      Path to Lucene index (default: <db>.lucene)
          --dict <path>       Path to lexical.db dictionary (default: <db>/../lexical.db)
          --limit <n>         Results per page (default: 25)
          --near <n>          Proximity slop (default: 5, 0=exact phrase)
          --book <id>         Filter by book ID
          --category <id>     Filter by category ID
          --base-only         Only search base books (not commentaries)
          --json              Output as JSON
          --no-snippets       Skip snippet generation (faster)
          --all               Fetch all pages (not just first)

        Examples:
          seforim-cli search "בראשית ברא" --limit 10
          seforim-cli search "אברהם" --book 123 --json
          seforim-cli books "בראש" --limit 5
          seforim-cli facets "משה" --base-only
        """.trimIndent()
    )
}

// JSON output models
@Serializable
data class SearchOutput(
    val query: String,
    val totalHits: Long,
    val results: List<SearchResultOutput>,
)

@Serializable
data class SearchResultOutput(
    val bookId: Long,
    val bookTitle: String,
    val lineId: Long,
    val lineIndex: Int,
    val snippet: String,
    val score: Float,
)

@Serializable
data class BookSearchOutput(
    val prefix: String,
    val bookIds: List<Long>,
)

@Serializable
data class FacetsOutput(
    val query: String,
    val totalHits: Long,
    val bookCounts: Map<String, Int>,
    val categoryCounts: Map<String, Int>,
)
