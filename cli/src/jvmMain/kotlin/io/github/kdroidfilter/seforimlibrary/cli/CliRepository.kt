package io.github.kdroidfilter.seforimlibrary.cli

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.search.LuceneSearchEngine
import io.github.kdroidfilter.seforimlibrary.search.SearchEngine
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Opens the Lucene search engine (and its backing repository) for the `search`/`books`/`facets`
 * commands. Requires both the database and the Lucene index to exist.
 */
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

/**
 * Opens just the SQLite repository (no Lucene index required) for the DB-navigation commands
 * `book`/`line`/`lines`/`toc`/`links`. Caller is responsible for closing the returned repository.
 */
fun createRepository(config: CliConfig): SeforimRepository {
    val dbPath = config.dbPath.toAbsolutePath()
    require(Files.exists(dbPath)) { "Database not found: $dbPath" }
    val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    return SeforimRepository(dbPath.toString(), driver)
}
