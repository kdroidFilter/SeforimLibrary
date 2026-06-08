package io.github.kdroidfilter.seforimlibrary.cli

import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess

private fun suppressLuceneWarnings() {
    Logger.getLogger("org.apache.lucene").level = Level.SEVERE
}

/**
 * CLI for searching and navigating the Seforim database.
 *
 * Commands are split across files: [parseArgs]/[CliConfig] (argument parsing),
 * [createSearchEngine]/[createRepository] (data access), [runSearch]/[runBookSearch]/[runFacets]
 * (Lucene search) and [runBook]/[runLine]/[runLines]/[runToc]/[runLinks] (DB navigation).
 */
fun main(args: Array<String>): Unit = exitProcess(runCli(args))

/**
 * Runs a single CLI command and returns a process exit code (0 = success, non-zero = error).
 *
 * Kept free of [exitProcess] so the exact same entry point can be invoked in-process — e.g. the
 * desktop app launched as `zayit cli search ...` calls this directly without the embedded CLI
 * being able to kill the host JVM. The standalone [main] wrapper turns the returned code into an
 * actual process exit.
 */
fun runCli(args: Array<String>): Int {
    suppressLuceneWarnings()

    if (args.isEmpty()) {
        printUsage()
        return 1
    }

    val config = parseArgs(args)

    return when (config.command) {
        "search" -> runSearch(config)
        "books" -> runBookSearch(config)
        "facets" -> runFacets(config)
        "book" -> runBook(config)
        "line" -> runLine(config)
        "lines" -> runLines(config)
        "toc" -> runToc(config)
        "links" -> runLinks(config)
        "help", "--help", "-h" -> {
            printUsage()
            0
        }
        else -> {
            System.err.println("Unknown command: ${config.command}")
            printUsage()
            1
        }
    }
}

fun printUsage() {
    println(
        """
        Seforim Search CLI

        Usage:
          Lucene search (relevance-ranked):
            seforim-cli search <query> [options]    Search for text in the database
            seforim-cli books <prefix>              Search books by title prefix (ids)
            seforim-cli facets <query>              Get hit counts by book/category

          Database navigation (no SQL needed — covers everything):
            seforim-cli book <substring>            Find books by title (contains), full metadata
            seforim-cli line <lineId>               Full content + heRef of one line
            seforim-cli lines <bookId> --from <i> --to <j>   Range of lines in a book
            seforim-cli lines --toc <tocEntryId>    All lines of a TOC section (e.g. a siman)
            seforim-cli toc <bookId> [--text <s>]   Table of contents (filter by label substring)
            seforim-cli links <lineId>              ALL links both directions (commentaries + sources)

            seforim-cli help                        Show this help

        Options:
          --db <path>         Path to seforim.db (default: same as SeforimApp)
          --index <path>      Path to Lucene index (default: <db>.lucene)
          --dict <path>       Path to lexical.db dictionary (default: <db>/../lexical.db)
          --limit <n>         Results per page (default: 25)
          --near <n>          Proximity slop (default: 5, 0=exact phrase)
          --book <id>         Filter by book ID (search/facets)
          --category <id>     Filter by category ID (search/facets)
          --base-only         Only search base books (not commentaries)
          --json              Output as JSON
          --no-snippets       Skip snippet generation (faster)
          --all               Fetch all pages (not just first)
          --from <i> --to <j> Line index range (lines)
          --toc <id>          TOC entry id (lines)
          --text <s>          Filter TOC entries by label substring (toc)
          --strip             Strip HTML tags from returned content (line/lines/links)
          --forward-only      links: only forward commentaries, skip reverse SOURCE direction

        Examples:
          seforim-cli search "בראשית ברא" --limit 10
          seforim-cli book "שולחן ערוך אורח חיים"
          seforim-cli toc 382 --text "סימן תנא"
          seforim-cli lines --toc 12345 --strip
          seforim-cli line 266232
          seforim-cli links 266232 --json
        """.trimIndent()
    )
}
