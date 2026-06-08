package io.github.kdroidfilter.seforimlibrary.cli

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/** `search <query>` — relevance-ranked Lucene full-text search. */
fun runSearch(config: CliConfig): Int {
    if (config.query.isBlank()) {
        System.err.println("Error: Search query required")
        return 1
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
            return 0
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
    return 0
}

/** `books <prefix>` — Lucene book-title prefix search, returning matching book IDs. */
fun runBookSearch(config: CliConfig): Int {
    if (config.query.isBlank()) {
        System.err.println("Error: Book prefix required")
        return 1
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
    return 0
}

/** `facets <query>` — hit counts grouped by book and by category for a search query. */
fun runFacets(config: CliConfig): Int {
    if (config.query.isBlank()) {
        System.err.println("Error: Search query required")
        return 1
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
            return 0
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
    return 0
}
