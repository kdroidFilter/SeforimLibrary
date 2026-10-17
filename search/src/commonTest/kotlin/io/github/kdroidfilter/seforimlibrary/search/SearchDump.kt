package io.github.kdroidfilter.seforimlibrary.search

/**
 * A text dump of what a search engine answers for [PARITY_QUERIES]: facets, ranked hits with their snippets,
 * highlights, title suggestions, find-in-book candidates. The same index gives the same dump on every platform (the
 * JVM, Android, iOS through its Java runtime): the parity tests compare them.
 */
object SearchDump {
    val PARITY_QUERIES =
        WARMUP_QUERIES +
            listOf(
                "שבת",
                "בראשית ברא",
                "רבי עקיבא",
                "\"ואהבת לרעך כמוך\"",
                "תפילין של ראש",
                "משה רבינו",
                "רש״י",
                "ה׳ אחד",
                "קידוש השם",
                "גמילות חסדים",
                "וַיֹּאמֶר אֱלֹהִים",
            )

    private const val PAGE_SIZE = 20
    private const val TOP_FACETS = 5

    suspend fun dump(engine: SearchEngine): String =
        buildString {
            for (query in PARITY_QUERIES) {
                appendLine("## $query")
                for (baseBookOnly in listOf(false, true)) {
                    val facets = engine.computeFacets(query, baseBookOnly = baseBookOnly)
                    appendLine("facets[base=$baseBookOnly] total=${facets?.totalHits}")
                    facets?.categoryCounts?.entries?.sortedWith(compareByDescending<Map.Entry<Long, Int>> { it.value }.thenBy { it.key })
                        ?.take(TOP_FACETS)
                        ?.forEach { appendLine("  category ${it.key}=${it.value}") }
                    facets?.bookCounts?.entries?.sortedWith(compareByDescending<Map.Entry<Long, Int>> { it.value }.thenBy { it.key })
                        ?.take(TOP_FACETS)
                        ?.forEach { appendLine("  book ${it.key}=${it.value}") }
                    engine.openSession(query, baseBookOnly = baseBookOnly)?.use { session ->
                        repeat(2) { pageIndex ->
                            val page = session.nextPage(PAGE_SIZE) ?: return@repeat
                            appendLine("page $pageIndex total=${page.totalHits} last=${page.isLastPage}")
                            for (hit in page.hits) {
                                appendLine("  ${hit.lineId} book=${hit.bookId} line=${hit.lineIndex} base=${hit.isBaseBook} score=${hit.score.format()}")
                                appendLine("    ${hit.snippet}")
                            }
                        }
                    } ?: appendLine("no session")
                }
                val sample = "בראשית ברא אלהים את השמים ואת הארץ, ויאמר ה׳ אחד ושבת קודש"
                appendLine("highlight ${engine.highlightRanges(sample, query)}")
                appendLine("snippet ${engine.buildSnippet(sample, query, 5)}")
            }
            appendLine("## titles")
            for (prefix in listOf("בראש", "משנה ב", "שולחן")) appendLine("$prefix -> ${engine.searchBooksByTitlePrefix(prefix, 10)}")
            appendLine("## find in book")
            val firstBook = engine.openSession("שבת", baseBookOnly = true)?.use { it.nextPage(1)?.hits?.firstOrNull()?.bookId }
            if (firstBook != null) {
                for (query in listOf("שבת", "ויכלו השמים", "ים")) {
                    appendLine("$query in $firstBook -> ${engine.findInBookCandidates(query, firstBook)?.sorted()?.take(30)}")
                }
            }
        }

    // Rounded: the scores are computed by the same Lucene everywhere, the rounding only keeps the dump readable
    private fun Float.format(): String {
        val scaled = kotlin.math.round(this * 1000).toLong()
        return "${scaled / 1000}.${(scaled % 1000).toString().padStart(3, '0')}"
    }
}
