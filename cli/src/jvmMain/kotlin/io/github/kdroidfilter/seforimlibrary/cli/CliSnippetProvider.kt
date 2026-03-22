package io.github.kdroidfilter.seforimlibrary.cli

import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.search.LineSnippetInfo
import io.github.kdroidfilter.seforimlibrary.search.SnippetProvider
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist

/**
 * CLI implementation of [SnippetProvider] that fetches line content from the database.
 */
class CliSnippetProvider(
    private val repository: SeforimRepository,
) : SnippetProvider {
    companion object {
        private const val SNIPPET_NEIGHBOR_WINDOW = 4
        private const val SNIPPET_MIN_LENGTH = 280
    }

    override suspend fun getSnippetSources(lines: List<LineSnippetInfo>): Map<Long, String> {
        if (lines.isEmpty()) return emptyMap()

        return withContext(Dispatchers.IO) {
            val byBook = lines.groupBy { it.bookId }
            val result = mutableMapOf<Long, String>()

            for ((bookId, bookLines) in byBook) {
                val minIdx = bookLines.minOf { it.lineIndex }
                val maxIdx = bookLines.maxOf { it.lineIndex }
                val rangeStart = (minIdx - SNIPPET_NEIGHBOR_WINDOW).coerceAtLeast(0)
                val rangeEnd = maxIdx + SNIPPET_NEIGHBOR_WINDOW

                val allLines = repository.getLines(bookId, rangeStart, rangeEnd)

                val plainByIndex = allLines.associate { line ->
                    line.lineIndex to cleanHtml(line.content)
                }

                for (info in bookLines) {
                    val basePlain = plainByIndex[info.lineIndex].orEmpty()
                    val snippetSource = if (basePlain.length >= SNIPPET_MIN_LENGTH) {
                        basePlain
                    } else {
                        val start = (info.lineIndex - SNIPPET_NEIGHBOR_WINDOW).coerceAtLeast(0)
                        val end = info.lineIndex + SNIPPET_NEIGHBOR_WINDOW
                        (start..end)
                            .mapNotNull { plainByIndex[it] }
                            .joinToString(" ")
                    }
                    result[info.lineId] = snippetSource
                }
            }

            result
        }
    }

    private fun cleanHtml(content: String): String =
        Jsoup
            .clean(content, Safelist.none())
            .replace("\\s+".toRegex(), " ")
            .trim()
}
