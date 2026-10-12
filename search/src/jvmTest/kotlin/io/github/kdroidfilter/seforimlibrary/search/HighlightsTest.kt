package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class HighlightsTest {
    // Words: the query wherever it is; meaning (unless quoted): the clause after the comma
    private val engine =
        object : SearchEngine {
            override fun openSession(
                query: String,
                near: Int,
                bookFilter: Long?,
                categoryFilter: Long?,
                bookIds: Collection<Long>?,
                lineIds: Collection<Long>?,
                baseBookOnly: Boolean
            ): SearchSession? = null

            override fun searchBooksByTitlePrefix(query: String, limit: Int): List<Long> = emptyList()

            override fun buildSnippet(rawText: String, query: String, near: Int): String = rawText

            override fun computeFacets(
                query: String,
                near: Int,
                bookFilter: Long?,
                categoryFilter: Long?,
                bookIds: Collection<Long>?,
                lineIds: Collection<Long>?,
                baseBookOnly: Boolean
            ): SearchFacets? = null

            override fun highlightRanges(text: String, query: String): List<IntRange> =
                Regex(query.trim('"')).findAll(text).map { it.range }.toList()

            override fun highlightsByMeaning(query: String): Boolean = !query.startsWith("\"")

            override suspend fun passagesByMeaning(query: String, texts: List<String>): List<String?> =
                texts.map { text -> text.substringAfter(", ", "").ifEmpty { null } }

            override fun close() = Unit
        }

    private fun List<TextHighlights>.texts(lines: List<String>) =
        mapIndexed { i, h -> h.byMeaning to h.ranges.map { lines[i].substring(it.first, it.last + 1) } }

    @Test
    fun `by meaning, the passage alone, the whole line when a single clause`() = runBlocking {
        val lines = listOf("שמור שבת, וזכור", "יום מנוחה, לנפש", "יום אחד", "")
        assertEquals(
            listOf(true to listOf("וזכור"), true to listOf("לנפש"), true to listOf("יום אחד"), true to emptyList()),
            engine.highlights(lines, "שבת").texts(lines),
        )
    }

    @Test
    fun `a quoted query highlights its words`() = runBlocking {
        val lines = listOf("שמור שבת, וזכור שבת")
        assertEquals(listOf(false to listOf("שבת", "שבת")), engine.highlights(lines, "\"שבת\"").texts(lines))
    }
}
