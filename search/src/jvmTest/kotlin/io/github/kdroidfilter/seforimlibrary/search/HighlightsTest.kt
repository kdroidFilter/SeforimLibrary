package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class HighlightsTest {
    // Words: "שבת" wherever it is; meaning: the clause after the comma
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
                Regex(query).findAll(text).map { it.range }.toList()

            override suspend fun passagesByMeaning(query: String, texts: List<String>): List<String?> =
                texts.map { text -> text.substringAfter(", ", "").ifEmpty { null } }

            override fun close() = Unit
        }

    private fun List<TextHighlights>.texts(lines: List<String>) =
        mapIndexed { i, h -> h.byMeaning to h.ranges.map { lines[i].substring(it.first, it.last + 1) } }

    @Test
    fun `the words where they show, else the passage closest in meaning`() = runBlocking {
        val lines = listOf("שמור שבת, וזכור", "יום מנוחה, לנפש", "יום אחד")
        assertEquals(
            listOf(false to listOf("שבת"), true to listOf("לנפש"), true to listOf("יום אחד")),
            engine.highlights(lines, "שבת").texts(lines),
        )
    }

    @Test
    fun `a find by meaning shows the passage beside the words`() = runBlocking {
        val lines = listOf("שמור שבת, וזכור שבת")
        assertEquals(listOf(true to listOf("שבת", "וזכור שבת")), engine.highlights(lines, "שבת", alwaysByMeaning = true).texts(lines))
    }
}
