package io.github.kdroidfilter.seforimlibrary.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SnippetBuilderTest {
    private fun plan(vararg tokens: TokenForms, phrases: List<String> = emptyList()) = HighlightPlan.build(tokens.toList(), phrases)

    private fun words(token: String) = TokenForms(HebrewTextUtils.normalizeHebrew(token))

    private fun bold(snippet: String): List<String> = Regex("<b>(.*?)</b>").findAll(snippet).map { it.groupValues[1] }.toList()

    @Test
    fun `highlights whole words, never part of one`() {
        val snippet = SnippetBuilder.build("נבראו בראשית ברא אלהים", plan(words("ברא")))
        assertEquals(listOf("ברא"), bold(snippet))
    }

    @Test
    fun `a word behind prefixes or inside a longer word counts from four letters`() {
        assertEquals(listOf("ובבראשית", "בראשיתו"), bold(SnippetBuilder.build("ובבראשית ואחר בראשיתו", plan(words("בראשית")))))
        // Three letters: prefixes only
        assertEquals(listOf("וברא"), bold(SnippetBuilder.build("וברא נבראו", plan(words("ברא")))))
    }

    @Test
    fun `a two-letter word only takes vav or he as prefix`() {
        assertEquals(listOf("כל וכל הכל"), bold(SnippetBuilder.build("כל וכל הכל שכל לכל", plan(words("כל")))))
    }

    @Test
    fun `the bold covers the word's diacritics and final letters match`() {
        val snippet = SnippetBuilder.build("תִּשְׁבְּתוּ וְהָעוֹלָם", plan(words("עולם"), words("תשבתו")))
        assertEquals(listOf("תִּשְׁבְּתוּ וְהָעוֹלָם"), bold(snippet))
    }

    @Test
    fun `acronyms match with any quote marks`() {
        val query = plan(words("רמב\"ם"))
        assertEquals(listOf("הרמב״ם", "רמב\"ם"), bold(SnippetBuilder.build("כתב הרמב״ם וכן רמב\"ם", query)))
        assertEquals(listOf("רש״י"), bold(SnippetBuilder.build("פירש רש״י", plan(words("רש\"י")))))
    }

    @Test
    fun `the divine name's lone he needs its geresh`() {
        val hashem = TokenForms("ה", extraLiterals = listOf("ה'", "יהוה"))
        val snippet = SnippetBuilder.build("סימן ה אמר ה׳ אלקים ויאמר ה' ליהוה", plan(hashem, words("אלקים")))
        assertEquals(listOf("ה׳ אלקים", "ה' ליהוה"), bold(snippet))
    }

    @Test
    fun `inflections count, not the dictionary's other forms`() {
        val shabbat = TokenForms("שבת", inflections = listOf("שבתות", "שבת"))
        assertEquals(listOf("שבת ושבתות"), bold(SnippetBuilder.build("ביום השביעי שבת ושבתות", plan(shabbat))))
    }

    @Test
    fun `a quoted phrase shows as its words in a row only`() {
        val snippet = SnippetBuilder.build("אין שם אין עומדין להתפלל אלא", plan(phrases = listOf("אין עומדין להתפלל")))
        assertEquals(listOf("אין עומדין להתפלל"), bold(snippet))
    }

    @Test
    fun `dictionary forms of several words`() {
        val token = TokenForms("כלבו", inflections = listOf("כל בו"))
        assertEquals(listOf("כל בו"), bold(SnippetBuilder.build("כתב בעל כל בו כל הלכה", plan(token))))
    }

    @Test
    fun `matched words in a row read as one span, across a maqaf too`() {
        assertEquals(listOf("קריעת ים־סוף"), bold(SnippetBuilder.build("נס קריעת ים־סוף גדול", plan(words("קריעת"), words("ים"), words("סוף")))))
        assertEquals(listOf("ים", "סוף"), bold(SnippetBuilder.build("ים, סוף", plan(words("ים"), words("סוף")))))
    }

    @Test
    fun `html entities are not words`() {
        val snippet = SnippetBuilder.build("a &amp; amp b", plan(TokenForms("amp")))
        assertEquals("a &amp; <b>amp</b> b", snippet)
    }

    @Test
    fun `the window shows the hit line's match over richer neighbors`() {
        val neighbor = (1..60).joinToString(" ") { "אלף בית גימל" }
        val line = "כאן אלף לבדו"
        val source = "$neighbor $line"
        val snippet = SnippetBuilder.build(source, plan(words("אלף"), words("בית"), words("גימל")), neighbor.length + 1, source.length)
        assertTrue(snippet.contains("כאן <b>אלף</b> לבדו"), snippet)
    }

    @Test
    fun `the snippet starts in the hit line even when the line before has more of the query`() {
        val before = "אלף בית גימל"
        val line = "ושוב אלף כאן"
        val source = "$before $line"
        val snippet = SnippetBuilder.build(source, plan(words("אלף"), words("בית"), words("גימל")), before.length + 1, source.length)
        assertTrue(snippet.substringBefore("<b>").contains("ושוב") || snippet.startsWith("...ושוב"), snippet)
    }

    @Test
    fun `the window gathers the most query words and cuts between words`() {
        val filler = (1..80).joinToString(" ") { "מילה$it" }
        val text = "ראשון $filler שני שלישי $filler"
        val snippet = SnippetBuilder.build(text, plan(words("ראשון"), words("שני"), words("שלישי")))
        assertTrue("<b>שני שלישי</b>" in snippet, snippet)
        // Whole words at the edges
        val inner = snippet.removePrefix("...").removeSuffix("...")
        val edges = inner.replace(Regex("</?b>"), "").split(' ')
        assertTrue(edges.first().startsWith("מילה") && edges.last().startsWith("מילה"), snippet)
        assertTrue(edges.all { it.matches(Regex("מילה\\d+|שני|שלישי")) }, snippet)
    }

    @Test
    fun `no match keeps the line start`() {
        val before = (1..80).joinToString(" ") { "לפני$it" }
        val source = "$before שורה כאן"
        val snippet = SnippetBuilder.build(source, plan(words("אחר")), before.length + 1, source.length)
        assertFalse("<b>" in snippet)
        assertEquals("...שורה כאן", snippet)
    }

    @Test
    fun `the snippet starts just before the matches, at their sentence when close`() {
        val before = (1..60).joinToString(" ") { "מילה$it" }
        val snippet = SnippetBuilder.build("$before. משפט חדש כאן מטרה ועוד", plan(words("מטרה")))
        assertTrue(snippet.startsWith("...משפט חדש כאן <b>מטרה</b>"), snippet)
        // No sentence end near: a few words of context
        val far = SnippetBuilder.build("$before מטרה ועוד", plan(words("מטרה")))
        val lead = far.removePrefix("...").substringBefore("<b>")
        assertTrue(lead.length in 1..40 && lead.startsWith("מילה"), far)
    }

    @Test
    fun `a passage snippet bolds the passage as one span`() {
        val text = "פתיחה ארוכה. ואמר החכם דבר נאה מאוד לתלמידיו וסיים"
        val passage = "דבר נאה מאוד"
        val at = text.indexOf(passage)
        val snippet = SnippetBuilder.build(text, listOf(Occurrence(at, at + passage.length, 0, EvidenceLevel.LITERAL)))
        assertEquals(listOf(passage), bold(snippet))
    }

    @Test
    fun `highlight ranges of a plain text are the snippet's words`() {
        val text = "וּבְרֵאשִׁית ברא נבראו"
        val ranges = mergedRanges(text, plan(words("בראשית"), words("ברא")).highlights(text))
        assertEquals(listOf("וּבְרֵאשִׁית ברא"), ranges.map { text.substring(it.first, it.last + 1) })
    }
}
