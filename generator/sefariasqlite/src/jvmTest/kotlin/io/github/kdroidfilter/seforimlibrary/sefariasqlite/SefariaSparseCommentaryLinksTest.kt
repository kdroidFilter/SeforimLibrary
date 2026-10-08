package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Regression for Zayit issue #462: Sefaria links most Darkhei Moshe seifim to the Beit Yosef
 * seif they gloss instead of the Tur, so they never showed next to the Tur.
 */
class SefariaSparseCommentaryLinksTest {
    private val tur = 1L
    private val beitYosef = 2L
    private val darkheiMoshe = 3L

    @Test
    fun collectsTheConfiguredBooksWithTheirSections() {
        val refs = listOf(
            RefEntry("Tur, Orach Chayim, 1:1", "", "tur", 1),
            RefEntry("Beit Yosef, Orach Chayim 1:1:1", "", "by", 1),
            RefEntry("Darkhei Moshe, Orach Chayim 1:2:1", "", "dm", 2),
            RefEntry("Darkhei Moshe (Amiel) 1:1", "", "amiel", 1),
        )
        val lineKeyToId = mapOf(
            ("tur" to 0) to 10L,
            ("by" to 0) to 20L,
            ("dm" to 1) to 30L,
            ("amiel" to 0) to 40L,
        )
        val lineIdToBookId = mapOf(10L to tur, 20L to beitYosef, 30L to darkheiMoshe, 40L to 9L)

        val commentary = collectSparseCommentaries(refs, lineKeyToId, lineIdToBookId).single()

        assertEquals(Triple(darkheiMoshe, beitYosef, tur), commentary.let {
            Triple(it.commentaryBookId, it.intermediateBookId, it.baseBookId)
        })
        assertEquals(listOf(SectionLine(30L, 1, "orach chayim 1", "orach chayim 1:2")), commentary.commentaryLines)
        assertEquals(listOf(SectionLine(10L, 0, "orach chayim 1", "orach chayim 1")), commentary.baseLines)
    }

    @Test
    fun unlinkedSeifimAreAnchoredThroughTheBeitYosef() {
        val turLines = listOf(
            SectionLine(100, 0, "orach chayim 1", "orach chayim 1"),
            SectionLine(101, 1, "orach chayim 1", "orach chayim 1"),
            SectionLine(200, 2, "orach chayim 2", "orach chayim 2"),
        )
        val dmLines = listOf(
            dmLine(1, "1:1"), // linked to the Tur
            dmLine(2, "1:1"), // second paragraph of 1:1: follows the seif's Tur link
            dmLine(3, "1:2"), // linked to Beit Yosef 1:2, itself on Tur line 101
            dmLine(4, "1:3"), // Beit Yosef seif in another siman: not followed
            dmLine(5, "1:4"), // linked to neither: stays unlinked
        )
        val commentary = SparseCommentary(darkheiMoshe, beitYosef, tur, dmLines, turLines)
        val links = SparseCommentaryLinks()
        listOf(
            link(tur, darkheiMoshe, 100, 1),
            link(beitYosef, darkheiMoshe, 50, 3),
            link(tur, beitYosef, 101, 50),
            link(beitYosef, darkheiMoshe, 60, 4),
            link(tur, beitYosef, 200, 60),
            link(tur, 9L, 100, 5), // unrelated book: ignored
        ).forEach { links.record(commentary, it) }

        val anchors = anchorUnlinkedCommentaryLines(commentary, links)

        assertEquals(
            listOf(100L to 2L, 101L to 3L),
            anchors.map { it.baseLineId to it.commentaryLine.lineId },
        )
    }

    private fun dmLine(id: Long, seif: String) =
        SectionLine(id, id.toInt(), "orach chayim ${seif.substringBefore(':')}", "orach chayim $seif")

    private fun link(sourceBook: Long, targetBook: Long, sourceLine: Long, targetLine: Long) = Link(
        id = 0,
        sourceBookId = sourceBook,
        targetBookId = targetBook,
        sourceLineId = sourceLine,
        targetLineId = targetLine,
        targetLineIndex = 0,
        connectionType = ConnectionType.COMMENTARY,
    )
}
