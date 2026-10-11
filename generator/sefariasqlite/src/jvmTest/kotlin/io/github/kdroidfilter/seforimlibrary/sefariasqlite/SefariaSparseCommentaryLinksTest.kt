package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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
            RefEntry("Darkhei Moshe, Choshen Mishpat 1:1:1", "", "dm", 3), // Otzaria: left out
        )
        val lineKeyToId = mapOf(
            ("tur" to 0) to 10L,
            ("by" to 0) to 20L,
            ("dm" to 1) to 30L,
            ("amiel" to 0) to 40L,
            ("dm" to 2) to 31L,
        )
        val lineIdToBookId = mapOf(10L to tur, 20L to beitYosef, 30L to darkheiMoshe, 31L to darkheiMoshe, 40L to 9L)
        val linesByPath = mapOf("tur" to listOf("x ${byMarker(1)}y"))

        val commentary = collectSparseCommentaries(refs, lineKeyToId, lineIdToBookId, linesByPath).single()

        assertEquals(Triple(darkheiMoshe, beitYosef, tur), commentary.let {
            Triple(it.commentaryBookId, it.intermediateBookId, it.baseBookId)
        })
        assertEquals(listOf(SectionLine(30L, 1, "orach chayim 1", "orach chayim 1:2")), commentary.commentaryLines)
        assertEquals(listOf(SectionLine(10L, 0, "orach chayim 1", "orach chayim 1")), commentary.baseLines)
        assertEquals(mapOf(20L to setOf(10L)), commentary.markedBaseLines)
    }

    // Otzaria: a Tur without Beit Yosef markers (changed markup) fails the build.
    @Test
    fun aBaseWithoutMarkersFailsLoudly() {
        val refs = listOf(
            RefEntry("Tur, Orach Chayim, 1:1", "", "tur", 1),
            RefEntry("Beit Yosef, Orach Chayim 1:1:1", "", "by", 1),
            RefEntry("Darkhei Moshe, Orach Chayim 1:1:1", "", "dm", 1),
            RefEntry("Darkhei Moshe, Choshen Mishpat 1:1:1", "", "dm", 2),
        )
        val lineKeyToId = mapOf(("tur" to 0) to 10L, ("by" to 0) to 20L, ("dm" to 0) to 30L, ("dm" to 1) to 31L)
        val lineIdToBookId = mapOf(10L to tur, 20L to beitYosef, 30L to darkheiMoshe, 31L to darkheiMoshe)

        assertFailsWith<IllegalArgumentException> {
            collectSparseCommentaries(refs, lineKeyToId, lineIdToBookId, mapOf("tur" to listOf("x y")))
        }
    }

    /**
     * Otzaria: Even HaEzer 66 in v32. Sefaria links Beit Yosef 18 to the Tur's first paragraph,
     * but the Tur prints its marker in the fourth; Darkhei Moshe 18–20 gloss it. Beit Yosef 19
     * has no marker in the Tur, so Darkhei Moshe 21 on it stays unlinked.
     */
    @Test
    fun anchorsFollowTheTursOwnBeitYosefMarkers() {
        val refs = listOf(
            RefEntry("Tur, Even HaEzer, 66:1", "", "tur", 1),
            RefEntry("Tur, Even HaEzer, 66:4", "", "tur", 2),
            RefEntry("Beit Yosef, Even HaEzer 66:18:1", "", "by", 1),
            RefEntry("Beit Yosef, Even HaEzer 66:19:1", "", "by", 2),
            RefEntry("Darkhei Moshe, Even HaEzer 66:18:1", "", "dm", 1),
            RefEntry("Darkhei Moshe, Even HaEzer 66:19:1", "", "dm", 2),
            RefEntry("Darkhei Moshe, Even HaEzer 66:20:1", "", "dm", 3),
            RefEntry("Darkhei Moshe, Even HaEzer 66:21:1", "", "dm", 4),
            RefEntry("Darkhei Moshe, Choshen Mishpat 1:1:1", "", "dm", 5),
        )
        val lineKeyToId = mapOf(
            ("tur" to 0) to 101L, ("tur" to 1) to 104L,
            ("by" to 0) to 218L, ("by" to 1) to 219L,
            ("dm" to 0) to 318L, ("dm" to 1) to 319L, ("dm" to 2) to 320L, ("dm" to 3) to 321L, ("dm" to 4) to 399L,
        )
        val lineIdToBookId = lineKeyToId.values.associateWith {
            when (it / 100) { 1L -> tur; 2L -> beitYosef; else -> darkheiMoshe }
        }
        val linesByPath = mapOf("tur" to listOf("a ${byMarker(17)}b", "c ${byMarker(18)}d"))
        val commentary = collectSparseCommentaries(refs, lineKeyToId, lineIdToBookId, linesByPath).single()
        val links = SparseCommentaryLinks()
        listOf(
            link(tur, beitYosef, 101, 218), // Sefaria's link, by paragraph position
            link(tur, beitYosef, 101, 219),
            link(beitYosef, darkheiMoshe, 218, 318),
            link(beitYosef, darkheiMoshe, 218, 319),
            link(beitYosef, darkheiMoshe, 218, 320),
            link(beitYosef, darkheiMoshe, 219, 321),
        ).forEach { links.record(commentary, it) }

        val anchors = anchorUnlinkedCommentaryLines(commentary, links)

        assertEquals(
            listOf(104L to 318L, 104L to 319L, 104L to 320L),
            anchors.map { it.baseLineId to it.commentaryLine.lineId },
        )
    }

    // Otzaria: a title missing from an export that has the others fails the build.
    @Test
    fun aMissingBookOfTheTrioFailsLoudly() {
        val refs = listOf(
            RefEntry("Tur, Orach Chayim, 1:1", "", "tur", 1),
            RefEntry("Darkhei Moshe, Orach Chayim 1:2:1", "", "dm", 1),
        )
        val lineKeyToId = mapOf(("tur" to 0) to 10L, ("dm" to 0) to 30L)
        val lineIdToBookId = mapOf(10L to tur, 30L to darkheiMoshe)

        assertFailsWith<IllegalArgumentException> {
            collectSparseCommentaries(refs, lineKeyToId, lineIdToBookId, emptyMap())
        }
        assertEquals(emptyList(), collectSparseCommentaries(emptyList(), emptyMap(), emptyMap(), emptyMap()))
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
            dmLine(3, "1:2"), // linked to Beit Yosef 1:2, marked on Tur line 101
            dmLine(4, "1:3"), // Beit Yosef seif in another siman: not followed
            dmLine(5, "1:4"), // linked to neither: stays unlinked
        )
        // Otzaria: Beit Yosef lines by the Tur lines printing their markers.
        val commentary = SparseCommentary(
            darkheiMoshe, beitYosef, tur, dmLines, turLines,
            markedBaseLines = mapOf(50L to setOf(101L), 60L to setOf(200L)),
        )
        val links = SparseCommentaryLinks()
        listOf(
            link(tur, darkheiMoshe, 100, 1),
            link(beitYosef, darkheiMoshe, 50, 3),
            link(beitYosef, darkheiMoshe, 60, 4),
            link(tur, 9L, 100, 5), // unrelated book: ignored
        ).forEach { links.record(commentary, it) }

        val anchors = anchorUnlinkedCommentaryLines(commentary, links)

        assertEquals(
            listOf(100L to 2L, 101L to 3L),
            anchors.map { it.baseLineId to it.commentaryLine.lineId },
        )
    }

    private fun byMarker(seif: Int) = """<i data-commentator="Beit Yosef" data-order="$seif.1"></i>"""

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
