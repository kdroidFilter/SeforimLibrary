package io.github.kdroidfilter.seforimlibrary.common.changes

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LineMatcherTest {

    private fun old(idx: Int, id: Long, content: String) = LineMatcher.OldLine(idx, id, content)
    private fun new(idx: Int, content: String) = LineMatcher.NewLine(idx, content)

    @Test
    fun `head insert preserves following line ids`() {
        // Old: A, B, C  →  New: X, A, B, C  (X inserted at the top).
        val oldLines = listOf(
            old(0, 100L, "A"),
            old(1, 101L, "B"),
            old(2, 102L, "C"),
        )
        val newLines = listOf(
            new(0, "X"),
            new(1, "A"),
            new(2, "B"),
            new(3, "C"),
        )
        val ops = LineMatcher().match(oldLines, newLines)
        val inserts = ops.filterIsInstance<LineMatcher.Op.Insert>()
        val keeps = ops.filterIsInstance<LineMatcher.Op.Keep>()
        assertEquals(1, inserts.size)
        assertEquals(0, inserts[0].newIndex)
        assertEquals(3, keeps.size)
        assertEquals(listOf(100L, 101L, 102L), keeps.map { it.lineId })
    }

    @Test
    fun `pure deletion drops one and keeps neighbours`() {
        val oldLines = listOf(
            old(0, 10L, "A"),
            old(1, 11L, "B"),
            old(2, 12L, "C"),
        )
        val newLines = listOf(
            new(0, "A"),
            new(1, "C"),
        )
        val ops = LineMatcher().match(oldLines, newLines)
        val deletes = ops.filterIsInstance<LineMatcher.Op.Delete>()
        val keeps = ops.filterIsInstance<LineMatcher.Op.Keep>()
        assertEquals(1, deletes.size)
        assertEquals(11L, deletes[0].lineId)
        assertEquals(setOf(10L, 12L), keeps.map { it.lineId }.toSet())
    }

    @Test
    fun `typo-like edit collapses delete plus insert into MODIFY`() {
        val oldLines = listOf(
            old(0, 1L, "Hello world how are you doing"),
            old(1, 2L, "Foo bar"),
        )
        // Edit the first line (similarity = 5 / 7 ≈ 0.71, above 0.6 threshold).
        val newLines = listOf(
            new(0, "Hello world how are u doing today"),
            new(1, "Foo bar"),
        )
        val ops = LineMatcher().match(oldLines, newLines)
        val modifies = ops.filterIsInstance<LineMatcher.Op.Modify>()
        assertEquals(1, modifies.size, "expected one MODIFY, got $ops")
        assertEquals(1L, modifies[0].lineId)
        assertTrue(modifies[0].similarity >= 0.6)
    }

    @Test
    fun `unrelated lines are NOT collapsed into MODIFY`() {
        val oldLines = listOf(old(0, 1L, "alpha beta gamma"))
        val newLines = listOf(new(0, "completely different content"))
        val ops = LineMatcher().match(oldLines, newLines)
        // Below 0.6 similarity → should remain as DELETE + INSERT, not MODIFY.
        assertTrue(ops.none { it is LineMatcher.Op.Modify }, ops.toString())
    }

    @Test
    fun `genesis-style prefix shift handled by heRef key not LineMatcher`() {
        // The cleanest fix for Sefaria's verse-prefix renumbering is the
        // heRef-based natural key (DELTA_UPDATE_PLAN.md §2.1). The LineMatcher
        // is the broader-stroke fallback for sources without stable refs
        // (Otzaria). When the only difference between two lines is the
        // parenthesised marker, the LineMatcher's greedy alignment cannot
        // see that the *post-prefix* text is identical because the entire
        // line content hash differs. This is fine in production — Sefaria
        // lines never reach the LineMatcher (their natural key is heRef-based
        // so unchanged verses keep their ids even when the prefix shifts).
        // This test documents the limitation rather than asserting a fix.
        val oldLines = listOf(
            old(0, 1L, "(א) בראשית ברא אלהים את השמים ואת הארץ"),
            old(1, 2L, "(ב) והארץ היתה תהו ובהו"),
            old(2, 3L, "(ג) ויאמר אלהים יהי אור"),
        )
        val newLines = listOf(
            new(0, "NEW INSERTED LINE"),
            new(1, "(ב) בראשית ברא אלהים את השמים ואת הארץ"),
            new(2, "(ג) והארץ היתה תהו ובהו"),
            new(3, "(ד) ויאמר אלהים יהי אור"),
        )
        val ops = LineMatcher().match(oldLines, newLines)
        // We get something — at minimum the inserted-at-top line is detected.
        assertTrue(ops.any { it is LineMatcher.Op.Insert }, ops.toString())
    }

    @Test
    fun `empty sequences produce no ops`() {
        assertTrue(LineMatcher().match(emptyList(), emptyList()).isEmpty())
    }

    @Test
    fun `all-new produces only INSERT`() {
        val ops = LineMatcher().match(emptyList(), listOf(new(0, "x"), new(1, "y")))
        assertEquals(2, ops.size)
        assertTrue(ops.all { it is LineMatcher.Op.Insert })
    }

    @Test
    fun `all-removed produces only DELETE`() {
        val ops = LineMatcher().match(listOf(old(0, 7L, "x"), old(1, 8L, "y")), emptyList())
        assertEquals(2, ops.size)
        assertEquals(setOf(7L, 8L), ops.filterIsInstance<LineMatcher.Op.Delete>().map { it.lineId }.toSet())
    }
}
