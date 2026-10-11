package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertEquals

/** Otzaria: a content match keeps an id only when it is unique both ways. */
class BookRenameDetectorUniqueTest {
    private fun set(prefix: String, count: Int = 20) = BookRenameDetector.LineHashSet(
        (1..count).mapTo(HashSet()) {
            BookRenameDetector.ContentHash(MessageDigest.getInstance("SHA-1").digest("$prefix $it".toByteArray()))
        },
    )

    private val detector = BookRenameDetector()

    @Test
    fun `a single match on both sides is an alias`() {
        val old = BookKey("A", "ספר - מחבר")
        val new = BookKey("A", "ספר")
        val aliases = detector.detectUnique(mapOf(old to set("a")), mapOf(new to set("a"), BookKey("A", "אחר") to set("b")))
        assertEquals(listOf(old to new), aliases.map { it.oldKey to it.newKey })
    }

    @Test
    fun `two new books with the old content give no alias`() {
        val aliases = detector.detectUnique(
            mapOf(BookKey("A", "ספר - מחבר") to set("a")),
            mapOf(BookKey("A", "ספר") to set("a"), BookKey("B", "ספר אחר") to set("a")),
        )
        assertEquals(emptyList(), aliases)
    }

    @Test
    fun `two old books matching one new book give no alias`() {
        val aliases = detector.detectUnique(
            mapOf(BookKey("A", "ספר א") to set("a"), BookKey("B", "ספר ב") to set("a")),
            mapOf(BookKey("C", "ספר") to set("a")),
        )
        assertEquals(emptyList(), aliases)
    }
}
