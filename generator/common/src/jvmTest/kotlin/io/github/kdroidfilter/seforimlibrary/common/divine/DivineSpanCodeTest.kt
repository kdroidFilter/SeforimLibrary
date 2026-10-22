package io.github.kdroidfilter.seforimlibrary.common.divine

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DivineSpanCodeTest {
    private val starts = listOf(0, 1, 127, 128, 32_767, 149_759, Int.MAX_VALUE - DivineSpanCode.MAX_LENGTH)
    private val lengths = listOf(1, 2, 13, 127, 128, 255, 256, DivineSpanCode.MAX_LENGTH - 1, DivineSpanCode.MAX_LENGTH)

    @Test
    fun `every kind, source and length extreme round-trips exactly`() {
        for (start in starts) for (length in lengths) for (kind in DivineKind.entries) for (source in DivineSource.entries) {
            val end = start + length
            val span = DivineSpanCode.encode(start, end, kind, source)
            assertEquals(DivineSpanCode.Decoded(end, kind, source), DivineSpanCode.decode(start, span.toLong()))
        }
    }

    @Test
    fun `cross-repo golden span values`() {
        // Same values in the Otzaria app's divine_name_spans_test.dart.
        assertEquals(145, DivineSpanCode.encode(0, 1, DivineKind.HVYH, DivineSource.HVYH_RULE))
        assertEquals(1_737, DivineSpanCode.encode(149_759, 149_772, DivineKind.YAH, DivineSource.EXCEPTION_LIST))
        assertEquals(677, DivineSpanCode.encode(10, 15, DivineKind.ELOHIM, DivineSource.NIKUD_RULE))
        assertEquals(
            Int.MAX_VALUE - 127 + (4 shl 4) + 9,
            DivineSpanCode.encode(0, DivineSpanCode.MAX_LENGTH, DivineKind.YAH, DivineSource.EXCEPTION_LIST),
        )
    }

    @Test
    fun `out-of-range spans fail loudly on encode`() {
        val k = DivineKind.HVYH
        val s = DivineSource.HVYH_RULE
        assertFailsWith<IllegalArgumentException> { DivineSpanCode.encode(5, 5, k, s) }
        assertFailsWith<IllegalArgumentException> { DivineSpanCode.encode(5, 4, k, s) }
        assertFailsWith<IllegalArgumentException> { DivineSpanCode.encode(-1, 3, k, s) }
        assertFailsWith<IllegalArgumentException> { DivineSpanCode.encode(0, DivineSpanCode.MAX_LENGTH + 1, k, s) }
        assertFailsWith<IllegalArgumentException> { DivineSpanCode.encode(Int.MIN_VALUE, Int.MAX_VALUE, k, s) }
    }

    @Test
    fun `undecodable spans fail loudly`() {
        fun span(length: Long, source: Int, kind: Int) = (length shl 7) or (source.toLong() shl 4) or kind.toLong()
        assertEquals(DivineSpanCode.Decoded(3, DivineKind.TZEVAOT, DivineSource.COLLOCATION), DivineSpanCode.decode(1, span(2, 3, 8)))
        for (bad in listOf(
            span(0, 1, 1), // empty
            span(2, 0, 1), span(2, 5, 1), span(2, 7, 1), // unknown source
            span(2, 1, 0), span(2, 1, 10), span(2, 1, 15), // unknown kind
            -1L, Int.MAX_VALUE + 1L, Long.MAX_VALUE,
        )) {
            assertFailsWith<IllegalArgumentException>("span $bad") { DivineSpanCode.decode(0, bad) }
        }
        assertFailsWith<IllegalArgumentException> { DivineSpanCode.decode(-1, span(2, 1, 1)) }
        assertFailsWith<IllegalArgumentException> { DivineSpanCode.decode(Int.MAX_VALUE, span(1, 1, 1)) }
    }
}
