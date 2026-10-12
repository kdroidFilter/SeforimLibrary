package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.divine.DivineKind
import io.github.kdroidfilter.seforimlibrary.common.divine.DivineNameOverrides
import io.github.kdroidfilter.seforimlibrary.common.divine.VerseRef
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The proposed ForDB/divine_name_overrides.csv, read the way the build reads it. */
class DivineNameOverridesForDbTest {
    private fun fixture(): String = requireNotNull(javaClass.getResourceAsStream("/fordb_divine_names/divine_name_overrides.csv")) {
        "fixture missing"
    }.readBytes().toString(Charsets.UTF_8)

    @Test
    fun `the proposed list parses cleanly through the ForDB CSV reader`() {
        val parsed = DivineNameOverrides.parse(parseForDbCsvRecords(forDbText(fixture().lines())))
        assertEquals(emptyList(), parsed.problems)
        val overrides = assertNotNull(parsed.overrides)
        assertEquals(setOf(DivineKind.EHYEH), overrides.sacredKinds(VerseRef("שמות", 3, 14)))
        assertTrue(overrides.isProfane(VerseRef("שופטים", 18, 30), DivineKind.ELOHIM))
        assertTrue(overrides.isProfane(VerseRef("בראשית", 19, 2), DivineKind.ADNUT))
        assertTrue(!overrides.isProfane(VerseRef("בראשית", 19, 18), DivineKind.ADNUT))
        assertTrue(!overrides.isProfane(VerseRef("שופטים", 17, 3), DivineKind.HVYH))
    }

    @Test
    fun `one row the build cannot read discards the whole list`() {
        val broken = fixture().trimEnd() + "\n\"Genesis 1:1\",\"EL\",\"profane\",\"\",\"x\"\n"
        val parsed = DivineNameOverrides.parse(parseForDbCsvRecords(broken))
        assertNull(parsed.overrides)
        assertEquals(1, parsed.problems.size)
    }
}
