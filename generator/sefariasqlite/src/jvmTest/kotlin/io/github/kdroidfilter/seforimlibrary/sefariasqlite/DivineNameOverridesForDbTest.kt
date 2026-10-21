package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.divine.DivineKind
import io.github.kdroidfilter.seforimlibrary.common.divine.DivineNameOverrides
import io.github.kdroidfilter.seforimlibrary.common.divine.VerseRef
import co.touchlab.kermit.Logger
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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

    private val logger = Logger.withTag("DivineNameOverridesForDbTest")

    private fun <T> withLocalCsv(path: String?, block: () -> T): T {
        val key = "divineNameOverridesCsv"
        val previous = System.getProperty(key)
        if (path == null) System.clearProperty(key) else System.setProperty(key, path)
        return try { block() } finally { if (previous == null) System.clearProperty(key) else System.setProperty(key, previous) }
    }

    @Test
    fun `the build loads the list from the ForDB archive`() {
        val overrides = withLocalCsv(null) { loadDivineNameOverrides(logger) { fixture().lines() } }
        assertTrue(overrides.entries.isNotEmpty())
    }

    @Test
    fun `an archive without the list fails the build by name`() {
        val failure = assertFailsWith<IllegalStateException> {
            withLocalCsv(null) { loadDivineNameOverrides(logger) { requiredForDbFile(emptyMap(), it) } }
        }
        assertTrue(DivineNameOverrides.FILE_NAME in failure.message.orEmpty(), failure.message)
    }

    @Test
    fun `a missing local list fails too`() {
        val missing = Files.createTempDirectory("divine").resolve(DivineNameOverrides.FILE_NAME).toString()
        val failure = assertFailsWith<IllegalStateException> {
            withLocalCsv(missing) { loadDivineNameOverrides(logger) { error("the ForDB archive must not be read") } }
        }
        assertTrue(missing in failure.message.orEmpty(), failure.message)
    }

    @Test
    fun `a row the build cannot read fails the build`() {
        val broken = fixture().trimEnd() + "\n\"Genesis 1:1\",\"EL\",\"profane\",\"\",\"x\"\n"
        val failure = assertFailsWith<IllegalStateException> {
            withLocalCsv(null) { loadDivineNameOverrides(logger) { broken.lines() } }
        }
        assertTrue("1 problem(s)" in failure.message.orEmpty(), failure.message)
    }
}
