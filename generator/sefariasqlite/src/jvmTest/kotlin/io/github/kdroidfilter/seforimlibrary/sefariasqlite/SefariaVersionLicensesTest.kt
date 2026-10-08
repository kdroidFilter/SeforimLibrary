package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.licenses.Licenses
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SefariaVersionLicensesTest {
    private val davidson = "William Davidson Edition - Vocalized Aramaic"
    private val koren = "https://korenpub.co.il"

    private fun record(title: String, versionTitle: String, source: String?, license: String?) =
        SefariaVersionRecord(title = title, versionTitle = versionTitle, versionSource = source, license = license)

    @Test
    fun declaredLicenseIsKeptAndCanonicalized() {
        val licenses = SefariaVersionLicenses(listOf(record("Mishnah Chullin", "Kaufmann", "x", "PD")))
        val resolved = assertNotNull(licenses.resolve("Mishnah Chullin", "Kaufmann"))
        assertEquals(Licenses.PUBLIC_DOMAIN, resolved.licenseCode)
        assertFalse(resolved.inferred)
    }

    @Test
    fun unknownInheritsTheSingleLicenseOfItsEdition() {
        val licenses = SefariaVersionLicenses(
            listOf(
                record("Berakhot", davidson, koren, "CC-BY-NC"),
                record("Shabbat", davidson, koren, "CC-BY-NC"),
                record("Chullin", davidson, koren, "unknown"),
            ),
        )
        val resolved = assertNotNull(licenses.resolve("Chullin", davidson))
        assertEquals("CC-BY-NC", resolved.licenseCode)
        assertTrue(resolved.inferred)
    }

    @Test
    fun unknownStaysUnknownWhenTheEditionIsMixed() {
        val licenses = SefariaVersionLicenses(
            listOf(
                record("A", "Torat Emet", "t", "Public Domain"),
                record("B", "Torat Emet", "t", "CC-BY-NC"),
                record("C", "Torat Emet", "t", null),
            ),
        )
        val resolved = assertNotNull(licenses.resolve("C", "Torat Emet"))
        assertEquals(Licenses.UNKNOWN, resolved.licenseCode)
        assertFalse(resolved.inferred)
    }

    @Test
    fun sameTitleFromAnotherSourceIsAnotherEdition() {
        val licenses = SefariaVersionLicenses(
            listOf(
                record("A", "Vilna Edition", "nli", "Public Domain"),
                record("B", "Vilna Edition", "other", "unknown"),
            ),
        )
        assertEquals(Licenses.UNKNOWN, licenses.resolve("B", "Vilna Edition")?.licenseCode)
    }

    @Test
    fun missingVersionIsNull() {
        assertNull(SefariaVersionLicenses(emptyList()).resolve("Chullin", davidson))
    }
}
