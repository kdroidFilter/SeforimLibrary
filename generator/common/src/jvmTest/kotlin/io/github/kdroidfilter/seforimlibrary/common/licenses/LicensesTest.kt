package io.github.kdroidfilter.seforimlibrary.common.licenses

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class LicensesTest {
    @Test
    fun foldsPublicDomainSpellings() {
        assertEquals(Licenses.PUBLIC_DOMAIN, Licenses.canonicalCode("PD"))
        assertEquals(Licenses.PUBLIC_DOMAIN, Licenses.canonicalCode(" Public Domain "))
    }

    @Test
    fun missingOrBlankIsUnknown() {
        assertEquals(Licenses.UNKNOWN, Licenses.canonicalCode(null))
        assertEquals(Licenses.UNKNOWN, Licenses.canonicalCode(""))
        assertEquals(Licenses.UNKNOWN, Licenses.canonicalCode("Unknown"))
    }

    @Test
    fun keepsUnrecognizedCodesWithoutPermissions() {
        assertEquals("Copyright: Koren", Licenses.canonicalCode("Copyright: Koren"))
        assertNull(Licenses.permissionsOf("Copyright: Koren"))
        assertNull(Licenses.permissionsOf(Licenses.UNKNOWN))
    }

    @Test
    fun nonCommercialForbidsCommercialUse() {
        val nc = assertNotNull(Licenses.permissionsOf("CC-BY-NC"))
        assertEquals(false, nc.commercial)
        assertEquals(true, nc.attribution)
    }
}
