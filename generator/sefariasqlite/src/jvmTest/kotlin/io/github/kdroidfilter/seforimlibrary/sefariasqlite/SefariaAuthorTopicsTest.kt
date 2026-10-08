package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SefariaAuthorTopicsTest {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun parsesExportRecords() {
        val topics = SefariaAuthorTopics(
            json.decodeFromString(
                """[{"slug":"rambam","heTitles":["רמב\"ם","משה בן מימון"],"birthYear":1137,
                   "birthYearIsApprox":null,"deathYear":1204,"deathYearIsApprox":false,"era":"RI"}]""",
            ),
        )
        val rambam = topics["rambam"]!!
        assertEquals(1204, rambam.deathYear)
        assertNull(rambam.birthYearIsApprox)
        assertEquals("RI", rambam.era)
        assertNull(topics["unknown-slug"])
    }

    @Test
    fun aliasesExcludeTheAuthorNameAndBlanks() {
        val topic = SefariaAuthorTopic(slug = "akiva-eiger", heTitles = listOf("עקיבא איגר", " רעק\"א ", "", "רעק\"א"))
        assertEquals(listOf("רעק\"א"), topic.aliasesOf("עקיבא איגר"))
    }
}
