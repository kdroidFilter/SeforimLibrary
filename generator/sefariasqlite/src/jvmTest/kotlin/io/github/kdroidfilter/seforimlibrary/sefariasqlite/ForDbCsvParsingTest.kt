package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** ForDB CSV parsing: fields must be kept verbatim, including edge spaces. */
class ForDbCsvParsingTest {
    @Test
    fun fieldValuesAreKeptVerbatim_includingEdgeSpaces() {
        val rows = parseRequiredCsvRows(
            listOf(
                """ דברי חמודות על ברכות,ראשונים""",
                """טעבעלה באנדי על הגדה של פסח ,אחרונים""",
                """" קרבן נתנאל על ביצה",ראשונים""",
            ),
            sourceName = "test.csv",
            minFields = 2,
        )

        assertEquals(
            listOf(
                listOf(" דברי חמודות על ברכות", "ראשונים"),
                listOf("טעבעלה באנדי על הגדה של פסח ", "אחרונים"),
                listOf(" קרבן נתנאל על ביצה", "ראשונים"),
            ),
            rows,
        )
    }

    @Test
    fun blankRowsAreSkipped_evenWhenMadeOfSpacesAndCommas() {
        val rows = parseRequiredCsvRows(
            listOf("", "   ", " , ", "בראשית,תנך"),
            sourceName = "test.csv",
            minFields = 2,
        )
        assertEquals(listOf(listOf("בראשית", "תנך")), rows)
    }

    @Test
    fun rowWithMissingRequiredFieldFails() {
        assertFailsWith<IllegalArgumentException> {
            parseRequiredCsvRows(listOf("בראשית,  "), sourceName = "test.csv", minFields = 2)
        }
    }
}
