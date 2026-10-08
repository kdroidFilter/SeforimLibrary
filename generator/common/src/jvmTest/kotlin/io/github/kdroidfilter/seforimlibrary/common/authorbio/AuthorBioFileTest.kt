package io.github.kdroidfilter.seforimlibrary.common.authorbio

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class AuthorBioFileTest {
    private val sample = """
        ---
        id: 11
        name: "רמב\"ם"
        db_books: 162
        confidence: high
        ---

        # רמב"ם

        ## תקציר

        רבי משה בן מימון, ראו «[מורה נבוכים](zayit://book/2200)».

        ## חייו

        נולד בקורדובה.

        ## מקורות

        - [רמב"ם](https://he.wikipedia.org/wiki/x)
    """.trimIndent()

    @Test
    fun parsesFrontMatterSummaryAndBody() {
        val bio = AuthorBioFile.parse(sample)
        assertEquals("רמב\"ם", bio.name)
        assertEquals("high", bio.confidence)
        assertEquals("רבי משה בן מימון, ראו «[מורה נבוכים](zayit://book/2200)».", bio.summary)
        assertTrue(bio.bodyMd.startsWith("## תקציר"))
        assertTrue(bio.bodyMd.endsWith("(https://he.wikipedia.org/wiki/x)"))
    }

    @Test
    fun emptyNameIsKept() {
        assertEquals("", AuthorBioFile.parse(sample.replace("name: \"רמב\\\"ם\"", "name: \"\"")).name)
    }

    @Test
    fun rejectsFilesWithoutFrontMatter() {
        assertFailsWith<IllegalArgumentException> { AuthorBioFile.parse("# title") }
    }
}
