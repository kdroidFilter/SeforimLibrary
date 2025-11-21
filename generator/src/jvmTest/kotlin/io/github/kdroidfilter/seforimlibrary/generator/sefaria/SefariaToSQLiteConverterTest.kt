package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SchemaNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SefariaToSQLiteConverterTest {

    @Test
    fun `collects section aliases with english titles`() {
        val root = SchemaNode(
            nodeType = "SchemaNode",
            nodes = listOf(
                SchemaNode(title = "Orach Chayim"),
                SchemaNode(enTitle = "Yoreh De'ah"),
                SchemaNode(
                    titles = listOf(
                        io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.Title(
                            text = "Even HaEzer",
                            lang = "en",
                            primary = true
                        )
                    )
                ),
                SchemaNode(key = "default") // should be ignored
            )
        )

        val aliases = SefariaToSQLiteConverter.collectEnglishSectionAliases("Tur", root)

        assertEquals(3, aliases.size)
        assertTrue(aliases.contains("Tur, Orach Chayim"))
        assertTrue(aliases.contains("Tur, Yoreh De'ah"))
        assertTrue(aliases.contains("Tur, Even HaEzer"))
    }

    @Test
    fun `skips links without positional references but keeps introductions`() {
        val parser = SefariaCitationParser()

        val missingRefs = parser.parse("Tur, Orach Chayim")
        assertNotNull(missingRefs, "parser should handle section-only citations")
        assertEquals(emptyList(), missingRefs.references, "section-only citations should have no numeric refs")
        assertFalse(SefariaToSQLiteConverter.hasPositionalReference(missingRefs))

        val intro = parser.parse("Tur, Orach Chayim, Introduction")
        assertNotNull(intro, "parser should handle introduction citations")
        assertTrue(SefariaToSQLiteConverter.hasPositionalReference(intro))
    }
}
