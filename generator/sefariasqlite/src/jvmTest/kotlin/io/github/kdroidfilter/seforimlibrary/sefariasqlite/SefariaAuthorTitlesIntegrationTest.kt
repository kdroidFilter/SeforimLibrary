package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End to end through the reader: authors.json must actually reach the payload.
 * The unit tests pin [SefariaAuthorTitles] itself; these pin the wiring — that
 * the schema's `slug` is read, that the chosen name lands on the author line in
 * the book body, and that every name form survives for blacklist matching.
 */
class SefariaAuthorTitlesIntegrationTest {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }
    private val logger = Logger.withTag("SefariaAuthorTitlesIntegrationTest")

    private fun layout(withAuthorsFile: Boolean): Path {
        val root = Files.createTempDirectory("seforim-authors-test")
        val schemaDir = Files.createDirectories(root.resolve("schemas"))
        val jsonDir = Files.createDirectories(root.resolve("json"))
        Files.writeString(schemaDir.resolve("Orot.json"), SCHEMA)
        Files.writeString(
            Files.createDirectories(jsonDir.resolve("Orot")).resolve("merged.json"),
            MERGED,
        )
        if (withAuthorsFile) Files.writeString(root.resolve(SefariaAuthorTitles.FILE_NAME), AUTHORS)
        return root
    }

    private fun read(
        root: Path,
        canonical: SefariaAuthorCanonicalNames = SefariaAuthorCanonicalNames.EMPTY,
    ): BookPayload = runBlocking {
        val titles = SefariaAuthorTitles.load(root, json, logger)
        val reader = SefariaBookPayloadReader(json, logger, titles, canonicalAuthorName = canonical::canonical)
        val schemaDir = root.resolve("schemas")
        val lookup = reader.buildSchemaLookup(schemaDir)
        reader.readBooksInParallel(root.resolve("json"), schemaDir, lookup).single()
    }

    @Test
    fun `the honorific form reaches the payload and the author line`() {
        val payload = read(layout(withAuthorsFile = true))
        assertEquals(listOf("הרב אברהם יצחק הכהן קוק"), payload.authors)
        // Line 0 is the h1 title; the author lines follow it.
        assertEquals("<h1>אורות</h1>", payload.lines[0])
        assertEquals("הרב אברהם יצחק הכהן קוק", payload.lines[1])
    }

    @Test
    fun `without the file the schema name is used unchanged`() {
        val payload = read(layout(withAuthorsFile = false))
        assertEquals(listOf("אברהם יצחק הכהן קוק"), payload.authors)
        assertEquals("אברהם יצחק הכהן קוק", payload.lines[1])
    }

    @Test
    fun `the author line count is identical either way`() {
        // This is what keeps the change safe: every later line index, and so
        // every ref, heading and alt-TOC entry, stays where it was.
        val with = read(layout(withAuthorsFile = true))
        val without = read(layout(withAuthorsFile = false))
        assertEquals(without.lines.size, with.lines.size)
        assertEquals(without.lines.drop(2), with.lines.drop(2))
        assertEquals(without.refEntries, with.refEntries)
    }

    @Test
    fun `the author line carries the canonical name the author table gets`() {
        // ForDB/sefaria_author_changes.csv used to reach only the `author`
        // table, so line 2 of the body kept Sefaria's spelling (issue #96).
        val canonical = SefariaAuthorCanonicalNames.parse(
            listOf("שם בספריא,שם קנוני", "הרב אברהם יצחק הכהן קוק,רבי אברהם יצחק הכהן קוק"),
        )
        val payload = read(layout(withAuthorsFile = true), canonical)
        assertEquals(listOf("רבי אברהם יצחק הכהן קוק"), payload.authors)
        assertEquals("רבי אברהם יצחק הכהן קוק", payload.lines[1])
        // Mapping the name again, as the importer does, changes nothing.
        assertEquals(payload.authors, payload.authors.map(canonical::canonical))
        // Same line count and refs as without the mapping.
        val unmapped = read(layout(withAuthorsFile = true))
        assertEquals(unmapped.lines.drop(2), payload.lines.drop(2))
        assertEquals(unmapped.refEntries, payload.refEntries)
        // A blacklist entry written under the canonical name still matches.
        assertTrue("רבי אברהם יצחק הכהן קוק" in payload.authorMatchKeys)
        assertTrue("הראי\"ה" in payload.authorMatchKeys)
    }

    @Test
    fun `blacklist keys keep the bare schema name as well as the variants`() {
        val payload = read(layout(withAuthorsFile = true))
        assertTrue("אברהם יצחק הכהן קוק" in payload.authorMatchKeys, payload.authorMatchKeys.toString())
        assertTrue("הרב אברהם יצחק הכהן קוק" in payload.authorMatchKeys)
        assertTrue("הראי\"ה" in payload.authorMatchKeys)
    }

    @Test
    fun `an author blacklisted under the bare name is still blocked after enrichment`() {
        // The regression this guards: the display name became the honorific
        // form, and a blacklist entry written bare stopped matching.
        val payload = read(layout(withAuthorsFile = true))
        val blacklists = SefariaBlacklists(
            bookTitleKeys = emptySet(),
            bookPathKeys = emptySet(),
            authorKeys = setOfNotNull(normalizeTitleKey("אברהם יצחק הכהן קוק")),
        )
        val result = filterBlacklistedPayloads(listOf(payload), blacklists)
        assertTrue(result.payloads.isEmpty(), "book should have been filtered out")
        assertEquals(1, result.skippedByAuthor)
    }

    @Test
    fun `an author blacklisted under an acronym is blocked too`() {
        val payload = read(layout(withAuthorsFile = true))
        val blacklists = SefariaBlacklists(
            bookTitleKeys = emptySet(),
            bookPathKeys = emptySet(),
            authorKeys = setOfNotNull(normalizeTitleKey("הראי\"ה")),
        )
        assertTrue(filterBlacklistedPayloads(listOf(payload), blacklists).payloads.isEmpty())
    }

    private companion object {
        const val SCHEMA = """
        {
          "title": "Orot",
          "heTitle": "אורות",
          "categories": ["Jewish Thought"],
          "heCategories": ["מחשבת ישראל"],
          "authors": [
            {"en": "Abraham Isaac Kook", "he": "אברהם יצחק הכהן קוק", "slug": "abraham-isaac-kook"}
          ],
          "schema": {
            "title": "Orot",
            "heTitle": "אורות",
            "nodeType": "JaggedArrayNode",
            "depth": 1,
            "addressTypes": ["Integer"],
            "sectionNames": ["Paragraph"]
          }
        }
        """

        const val MERGED = """
        {"title":"Orot","heTitle":"אורות","language":"he",
         "categories":["Jewish Thought"],
         "versions":[["גרסה","http://example.org"]],
         "text":["פסקה ראשונה","פסקה שניה"]}
        """

        const val AUTHORS = """
        [{"slug":"abraham-isaac-kook","primaryHe":"אברהם יצחק הכהן קוק","primaryEn":"Abraham Isaac Kook",
          "titles":[{"text":"אברהם יצחק הכהן קוק","lang":"he","primary":true},
                    {"text":"הרב אברהם יצחק הכהן קוק","lang":"he"},
                    {"text":"הראי\"ה","lang":"he"},
                    {"text":"Abraham Isaac Kook","lang":"en","primary":true}]}]
        """
    }
}
