package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * Otzaria/otzaria#1979: Be'er HaGolah's notes show the letter the Shulchan Arukh prints on
 * them (its itag's data-label), not their position: the alphabet restarts every siman and
 * skips the unlettered notes ("•").
 */
class SefariaBaseTextLettersTest {
    @Test
    fun notesShowTheLetterTheShulchanArukhPrintsOnThem() {
        // Shulchan Arukh, Orach Chayim 151 as Sefaria has it: the second note is unlettered.
        val notes = listOf("מגילה דף כ\"ח", "לאפוקי מגירסת הרא\"ש", "כך משמע שם בברכות")
        val payloads = read(
            Book(OC, "שולחן ערוך אורח חיים", seifim(COMMENTATOR, "א" to 1, "•" to 2, "ב" to 3)),
            Book(BH_OC, "באר הגולה על שולחן ערוך אורח חיים", notes),
        )
        val beerHaGolah = payloads.getValue(BH_OC)

        assertEquals(listOf("(א) מגילה דף כ\"ח", "לאפוקי מגירסת הרא\"ש", "(ב) כך משמע שם בברכות"), beerHaGolah.lines.drop(2))
        // Line ids stay: the key always hashed a note without its prefix.
        val keys = requireNotNull(beerHaGolah.precomputed?.lineKeyHashes)
        notes.forEachIndexed { i, note ->
            assertContentEquals(IdAllocatorBindings.lineNaturalKeyHash(note), keys[i + 2])
        }
    }

    @Test
    fun choshenMishpatNotesKeepTheLetterTheyPrint() {
        val payloads = read(
            Book(CM, "שולחן ערוך חושן משפט", seifim(COMMENTATOR, "א" to 1, "•" to 2, "ב" to 3, "ג" to 4)),
            Book(BH_CM, "באר הגולה על שולחן ערוך חושן משפט", listOf("א) מעובדא דאביי", "פירוש שמעידין שבפנינו", "ב) כן כ' הטור", "טור ס\"ג")),
        )

        assertEquals(
            listOf("א) מעובדא דאביי", "פירוש שמעידין שבפנינו", "ב) כן כ' הטור", "(ג) טור ס\"ג"),
            payloads.getValue(BH_CM).lines.drop(2),
        )
    }

    @Test
    fun yorehDeahKeepsTheNoteNumberItsAnchorsShow() {
        // Its itags carry data-order alone, so the anchor shows the note number too.
        val payloads = read(Book(BH_YD, "באר הגולה על שולחן ערוך יורה דעה", listOf("משנה ריש חולין", "תוס' שם")))

        assertEquals(listOf("(א) משנה ריש חולין", "(ב) תוס' שם"), payloads.getValue(BH_YD).lines.drop(2))
    }

    @Test
    fun aNoteTheShulchanArukhDoesNotLetterFails() {
        assertFailsWith<SefariaSchemaException> {
            read(
                Book(OC, "שולחן ערוך אורח חיים", seifim(COMMENTATOR, "א" to 1)),
                Book(BH_OC, "באר הגולה על שולחן ערוך אורח חיים", listOf("מגילה דף כ\"ח", "שם בגמ'")),
            )
        }
    }

    @Test
    fun beerHaGolahWithoutItsShulchanArukhFails() {
        assertFailsWith<SefariaSchemaException> {
            read(Book(BH_OC, "באר הגולה על שולחן ערוך אורח חיים", listOf("מגילה דף כ\"ח", "שם בגמ'")))
        }
    }

    private class Book(val title: String, val heTitle: String, val siman: List<String>)

    /** One-siman books, read together as the export holds them. */
    private fun read(vararg books: Book): Map<String, BookPayload> = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-base-text-letters")
        val schemaDir = Files.createDirectories(tempDir.resolve("schemas"))
        for (book in books) {
            val bookDir = Files.createDirectories(tempDir.resolve("json").resolve(book.title))
            Files.writeString(schemaDir.resolve("${book.title.replace(' ', '_')}.json"), schemaJson(book.title, book.heTitle))
            val siman = JsonArray(book.siman.map(::JsonPrimitive))
            Files.writeString(
                bookDir.resolve("merged.json"),
                """{"title": ${JsonPrimitive(book.title)}, "heTitle": "${book.heTitle}", "text": [$siman]}""",
            )
        }
        val reader = SefariaBookPayloadReader(
            Json { ignoreUnknownKeys = true; coerceInputValues = true },
            Logger.withTag("SefariaBaseTextLettersTest"),
        )
        reader.readBooksInParallel(tempDir.resolve("json"), schemaDir, reader.buildSchemaLookup(schemaDir))
            .associateBy { it.enTitle }
    }

    private fun schemaJson(title: String, heTitle: String) = """
        {
          "title": ${JsonPrimitive(title)},
          "heTitle": "$heTitle",
          "schema": {
            "nodeType": "JaggedArrayNode",
            "depth": 2,
            "addressTypes": ["Siman", "Seif"],
            "sectionNames": ["Siman", "Seif"],
            "heSectionNames": ["סימן", "סעיף"],
            "title": ${JsonPrimitive(title)},
            "heTitle": "$heTitle"
          }
        }
    """.trimIndent()

    private companion object {
        const val OC = "Shulchan Arukh, Orach Chayim"
        const val CM = "Shulchan Arukh, Choshen Mishpat"
        const val BH_OC = "Be'er HaGolah on Shulchan Arukh, Orach Chayim"
        const val BH_CM = "Be'er HaGolah on Shulchan Arukh, Choshen Mishpat"
        const val BH_YD = "Be'er HaGolah on Shulchan Arukh, Yoreh De'ah"
        const val COMMENTATOR = "Be'er HaGolah"

        /** A seif per itag, each opening with its (label, order) tag. */
        fun seifim(commentator: String, vararg tags: Pair<String, Int>) = tags.map { (label, order) ->
            """<i data-commentator="$commentator" data-label="$label" data-order="$order"></i>בתי כנסיות"""
        }
    }
}
