package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Issue otzaria-library#45: the siman's topic line stands above se'if א instead of inside it.
// Fixtures are Sefaria's own segments (merged.json), including the `<br>` closing the topic.
class SefariaSimanTopicLinesTest {

    private val json = Json { ignoreUnknownKeys = true }
    private val reader = SefariaBookPayloadReader(json, Logger.withTag("SefariaSimanTopicLinesTest"))
    private val schema = json.parseToJsonElement(
        """{"depth":2,"sectionNames":["Siman","Seif"],"heSectionNames":["סימן","סעיף"],"addressTypes":["Integer","Integer"]}""",
    ).jsonObject
    private val paragraphSchema = json.parseToJsonElement(
        """{"depth":2,"sectionNames":["Siman","Paragraph"],"heSectionNames":["סימן","פסקה"],"addressTypes":["Integer","Integer"]}""",
    ).jsonObject
    private val orachChayim = "שולחן ערוך, אורח חיים"
    private val bh = """<i data-commentator="Be'er HaGolah" data-label="א" data-order="1"></i>"""

    private fun build(heTitle: String, vararg simanim: List<String>): SefariaBookPayloadReader.BuiltBookContent =
        buildWith(schema, heTitle, *simanim)

    private fun buildWith(schemaObj: JsonObject, heTitle: String, vararg simanim: List<String>) =
        reader.walkTextWithSchema(
            schemaObj = schemaObj,
            textElement = JsonArray(simanim.map { seifim -> JsonArray(seifim.map(::JsonPrimitive)) }),
            bookHeTitle = heTitle,
            bookEnTitle = "Shulchan Arukh, Orach Chayim",
        )

    private fun keyHash(built: SefariaBookPayloadReader.BuiltBookContent, lineIndex: Int): ByteArray {
        val payload = BookPayload(
            heTitle = orachChayim, enTitle = "Shulchan Arukh, Orach Chayim", categoriesHe = listOf("הלכה"),
            lines = built.lines, refEntries = built.refs, headings = built.headings,
            authors = emptyList(), description = null, heShortDesc = null,
            pubDates = emptyList(), altStructures = emptyList(),
            cleanShiftByLineIndex = built.cleanShifts,
            lineKeyHashOverrides = built.lineKeyHashOverrides,
        ).precomputeLineData()
        return requireNotNull(payload.precomputed).lineKeyHashes!![lineIndex]
    }

    @Test
    fun `topic moves to a bold line between the siman and its first seif`() {
        val first = "<b>דין השכמת הבוקר. ובו ט סעיפים:</b><br>${bh}יתגבר כארי"
        val built = build(orachChayim, listOf(first, "שני"))

        assertEquals(
            listOf(
                "<h1>שולחן ערוך, אורח חיים</h1>",
                "<h2>סימן א</h2>",
                "<b>דין השכמת הבוקר. ובו ט סעיפים:</b>",
                "(א) ${bh}יתגבר כארי",
                "(ב) שני",
            ),
            built.lines,
        )
        assertEquals(setOf(2), built.simanTopicLines)
        // Not a navigation entry: the topic line adds no heading.
        assertEquals(listOf(orachChayim, "סימן א"), built.headings.map { it.title })
        // The se'if keeps its ref and the line id it had before the split.
        assertEquals(4, built.refs.first().lineIndex)
        assertContentEquals(
            IdAllocatorBindings.lineNaturalKeyHash(cleanSefariaLine(first, collapseInlineBreaks = true)),
            keyHash(built, 3),
        )
        assertTrue(lineWasModifiedByCleaning(requireNotNull(built.cleanShifts[3])))
    }

    @Test
    fun `whitespace after the break does not open the seif`() {
        val built = build("שולחן ערוך, יורה דעה", listOf("<b>מי הם הכשרים לשחוט. ובו י\"ד סעיפים:</b><br> ${bh}הכל שוחטין", "שני"))

        assertEquals("(א) ${bh}הכל שוחטין", built.lines[3])
    }

    @Test
    fun `commentator markers around the topic stay on the seif line in order`() {
        val peleti1 = """<i data-commentator="Peleti" data-order="1"></i>"""
        val peleti2 = """<i data-commentator="Peleti" data-order="2"></i>"""
        val built = build(
            "שולחן ערוך, יורה דעה",
            listOf("$peleti1<b>שמונה ${peleti2}מיני טריפות וסימנם. ובו סעיף אחד:</b><br> שמונה מיני טריפות הן"),
        )

        assertEquals("<b>שמונה מיני טריפות וסימנם. ובו סעיף אחד:</b>", built.lines[2])
        assertEquals("$peleti1${peleti2}שמונה מיני טריפות הן", built.lines[3])
    }

    @Test
    fun `single seif siman gets its topic line too`() {
        val built = build(orachChayim, listOf("<b>כוונת הברכות. ובו סעיף אחד:</b><br>יכוין"))

        assertEquals(listOf("<b>כוונת הברכות. ובו סעיף אחד:</b>", "יכוין"), built.lines.drop(2))
    }

    @Test
    fun `tur halachot heading moves above its first seif`() {
        val by = """<i data-commentator="Beit Yosef" data-order="1.1"></i>"""
        val built = build("טור", listOf("<b>הלכות דיינים</b><br> $by רבן שמעון בן גמליאל אומר", "שני"))

        assertEquals(listOf("<b>הלכות דיינים</b>", "(א) $by רבן שמעון בן גמליאל אומר"), built.lines.subList(2, 4))
        assertEquals(setOf(2), built.simanTopicLines)
    }

    @Test
    fun `a book without seif prefixes splits the same way`() {
        // ערוך השולחן: the leaf is a פסקה, so the se'if line carries no "(א) ".
        val built = buildWith(paragraphSchema, "ערוך השולחן", listOf("<b>דיני מליחת הלב והריאה. ובו י\"ח סעיפים</b> <br>יש בלב", "שני"))

        assertEquals(listOf("<b>דיני מליחת הלב והריאה. ובו י\"ח סעיפים</b>", "יש בלב", "שני"), built.lines.drop(2))
        assertTrue(SefariaSimanTopicLines.isTopicLine(built.lines[2]))
        assertTrue(!SefariaSimanTopicLines.isTopicLine(built.lines[3]))
    }

    @Test
    fun `only a first seif opening with a bold run and a break is split`() {
        val topic = "<b>דין אחר. ובו ב סעיפים:</b><br>טקסט"
        assertEquals("(ב) $topic", build(orachChayim, listOf("ראשון", topic)).lines.last())
        assertEquals("(א) $topic", build("ספר אחר", listOf(topic, "שני")).lines[2])
        // A bold run the text continues after is not a topic.
        assertEquals("(א) <b>דין אחר</b> טקסט", build(orachChayim, listOf("<b>דין אחר</b> טקסט", "שני")).lines[2])
        assertEquals(emptySet(), build(orachChayim, listOf("<b>דין אחר</b> טקסט <b>ב</b><br>ג", "שני")).simanTopicLines)
    }

    @Test
    fun `a Topic alt-toc entry opening a siman points at its topic line`() = runBlocking {
        val built = build(orachChayim, listOf("<b>דין השכמת הבוקר. ובו ט סעיפים:</b><br>יתגבר", "שני"))
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        val repository = SeforimRepository(":memory:", driver)
        val bookId = repository.insertBook(
            Book(
                id = 0, categoryId = repository.insertCategory(Category(id = 0, parentId = null, title = "שולחן ערוך", level = 0, order = 0)),
                sourceId = repository.insertSource("Sefaria"), title = orachChayim, heShortDesc = null, notesContent = null,
                order = 0f, totalLines = built.lines.size, isBaseBook = true, hasAltStructures = true,
            ),
        )
        val lineKeyToId = built.lines.indices.associate { i ->
            (orachChayim to i) to repository.insertLine(Line(id = 0, bookId = bookId, lineIndex = i, content = built.lines[i], heRef = null))
        }
        val node = AltNodePayload(
            title = "Laws of Morning Conduct", heTitle = "הלכות הנהגת האדם בבוקר",
            wholeRef = "Shulchan Arukh, Orach Chayim 1", refs = listOf("Shulchan Arukh, Orach Chayim 1:1"),
            addressTypes = listOf("Siman"), childLabel = null, addresses = emptyList(), skippedAddresses = emptyList(),
            startingAddress = null, offset = null, children = emptyList(),
        )
        val payload = BookPayload(
            heTitle = orachChayim, enTitle = "Shulchan Arukh, Orach Chayim", categoriesHe = listOf("שולחן ערוך"),
            lines = built.lines, refEntries = built.refs, headings = built.headings,
            authors = emptyList(), description = null, heShortDesc = null, pubDates = emptyList(),
            altStructures = listOf(AltStructurePayload(key = "Topic", title = "Topic", heTitle = orachChayim, nodes = listOf(node))),
            simanTopicLines = built.simanTopicLines,
        )

        val bindings = IdAllocatorBindings(InMemoryIdAllocator.load(path = null), repository)
        SefariaAltTocBuilder(repository, bindings).buildAltTocStructuresForBook(payload, bookId, orachChayim, lineKeyToId, built.lines.size)

        val structureId = repository.getAltTocStructuresForBook(bookId).single().id
        val entry = repository.getAltTocEntriesForStructure(structureId).single { it.text == "הלכות הנהגת האדם בבוקר" }
        assertEquals(lineKeyToId[orachChayim to 2], entry.lineId)
        driver.close()
    }
}
