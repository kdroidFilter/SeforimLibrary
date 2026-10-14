package io.github.kdroidfilter.seforimlibrary.common.notes

import io.github.kdroidfilter.seforimlibrary.common.notes.InlineNotes.Note
import io.github.kdroidfilter.seforimlibrary.common.notes.InlineNotes.Parsed
import kotlin.test.Test
import kotlin.test.assertEquals

class InlineNotesTest {

    private fun notes(line: String): List<Note> = (InlineNotes.parse(line) as Parsed.Notes).notes

    @Test
    fun `an anchored note keeps its sup and its body ends at the depth-matched close`() {
        val line = "א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">גוף <i>מוטה</i> וסוף</i> ב"
        val found = notes(line)
        assertEquals(listOf("1"), found.map { it.marker })
        assertEquals(listOf("גוף <i>מוטה</i> וסוף"), found.map { it.body })
        assertEquals("א<sup class=\"footnote-marker\">1</sup> ב", InlineNotes.strip(line, found))
    }

    @Test
    fun `an unclosed body is reported, not guessed`() {
        val line = "א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">גוף <i>מוטה</i> בלי סוף"
        assertEquals(Parsed.Unclosed(line.indexOf("<i class")), InlineNotes.parse(line))
    }

    @Test
    fun `a note without an adjacent sup is unanchored and a distant plain sup stays as is`() {
        val line = "טקסט<sup>3</sup> מילה<i class=\"footnote\">א</i>.<i class=\"footnote\">ב</i>"
        val found = notes(line)
        assertEquals(listOf(null, null), found.map { it.marker })
        assertEquals("טקסט<sup>3</sup> מילה.", InlineNotes.strip(line, found))
        assertEquals(0, InlineNotes.countMarkers(InlineNotes.strip(line, found)))
    }

    @Test
    fun `a plain sup adjacent to a body anchors it and gains the marker class`() {
        val line = "א<sup>1</sup>&nbsp;<i class=\"footnote\">גוף</i> ב<sup style=\"color:red\" class=\"x\">2</sup><i class=\"footnote\">שני</i>"
        val found = notes(line)
        assertEquals(listOf("1", "2"), found.map { it.marker })
        val stripped = InlineNotes.strip(line, found)
        assertEquals(
            "א<sup class=\"footnote-marker\">1</sup>&nbsp; ב<sup class=\"footnote-marker\" style=\"color:red\">2</sup>",
            stripped,
        )
        assertEquals(2, InlineNotes.countMarkers(stripped))
    }

    @Test
    fun `classed and plain anchored markers in one line keep reading order`() {
        val line = "<sup>א</sup> <i class=\"footnote\">1</i> <sup>ב</sup> טקסט " +
            "<sup class=\"footnote-marker\">ג</sup><i class=\"footnote\">2</i> <sup>ד</sup><i class=\"footnote\">3</i>"
        val found = notes(line)
        assertEquals(listOf("א", "ג", "ד"), found.map { it.marker })
        val stripped = InlineNotes.strip(line, found)
        assertEquals(
            "<sup class=\"footnote-marker\">א</sup>  <sup>ב</sup> טקסט " +
                "<sup class=\"footnote-marker\">ג</sup> <sup class=\"footnote-marker\">ד</sup>",
            stripped,
        )
        val sups = Regex("<sup class=\"footnote-marker\">(.*?)</sup>").findAll(stripped).map { it.groupValues[1] }.toList()
        assertEquals(found.mapNotNull { it.marker }, sups)
    }

    @Test
    fun `whitespace between the sup and the note still anchors it`() {
        val line = "<sup class=\"footnote-marker\">*</sup> \n<i class=\"footnote\">x</i>"
        assertEquals(listOf("*"), notes(line).map { it.marker })
    }

    @Test
    fun `no-break spaces between the sup and the note still anchor it`() {
        for (gap in listOf("&nbsp;", "&#160;", "&#xA0;", "\u00A0", " &NBSP; ")) {
            val line = "<sup class=\"footnote-marker\">1</sup>$gap<i class=\"footnote\">x</i>"
            assertEquals(listOf("1"), notes(line).map { it.marker }, gap)
        }
        assertEquals(listOf(null), notes("<sup class=\"footnote-marker\">1</sup>&amp;<i class=\"footnote\">x</i>").map { it.marker })
    }

    @Test
    fun `tag and attribute case and spacing do not matter`() {
        val line = "<SUP CLASS = \"footnote-marker\">1</SUP><I Class=\"footnote\">x</I>"
        assertEquals(listOf("1" to "x"), notes(line).map { it.marker to it.body })
    }

    @Test
    fun `several notes keep reading order and each marker's raw inner HTML`() {
        val line = "א<sup class=\"footnote-marker\"><b>2</b></sup><i class=\"footnote\">ראשונה</i> ב " +
            "<i class=\"footnote\">בלי סמן</i> ג<sup class=\"footnote-marker\">ג</sup><i class=\"footnote\">שלישית</i>"
        val found = notes(line)
        assertEquals(listOf("<b>2</b>", null, "ג"), found.map { it.marker })
        assertEquals(listOf("ראשונה", "בלי סמן", "שלישית"), found.map { it.body })
        val stripped = InlineNotes.strip(line, found)
        assertEquals(
            "א<sup class=\"footnote-marker\"><b>2</b></sup> ב  ג<sup class=\"footnote-marker\">ג</sup>",
            stripped,
        )
        assertEquals(2, InlineNotes.countMarkers(stripped))
    }

    @Test
    fun `a note on a heading line is parsed inside the heading tags`() {
        val line = "<h2>פרק א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">הערה</i></h2>"
        val found = notes(line)
        assertEquals("<h2>פרק א<sup class=\"footnote-marker\">1</sup></h2>", InlineNotes.strip(line, found))
    }

    @Test
    fun `a footnote nested in a body belongs to that body`() {
        val line = "<i class=\"footnote\">חוץ <i class=\"footnote\">פנים</i> סוף</i>"
        assertEquals(listOf("חוץ <i class=\"footnote\">פנים</i> סוף"), notes(line).map { it.body })
    }

    @Test
    fun `other italics and lines without notes are untouched`() {
        assertEquals(emptyList(), notes("<i>מוטה</i> <img src=\"x\">"))
        assertEquals(emptyList(), notes("<sup class=\"footnote-marker\">1</sup> בלי גוף"))
    }

    @Test
    fun `stripMarkersAndBodies drops every marker and body, an unclosed body to the end`() {
        assertEquals(
            "א ב",
            InlineNotes.stripMarkersAndBodies("א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">x <i>y</i></i> ב"),
        )
        assertEquals("א", InlineNotes.stripMarkersAndBodies("א<i class=\"footnote\">x <i>y</i> בלי סוף"))
    }
}
