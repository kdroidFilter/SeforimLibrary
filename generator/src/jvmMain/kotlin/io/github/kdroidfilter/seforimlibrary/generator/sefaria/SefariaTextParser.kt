package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SefariaMergedText
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory

/**
 * Parses Sefaria text structures and converts them to Lines and TOC entries
 */
class SefariaTextParser {
    private val logger = LoggerFactory.getLogger(SefariaTextParser::class.java)

    data class ParsedText(
        val lines: List<Line>,
        val tocEntries: List<TocEntry>
    )

    /**
     * Parse a merged.json text structure into Lines and TOC entries
     */
    fun parse(bookId: Long, mergedText: SefariaMergedText): ParsedText {
        val lines = mutableListOf<Line>()
        val tocEntries = mutableListOf<TocEntry>()

        // Determine structure type
        val textElement = mergedText.text
        val hasSectionNames = mergedText.sectionNames != null
        val hasSchema = mergedText.schema != null

        when {
            hasSectionNames && textElement is JsonArray -> {
                // Simple model: text is a nested array
                parseSimpleArray(bookId, mergedText, lines, tocEntries)
            }
            textElement is JsonObject || hasSchema -> {
                // Complex model: text is an object with named sections
                parseComplexSchema(bookId, mergedText, lines, tocEntries)
            }
            else -> {
                logger.warn("Unknown text structure for book $bookId")
            }
        }

        return ParsedText(lines, tocEntries)
    }

    private fun parseSimpleArray(
        bookId: Long,
        mergedText: SefariaMergedText,
        lines: MutableList<Line>,
        tocEntries: MutableList<TocEntry>
    ) {
        val sectionNames = mergedText.sectionNames ?: return
        val depth = sectionNames.size

        val textElement = mergedText.text
        if (textElement !is JsonArray) {
            logger.warn("Expected JsonArray for simple array schema in book $bookId, got ${textElement::class.simpleName}")
            return
        }

        when (depth) {
            1 -> parseDepth1(bookId, textElement, sectionNames[0], lines, tocEntries)
            2 -> parseDepth2(bookId, textElement, sectionNames, lines, tocEntries)
            3 -> parseDepth3(bookId, textElement, sectionNames, lines, tocEntries)
            else -> logger.warn("Unsupported depth: $depth for book $bookId")
        }
    }

    private fun parseDepth1(
        bookId: Long,
        textArray: JsonArray,
        sectionName: String,
        lines: MutableList<Line>,
        tocEntries: MutableList<TocEntry>
    ) {
        textArray.forEachIndexed { index, element ->
            if (element is JsonPrimitive && element.isString) {
                val content = element.content
                if (content.isNotBlank()) {
                    val line = Line(
                        bookId = bookId,
                        lineIndex = lines.size,
                        content = content
                    )
                    lines.add(line)

                    // Create TOC entry for each section
                    val tocEntry = TocEntry(
                        bookId = bookId,
                        level = 0,
                        text = "${translateSectionName(sectionName)} ${index + 1}"
                    )
                    tocEntries.add(tocEntry)
                }
            }
        }
    }

    private fun parseDepth2(
        bookId: Long,
        textArray: JsonArray,
        sectionNames: List<String>,
        lines: MutableList<Line>,
        tocEntries: MutableList<TocEntry>
    ) {
        textArray.forEachIndexed { chapterIndex, chapterElement ->
            if (chapterElement is JsonArray) {
                // Create TOC entry for chapter
                val chapterTocEntry = TocEntry(
                    bookId = bookId,
                    level = 0,
                    text = "${translateSectionName(sectionNames[0])} ${chapterIndex + 1}"
                )
                tocEntries.add(chapterTocEntry)

                chapterElement.forEachIndexed { verseIndex, verseElement ->
                    if (verseElement is JsonPrimitive && verseElement.isString) {
                        val content = verseElement.content
                        if (content.isNotBlank()) {
                            val line = Line(
                                bookId = bookId,
                                lineIndex = lines.size,
                                content = content
                            )
                            lines.add(line)
                        }
                    }
                }
            }
        }
    }

    private fun parseDepth3(
        bookId: Long,
        textArray: JsonArray,
        sectionNames: List<String>,
        lines: MutableList<Line>,
        tocEntries: MutableList<TocEntry>
    ) {
        textArray.forEachIndexed { level1Index, level1Element ->
            if (level1Element is JsonArray) {
                // Create TOC entry for level 1
                val level1TocEntry = TocEntry(
                    bookId = bookId,
                    level = 0,
                    text = "${translateSectionName(sectionNames[0])} ${level1Index + 1}"
                )
                tocEntries.add(level1TocEntry)

                level1Element.forEachIndexed { level2Index, level2Element ->
                    if (level2Element is JsonArray) {
                        // Create TOC entry for level 2
                        val level2TocEntry = TocEntry(
                            bookId = bookId,
                            level = 1,
                            text = "${translateSectionName(sectionNames[1])} ${level2Index + 1}"
                        )
                        tocEntries.add(level2TocEntry)

                        level2Element.forEachIndexed { level3Index, level3Element ->
                            if (level3Element is JsonPrimitive && level3Element.isString) {
                                val content = level3Element.content
                                if (content.isNotBlank()) {
                                    val line = Line(
                                        bookId = bookId,
                                        lineIndex = lines.size,
                                        content = content
                                    )
                                    lines.add(line)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private fun parseComplexSchema(
        bookId: Long,
        mergedText: SefariaMergedText,
        lines: MutableList<Line>,
        tocEntries: MutableList<TocEntry>
    ) {
        val textElement = mergedText.text
        if (textElement !is JsonObject) {
            logger.warn("Expected JsonObject for complex schema in book $bookId, got ${textElement::class.simpleName}")
            return
        }

        val textObject = textElement

        textObject.forEach { (sectionKey, sectionValue) ->
            // Create TOC entry for section
            val sectionName = if (sectionKey.isEmpty()) "ראשי" else sectionKey
            val sectionTocEntry = TocEntry(
                bookId = bookId,
                level = 0,
                text = sectionName
            )
            tocEntries.add(sectionTocEntry)

            // Parse section content
            when (sectionValue) {
                is JsonArray -> parseArrayContent(bookId, sectionValue, lines, 1, tocEntries)
                is JsonObject -> parseObjectContent(bookId, sectionValue, lines, 1, tocEntries)
                is JsonPrimitive -> {
                    if (sectionValue.isString && sectionValue.content.isNotBlank()) {
                        lines.add(Line(bookId = bookId, lineIndex = lines.size, content = sectionValue.content))
                    }
                }
                else -> logger.warn("Unexpected section value type: ${sectionValue::class.simpleName}")
            }
        }
    }

    private fun parseArrayContent(
        bookId: Long,
        array: JsonArray,
        lines: MutableList<Line>,
        level: Int,
        tocEntries: MutableList<TocEntry>
    ) {
        array.forEachIndexed { index, element ->
            when (element) {
                is JsonArray -> {
                    // Nested array - create TOC entry
                    val tocEntry = TocEntry(
                        bookId = bookId,
                        level = level,
                        text = "${index + 1}"
                    )
                    tocEntries.add(tocEntry)
                    parseArrayContent(bookId, element, lines, level + 1, tocEntries)
                }
                is JsonPrimitive -> {
                    if (element.isString && element.content.isNotBlank()) {
                        lines.add(Line(bookId = bookId, lineIndex = lines.size, content = element.content))
                    }
                }
                else -> logger.warn("Unexpected array element type: ${element::class.simpleName}")
            }
        }
    }

    private fun parseObjectContent(
        bookId: Long,
        obj: JsonObject,
        lines: MutableList<Line>,
        level: Int,
        tocEntries: MutableList<TocEntry>
    ) {
        obj.forEach { (key, value) ->
            // Create TOC entry for subsection
            val tocEntry = TocEntry(
                bookId = bookId,
                level = level,
                text = key
            )
            tocEntries.add(tocEntry)

            when (value) {
                is JsonArray -> parseArrayContent(bookId, value, lines, level + 1, tocEntries)
                is JsonObject -> parseObjectContent(bookId, value, lines, level + 1, tocEntries)
                is JsonPrimitive -> {
                    if (value.isString && value.content.isNotBlank()) {
                        lines.add(Line(bookId = bookId, lineIndex = lines.size, content = value.content))
                    }
                }
                else -> logger.warn("Unexpected object value type: ${value::class.simpleName}")
            }
        }
    }

    /**
     * Translate section names to Hebrew
     */
    private fun translateSectionName(englishName: String): String {
        return when (englishName.lowercase()) {
            "chapter" -> "פרק"
            "verse" -> "פסוק"
            "paragraph" -> "פסקה"
            "page" -> "דף"
            "section" -> "סעיף"
            "comment" -> "פירוש"
            "mishnah" -> "משנה"
            "halakhah" -> "הלכה"
            "siman" -> "סימן"
            "seif" -> "סעיף"
            else -> englishName
        }
    }
}
