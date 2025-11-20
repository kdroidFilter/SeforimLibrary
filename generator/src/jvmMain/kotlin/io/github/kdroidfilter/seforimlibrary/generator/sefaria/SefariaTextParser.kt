package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SefariaMergedText
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SefariaSchema
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SchemaNode
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
     * @param bookId The book ID
     * @param mergedText The merged text JSON
     * @param schema Optional schema for generating TOC from schema structure
     */
    fun parse(bookId: Long, mergedText: SefariaMergedText, schema: SefariaSchema? = null): ParsedText {
        val lines = mutableListOf<Line>()
        val tocEntries = mutableListOf<TocEntry>()

        // First, parse the text to create lines
        val textElement = mergedText.text
        val hasSectionNames = mergedText.sectionNames != null
        val hasSchema = mergedText.schema != null

        when {
            hasSectionNames && textElement is JsonArray -> {
                // Simple model: text is a nested array
                parseSimpleArrayLines(bookId, mergedText, lines)
            }
            textElement is JsonObject || hasSchema -> {
                // Complex model: text is an object with named sections
                parseComplexSchemaLines(bookId, mergedText, lines)
            }
            else -> {
                logger.warn("Unknown text structure for book $bookId")
            }
        }

        // Generate TOC from schema if available, otherwise use text-based approach
        if (schema?.schema != null) {
            generateTocFromSchema(bookId, schema.schema!!, mergedText, lines, tocEntries)
        } else {
            // Fallback to text-based TOC generation
            generateTocFromText(bookId, mergedText, lines, tocEntries)
        }

        return ParsedText(lines, tocEntries)
    }

    private fun parseSimpleArrayLines(
        bookId: Long,
        mergedText: SefariaMergedText,
        lines: MutableList<Line>
    ) {
        val textElement = mergedText.text
        if (textElement !is JsonArray) {
            logger.warn("Expected JsonArray for simple array schema in book $bookId, got ${textElement::class.simpleName}")
            return
        }

        parseArrayToLines(textElement, bookId, lines)
    }

    private fun parseArrayToLines(array: JsonArray, bookId: Long, lines: MutableList<Line>) {
        array.forEach { element ->
            when (element) {
                is JsonArray -> parseArrayToLines(element, bookId, lines)
                is JsonPrimitive -> {
                    if (element.isString && element.content.isNotBlank()) {
                        lines.add(Line(bookId = bookId, lineIndex = lines.size, content = element.content))
                    }
                }
                else -> {}
            }
        }
    }

    private fun parseComplexSchemaLines(
        bookId: Long,
        mergedText: SefariaMergedText,
        lines: MutableList<Line>
    ) {
        val textElement = mergedText.text
        if (textElement !is JsonObject) {
            logger.warn("Expected JsonObject for complex schema in book $bookId, got ${textElement::class.simpleName}")
            return
        }

        textElement.forEach { (_, sectionValue) ->
            parseJsonElementToLines(sectionValue, bookId, lines)
        }
    }

    private fun parseJsonElementToLines(element: JsonElement, bookId: Long, lines: MutableList<Line>) {
        when (element) {
            is JsonArray -> element.forEach { parseJsonElementToLines(it, bookId, lines) }
            is JsonObject -> element.forEach { (_, value) -> parseJsonElementToLines(value, bookId, lines) }
            is JsonPrimitive -> {
                if (element.isString && element.content.isNotBlank()) {
                    lines.add(Line(bookId = bookId, lineIndex = lines.size, content = element.content))
                }
            }
        }
    }

    /**
     * Generate hierarchical TOC entries from schema structure
     * Inspired by Otzaria generator approach
     */
    private fun generateTocFromSchema(
        bookId: Long,
        schemaNode: SchemaNode,
        mergedText: SefariaMergedText,
        lines: List<Line>,
        tocEntries: MutableList<TocEntry>
    ) {
        // Track parent TOC IDs at each level (like Otzaria's parentStack)
        val parentStack = mutableMapOf<Int, Long>()
        // Track current line index
        val lineIndexCounter = IntArray(1) // [0] = current line index
        // Track next TOC ID to assign
        var nextTocId = 1L

        // Track entries by parent for second pass
        val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()

        // If schema has multiple nodes, it's a complex structure (like Tur with sections)
        if (schemaNode.nodes != null && schemaNode.nodes.isNotEmpty()) {
            nextTocId = processSchemaNodesHierarchical(
                bookId = bookId,
                nodes = schemaNode.nodes,
                textElement = mergedText.text,
                tocEntries = tocEntries,
                level = 0,
                parentStack = parentStack,
                lineIndexCounter = lineIndexCounter,
                nextTocId = nextTocId,
                entriesByParent = entriesByParent
            )
        } else {
            // Simple JaggedArrayNode - generate TOC based on sectionNames
            nextTocId = processSingleSchemaNodeHierarchical(
                bookId = bookId,
                node = schemaNode,
                textElement = mergedText.text,
                tocEntries = tocEntries,
                level = 0,
                parentStack = parentStack,
                lineIndexCounter = lineIndexCounter,
                nextTocId = nextTocId,
                parentId = null,
                entriesByParent = entriesByParent
            )
        }
    }

    private fun processSchemaNodesHierarchical(
        bookId: Long,
        nodes: List<SchemaNode>,
        textElement: JsonElement,
        tocEntries: MutableList<TocEntry>,
        level: Int,
        parentStack: MutableMap<Int, Long>,
        lineIndexCounter: IntArray,
        nextTocId: Long,
        entriesByParent: MutableMap<Long?, MutableList<Long>>
    ): Long {
        if (textElement !is JsonObject) return nextTocId

        var currentTocId = nextTocId

        nodes.forEach { node ->
            // Use ONLY Hebrew title for TOC display
            val heTitle = node.heTitle ?: ""

            // Get the text for this node
            // Try multiple keys: key, all English titles, then enTitle as fallback
            val nodeText = when {
                node.key == "default" -> textElement[""]  // Default node uses empty key
                node.key != null && textElement[node.key] != null -> textElement[node.key]
                // Try all English title variants from the titles list
                node.titles != null -> {
                    val englishTitles = node.titles.filter { it.lang == "en" }.map { it.text }
                    englishTitles.firstNotNullOfOrNull { title -> textElement[title] }
                }
                node.enTitle != null -> textElement[node.enTitle]
                else -> null
            }

            if (nodeText != null) {
                // Calculate parent ID (same as Otzaria: find parent at level-1)
                val parentId = if (level > 0) parentStack[level - 1] else null

                // Only create TOC entry if this node has a Hebrew title (skip "default" nodes without title)
                val thisTocId = if (heTitle.isNotEmpty()) {
                    // Create TOC entry for this section
                    val tocEntry = TocEntry(
                        id = currentTocId,
                        bookId = bookId,
                        parentId = parentId,
                        level = level,
                        text = heTitle,
                        lineId = null  // Will be set later based on first line of section
                    )
                    tocEntries.add(tocEntry)

                    // Track this TOC ID for its parent
                    entriesByParent.getOrPut(parentId) { mutableListOf() }.add(currentTocId)

                    // Update parent stack for this level
                    parentStack[level] = currentTocId

                    val id = currentTocId
                    currentTocId++
                    id
                } else {
                    // No TOC entry for this node (e.g., "default" node), use parent's ID
                    parentId
                }

                // If this node has sub-nodes, process them
                if (node.nodes != null && node.nodes.isNotEmpty()) {
                    currentTocId = processSchemaNodesHierarchical(
                        bookId, node.nodes, nodeText, tocEntries, level + 1,
                        parentStack, lineIndexCounter, currentTocId, entriesByParent
                    )
                } else {
                    // This is a leaf node (JaggedArrayNode) - generate TOC for array elements
                    currentTocId = processJaggedArrayNodeHierarchical(
                        bookId, node, nodeText, tocEntries, level + 1,
                        parentStack, lineIndexCounter, currentTocId, thisTocId, entriesByParent
                    )
                }
            }
        }

        return currentTocId
    }

    private fun processSingleSchemaNodeHierarchical(
        bookId: Long,
        node: SchemaNode,
        textElement: JsonElement,
        tocEntries: MutableList<TocEntry>,
        level: Int,
        parentStack: MutableMap<Int, Long>,
        lineIndexCounter: IntArray,
        nextTocId: Long,
        parentId: Long?,
        entriesByParent: MutableMap<Long?, MutableList<Long>>
    ): Long {
        if (node.nodeType == "JaggedArrayNode") {
            return processJaggedArrayNodeHierarchical(
                bookId, node, textElement, tocEntries, level,
                parentStack, lineIndexCounter, nextTocId, parentId, entriesByParent
            )
        }
        return nextTocId
    }

    private fun processJaggedArrayNodeHierarchical(
        bookId: Long,
        node: SchemaNode,
        textElement: JsonElement,
        tocEntries: MutableList<TocEntry>,
        level: Int,
        parentStack: MutableMap<Int, Long>,
        lineIndexCounter: IntArray,
        nextTocId: Long,
        parentId: Long?,
        entriesByParent: MutableMap<Long?, MutableList<Long>>
    ): Long {
        // Get Hebrew section names (e.g., "סימן", "סעיף") - prefer Hebrew, fallback to English
        val heSectionNames = node.heSectionNames ?: node.sectionNames ?: return nextTocId
        val depth = node.depth ?: heSectionNames.size

        if (textElement is JsonArray && depth >= 1) {
            return processJaggedArrayHierarchical(
                bookId = bookId,
                array = textElement,
                tocEntries = tocEntries,
                level = level,
                depth = depth,
                currentDepth = 0,
                sectionNames = heSectionNames,
                parentStack = parentStack,
                lineIndexCounter = lineIndexCounter,
                nextTocId = nextTocId,
                parentId = parentId,
                entriesByParent = entriesByParent,
                lines = emptyList() // Will get lineId from lineIndexCounter
            )
        }
        return nextTocId
    }

    private fun processJaggedArrayHierarchical(
        bookId: Long,
        array: JsonArray,
        tocEntries: MutableList<TocEntry>,
        level: Int,
        depth: Int,
        currentDepth: Int,
        sectionNames: List<String>,
        parentStack: MutableMap<Int, Long>,
        lineIndexCounter: IntArray,
        nextTocId: Long,
        parentId: Long?,
        entriesByParent: MutableMap<Long?, MutableList<Long>>,
        lines: List<Line>
    ): Long {
        var currentTocId = nextTocId

        array.forEachIndexed { index, element ->
            when {
                // Only create TOC for top level (Simanim), not for deeper levels (Seifim)
                currentDepth == 0 && element is JsonArray -> {
                    // Get Hebrew section name (e.g., "סימן")
                    val sectionName = sectionNames.getOrNull(currentDepth) ?: ""
                    // Use Hebrew numerals (א, ב, ג...) instead of Arabic (1, 2, 3...)
                    val hebrewNumber = toHebrewNumeral(index + 1)
                    val tocText = "$sectionName $hebrewNumber"

                    // Calculate parent (use parentId passed from parent node)
                    val tocParentId = if (level > 0) parentId else null

                    // Create TOC entry
                    val tocEntry = TocEntry(
                        id = currentTocId,
                        bookId = bookId,
                        parentId = tocParentId,
                        level = level,
                        text = tocText,
                        lineId = null // Will be set by repository based on lineIndex
                    )
                    tocEntries.add(tocEntry)

                    // Track for parent
                    entriesByParent.getOrPut(tocParentId) { mutableListOf() }.add(currentTocId)

                    // Update parent stack
                    parentStack[level] = currentTocId

                    currentTocId++

                    // Recurse into nested array and count lines
                    countTextLines(element, lineIndexCounter)
                }
                currentDepth > 0 && element is JsonArray -> {
                    // Just count lines, don't create TOC for seifim
                    countTextLines(element, lineIndexCounter)
                }
                element is JsonPrimitive && element.isString && element.content.isNotBlank() -> {
                    // Count this line
                    lineIndexCounter[0]++
                }
                element is JsonArray -> {
                    // Leaf level array - count all text lines
                    countTextLines(element, lineIndexCounter)
                }
            }
        }

        return currentTocId
    }

    private fun countTextLines(element: JsonElement, counter: IntArray) {
        when (element) {
            is JsonArray -> element.forEach { countTextLines(it, counter) }
            is JsonObject -> element.values.forEach { countTextLines(it, counter) }
            is JsonPrimitive -> {
                if (element.isString && element.content.isNotBlank()) {
                    counter[0]++
                }
            }
        }
    }

    /**
     * Convert number to Hebrew letters (Gematria)
     * 1 = א, 2 = ב, 3 = ג, ..., 10 = י, 11 = יא, 20 = כ, etc.
     */
    private fun toHebrewNumeral(num: Int): String {
        if (num <= 0) return ""

        val ones = listOf("", "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט")
        val tens = listOf("", "י", "כ", "ל", "מ", "נ", "ס", "ע", "פ", "צ")
        val hundreds = listOf("", "ק", "ר", "ש", "ת", "תק", "תר", "תש", "תת", "תתק")

        val result = StringBuilder()

        // Handle hundreds
        val h = (num / 100) % 10
        if (h > 0 && h < hundreds.size) {
            result.append(hundreds[h])
        }

        // Handle tens
        val t = (num / 10) % 10
        if (t > 0 && t < tens.size) {
            result.append(tens[t])
        }

        // Handle ones
        val o = num % 10
        if (o > 0 && o < ones.size) {
            result.append(ones[o])
        }

        return result.toString()
    }

    /**
     * Fallback: generate TOC from text structure (old method)
     */
    private fun generateTocFromText(
        bookId: Long,
        mergedText: SefariaMergedText,
        lines: List<Line>,
        tocEntries: MutableList<TocEntry>
    ) {
        // Use simple numbering based on structure
        val textElement = mergedText.text
        if (textElement is JsonObject) {
            textElement.forEach { (key, _) ->
                tocEntries.add(
                    TocEntry(
                        bookId = bookId,
                        level = 0,
                        text = if (key.isEmpty()) "ראשי" else key
                    )
                )
            }
        }
    }

}
