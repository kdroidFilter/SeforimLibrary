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

    // Global counter for generating unique temporary TOC IDs across all books
    private var globalTocIdCounter = 1L

    data class ParsedText(
        val lines: List<Line>,
        val tocEntries: List<TocEntry>,
        val tocLineIndices: Map<Long, Int>  // Maps temporary TOC ID to starting line index
    )

    /**
     * Helper class to track TOC entries with their line indices during parsing
     */
    private data class TocWithLineIndex(
        val tocEntry: TocEntry,
        val lineIndex: Int
    )

    /**
     * Parse a merged.json text structure into Lines and TOC entries
     * @param bookId The book ID
     * @param mergedText The merged text JSON
     * @param schema Optional schema for generating TOC from schema structure
     */
    fun parse(bookId: Long, mergedText: SefariaMergedText, schema: SefariaSchema? = null): ParsedText {
        val lines = mutableListOf<Line>()
        val tocsWithIndices = mutableListOf<TocWithLineIndex>()

        // Parse text and generate TOCs simultaneously
        val textElement = mergedText.text
        val hasSectionNames = mergedText.sectionNames != null
        val hasSchema = mergedText.schema != null

        when {
            hasSectionNames && textElement is JsonArray && schema?.schema != null -> {
                // Simple model with schema: parse with TOC generation
                parseSimpleArrayWithToc(bookId, mergedText, schema.schema!!, lines, tocsWithIndices)
            }
            textElement is JsonObject && schema?.schema != null -> {
                // Complex model with schema: parse with TOC generation
                parseComplexSchemaWithToc(bookId, mergedText, schema.schema!!, lines, tocsWithIndices)
            }
            hasSectionNames && textElement is JsonArray -> {
                // Simple model without schema: just parse lines
                parseSimpleArrayLines(bookId, mergedText, lines)
            }
            textElement is JsonObject || hasSchema -> {
                // Complex model without schema: just parse lines
                parseComplexSchemaLines(bookId, mergedText, lines)
            }
            else -> {
                logger.warn("Unknown text structure for book $bookId")
            }
        }

        // Extract TOC entries and build index map
        val tocEntries = tocsWithIndices.map { it.tocEntry }
        val tocLineIndices = tocsWithIndices.associate { it.tocEntry.id to it.lineIndex }

        return ParsedText(lines, tocEntries, tocLineIndices)
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
     * Parse simple array structure with TOC generation during parsing
     */
    private fun parseSimpleArrayWithToc(
        bookId: Long,
        mergedText: SefariaMergedText,
        schemaNode: SchemaNode,
        lines: MutableList<Line>,
        tocsWithIndices: MutableList<TocWithLineIndex>
    ) {
        val textElement = mergedText.text
        if (textElement !is JsonArray) {
            logger.warn("Expected JsonArray for simple array schema in book $bookId")
            return
        }

        val heSectionNames = schemaNode.heSectionNames ?: schemaNode.sectionNames ?: emptyList()
        val depth = schemaNode.depth ?: heSectionNames.size

        // Parse with TOC generation
        globalTocIdCounter = parseArrayWithToc(
            array = textElement,
            bookId = bookId,
            lines = lines,
            tocsWithIndices = tocsWithIndices,
            sectionNames = heSectionNames,
            depth = depth,
            currentDepth = 0,
            level = 0,
            parentId = null,
            nextTocId = globalTocIdCounter
        )
    }

    /**
     * Parse complex schema structure with TOC generation during parsing
     * IMPORTANT: This function follows JSON key order to ensure line indices match buildSeifIndexFromMerged
     */
    private fun parseComplexSchemaWithToc(
        bookId: Long,
        mergedText: SefariaMergedText,
        schemaNode: SchemaNode,
        lines: MutableList<Line>,
        tocsWithIndices: MutableList<TocWithLineIndex>
    ) {
        val textElement = mergedText.text
        if (textElement !is JsonObject) {
            logger.warn("Expected JsonObject for complex schema in book $bookId")
            return
        }

        // If schema has multiple nodes, it's a complex structure (like Tur with sections)
        if (schemaNode.nodes != null && schemaNode.nodes.isNotEmpty()) {
            globalTocIdCounter = parseSchemaNodesWithLines(
                bookId = bookId,
                nodes = schemaNode.nodes,
                textElement = textElement,
                lines = lines,
                tocsWithIndices = tocsWithIndices,
                level = 0,
                parentId = null,
                nextTocId = globalTocIdCounter
            )
        } else {
            // Simple JaggedArrayNode
            val heSectionNames = schemaNode.heSectionNames ?: schemaNode.sectionNames ?: emptyList()
            val depth = schemaNode.depth ?: heSectionNames.size

            if (textElement is JsonArray) {
                globalTocIdCounter = parseArrayWithToc(
                    array = textElement,
                    bookId = bookId,
                    lines = lines,
                    tocsWithIndices = tocsWithIndices,
                    sectionNames = heSectionNames,
                    depth = depth,
                    currentDepth = 0,
                    level = 0,
                    parentId = null,
                    nextTocId = globalTocIdCounter
                )
            }
        }
    }

    /**
     * Parse schema nodes with lines and TOCs simultaneously
     * CRITICAL: Follow JSON key order, NOT schema node order, to match buildSeifIndexFromMerged
     */
    private fun parseSchemaNodesWithLines(
        bookId: Long,
        nodes: List<SchemaNode>,
        textElement: JsonObject,
        lines: MutableList<Line>,
        tocsWithIndices: MutableList<TocWithLineIndex>,
        level: Int,
        parentId: Long?,
        nextTocId: Long
    ): Long {
        var currentTocId = nextTocId

        // IMPORTANT: Follow the order of keys in the JSON, NOT the order of nodes in the schema
        // This ensures line indices match the order used by buildSeifIndexFromMerged
        textElement.forEach { (jsonKey, jsonValue) ->
            // Find the schema node that corresponds to this JSON key
            val node = nodes.firstOrNull { schemaNode ->
                when {
                    schemaNode.key == "default" && jsonKey == "" -> true
                    schemaNode.key == jsonKey -> true
                    schemaNode.titles?.any { it.lang == "en" && it.text == jsonKey } == true -> true
                    schemaNode.enTitle == jsonKey -> true
                    else -> false
                }
            }

            if (node != null) {
                val heTitle = node.heTitle ?: ""

                // Only create TOC entry if this node has a Hebrew title
                val thisTocId = if (heTitle.isNotEmpty()) {
                    val startLineIndex = lines.size

                    // Create TOC entry
                    val tocEntry = TocEntry(
                        id = currentTocId,
                        bookId = bookId,
                        parentId = parentId,
                        level = level,
                        text = heTitle,
                        lineId = null  // Will be linked later in converter
                    )
                    tocsWithIndices.add(TocWithLineIndex(tocEntry, startLineIndex))

                    val id = currentTocId
                    currentTocId++
                    id
                } else {
                    null
                }

                // Parse the content of this node
                when {
                    // If this node has sub-nodes, process them recursively
                    node.nodes != null && node.nodes.isNotEmpty() && jsonValue is JsonObject -> {
                        // Handle special case: node with both Introduction and default ("") key
                        // Process non-default keys first, then default key
                        if (jsonValue.containsKey("")) {
                            // Process non-empty keys first (like "Introduction")
                            jsonValue.forEach { (subKey, subValue) ->
                                if (subKey.isNotEmpty()) {
                                    parseJsonElementToLines(subValue, bookId, lines)
                                }
                            }
                            // Now process the default "" key with schema nodes
                            jsonValue[""]?.let { defaultValue ->
                                if (defaultValue is JsonObject) {
                                    currentTocId = parseSchemaNodesWithLines(
                                        bookId, node.nodes, defaultValue, lines, tocsWithIndices,
                                        level + 1, thisTocId, currentTocId
                                    )
                                } else {
                                    parseJsonElementToLines(defaultValue, bookId, lines)
                                }
                            }
                        } else {
                            currentTocId = parseSchemaNodesWithLines(
                                bookId, node.nodes, jsonValue, lines, tocsWithIndices,
                                level + 1, thisTocId, currentTocId
                            )
                        }
                    }
                    // This is a leaf node (JaggedArrayNode) - parse array with TOCs
                    jsonValue is JsonArray -> {
                        val heSectionNames = node.heSectionNames ?: node.sectionNames ?: emptyList()
                        val depth = node.depth ?: heSectionNames.size

                        currentTocId = parseArrayWithToc(
                            array = jsonValue,
                            bookId = bookId,
                            lines = lines,
                            tocsWithIndices = tocsWithIndices,
                            sectionNames = heSectionNames,
                            depth = depth,
                            currentDepth = 0,
                            level = level + 1,
                            parentId = thisTocId,
                            nextTocId = currentTocId
                        )
                    }
                    // Otherwise just parse the JSON element to lines
                    else -> {
                        parseJsonElementToLines(jsonValue, bookId, lines)
                    }
                }
            } else {
                // No matching schema node, just parse the content
                parseJsonElementToLines(jsonValue, bookId, lines)
            }
        }

        return currentTocId
    }

    /**
     * Parse array with TOC generation for JaggedArrayNodes
     */
    private fun parseArrayWithToc(
        array: JsonArray,
        bookId: Long,
        lines: MutableList<Line>,
        tocsWithIndices: MutableList<TocWithLineIndex>,
        sectionNames: List<String>,
        depth: Int,
        currentDepth: Int,
        level: Int,
        parentId: Long?,
        nextTocId: Long
    ): Long {
        var currentTocId = nextTocId

        array.forEachIndexed { index, element ->
            when {
                // Only create TOC for top level (Simanim), not for deeper levels (Seifim)
                currentDepth == 0 && element is JsonArray && depth >= 1 -> {
                    val startLineIndex = lines.size

                    // Get Hebrew section name (e.g., "סימן")
                    val sectionName = sectionNames.getOrNull(currentDepth) ?: ""
                    // Use Hebrew numerals (א, ב, ג...) instead of Arabic (1, 2, 3...)
                    val hebrewNumber = toHebrewNumeral(index + 1)
                    val tocText = "$sectionName $hebrewNumber"

                    // Create TOC entry
                    val tocEntry = TocEntry(
                        id = currentTocId,
                        bookId = bookId,
                        parentId = parentId,
                        level = level,
                        text = tocText,
                        lineId = null  // Will be linked later in converter
                    )
                    tocsWithIndices.add(TocWithLineIndex(tocEntry, startLineIndex))

                    currentTocId++

                    // Parse nested array to lines
                    parseArrayToLines(element, bookId, lines)
                }
                element is JsonArray -> {
                    // Deeper level or no TOC needed - just parse lines
                    parseArrayToLines(element, bookId, lines)
                }
                element is JsonPrimitive && element.isString && element.content.isNotBlank() -> {
                    lines.add(Line(bookId = bookId, lineIndex = lines.size, content = element.content))
                }
            }
        }

        return currentTocId
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

}
