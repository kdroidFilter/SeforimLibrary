package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import org.slf4j.LoggerFactory

/**
 * Parses Sefaria citations and converts them to book/line references
 */
class SefariaCitationParser {
    private val logger = LoggerFactory.getLogger(SefariaCitationParser::class.java)

    data class Citation(
        val bookTitle: String,
        val section: String? = null,
        val references: List<Int> = emptyList()
    )

    /**
     * Parse a Sefaria citation string
     *
     * Examples:
     * - "Genesis 1:1" -> bookTitle="Genesis", references=[1, 1]
     * - "Beit Yosef, Orach Chayim 325:34:1" -> bookTitle="Beit Yosef", section="Orach Chayim", references=[325, 34, 1]
     * - "Tur, Orach Chayim, Introduction" -> bookTitle="Tur", section="Orach Chayim, Introduction", references=[]
     * - "Shabbat 45b:3" -> bookTitle="Shabbat", references=[page for 45b, 3]
     */
    fun parse(citation: String): Citation? {
        try {
            // Handle citations with sections (e.g., "Beit Yosef, Orach Chayim 325:34:1")
            if (citation.contains(",")) {
                val parts = citation.split(",", limit = 2)
                val bookTitle = parts[0].trim()
                val rest = parts[1].trim()

                // Extract section name (may contain spaces) and trailing numeric refs
                val lastSpaceIndex = rest.lastIndexOf(' ')
                if (lastSpaceIndex > 0) {
                    val section = rest.substring(0, lastSpaceIndex).trim()
                    val refsStr = rest.substring(lastSpaceIndex + 1).trim()
                    val refs = parseReferences(refsStr)
                    // If refs is empty (e.g., "Introduction" could not be parsed), treat entire rest as section
                    if (refs.isEmpty()) {
                        return Citation(bookTitle, rest, refs)
                    }
                    return Citation(bookTitle, section, refs)
                } else {
                    // No space found, treat entire rest as section without references
                    return Citation(bookTitle, rest, emptyList())
                }
            }

            // Handle simple citations (e.g., "Genesis 1:1")
            val lastSpaceIndex = citation.lastIndexOf(' ')
            if (lastSpaceIndex > 0) {
                val bookTitle = citation.substring(0, lastSpaceIndex).trim()
                val refsStr = citation.substring(lastSpaceIndex + 1).trim()
                val refs = parseReferences(refsStr)
                // If refs is empty, treat entire citation as book title
                if (refs.isEmpty()) {
                    return Citation(citation.trim(), null, refs)
                }
                return Citation(bookTitle, null, refs)
            }

            // No space or comma found - treat as book title only
            return Citation(citation.trim(), null, emptyList())
        } catch (e: Exception) {
            logger.error("Error parsing citation: $citation", e)
            return null
        }
    }

    /**
     * Parse reference string like "1:1", "45b:3", "325:34:1"
     */
    private fun parseReferences(refsStr: String): List<Int> {
        val refs = mutableListOf<Int>()

        // Handle Talmud page references (e.g., "45b")
        if (refsStr.contains(Regex("[ab]"))) {
            val page = refsStr.substringBefore(":").trim()
            val pageNum = page.replace(Regex("[ab]"), "").toIntOrNull() ?: 0
            val side = if (page.endsWith("b")) 1 else 0
            refs.add(pageNum * 2 + side) // Convert to linear index

            // Add remaining references
            val remaining = refsStr.substringAfter(":", "")
            if (remaining.isNotEmpty()) {
                remaining.split(":").forEach { ref ->
                    ref.toIntOrNull()?.let { refs.add(it) }
                }
            }
        } else {
            // Simple numeric references
            refsStr.split(":").forEach { ref ->
                ref.trim().toIntOrNull()?.let { refs.add(it) }
            }
        }

        return refs
    }

    /**
     * Calculate line index from citation references
     * This is a simplified approach - actual implementation may need book-specific logic
     */
    fun calculateLineIndex(citation: Citation, bookStructure: BookStructure): Int {
        return when (citation.references.size) {
            1 -> citation.references[0] - 1 // Direct index
            2 -> {
                // Chapter:Verse
                val chapter = citation.references[0] - 1
                val verse = citation.references[1] - 1
                val chapterOffset = bookStructure.getChapterOffset(chapter)
                chapterOffset + verse
            }
            3 -> {
                // Chapter:Verse:Comment (or Siman:Seif:Subsection)
                val level1 = citation.references[0] - 1
                val level2 = citation.references[1] - 1
                val level3 = citation.references[2] - 1
                bookStructure.getTripleOffset(level1, level2, level3)
            }
            else -> 0
        }
    }

    /**
     * Represents the structure of a book for calculating line offsets
     */
    data class BookStructure(
        val chapterLengths: List<Int> = emptyList(),
        val sectionLengths: List<List<Int>> = emptyList()
    ) {
        fun getChapterOffset(chapter: Int): Int {
            return chapterLengths.take(chapter).sum()
        }

        fun getTripleOffset(level1: Int, level2: Int, level3: Int): Int {
            var offset = 0
            for (i in 0 until level1) {
                offset += sectionLengths.getOrNull(i)?.sum() ?: 0
            }
            offset += sectionLengths.getOrNull(level1)?.take(level2)?.sum() ?: 0
            offset += level3
            return offset
        }
    }
}
