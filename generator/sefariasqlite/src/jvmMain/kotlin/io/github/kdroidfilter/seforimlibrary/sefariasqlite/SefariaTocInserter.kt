package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository

internal class SefariaTocInserter(
    private val repository: SeforimRepository,
    private val bindings: IdAllocatorBindings,
) {
    /**
     * Insert TOC entries hierarchically and build `line_toc` mappings.
     */
    suspend fun insertTocEntriesOptimized(
        payload: BookPayload,
        bookId: Long,
        bookPath: String,
        categoryId: Long,
        bookTitle: String,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineTocBatch: MutableList<Pair<Long, Long>>
    ) {
        val levelStack = ArrayDeque<Pair<Int, Long>>()
        // Ancestor-path stack (textIds) used to derive a stable tocEntry id.
        val ancestorPathStack = ArrayDeque<Long>()
        val headingLineToToc = mutableMapOf<Int, Long>()
        val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
        val allTocIds = mutableListOf<Long>()
        val tocParentMap = mutableMapOf<Long, Long?>()

        payload.headings.sortedBy { it.lineIndex }.forEach { h ->
            while (levelStack.isNotEmpty() && levelStack.last().first >= h.level) {
                levelStack.removeLast()
                if (ancestorPathStack.isNotEmpty()) ancestorPathStack.removeLast()
            }
            val parent = levelStack.lastOrNull()?.second
            val lineIdForHeading = lineKeyToId[bookPath to h.lineIndex]
            val textId = bindings.upsertTocText(h.title)
            // Encode line index alongside textId so that two distinct headings
            // sharing the same title under the same parent (legit in Sefaria's
            // sources) resolve to distinct stable tocEntry ids.
            ancestorPathStack.addLast(textId)
            val ancestorPath = ancestorPathStack.joinToString("/") + "@${h.lineIndex}"
            val tocId = bindings.insertTocEntryStable(
                TocEntry(
                    id = 0,
                    bookId = bookId,
                    parentId = parent,
                    textId = textId,
                    text = h.title,
                    level = h.level,
                    lineId = lineIdForHeading,
                    isLastChild = false,
                    hasChildren = false,
                ),
                ancestorPath = ancestorPath,
            )
            headingLineToToc[h.lineIndex] = tocId
            levelStack.addLast(h.level to tocId)
            allTocIds.add(tocId)
            tocParentMap[tocId] = parent
            entriesByParent.getOrPut(parent) { mutableListOf() }.add(tocId)
        }

        // Update hasChildren and isLastChild
        val parentIds = tocParentMap.values.filterNotNull().toSet()
        for (tocId in allTocIds) {
            if (tocId in parentIds) {
                repository.updateTocEntryHasChildren(tocId, true)
            }
        }
        for ((_, children) in entriesByParent) {
            if (children.isNotEmpty()) {
                val lastChildId = children.last()
                repository.updateTocEntryIsLastChild(lastChildId, true)
            }
        }

        // Build line_toc mappings - add to batch instead of individual inserts
        val sortedKeys = headingLineToToc.keys.sorted()
        for (lineIdx in payload.lines.indices) {
            val key = sortedKeys.lastOrNull { it <= lineIdx } ?: continue
            val tocId = headingLineToToc[key] ?: continue
            val lineId = lineKeyToId[bookPath to lineIdx] ?: continue
            synchronized(lineTocBatch) {
                lineTocBatch.add(lineId to tocId)
            }
        }
    }
}
