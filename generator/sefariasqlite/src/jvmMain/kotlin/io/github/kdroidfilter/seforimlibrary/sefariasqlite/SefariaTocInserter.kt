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

        // Update hasChildren and isLastChild in one batched transaction
        // (per-row updates dominated this method's wall-time previously).
        val parentIds = tocParentMap.values.filterNotNull().toSet()
        val hasChildrenIds = allTocIds.filter { it in parentIds }
        val lastChildIds = entriesByParent.values.mapNotNull { it.lastOrNull() }
        repository.bulkUpdateTocEntryFlags(hasChildrenIds, lastChildIds)

        // Build line_toc mappings — forward-sweep instead of O(N) lastOrNull
        // per line. `sortedKeys` are heading positions sorted ascending; we
        // advance a single pointer across them as we walk the lines, giving
        // O(M + N) instead of the original O(M * N).
        val sortedKeys = headingLineToToc.keys.toIntArray().also { it.sort() }
        if (sortedKeys.isNotEmpty()) {
            // Local snapshot of the batch we'll splice in at the end, so we
            // synchronise on the shared list exactly once instead of per row.
            val localBatch = ArrayList<Pair<Long, Long>>(payload.lines.size)
            var heading = -1 // index into sortedKeys of the currently-active heading
            for (lineIdx in payload.lines.indices) {
                while (heading + 1 < sortedKeys.size && sortedKeys[heading + 1] <= lineIdx) {
                    heading++
                }
                if (heading < 0) continue
                val tocId = headingLineToToc[sortedKeys[heading]] ?: continue
                val lineId = lineKeyToId[bookPath to lineIdx] ?: continue
                localBatch.add(lineId to tocId)
            }
            if (localBatch.isNotEmpty()) {
                synchronized(lineTocBatch) { lineTocBatch.addAll(localBatch) }
            }
        }
    }
}
