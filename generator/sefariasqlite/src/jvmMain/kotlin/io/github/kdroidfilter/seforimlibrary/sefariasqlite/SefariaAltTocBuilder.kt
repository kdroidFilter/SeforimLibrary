package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocStructure
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository

internal class SefariaAltTocBuilder(
    private val repository: SeforimRepository
) {
    suspend fun buildAltTocStructuresForBook(
        payload: BookPayload,
        bookId: Long,
        bookPath: String,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        totalLines: Int
    ): Boolean {
        if (payload.altStructures.isEmpty()) return false

        var hasGeneratedAltStructures = false

        val isTalmudTractate = payload.categoriesHe.any { it.contains("תלמוד") }
        val isShulchanArukhCode = payload.categoriesHe.any { it.contains("שולחן ערוך") }
        val isTurCode = payload.categoriesHe.any { it.contains("טור") }

        val refsForBook = payload.refEntries.map { it.copy(path = bookPath) }
        val bookAliasKeys = buildSet {
            val titles = listOf(
                payload.enTitle,
                payload.heTitle,
                sanitizeFolder(payload.enTitle),
                sanitizeFolder(payload.heTitle)
            )
            titles.forEach { title ->
                add(canonicalCitation(title))
                normalizeTitleKey(title)?.let { normalized ->
                    add(canonicalCitation(normalized))
                }
            }
        }.filterNot { it.isBlank() }.toSet()

        val canonicalToLine: Map<String, Pair<Long?, Int?>> = buildMap {
            refsForBook.forEach { entry ->
                val lineIdx = entry.lineIndex - 1
                val lineId = lineKeyToId[bookPath to lineIdx]
                val refsForEntry = listOfNotNull(entry.ref, entry.heRef)
                refsForEntry.forEach { value ->
                    val canonical = canonicalCitation(value)
                    fun addKey(key: String?) {
                        if (key.isNullOrBlank()) return
                        val current = this[key]?.second
                        if (current == null || (lineIdx in 0..<current)) {
                            put(key, lineId to lineIdx)
                        }
                    }
                    addKey(canonical)
                    addKey(stripBookAlias(canonical, bookAliasKeys))
                    addKey(canonicalTail(value))
                }
            }
        }
        val maxColonDepth = canonicalToLine.keys.maxOfOrNull { key -> key.count { it == ':' } } ?: 0

        payload.altStructures.forEach { structure ->
            val isPsalms30DayCycle = structure.key == "30 Day Cycle"
            val structureId = repository.upsertAltTocStructure(
                AltTocStructure(
                    bookId = bookId,
                    key = structure.key,
                    title = structure.title,
                    heTitle = structure.heTitle
                )
            )

            val headingLineToToc = mutableMapOf<Int, Long>()
            val entriesByParent = mutableMapOf<Long?, MutableList<Long>>()
            val entryLineInfo = mutableMapOf<Long, Pair<Long?, Int?>>()
            val usedLineIdsByParent = mutableMapOf<Long?, MutableSet<Long>>()

            fun parseDafIndex(address: String?): Int? {
                if (address.isNullOrBlank()) return null
                val match = Regex("(\\d+)([ab])?", RegexOption.IGNORE_CASE).find(address.trim())
                val (pageStr, amudRaw) = match?.destructured ?: return null
                val page = pageStr.toIntOrNull() ?: return null
                val amud = amudRaw.lowercase()
                val offset = if (amud == "b") 2 else 1
                return ((page - 1) * 2) + offset
            }

            fun computeAddressValue(node: AltNodePayload, idx: Int): Int? {
                node.addresses.getOrNull(idx)?.let { return it }
                val skip = node.skippedAddresses.toSet()
                val base = node.offset
                    ?: parseDafIndex(node.startingAddress)?.minus(1)
                    ?: -1
                if (base < 0) return null
                var current = base
                var steps = idx
                while (steps >= 0) {
                    current += 1
                    if (current in skip) continue
                    steps--
                }
                return current
            }

            fun resolveLineForCitation(
                citation: String?,
                isChapterOrSimanLevel: Boolean,
                allowChapterFallback: Boolean = true,
                allowTailFallback: Boolean = true
            ): Pair<Long?, Int?> {
                if (citation.isNullOrBlank()) return null to null

                fun expandedCandidates(base: String): List<String> {
                    if (base.isBlank() || maxColonDepth <= 0) return emptyList()
                    val colonCount = base.count { it == ':' }
                    if (colonCount >= maxColonDepth) return emptyList()
                    val expansions = mutableListOf<String>()
                    var current = base
                    repeat(maxColonDepth - colonCount) {
                        current += ":1"
                        expansions += current
                    }
                    return expansions
                }

                fun matchKey(key: String): Pair<Long?, Int?>? {
                    val variants = linkedSetOf(key).apply {
                        if (key.contains('.')) {
                            add(key.replace('.', ' '))
                            add(key.replace(Regex("\\.(\\d+)")) { match -> ":${match.groupValues[1]}" })
                            add(key.replace(Regex("\\.(\\d+)")) { match -> " ${match.groupValues[1]}" })
                            add(key.replace(".", ""))
                        }
                    }.filter { it.isNotBlank() }
                    variants.forEach { variant ->
                        canonicalToLine[variant]?.let { return it }
                        for (expanded in expandedCandidates(variant)) {
                            canonicalToLine[expanded]?.let { return it }
                        }
                    }
                    return null
                }

                fun fallbackWithinChapter(canonical: String): Pair<Long?, Int?>? {
                    if (!canonical.contains(':')) return null
                    val base = canonical.substringBefore(':')
                    val numStr = canonical.substringAfter(':').takeWhile { it.isDigit() }
                    val start = numStr.toIntOrNull() ?: return null
                    for (n in start downTo 1) {
                        val candidate = "$base:$n"
                        val candidates = listOf(candidate, stripBookAlias(candidate, bookAliasKeys))
                        candidates.forEach { key ->
                            if (key.isNotBlank()) {
                                matchKey(key)?.let { return it }
                            }
                        }
                    }
                    return null
                }

                fun lookup(raw: String): Pair<Long?, Int?>? {
                    val canonical = canonicalCitation(raw)
                    val stripped = stripBookAlias(canonical, bookAliasKeys)
                    val tail = canonicalTail(raw)
                    val candidates = buildList {
                        add(canonical)
                        add(stripped)
                        if (allowTailFallback) add(tail)
                    }
                    candidates.forEach { key ->
                        if (key.isNotBlank()) {
                            matchKey(key)?.let { return it }
                        }
                    }
                    val rangeStart = citationRangeStart(canonical)
                    if (rangeStart != null) {
                        val rangeCandidates = buildList {
                            add(rangeStart)
                            add(stripBookAlias(rangeStart, bookAliasKeys))
                            if (allowTailFallback) add(canonicalTail(rangeStart))
                        }
                        rangeCandidates.forEach { key ->
                            if (key.isNotBlank()) {
                                matchKey(key)?.let { return it }
                            }
                        }
                    }

                    if (allowChapterFallback) {
                        val chapterKey = canonical.substringBefore(':').takeIf { it.isNotBlank() }
                        if (chapterKey != null) {
                            val chapterStart = "$chapterKey:1"
                            val chapterCandidates = listOf(
                                chapterStart,
                                stripBookAlias(chapterStart, bookAliasKeys),
                                canonicalTail(chapterStart),
                                chapterKey,
                                stripBookAlias(chapterKey, bookAliasKeys)
                            )
                            chapterCandidates.forEach { key ->
                                if (key.isNotBlank()) {
                                    matchKey(key)?.let { return it }
                                }
                            }
                        }
                    }
                    fallbackWithinChapter(canonical)?.let { return it }
                    return null
                }

                lookup(citation)?.let { return it }

                if (isChapterOrSimanLevel) {
                    val canonical = canonicalCitation(citation)
                    val base = canonical.substringBefore('-').trim()
                    if (!base.contains(':')) {
                        val withColon = "$base:1"
                        lookup(withColon)?.let { return it }
                    }
                }

                return null to null
            }

            fun mapBaseToHebrew(base: String?): String? {
                if (base.isNullOrBlank()) return null
                val norm = base.lowercase()
                return when {
                    "aliyah" in norm -> "עליה"
                    "daf" in norm -> "דף"
                    "chapter" in norm -> "פרק"
                    "perek" in norm -> "פרק"
                    "siman" in norm -> "סימן"
                    "section" in norm -> "סימן"
                    "klal" in norm -> "כלל"
                    "psalm" in norm || "psalms" in norm -> "מזמור"
                    "day" in norm -> "יום"
                    else -> base
                }
            }

            fun buildChildLabel(base: String?, idx: Int, addressValue: Int?, addressType: String?): String {
                val numericValue = (addressValue ?: (idx + 1)).coerceAtLeast(1)
                val suffix = if (addressType.equals("Talmud", ignoreCase = true)) {
                    toDaf(numericValue)
                } else {
                    toGematria(numericValue)
                }
                val hebBase = mapBaseToHebrew(base)
                val cleanBase = hebBase?.takeIf { it.isNotBlank() }
                return cleanBase?.let { "$it $suffix" } ?: suffix
            }

            suspend fun updateParentLineIfMissing(tocId: Long) {
                val current = entryLineInfo[tocId]
                if (current?.second != null) return
                val childWithLine = entriesByParent[tocId]
                    ?.firstNotNullOfOrNull { childId ->
                        entryLineInfo[childId]?.second?.let { _ -> childId to (entryLineInfo[childId]!!) }
                    }
                val childLine = childWithLine?.second ?: return
                val lineId = childLine.first ?: return
                val lineIndex = childLine.second ?: return
                repository.updateAltTocEntryLineId(tocId, lineId)
                entryLineInfo[tocId] = lineId to lineIndex
                headingLineToToc[lineIndex] = tocId
            }

            fun nodeLabel(node: AltNodePayload, position: Int?): String {
                if (!node.heTitle.isNullOrBlank()) return node.heTitle
                if (!node.title.isNullOrBlank()) return node.title

                val addressType = node.addressTypes.firstOrNull()
                val addrValue = computeAddressValue(node, 0)
                val base = mapBaseToHebrew(node.childLabel)
                    ?: if (addressType.equals("Talmud", ignoreCase = true)) "דף" else null
                val suffix = when {
                    addrValue != null && addressType.equals("Talmud", ignoreCase = true) -> toDaf(addrValue)
                    addrValue != null -> toGematria(addrValue)
                    position != null -> toGematria(position + 1)
                    else -> toGematria(1)
                }
                return base?.let { "$it $suffix" } ?: "פרק $suffix"
            }

            suspend fun addEntry(node: AltNodePayload, level: Int, parentId: Long?, position: Int?): Long {
                val isChapterOrSimanLevel = node.addressTypes.any {
                    it.equals("Siman", ignoreCase = true) ||
                            it.equals("Perek", ignoreCase = true) ||
                            it.equals("Chapter", ignoreCase = true) ||
                            it.equals("Integer", ignoreCase = true)
                }
                val isDafNode = node.addressTypes.any { it.equals("Talmud", ignoreCase = true) }

                val primaryCandidates = buildList {
                    node.wholeRef?.let { add(it) }
                    addAll(node.refs)
                }
                var lineId: Long? = null
                var lineIndex: Int? = null
                for (candidate in primaryCandidates) {
                    val (lid, lidx) = resolveLineForCitation(
                        candidate,
                        isChapterOrSimanLevel,
                        allowChapterFallback = !isDafNode,
                        allowTailFallback = !isDafNode
                    )
                    if (lid != null && lidx != null) {
                        lineId = lid
                        lineIndex = lidx
                        break
                    }
                }
                if (lineId == null || lineIndex == null) return 0L
                val text = nodeLabel(node, position)

                val used = usedLineIdsByParent.getOrPut(parentId) { mutableSetOf() }
                if (lineId in used) return 0L
                used += lineId

                val tocId = repository.insertAltTocEntry(
                    AltTocEntry(
                        structureId = structureId,
                        parentId = parentId,
                        textId = null,
                        text = text,
                        level = level,
                        lineId = lineId,
                        isLastChild = false,
                        hasChildren = false
                    )
                )
                hasGeneratedAltStructures = true
                entryLineInfo[tocId] = lineId to lineIndex
                entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocId)
                headingLineToToc[lineIndex] = tocId

                var hasChild = false
                if (!isTalmudTractate && !isShulchanArukhCode && !isTurCode && !isPsalms30DayCycle && node.refs.isNotEmpty()) {
                    for ((idx, ref) in node.refs.withIndex()) {
                        val (childLineId, childLineIndex) = resolveLineForCitation(
                            ref,
                            isChapterOrSimanLevel,
                            allowChapterFallback = !isDafNode,
                            allowTailFallback = !isDafNode
                        )
                        if (childLineId == null || childLineIndex == null) continue

                        val used = usedLineIdsByParent.getOrPut(tocId) { mutableSetOf() }
                        if (childLineId in used) continue
                        used += childLineId

                        val addressValue = computeAddressValue(node, idx)
                        val label = buildChildLabel(node.childLabel, idx, addressValue, node.addressTypes.firstOrNull())
                        val childTocId = repository.insertAltTocEntry(
                            AltTocEntry(
                                structureId = structureId,
                                parentId = tocId,
                                textId = null,
                                text = label,
                                level = level + 1,
                                lineId = childLineId,
                                isLastChild = false,
                                hasChildren = false
                            )
                        )
                        hasGeneratedAltStructures = true
                        hasChild = true
                        entryLineInfo[childTocId] = childLineId to childLineIndex
                        entriesByParent.getOrPut(tocId) { mutableListOf() }.add(childTocId)
                        headingLineToToc[childLineIndex] = childTocId
                    }
                }

                if (hasChild) {
                    repository.updateAltTocEntryHasChildren(tocId, true)
                }

                return tocId
            }

            suspend fun createContainerEntry(node: AltNodePayload, level: Int, parentId: Long?, position: Int?): Long {
                val text = when {
                    !node.heTitle.isNullOrBlank() -> node.heTitle
                    position != null -> "פרק ${toGematria(position + 1)}"
                    !node.title.isNullOrBlank() -> node.title
                    !structure.heTitle.isNullOrBlank() -> structure.heTitle
                    !structure.title.isNullOrBlank() -> structure.title
                    else -> structure.key
                }
                val tocId = repository.insertAltTocEntry(
                    AltTocEntry(
                        structureId = structureId,
                        parentId = parentId,
                        textId = null,
                        text = text,
                        level = level,
                        lineId = null,
                        isLastChild = false,
                        hasChildren = false
                    )
                )
                entryLineInfo[tocId] = null to null
                entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocId)
                return tocId
            }

            suspend fun traverseAltNode(node: AltNodePayload, level: Int, parentId: Long?, position: Int?): Boolean {
                val hasOwnRefs = node.wholeRef != null || node.refs.isNotEmpty()
                val hasTitle = !node.heTitle.isNullOrBlank() || !node.title.isNullOrBlank()
                val isDafNode = node.addressTypes.any { it.equals("Talmud", ignoreCase = true) }
                val inlineChildrenOnly = isDafNode && node.refs.isNotEmpty() && !hasTitle
                var currentParent = parentId
                var containerId: Long? = null
                var inserted = false

                if (!hasOwnRefs && node.children.isNotEmpty() && hasTitle) {
                    containerId = createContainerEntry(node, level, parentId, position)
                    currentParent = containerId
                }

                if (inlineChildrenOnly) {
                    node.refs.forEachIndexed { idx, ref ->
                        val (childLineId, childLineIndex) = resolveLineForCitation(
                            ref,
                            isChapterOrSimanLevel = false,
                            allowChapterFallback = false,
                            allowTailFallback = false
                        )
                        if (childLineId == null || childLineIndex == null) return@forEachIndexed
                        val addressValue = computeAddressValue(node, idx)
                        val label = buildChildLabel(node.childLabel, idx, addressValue, node.addressTypes.firstOrNull())
                        if (childLineId in usedLineIdsByParent.getOrPut(currentParent) { mutableSetOf() }) return@forEachIndexed
                        usedLineIdsByParent.getOrPut(currentParent) { mutableSetOf() } += childLineId

                        val childId = repository.insertAltTocEntry(
                            AltTocEntry(
                                structureId = structureId,
                                parentId = currentParent,
                                textId = null,
                                text = label,
                                level = level,
                                lineId = childLineId,
                                isLastChild = false,
                                hasChildren = false
                            )
                        )
                        hasGeneratedAltStructures = true
                        inserted = true
                        entryLineInfo[childId] = childLineId to childLineIndex
                        entriesByParent.getOrPut(currentParent) { mutableListOf() }.add(childId)
                        headingLineToToc[childLineIndex] = childId
                    }
                } else if (hasOwnRefs) {
                    val tocId = addEntry(node, level, parentId, position)
                    if (tocId != 0L) {
                        entriesByParent.getOrPut(parentId) { mutableListOf() }.add(tocId)
                        inserted = true
                        if (node.children.isNotEmpty()) {
                            currentParent = tocId
                        }
                    }
                }

                var childInserted = false
                if (node.children.isNotEmpty()) {
                    val childLevel = level + if (currentParent != null && currentParent != parentId) 1 else 0
                    node.children.forEachIndexed { idx, child ->
                        if (traverseAltNode(child, childLevel, currentParent, idx)) {
                            childInserted = true
                        }
                    }
                    if (currentParent == containerId && childInserted) {
                        repository.updateAltTocEntryHasChildren(containerId!!, true)
                    } else if (currentParent != null && currentParent != parentId && childInserted) {
                        repository.updateAltTocEntryHasChildren(currentParent, true)
                    }
                }

                if (containerId != null) {
                    val hasChildren = entriesByParent[containerId].orEmpty().isNotEmpty()
                    if (hasChildren) {
                        repository.updateAltTocEntryHasChildren(containerId, true)
                        updateParentLineIfMissing(containerId)
                        if (entryLineInfo[containerId]?.second != null) {
                            hasGeneratedAltStructures = true
                            inserted = true
                        }
                    } else {
                        repository.executeRawQuery("DELETE FROM alt_toc_entry WHERE id=$containerId")
                        entriesByParent[parentId]?.remove(containerId)
                        entryLineInfo.remove(containerId)
                    }
                }

                return inserted || childInserted
            }

            structure.nodes.forEachIndexed { idx, node ->
                traverseAltNode(node, level = 0, parentId = null, position = idx)
            }

            for ((_, children) in entriesByParent) {
                if (children.isNotEmpty()) {
                    val lastChildId = children.last()
                    repository.updateAltTocEntryIsLastChild(lastChildId, true)
                }
            }

            val sortedKeys = headingLineToToc.keys.sorted()
            for (lineIdx in 0 until totalLines) {
                val key = sortedKeys.lastOrNull { it <= lineIdx } ?: continue
                val tocId = headingLineToToc[key] ?: continue
                val lineId = lineKeyToId[bookPath to lineIdx] ?: continue
                repository.upsertLineAltToc(lineId, structureId, tocId)
            }
        }
        return hasGeneratedAltStructures
    }
}
