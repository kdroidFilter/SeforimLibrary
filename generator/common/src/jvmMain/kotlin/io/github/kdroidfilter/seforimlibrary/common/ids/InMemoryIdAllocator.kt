package io.github.kdroidfilter.seforimlibrary.common.ids

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.AltTocEntryKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.AltTocStructureKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookAlias
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateReader
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSchema
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateWriter
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.buildstate.LineKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.LinkKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.TocEntryKey
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * RAM-resident [IdAllocator] backed by a [BuildStateSnapshot].
 *
 * - Look-ups go through a [ConcurrentHashMap] per key shape: O(1), no contention.
 * - Fresh allocations come from a per-table [AtomicLong] counter.
 * - Counters start at `max(previous next_id, previous max(id) + 1, 1)` so we
 *   never collide with reused ids even if the previous snapshot was incomplete.
 *
 * See DELTA_UPDATE_PLAN.md §3.5.
 */
class InMemoryIdAllocator private constructor(
    previous: BuildStateSnapshot,
    private val logger: Logger,
) : IdAllocator {

    // Lookup tables — share a single map type because keys are all single strings.
    private val lookupMaps: Map<IdTable, ConcurrentHashMap<String, Long>> = IdTable.values()
        .filter { it.lookupKind != null }
        .associateWith { table ->
            ConcurrentHashMap<String, Long>().apply {
                previous.lookups[table]?.let { putAll(it) }
            }
        }

    private val books = ConcurrentHashMap<BookKey, Long>().apply { putAll(previous.books) }
    private val lines = ConcurrentHashMap<LineKey, Long>().apply { putAll(previous.lines) }
    private val tocEntries = ConcurrentHashMap<TocEntryKey, Long>().apply { putAll(previous.tocEntries) }
    private val altTocStructures =
        ConcurrentHashMap<AltTocStructureKey, Long>().apply { putAll(previous.altTocStructures) }
    private val altTocEntries = ConcurrentHashMap<AltTocEntryKey, Long>().apply { putAll(previous.altTocEntries) }
    private val links = ConcurrentHashMap<LinkKey, Long>().apply { putAll(previous.links) }

    private val bookAliases = ConcurrentHashMap<BookKey, BookAlias>().apply {
        previous.bookAliases.forEach { put(it.oldKey, it) }
    }

    // Per-table counters: max(snapshot.next_id, max(known ids)+1, 1).
    private val counters: Map<IdTable, AtomicLong> = run {
        val maxByTable = HashMap<IdTable, Long>()
        lookupMaps.forEach { (table, map) ->
            maxByTable[table] = map.values.maxOrNull() ?: 0L
        }
        maxByTable[IdTable.BOOK] = books.values.maxOrNull() ?: 0L
        maxByTable[IdTable.LINE] = lines.values.maxOrNull() ?: 0L
        maxByTable[IdTable.TOC_ENTRY] = tocEntries.values.maxOrNull() ?: 0L
        maxByTable[IdTable.ALT_TOC_STRUCTURE] = altTocStructures.values.maxOrNull() ?: 0L
        maxByTable[IdTable.ALT_TOC_ENTRY] = altTocEntries.values.maxOrNull() ?: 0L
        maxByTable[IdTable.LINK] = links.values.maxOrNull() ?: 0L

        IdTable.values().associateWith { table ->
            val fromSnapshot = previous.counters[table] ?: 1L
            val fromMax = (maxByTable[table] ?: 0L) + 1L
            AtomicLong(maxOf(fromSnapshot, fromMax, 1L))
        }
    }

    private val reusedCount: Map<IdTable, AtomicLong> =
        IdTable.values().associateWith { AtomicLong(0) }
    private val freshCount: Map<IdTable, AtomicLong> =
        IdTable.values().associateWith { AtomicLong(0) }

    private val previousMeta: Map<String, String> = previous.meta

    // ─── Lookup-table accessors ────────────────────────────────────────────────

    private fun allocateLookup(table: IdTable, key: String): Long {
        val map = lookupMaps.getValue(table)
        map[key]?.let {
            reusedCount.getValue(table).incrementAndGet()
            return it
        }
        // computeIfAbsent guarantees the counter is only bumped once per fresh key.
        return map.computeIfAbsent(key) {
            freshCount.getValue(table).incrementAndGet()
            counters.getValue(table).getAndIncrement()
        }
    }

    override fun sourceId(name: String): Long = allocateLookup(IdTable.SOURCE, name)
    override fun authorId(name: String): Long = allocateLookup(IdTable.AUTHOR, name)
    override fun topicId(name: String): Long = allocateLookup(IdTable.TOPIC, name)
    override fun pubPlaceId(name: String): Long = allocateLookup(IdTable.PUB_PLACE, name)
    override fun pubDateId(date: String): Long = allocateLookup(IdTable.PUB_DATE, date)
    override fun connectionTypeId(name: String): Long = allocateLookup(IdTable.CONNECTION_TYPE, name)
    override fun categoryId(canonicalPath: String): Long = allocateLookup(IdTable.CATEGORY, canonicalPath)
    override fun tocTextId(text: String): Long = allocateLookup(IdTable.TOC_TEXT, text)

    // ─── Composite-keyed accessors ─────────────────────────────────────────────

    override fun bookId(sourceName: String, canonicalHeTitle: String): Long {
        val key = BookKey(sourceName, canonicalHeTitle)
        // Honour aliases so renamed books keep their original id.
        bookAliases[key]?.let { alias ->
            books[alias.newKey]?.let { return it }
        }
        books[key]?.let {
            reusedCount.getValue(IdTable.BOOK).incrementAndGet()
            return it
        }
        return books.computeIfAbsent(key) {
            freshCount.getValue(IdTable.BOOK).incrementAndGet()
            counters.getValue(IdTable.BOOK).getAndIncrement()
        }
    }

    override fun lineId(bookId: Long, contentHash: ByteArray, occurrenceIdx: Int): Long {
        require(contentHash.size == 20) { "contentHash must be 20-byte sha1, got ${contentHash.size}" }
        val key = LineKey(bookId, contentHash, occurrenceIdx)
        lines[key]?.let {
            reusedCount.getValue(IdTable.LINE).incrementAndGet()
            return it
        }
        return lines.computeIfAbsent(key) {
            freshCount.getValue(IdTable.LINE).incrementAndGet()
            counters.getValue(IdTable.LINE).getAndIncrement()
        }
    }

    override fun tocEntryId(bookId: Long, ancestorPath: String): Long {
        val key = TocEntryKey(bookId, ancestorPath)
        tocEntries[key]?.let {
            reusedCount.getValue(IdTable.TOC_ENTRY).incrementAndGet()
            return it
        }
        return tocEntries.computeIfAbsent(key) {
            freshCount.getValue(IdTable.TOC_ENTRY).incrementAndGet()
            counters.getValue(IdTable.TOC_ENTRY).getAndIncrement()
        }
    }

    override fun altTocStructureId(bookId: Long, key: String): Long {
        val k = AltTocStructureKey(bookId, key)
        altTocStructures[k]?.let {
            reusedCount.getValue(IdTable.ALT_TOC_STRUCTURE).incrementAndGet()
            return it
        }
        return altTocStructures.computeIfAbsent(k) {
            freshCount.getValue(IdTable.ALT_TOC_STRUCTURE).incrementAndGet()
            counters.getValue(IdTable.ALT_TOC_STRUCTURE).getAndIncrement()
        }
    }

    override fun altTocEntryId(structureId: Long, ancestorPath: String): Long {
        val key = AltTocEntryKey(structureId, ancestorPath)
        altTocEntries[key]?.let {
            reusedCount.getValue(IdTable.ALT_TOC_ENTRY).incrementAndGet()
            return it
        }
        return altTocEntries.computeIfAbsent(key) {
            freshCount.getValue(IdTable.ALT_TOC_ENTRY).incrementAndGet()
            counters.getValue(IdTable.ALT_TOC_ENTRY).getAndIncrement()
        }
    }

    override fun linkId(srcLineId: Long, tgtLineId: Long, connectionTypeId: Long): Long {
        val key = LinkKey(srcLineId, tgtLineId, connectionTypeId)
        links[key]?.let {
            reusedCount.getValue(IdTable.LINK).incrementAndGet()
            return it
        }
        return links.computeIfAbsent(key) {
            freshCount.getValue(IdTable.LINK).incrementAndGet()
            counters.getValue(IdTable.LINK).getAndIncrement()
        }
    }

    override fun peekBookId(sourceName: String, canonicalHeTitle: String): Long? =
        books[BookKey(sourceName, canonicalHeTitle)]

    override fun registerBookAlias(oldKey: BookKey, newKey: BookKey, atVersion: Int) {
        val existing = books[oldKey] ?: return
        // Move the id under the new natural key so subsequent lookups hit.
        books.putIfAbsent(newKey, existing)
        bookAliases[oldKey] = BookAlias(oldKey, newKey, atVersion)
        logger.i { "Registered book alias: $oldKey -> $newKey (id=$existing, version=$atVersion)" }
    }

    override fun stats(): AllocatorStats {
        val perTable = IdTable.values().associateWith { table ->
            val reused = reusedCount.getValue(table).get()
            val fresh = freshCount.getValue(table).get()
            AllocatorStats.TableStats(total = reused + fresh, reused = reused, freshlyAllocated = fresh)
        }
        return AllocatorStats(perTable)
    }

    override fun snapshotTo(target: Path, extraMeta: Map<String, String>) {
        val snapshot = BuildStateSnapshot(
            schemaVersion = BuildStateSchema.CURRENT_VERSION,
            meta = previousMeta + extraMeta,
            counters = counters.mapValues { it.value.get() },
            lookups = lookupMaps.mapValues { it.value.toMap() },
            books = books.toMap(),
            lines = lines.toMap(),
            tocEntries = tocEntries.toMap(),
            altTocStructures = altTocStructures.toMap(),
            altTocEntries = altTocEntries.toMap(),
            links = links.toMap(),
            bookAliases = bookAliases.values.toList(),
        )
        BuildStateWriter(logger).write(snapshot, target)
        val stats = stats()
        logger.i {
            "IdAllocator snapshot: " + stats.perTable
                .filterValues { it.total > 0 }
                .entries
                .joinToString { (t, s) -> "${t.tableName}(reused=${s.reused}, fresh=${s.freshlyAllocated})" }
        }
    }

    companion object {
        /** Loads a previous build_state from [path] (empty if missing) and returns an allocator. */
        fun load(path: Path?, logger: Logger = Logger.withTag("IdAllocator")): InMemoryIdAllocator {
            val previous = if (path == null) {
                BuildStateSnapshot.empty()
            } else {
                BuildStateReader(logger).read(path)
            }
            return InMemoryIdAllocator(previous, logger)
        }

        /** Test helper: build an allocator from an in-memory snapshot. */
        fun fromSnapshot(
            snapshot: BuildStateSnapshot,
            logger: Logger = Logger.withTag("IdAllocator"),
        ): InMemoryIdAllocator = InMemoryIdAllocator(snapshot, logger)
    }
}
