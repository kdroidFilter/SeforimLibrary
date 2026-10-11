package io.github.kdroidfilter.seforimlibrary.common.ids

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.AltTocEntryKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.AltTocStructureKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookAlias
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
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

    // Snapshot of source hashes recorded by previous builds; immutable in this run.
    private val previousSourceHashes: Map<BookKey, BookSourceHash> = previous.sourceHashes
    // Source hashes recorded during the current run; merged with `previousSourceHashes`
    // for unaffected keys when [snapshotTo] is called.
    private val currentSourceHashes = ConcurrentHashMap<BookKey, BookSourceHash>()

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

    /**
     * Raises [table]'s next-id counter to at least [minNext]. Required before allocating
     * fresh ids into a DB that already holds rows ABOVE the persisted counter — Havrouta
     * links are deleted+recreated each build with implicit rowids the allocator never
     * sees, so a later allocator-based inserter (Phase-2 LINKER) would collide with them.
     */
    fun ensureCounterAtLeast(table: IdTable, minNext: Long) {
        counters.getValue(table).updateAndGet { maxOf(it, minNext) }
    }

    private val reusedCount: Map<IdTable, AtomicLong> =
        IdTable.values().associateWith { AtomicLong(0) }
    private val freshCount: Map<IdTable, AtomicLong> =
        IdTable.values().associateWith { AtomicLong(0) }

    private val previousMeta: Map<String, String> = previous.meta

    /** Line keys the seed build_state carried in — 0 means there was nothing to migrate. */
    private val seedLineCount: Int = previous.lines.size

    // The shim reads the seed only, never the live `lines` map: a line processed
    // earlier this build may already have moved or claimed the id it would find.
    private val seedLines: Map<LineKey, Long> = previous.lines

    // id -> key issued this build. One id per line is the invariant this guards.
    private val issuedLineIds = ConcurrentHashMap<Long, LineKey>()

    // Transition shim counters (see LegacyLineKey); reported by snapshotTo.
    private val legacyLineKeysMigrated = AtomicLong(0)
    private val legacyLineKeyLookups = AtomicLong(0)
    private val legacyLineKeyCollisions = AtomicLong(0)

    /** How many line ids were carried over from a pre-#1211 build_state. */
    fun legacyLineKeysMigrated(): Long = legacyLineKeysMigrated.get()

    /** Lines whose legacy key differed from the new one, so the shim was consulted. */
    fun legacyLineKeyLookups(): Long = legacyLineKeyLookups.get()

    /** Lines the shim was consulted for and could not resolve — they got a fresh id. */
    fun legacyLineKeysMissed(): Long = legacyLineKeyLookups.get() - legacyLineKeysMigrated.get()

    /** Legacy hits whose seed id another line had already taken this build — given a fresh id. */
    fun legacyLineKeyCollisions(): Long = legacyLineKeyCollisions.get()

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

    override fun reallocateCategoryId(canonicalPath: String): Long {
        val stale = lookupMaps.getValue(IdTable.CATEGORY).remove(canonicalPath)
        val fresh = allocateLookup(IdTable.CATEGORY, canonicalPath)
        logger.w { "Category id $stale for '$canonicalPath' is occupied by a foreign row; reallocated to $fresh" }
        return fresh
    }

    override fun peekCategoryId(canonicalPath: String): Long? =
        lookupMaps.getValue(IdTable.CATEGORY)[canonicalPath]

    override fun categoryKeys(): Map<String, Long> = lookupMaps.getValue(IdTable.CATEGORY).toMap()

    override fun pointCategoryKey(canonicalPath: String, id: Long): Boolean {
        require(id > 0) { "Category id must be positive, got $id for '$canonicalPath'" }
        val stale = lookupMaps.getValue(IdTable.CATEGORY).put(canonicalPath, id)
        // The id came from the build state, so the counter is normally past it already.
        counters.getValue(IdTable.CATEGORY).updateAndGet { maxOf(it, id + 1) }
        logger.i { "Category key '$canonicalPath' now holds $id (was ${stale ?: "unset"})" }
        return true
    }

    // bookId is itself build-stable, so the encoded string key is stable too.
    override fun bookVersionId(bookId: Long, versionTitle: String): Long =
        allocateLookup(IdTable.BOOK_VERSION, "$bookId $versionTitle")

    // ─── Composite-keyed accessors ─────────────────────────────────────────────

    override fun bookId(sourceName: String, canonicalHeTitle: String): Long {
        val key = BookKey(sourceName, canonicalHeTitle)
        // Honour aliases so renamed books keep their original id.
        // Otzaria: through every later rename too, not just the first.
        currentKeyOf(key)?.let { current ->
            books[current]?.let { return it }
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

    override fun lineId(bookId: Long, contentHash: ByteArray, occurrenceIdx: Int): Long =
        lineId(bookId, contentHash, occurrenceIdx, legacy = null)

    override fun lineId(
        bookId: Long,
        contentHash: ByteArray,
        occurrenceIdx: Int,
        legacy: LegacyLineKey?,
    ): Long {
        require(contentHash.size == 20) { "contentHash must be 20-byte sha1, got ${contentHash.size}" }
        val key = LineKey(bookId, contentHash, occurrenceIdx)
        lines[key]?.let {
            reusedCount.getValue(IdTable.LINE).incrementAndGet()
            return claimLineId(key, it)
        }
        migrateLegacyLineKey(bookId, key, legacy)?.let {
            reusedCount.getValue(IdTable.LINE).incrementAndGet()
            return it
        }
        val fresh = lines.computeIfAbsent(key) {
            freshCount.getValue(IdTable.LINE).incrementAndGet()
            counters.getValue(IdTable.LINE).getAndIncrement()
        }
        return claimLineId(key, fresh)
    }

    /** Records that [id] belongs to [key] this build; a second key for the same id is a corrupt seed. */
    private fun claimLineId(key: LineKey, id: Long): Long {
        val holder = issuedLineIds.putIfAbsent(id, key) ?: return id
        check(holder == key) { "line id $id is filed under two keys: $holder and $key" }
        return id
    }

    /**
     * Transition shim (see [LegacyLineKey]): moves the id a pre-#1211 build
     * filed under the heRef-based key over to [newKey]. Tried for every line,
     * not only ref-bearing ones — a generated prefix moved into the old key too.
     */
    private fun migrateLegacyLineKey(bookId: Long, newKey: LineKey, legacy: LegacyLineKey?): Long? {
        if (legacy == null) return null
        val legacyKey = LineKey(bookId, legacy.contentHash, legacy.occurrenceIdx)
        if (legacyKey == newKey) return null
        legacyLineKeyLookups.incrementAndGet()
        val id = seedLines[legacyKey] ?: return null
        // The seed id may already be live under a different new key (same content, a
        // shifted occurrence index). Handing it out twice would drop a line on insert.
        val holder = issuedLineIds.putIfAbsent(id, newKey)
        if (holder != null && holder != newKey) {
            legacyLineKeyCollisions.incrementAndGet()
            return null
        }
        legacyLineKeysMigrated.incrementAndGet()
        lines.remove(legacyKey, id)
        val existing = lines.putIfAbsent(newKey, id)
        if (existing != null && existing != id) {
            // Raced with a direct hit on newKey: yield to it and release our claim.
            issuedLineIds.remove(id, newKey)
            return claimLineId(newKey, existing)
        }
        return id
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

    override fun bookIds(): Map<BookKey, Long> = books.toMap()

    override fun lineHashesByBook(bookIds: Set<Long>): Map<Long, List<ByteArray>> {
        val out = HashMap<Long, MutableList<ByteArray>>()
        lines.keys.forEach { key ->
            if (key.bookId in bookIds) out.getOrPut(key.bookId) { ArrayList() } += key.contentHash
        }
        return out
    }

    override fun registerBookAlias(oldKey: BookKey, newKey: BookKey, atVersion: Int) {
        val existing = books[oldKey] ?: return
        // Move the id under the new natural key so subsequent lookups hit.
        // Otzaria: move, not copy. id_book holds one key per id (UNIQUE), so a copy failed
        // the snapshot; the alias still resolves the old key.
        val held = books.putIfAbsent(newKey, existing)
        check(held == null || held == existing) { "Book alias $oldKey -> $newKey: $newKey already holds id $held" }
        books.remove(oldKey, existing)
        bookAliases[oldKey] = BookAlias(oldKey, newKey, atVersion)
        // Otzaria: newKey names a book again, so an alias it had from an older rename is stale.
        bookAliases.remove(newKey)
        logger.i { "Registered book alias: $oldKey -> $newKey (id=$existing, version=$atVersion)" }
    }

    override fun recordSourceHash(key: BookKey, sourceHash: BookSourceHash) {
        currentSourceHashes[key] = sourceHash
    }

    override fun previousSourceHash(key: BookKey): BookSourceHash? =
        previousSourceHashes[key]

    // Otzaria: an aliased key left [books] for its new key, and still names that book,
    // however many times it was renamed since.
    override fun knownBookKeys(): Map<BookKey, Long> =
        books.toMap() + bookAliases.keys.mapNotNull { old -> currentKeyOf(old)?.let { books[it] }?.let { old to it } }

    /** Otzaria: the key a renamed book has now, following each rename after [oldKey]'s. */
    private fun currentKeyOf(oldKey: BookKey): BookKey? {
        val seen = hashSetOf(oldKey)
        var key = bookAliases[oldKey]?.newKey ?: return null
        while (!books.containsKey(key)) {
            check(seen.add(key)) { "Book aliases loop: ${seen.joinToString(" -> ")} -> $key" }
            key = bookAliases[key]?.newKey ?: return null
        }
        return key
    }

    override fun lineContentHashes(bookIds: Set<Long>): Map<Long, List<ByteArray>> {
        if (bookIds.isEmpty()) return emptyMap()
        val out = HashMap<Long, MutableList<ByteArray>>()
        // The keys a book issued this run are all the lines it now has; its other
        // keys are earlier versions of it, which snapshotTo prunes.
        for (key in issuedLineIds.values) {
            if (key.bookId in bookIds) out.getOrPut(key.bookId) { ArrayList() }.add(key.contentHash)
        }
        val issuedBooks = out.keys.toHashSet()
        for (key in lines.keys) {
            if (key.bookId in bookIds && key.bookId !in issuedBooks) {
                out.getOrPut(key.bookId) { ArrayList() }.add(key.contentHash)
            }
        }
        return out
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
        // Merge previous source hashes with the ones recorded this run.
        // Current run wins (touched / added books emit fresh hashes); previous
        // hashes for unaffected keys are preserved so future builds still see them.
        val mergedSourceHashes = HashMap<BookKey, BookSourceHash>(previousSourceHashes)
        mergedSourceHashes.putAll(currentSourceHashes)

        // ─── Phase 8 orphan GC ─────────────────────────────────────────────
        // Drop any composite-keyed entries whose bookId / structureId is no
        // longer present in this build. Without this pass, removed books leak
        // their line/tocEntry/link/altToc entries into build_state forever.
        // Source hashes for removed books are pruned along with them — a
        // future re-add would be classified as `added`, not `unchanged`.
        val liveBookIds = books.values.toHashSet()
        val gcLines = lines.entries.removeIfMatches { it.key.bookId !in liveBookIds }
        // A book processed this build allocated every line it has; any other key of that
        // book is a leftover (a legacy key whose id went elsewhere) and must not be reseeded.
        val touchedBookIds = issuedLineIds.values.mapTo(HashSet()) { it.bookId }
        val gcStaleLineKeys = lines.entries.removeIfMatches {
            it.key.bookId in touchedBookIds && issuedLineIds[it.value] != it.key
        }
        val gcTocs = tocEntries.entries.removeIfMatches { it.key.bookId !in liveBookIds }
        val gcAltStructs = altTocStructures.entries.removeIfMatches { it.key.bookId !in liveBookIds }
        val liveStructureIds = altTocStructures.values.toHashSet()
        val gcAltEntries = altTocEntries.entries.removeIfMatches { it.key.structureId !in liveStructureIds }
        val liveLineIds = lines.values.toHashSet()
        val gcLinks = links.entries.removeIfMatches { k ->
            k.key.srcLineId !in liveLineIds || k.key.tgtLineId !in liveLineIds
        }
        val liveBookKeys = books.keys.toHashSet()
        val gcSourceHashes = mergedSourceHashes.entries.removeIfMatches { it.key !in liveBookKeys }
        if (gcLines + gcStaleLineKeys + gcTocs + gcAltStructs + gcAltEntries + gcLinks + gcSourceHashes > 0) {
            logger.i {
                "Phase-8 GC pruned orphan entries: " +
                    "lines=$gcLines, staleLineKeys=$gcStaleLineKeys, tocEntries=$gcTocs, " +
                    "altStructures=$gcAltStructs, altEntries=$gcAltEntries, " +
                    "links=$gcLinks, sourceHashes=$gcSourceHashes"
            }
        }

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
            sourceHashes = mergedSourceHashes,
        )
        logLegacyLineKeyTransition()
        BuildStateWriter(logger).write(snapshot, target)
        val stats = stats()
        logger.i {
            "IdAllocator snapshot: " + stats.perTable
                .filterValues { it.total > 0 }
                .entries
                .joinToString { (t, s) -> "${t.tableName}(reused=${s.reused}, fresh=${s.freshlyAllocated})" }
        }
    }

    /**
     * Build-summary line for the #1211 key change: a transition build that did
     * NOT carry its ids over is the failure mode worth seeing, so it warns.
     */
    private fun logLegacyLineKeyTransition() {
        val lookups = legacyLineKeyLookups.get()
        if (seedLineCount == 0 || lookups == 0L) return
        val migrated = legacyLineKeysMigrated.get()
        val missed = lookups - migrated
        logger.i {
            "Legacy line-key transition: seed held $seedLineCount line keys; " +
                "$lookups lines re-keyed, $migrated migrated, $missed given fresh ids " +
                "(${legacyLineKeyCollisions.get()} of them because the seed id was already taken)"
        }
        if (missed > lookups * LEGACY_MISS_WARN_FRACTION) {
            logger.w {
                "Legacy line-key transition looks unclean: $missed of $lookups re-keyed lines " +
                    "(${"%.1f".format(missed * 100.0 / lookups)}%) got a fresh id instead of the seed's — " +
                    "expect a large delta for this release"
            }
        }
    }

    /**
     * Removes entries matching [predicate] from this mutable entry set and
     * returns how many it removed. Avoids the explicit double-iteration
     * (collect candidates, then remove) that the call sites would otherwise
     * need for each map.
     */
    private inline fun <K, V> MutableSet<MutableMap.MutableEntry<K, V>>.removeIfMatches(
        crossinline predicate: (MutableMap.MutableEntry<K, V>) -> Boolean,
    ): Int {
        var count = 0
        val it = iterator()
        while (it.hasNext()) {
            val e = it.next()
            if (predicate(e)) { it.remove(); count++ }
        }
        return count
    }

    companion object {
        /** Above this share of re-keyed lines missing from the seed, the transition warns. */
        private const val LEGACY_MISS_WARN_FRACTION = 0.01

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
