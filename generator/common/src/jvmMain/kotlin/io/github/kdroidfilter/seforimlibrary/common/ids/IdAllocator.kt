package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import io.github.kdroidfilter.seforimlibrary.common.buildstate.AltTocEntryKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.AltTocStructureKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.buildstate.LineKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.LinkKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.TocEntryKey
import java.nio.file.Path

/**
 * Hands out stable primary-key values for every entity inserted into seforim.db.
 *
 * Contract: for a given natural key, [bookId] / [lineId] / etc. MUST return the
 * same value across builds. New natural keys get a fresh id from a per-table
 * monotonic counter. Implementations are thread-safe.
 *
 * See DELTA_UPDATE_PLAN.md §3.3 and §3.5.
 */
interface IdAllocator {

    // ─── Lookup tables (single string natural key) ─────────────────────────────
    fun sourceId(name: String): Long
    fun authorId(name: String): Long
    fun topicId(name: String): Long
    fun pubPlaceId(name: String): Long
    fun pubDateId(date: String): Long
    fun connectionTypeId(name: String): Long
    fun categoryId(canonicalPath: String): Long
    fun tocTextId(text: String): Long

    // ─── Composite-keyed tables ────────────────────────────────────────────────
    fun bookId(sourceName: String, canonicalHeTitle: String): Long
    fun lineId(bookId: Long, contentHash: ByteArray, occurrenceIdx: Int): Long
    fun tocEntryId(bookId: Long, ancestorPath: String): Long
    fun altTocStructureId(bookId: Long, key: String): Long
    fun altTocEntryId(structureId: Long, ancestorPath: String): Long
    fun linkId(srcLineId: Long, tgtLineId: Long, connectionTypeId: Long): Long

    /** Returns the previously-allocated id for the given key, or `null` if unknown. */
    fun peekBookId(sourceName: String, canonicalHeTitle: String): Long?

    /**
     * Records a book rename detected at the current build version.
     * Following calls to [bookId] with [newKey] will return the id originally
     * assigned to [oldKey].
     */
    fun registerBookAlias(oldKey: BookKey, newKey: BookKey, atVersion: Int)

    /**
     * Records the canonical sha256 source hash for a book observed at the
     * current build. Persisted in build_state.db and consumed by
     * `TouchedBookDetector` on the next run.
     */
    fun recordSourceHash(key: BookKey, sourceHash: BookSourceHash)

    /** Returns the source hash recorded in a previous build for [key], if any. */
    fun previousSourceHash(key: BookKey): BookSourceHash?

    /** Stats for logging / metrics. */
    fun stats(): AllocatorStats

    /** Persist current state to disk as a [BuildStateSnapshot]. */
    fun snapshotTo(target: Path, extraMeta: Map<String, String> = emptyMap())
}

data class AllocatorStats(
    val perTable: Map<IdTable, TableStats>,
) {
    data class TableStats(val total: Long, val reused: Long, val freshlyAllocated: Long)
}
