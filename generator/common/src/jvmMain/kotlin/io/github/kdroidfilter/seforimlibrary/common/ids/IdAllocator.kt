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

    /**
     * Forgets the id cached for [canonicalPath] and hands out a fresh one.
     *
     * Only for the case where the reserved id turns out to be already occupied in
     * the DB by a row this allocator never created — categories inserted outside the
     * allocator. `renameCategories`' leaf auto-creation for book_moves.csv used to be
     * one (implicit rowid, v19 and v30); it now takes its ids from the build state.
     * The default implementation cannot reallocate and keeps the current id.
     */
    fun reallocateCategoryId(canonicalPath: String): Long = categoryId(canonicalPath)

    /** The id [canonicalPath] already holds, without allocating one; null when it has none. */
    fun peekCategoryId(canonicalPath: String): Long? = null

    /** Every category key and the id it holds, as of this call. */
    fun categoryKeys(): Map<String, Long> = emptyMap()

    /**
     * Points [canonicalPath] at [id], an id that is already allocated to the same folder
     * under another key (a book_moves.csv leaf, see [BOOK_MOVE_LEAF_KEY_PREFIX]). The id
     * the key held before is left unheld. Returns false if this allocator cannot do it.
     */
    fun pointCategoryKey(canonicalPath: String, id: Long): Boolean = false

    /** Stable id for a book edition; natural key = (bookId, versionTitle). */
    fun bookVersionId(bookId: Long, versionTitle: String): Long

    // ─── Composite-keyed tables ────────────────────────────────────────────────
    fun bookId(sourceName: String, canonicalHeTitle: String): Long
    fun lineId(bookId: Long, contentHash: ByteArray, occurrenceIdx: Int): Long

    /**
     * As [lineId], but reuses the id stored under [legacy] when the new-scheme
     * key is unknown. Transition shim — see [LegacyLineKey].
     */
    fun lineId(bookId: Long, contentHash: ByteArray, occurrenceIdx: Int, legacy: LegacyLineKey?): Long =
        lineId(bookId, contentHash, occurrenceIdx)
    fun tocEntryId(bookId: Long, ancestorPath: String): Long
    fun altTocStructureId(bookId: Long, key: String): Long
    fun altTocEntryId(structureId: Long, ancestorPath: String): Long
    fun linkId(srcLineId: Long, tgtLineId: Long, connectionTypeId: Long): Long

    /** Returns the previously-allocated id for the given key, or `null` if unknown. */
    fun peekBookId(sourceName: String, canonicalHeTitle: String): Long?

    /** Copy of every known book key and its id, aliased keys included. */
    fun bookIds(): Map<BookKey, Long>

    /** Line content hashes (20-byte sha1) known for each of [bookIds]. */
    fun lineHashesByBook(bookIds: Set<Long>): Map<Long, List<ByteArray>>

    /**
     * Records a book rename detected at the current build version.
     * Following calls to [bookId] with [newKey] will return the id originally
     * assigned to [oldKey].
     */
    fun registerBookAlias(oldKey: BookKey, newKey: BookKey, atVersion: Int)

    /**
     * Otzaria: drops [key], held by a book no longer there, so that a renamed book can take
     * it. The id is not handed out again, and the aliases that led to it go with it.
     *
     * @return the retired id.
     */
    fun retireBookKey(key: BookKey): Long

    /**
     * Records the canonical sha256 source hash for a book observed at the
     * current build. Persisted in build_state.db and consumed by
     * `TouchedBookDetector` on the next run.
     */
    fun recordSourceHash(key: BookKey, sourceHash: BookSourceHash)

    /** Returns the source hash recorded in a previous build for [key], if any. */
    fun previousSourceHash(key: BookKey): BookSourceHash?

    /**
     * Every book key this allocator knows, from the build state and from this run,
     * with its id. Keys are never dropped, so a renamed book's old title stays here:
     * under the book's own id when the rename kept it (book_renames.csv), under a
     * dead id when the book came back as a new key. Read-only view.
     */
    fun knownBookKeys(): Map<BookKey, Long> = emptyMap()

    /**
     * The line content hashes held for each of [bookIds]. A book that allocated
     * lines in this run reports those only; any other book reports what the build
     * state kept for it, which for a removed book is its last content.
     */
    fun lineContentHashes(bookIds: Set<Long>): Map<Long, List<ByteArray>> = emptyMap()

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
