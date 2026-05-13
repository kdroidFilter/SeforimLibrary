package io.github.kdroidfilter.seforimlibrary.common.changes

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot

/**
 * Detects "this added book is really the renamed version of that removed
 * book" pairings by computing a Jaccard similarity over the two books'
 * line content hashes.
 *
 * Inputs are abstract on purpose: callers supply (a) the set of removed
 * books with the line-hash set each had in the previous build, and (b) the
 * added books with the line-hash set they have in the current sources.
 * That lets the detector work without coupling to a particular DB layout.
 *
 * Algorithm (per `DELTA_UPDATE_PLAN.md` §4.5 / §6.3):
 *
 *  1. For each removed × added pair, compute `|A ∩ B| / |A ∪ B|`.
 *  2. If the max for a removed book reaches `threshold` (default 0.8),
 *     record the rename and remove both keys from further consideration.
 *  3. Returns the resulting list of [Alias] records.
 *
 * Cost is O(|removed| × |added|) Jaccard computations, each O(min(|A|,|B|))
 * thanks to hash-based set intersection. Acceptable while the cross-product
 * stays in the low hundreds (which is the realistic case: at most a handful
 * of renames per release).
 */
class BookRenameDetector(
    private val threshold: Double = DEFAULT_THRESHOLD,
    private val logger: Logger = Logger.withTag("BookRenameDetector"),
) {

    /** Result of a successful rename match. */
    data class Alias(
        val oldKey: BookKey,
        val newKey: BookKey,
        val jaccard: Double,
    )

    /**
     * Set of line hashes for one book. The hash is whatever the importer
     * uses for line content (sha1 in Phase 1; can grow to normalised hash
     * in Phase 3 without changing this detector).
     *
     * Stored as a wrapper around a Set<ContentHash> with a structural
     * equals on the underlying bytes via [ContentHash].
     */
    class LineHashSet(val hashes: Set<ContentHash>) {
        val size: Int get() = hashes.size
        fun jaccard(other: LineHashSet): Double {
            if (hashes.isEmpty() && other.hashes.isEmpty()) return 0.0
            val small = if (hashes.size <= other.hashes.size) hashes else other.hashes
            val large = if (hashes.size <= other.hashes.size) other.hashes else hashes
            var inter = 0
            for (h in small) if (h in large) inter++
            val union = small.size + large.size - inter
            return if (union == 0) 0.0 else inter.toDouble() / union.toDouble()
        }
    }

    /** Structural byte-array wrapper usable as a HashMap/Set key. */
    class ContentHash(val bytes: ByteArray) {
        override fun equals(other: Any?): Boolean =
            other is ContentHash && bytes.contentEquals(other.bytes)
        override fun hashCode(): Int = bytes.contentHashCode()
    }

    /**
     * @param removed books removed since the previous build, paired with their
     *   line-hash set (read from the previous DB or build_state).
     * @param added books added in the current build, paired with their
     *   line-hash set (computed from the current sources).
     * @param atVersion the build version that observes this rename; persisted
     *   in `book_aliases.detected_at_version`.
     */
    fun detect(
        removed: Map<BookKey, LineHashSet>,
        added: Map<BookKey, LineHashSet>,
        atVersion: Int,
    ): List<Alias> {
        if (removed.isEmpty() || added.isEmpty()) return emptyList()

        val mutableAdded = HashMap(added)
        val aliases = ArrayList<Alias>()

        for ((oldKey, oldHashes) in removed) {
            if (mutableAdded.isEmpty()) break
            if (oldHashes.size == 0) continue
            var bestKey: BookKey? = null
            var bestScore = 0.0
            for ((newKey, newHashes) in mutableAdded) {
                val score = oldHashes.jaccard(newHashes)
                if (score > bestScore) {
                    bestScore = score
                    bestKey = newKey
                }
            }
            if (bestKey != null && bestScore >= threshold) {
                aliases += Alias(oldKey, bestKey, bestScore)
                mutableAdded.remove(bestKey)
                logger.i { "Rename detected: $oldKey → $bestKey (jaccard=$bestScore)" }
            }
        }
        return aliases
    }

    /**
     * Convenience overload: reads `removed` line hashes from a previous
     * [BuildStateSnapshot] (via its `lines` map keyed by `bookId` lookup),
     * using the snapshot's `books` map to resolve `BookKey → bookId`.
     */
    fun detectFromSnapshot(
        previous: BuildStateSnapshot,
        addedSources: Map<BookKey, LineHashSet>,
        removedKeys: Set<BookKey>,
        atVersion: Int,
    ): List<Alias> {
        val removed = removedKeys.associateWith { key ->
            val bookId = previous.books[key] ?: return@associateWith LineHashSet(emptySet())
            val hashes = previous.lines.entries.asSequence()
                .filter { it.key.bookId == bookId }
                .map { ContentHash(it.key.contentHash) }
                .toCollection(HashSet())
            LineHashSet(hashes)
        }
        return detect(removed, addedSources, atVersion)
    }

    companion object {
        const val DEFAULT_THRESHOLD: Double = 0.8
    }
}
