package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash

/**
 * Classifies every book of a build into one of four buckets by comparing
 * the current source hashes (produced by a [SourceHashComputer]) against the
 * hashes recorded by the previous build (held in `build_state.db`).
 *
 * Output drives:
 *  - the fast-path "unchanged book" optimisation (P2.5 follow-up),
 *  - the rename detector (`BookRenameDetector`) which only runs on the
 *    `added × removed` cross-product,
 *  - the delta producer's per-book scope (Phase 4).
 *
 * See `DELTA_UPDATE_PLAN.md` §6.3.
 */
object TouchedBookDetector {

    data class Classification(
        val unchanged: Set<BookKey>,
        val touched: Set<BookKey>,
        val added: Set<BookKey>,
        val removed: Set<BookKey>,
    ) {
        val totalCurrent: Int get() = unchanged.size + touched.size + added.size
        val totalPrevious: Int get() = unchanged.size + touched.size + removed.size

        fun summary(): String =
            "unchanged=${unchanged.size}, touched=${touched.size}, " +
                "added=${added.size}, removed=${removed.size} " +
                "(prev=${totalPrevious}, current=${totalCurrent})"
    }

    fun classify(
        previous: Map<BookKey, BookSourceHash>,
        current: Map<BookKey, BookSourceHash>,
    ): Classification {
        val unchanged = HashSet<BookKey>()
        val touched = HashSet<BookKey>()
        val added = HashSet<BookKey>()
        val removed = HashSet<BookKey>()

        for ((key, currentHash) in current) {
            val prev = previous[key]
            when {
                prev == null -> added += key
                prev.hash.contentEquals(currentHash.hash) -> unchanged += key
                else -> touched += key
            }
        }
        for (key in previous.keys) {
            if (key !in current) removed += key
        }

        return Classification(unchanged, touched, added, removed)
    }
}
