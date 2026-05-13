package io.github.kdroidfilter.seforimlibrary.common.changes

/**
 * Aligns two sequences of lines and emits KEEP / INSERT / DELETE / MODIFY ops.
 *
 * Implements a hand-rolled Patience Diff (DELTA_UPDATE_PLAN.md §3.6):
 *
 *  1. Compute the **normalised hash** of every line on both sides — we
 *     intentionally key on a normalised form (HTML stripped, diacritics
 *     removed, sofit normalised) so that two lines whose only differences
 *     are cosmetic still align (see [LineNormalizer]).
 *  2. Find the set of hashes that appear **exactly once on each side** —
 *     these are the unique anchors.
 *  3. Match anchors by hash, then find the longest *strictly-increasing*
 *     subsequence of `(oldIdx, newIdx)` pairs by `newIdx` — this is the
 *     Patience LIS step. The matched anchors give us KEEP ops; everything
 *     between them is recursed upon.
 *  4. Inside each non-anchor segment, fall back to a greedy LCS that uses
 *     the normalised hash for equality. Any leftover lines on the old side
 *     become DELETE; on the new side they become INSERT.
 *  5. Post-pass: adjacent DELETE / INSERT pairs whose token similarity
 *     reaches `mergeSimilarityThreshold` (default 0.6) are collapsed into a
 *     single MODIFY — preserving the old line's id while letting the
 *     content evolve.
 *
 * The matcher is pure and stateless; it does NOT touch the database. Wiring
 * into the importer (only on **touched** books) is the caller's job.
 */
class LineMatcher(
    private val mergeSimilarityThreshold: Double = DEFAULT_MERGE_THRESHOLD,
    private val normalise: (String) -> String = LineNormalizer::normalize,
) {

    /** A line as observed in the previous build, indexed by its lineIndex. */
    data class OldLine(val lineIndex: Int, val lineId: Long, val content: String)

    /** A line as observed in the new build, indexed by its lineIndex. */
    data class NewLine(val lineIndex: Int, val content: String)

    sealed interface Op {
        /** Same content position in both sides; old line id is reusable. */
        data class Keep(val oldIndex: Int, val newIndex: Int, val lineId: Long) : Op

        /** Line existed before and still exists, content (possibly) edited. */
        data class Modify(
            val oldIndex: Int,
            val newIndex: Int,
            val lineId: Long,
            val newContent: String,
            val similarity: Double,
        ) : Op

        /** Brand-new line that had no counterpart in the previous build. */
        data class Insert(val newIndex: Int, val content: String) : Op

        /** Line existed before and is gone in the new build. */
        data class Delete(val oldIndex: Int, val lineId: Long) : Op
    }

    fun match(old: List<OldLine>, new: List<NewLine>): List<Op> {
        // Hash inputs once.
        val oldHashes = IntArray(old.size) { normalise(old[it].content).hashCode() }
        val newHashes = IntArray(new.size) { normalise(new[it].content).hashCode() }

        val raw = patienceDiff(oldHashes, newHashes)
        return mergeAdjacentEditsIntoModify(raw, old, new)
    }

    // ─── Patience anchors + greedy fill ────────────────────────────────────────

    private fun patienceDiff(oldHashes: IntArray, newHashes: IntArray): List<Op> {
        return diffSegment(oldHashes, newHashes, 0, oldHashes.size, 0, newHashes.size)
    }

    private fun diffSegment(
        oldHashes: IntArray, newHashes: IntArray,
        oFrom: Int, oTo: Int, nFrom: Int, nTo: Int,
    ): List<Op> {
        if (oFrom >= oTo && nFrom >= nTo) return emptyList()
        if (oFrom >= oTo) return (nFrom until nTo).map { Op.Insert(it, "") }
        if (nFrom >= nTo) return (oFrom until oTo).map { Op.Delete(it, 0L) }

        // 1. Find unique-on-both-sides hashes within this window.
        val oldCounts = HashMap<Int, Int>()
        val newCounts = HashMap<Int, Int>()
        for (i in oFrom until oTo) oldCounts.merge(oldHashes[i], 1) { a, _ -> a + 1 }
        for (i in nFrom until nTo) newCounts.merge(newHashes[i], 1) { a, _ -> a + 1 }
        val uniqueBoth = HashSet<Int>().also { set ->
            oldCounts.forEach { (k, v) -> if (v == 1 && newCounts[k] == 1) set += k }
        }

        if (uniqueBoth.isEmpty()) {
            // No anchors → fall back to a simple LCS-ish greedy pass.
            return greedyAlign(oldHashes, newHashes, oFrom, oTo, nFrom, nTo)
        }

        // 2. Collect anchor pairs (oldIdx, newIdx) for unique hashes.
        val anchors = ArrayList<IntArray>()
        val newPosByHash = HashMap<Int, Int>()
        for (i in nFrom until nTo) if (newHashes[i] in uniqueBoth) newPosByHash[newHashes[i]] = i
        for (i in oFrom until oTo) if (oldHashes[i] in uniqueBoth) {
            val j = newPosByHash[oldHashes[i]] ?: continue
            anchors += intArrayOf(i, j)
        }
        // 3. Patience LIS on anchors by newIdx.
        val lis = longestIncreasingSubsequence(anchors)

        // 4. Walk anchors and recurse on the gaps.
        val result = ArrayList<Op>()
        var oPos = oFrom
        var nPos = nFrom
        for (a in lis) {
            val (oi, ni) = a[0] to a[1]
            result += diffSegment(oldHashes, newHashes, oPos, oi, nPos, ni)
            result += Op.Keep(oi, ni, 0L)
            oPos = oi + 1
            nPos = ni + 1
        }
        result += diffSegment(oldHashes, newHashes, oPos, oTo, nPos, nTo)
        return result
    }

    private fun longestIncreasingSubsequence(anchors: List<IntArray>): List<IntArray> {
        if (anchors.isEmpty()) return emptyList()
        val tails = ArrayList<IntArray>()
        val prev = IntArray(anchors.size) { -1 }
        val tailIdxs = IntArray(anchors.size)
        for ((i, a) in anchors.withIndex()) {
            val n = a[1]
            // Binary-search the leftmost tail whose newIdx ≥ n.
            var lo = 0; var hi = tails.size
            while (lo < hi) {
                val mid = (lo + hi) ushr 1
                if (tails[mid][1] < n) lo = mid + 1 else hi = mid
            }
            if (lo > 0) prev[i] = tailIdxs[lo - 1]
            if (lo == tails.size) tails += a else tails[lo] = a
            tailIdxs[lo] = i
        }
        // Reconstruct.
        val out = ArrayDeque<IntArray>()
        var k = tailIdxs[tails.size - 1]
        while (k != -1) {
            out.addFirst(anchors[k])
            k = prev[k]
        }
        return out.toList()
    }

    /** Greedy alignment when no anchors exist (rare). */
    private fun greedyAlign(
        oldHashes: IntArray, newHashes: IntArray,
        oFrom: Int, oTo: Int, nFrom: Int, nTo: Int,
    ): List<Op> {
        val out = ArrayList<Op>()
        var o = oFrom
        var n = nFrom
        while (o < oTo && n < nTo) {
            if (oldHashes[o] == newHashes[n]) {
                out += Op.Keep(o, n, 0L)
                o++; n++
            } else {
                // Look one step ahead on each side.
                val nextOldMatchesNewHere = (o + 1 until oTo).any { oldHashes[it] == newHashes[n] }
                val nextNewMatchesOldHere = (n + 1 until nTo).any { newHashes[it] == oldHashes[o] }
                when {
                    nextOldMatchesNewHere && !nextNewMatchesOldHere -> { out += Op.Delete(o, 0L); o++ }
                    nextNewMatchesOldHere && !nextOldMatchesNewHere -> { out += Op.Insert(n, ""); n++ }
                    else -> { out += Op.Delete(o, 0L); out += Op.Insert(n, ""); o++; n++ }
                }
            }
        }
        while (o < oTo) { out += Op.Delete(o, 0L); o++ }
        while (n < nTo) { out += Op.Insert(n, ""); n++ }
        return out
    }

    // ─── Post-pass: collapse DELETE+INSERT into MODIFY when similar ────────────

    private fun mergeAdjacentEditsIntoModify(
        raw: List<Op>,
        old: List<OldLine>,
        new: List<NewLine>,
    ): List<Op> {
        // Hydrate ids/contents from the lookup tables.
        val hydrated = raw.map { op ->
            when (op) {
                is Op.Keep -> op.copy(lineId = old[op.oldIndex].lineId)
                is Op.Delete -> op.copy(lineId = old[op.oldIndex].lineId)
                is Op.Insert -> op.copy(content = new[op.newIndex].content)
                else -> op
            }
        }

        // Single forward pass: collapse DELETE→INSERT (or INSERT→DELETE) pairs.
        val out = ArrayList<Op>(hydrated.size)
        var i = 0
        while (i < hydrated.size) {
            val a = hydrated[i]
            val b = hydrated.getOrNull(i + 1)
            val pair = when {
                a is Op.Delete && b is Op.Insert -> a to b
                a is Op.Insert && b is Op.Delete -> b to a
                else -> null
            }
            if (pair != null) {
                val (del, ins) = pair
                val similarity = tokenSimilarity(
                    normalise(old[del.oldIndex].content),
                    normalise(new[ins.newIndex].content),
                )
                if (similarity >= mergeSimilarityThreshold) {
                    out += Op.Modify(
                        oldIndex = del.oldIndex,
                        newIndex = ins.newIndex,
                        lineId = del.lineId,
                        newContent = ins.content,
                        similarity = similarity,
                    )
                    i += 2
                    continue
                }
            }
            out += a
            i++
        }
        return out
    }

    private fun tokenSimilarity(a: String, b: String): Double {
        if (a.isEmpty() && b.isEmpty()) return 1.0
        val tokensA = a.split(' ').filter { it.isNotEmpty() }.toSet()
        val tokensB = b.split(' ').filter { it.isNotEmpty() }.toSet()
        if (tokensA.isEmpty() && tokensB.isEmpty()) return 1.0
        val inter = tokensA.intersect(tokensB).size
        val union = tokensA.size + tokensB.size - inter
        return if (union == 0) 0.0 else inter.toDouble() / union.toDouble()
    }

    companion object {
        const val DEFAULT_MERGE_THRESHOLD: Double = 0.6
    }
}
