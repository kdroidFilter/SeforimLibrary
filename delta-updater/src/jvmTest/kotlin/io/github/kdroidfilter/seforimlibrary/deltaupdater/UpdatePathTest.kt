package io.github.kdroidfilter.seforimlibrary.deltaupdater

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class UpdatePathTest {

    private fun delta(from: Int, to: Int, size: Long): DeltaEntry =
        DeltaEntry(from, to, manifestUrl = "https://x/$from-$to.json", totalSize = size)

    private fun meta(latest: Int, fullBundleBytes: Long, deltas: List<DeltaEntry>): ReleaseMeta =
        ReleaseMeta(
            latestVersion = latest,
            fullBundle = FullBundleEntry(latest, "https://x/full.tar.zst", "abcd", fullBundleBytes),
            deltas = deltas,
        )

    @Test
    fun `up to date returns UpToDate`() {
        val r = chooseUpdatePath(5, meta(5, 1_000_000_000L, emptyList()))
        assertEquals(UpdatePath.UpToDate, r)
    }

    @Test
    fun `local newer than latest still returns UpToDate`() {
        val r = chooseUpdatePath(10, meta(5, 1_000L, emptyList()))
        assertEquals(UpdatePath.UpToDate, r)
    }

    @Test
    fun `local before retention window forces full bundle`() {
        val r = chooseUpdatePath(
            localVersion = 1,
            meta = meta(
                latest = 5,
                fullBundleBytes = 1_000_000_000L,
                deltas = listOf(delta(3, 4, 1_000L), delta(4, 5, 1_000L)),
            ),
        )
        assertIs<UpdatePath.FullBundle>(r)
    }

    @Test
    fun `gap in chain forces full bundle`() {
        val r = chooseUpdatePath(
            localVersion = 3,
            meta = meta(
                latest = 6,
                fullBundleBytes = 1_000_000_000L,
                deltas = listOf(delta(3, 4, 1_000L), delta(5, 6, 1_000L)), // missing 4 → 5
            ),
        )
        assertIs<UpdatePath.FullBundle>(r)
    }

    @Test
    fun `chain cheaper than fallback ratio is used`() {
        val r = chooseUpdatePath(
            localVersion = 3,
            meta = meta(
                latest = 6,
                fullBundleBytes = 1_000_000_000L,
                deltas = listOf(
                    delta(3, 4, 10_000L),
                    delta(4, 5, 10_000L),
                    delta(5, 6, 10_000L),
                ),
            ),
        )
        assertIs<UpdatePath.Chain>(r)
        assertEquals(3, (r as UpdatePath.Chain).deltas.size)
    }

    @Test
    fun `chain heavier than fallback ratio degrades to full bundle`() {
        val r = chooseUpdatePath(
            localVersion = 3,
            meta = meta(
                latest = 6,
                fullBundleBytes = 1_000L,
                deltas = listOf(
                    delta(3, 4, 500L),
                    delta(4, 5, 500L),
                    delta(5, 6, 500L),
                ),
            ),
            fallbackRatio = 0.7,
        )
        assertIs<UpdatePath.FullBundle>(r)
    }
}
