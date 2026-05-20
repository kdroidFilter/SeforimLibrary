package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * End-to-end check of the density-based sibling chaining for the
 * Mizrachi / Rashi-on-Torah super-commentary pair. Reproduces the
 * real-world counts pulled from Sefaria's `links_by_book.csv`.
 */
class MizrachiChainTest {
    @Test
    fun mizrachiGetsAllFiveRashiVolumesAndRashiDoesNotGetMizrachi() {
        val mizrachi = 5137L
        // Genesis=1 .. Deuteronomy=5
        val torah = (1L..5L).toList()
        // Rashi volumes — bookIds picked to match the real DB.
        val rashiOn = mapOf(
            1L to 5469L, 2L to 5468L, 3L to 5467L, 4L to 5466L, 5L to 5470L,
        )

        val bookMeta = HashMap<Long, BookMeta>()
        torah.forEach { bookMeta[it] = BookMeta(false, 0, null) }
        rashiOn.forEach { (torahId, rashiId) ->
            bookMeta[rashiId] = BookMeta(
                false, 1, null,
                dependence = Dependence.COMMENTARY,
                baseTextBookIds = setOf(torahId),
                collectiveTitleEn = "Rashi",
            )
        }
        bookMeta[mizrachi] = BookMeta(
            false, 1, null,
            dependence = Dependence.COMMENTARY,
            baseTextBookIds = torah.toSet(),
            collectiveTitleEn = "Mizrachi",
        )

        // Real counts from links_by_book.csv.
        val counts = mapOf(
            // Mizrachi ↔ Torah
            (mizrachi to 1L).norm() to 1659,
            (mizrachi to 2L).norm() to 1258,
            (mizrachi to 3L).norm() to 1076,
            (mizrachi to 4L).norm() to 809,
            (mizrachi to 5L).norm() to 792,
            // Rashi-on-X ↔ Torah (required by the asymmetry check —
            // Rashi is the canonical primary commentary on Torah)
            (5469L to 1L).norm() to 2426,
            (5468L to 2L).norm() to 2126,
            (5467L to 3L).norm() to 1536,
            (5466L to 4L).norm() to 900, // approx; real value is well above Mizrachi's 809
            (5470L to 5L).norm() to 900, // approx; real value is well above Mizrachi's 792
            // Mizrachi ↔ Rashi-on-X
            (mizrachi to 5469L).norm() to 1602,
            (mizrachi to 5468L).norm() to 1192,
            (mizrachi to 5467L).norm() to 982,
            (mizrachi to 5466L).norm() to 743,
            (mizrachi to 5470L).norm() to 745,
        )

        applyLinkDensitySiblingChaining(bookMeta, counts, Logger.withTag("test"))

        val mizrachiBases = bookMeta[mizrachi]!!.baseTextBookIds
        println("Mizrachi.baseTextBookIds = $mizrachiBases")
        rashiOn.values.forEach { rashiId ->
            assertTrue(rashiId in mizrachiBases, "Mizrachi should chain to $rashiId")
        }
        // Critical: the asymmetry guard must prevent the reverse edge.
        // Without it, Mizrachi would also appear in Rashi-on-Genesis's bases
        // and the resolver would see mutual declaration → no swap → Mizrachi
        // would still appear as a "source" of Rashi.
        rashiOn.values.forEach { rashiId ->
            val rashiBases = bookMeta[rashiId]!!.baseTextBookIds
            assertFalse(
                mizrachi in rashiBases,
                "Rashi $rashiId must NOT chain back to Mizrachi (asymmetry guard)",
            )
        }
    }

    @Test
    fun emptyBaseDependantInheritsPrimaryBasesViaDensity() {
        // Bartenura on Torah: dependence=Commentary, base_text_titles=[]
        // Real link counts: Genesis 340, Exodus 302, Rashi-on-Gen 221, ...
        val bartenura = 5588L
        val torah = (1L..5L).toList() // primary
        val rashiOn = mapOf(1L to 5469L, 2L to 5468L, 3L to 5467L, 4L to 5466L, 5L to 5470L)

        val bookMeta = HashMap<Long, BookMeta>()
        torah.forEach { bookMeta[it] = BookMeta(false, 0, null) }
        rashiOn.forEach { (t, r) ->
            bookMeta[r] = BookMeta(
                false, 1, null,
                dependence = Dependence.COMMENTARY, baseTextBookIds = setOf(t),
                collectiveTitleEn = "Rashi",
            )
        }
        // Bartenura with no declared base.
        bookMeta[bartenura] = BookMeta(
            false, 1, null,
            dependence = Dependence.COMMENTARY,
            collectiveTitleEn = "Bartenura",
        )

        val counts = mapOf(
            (bartenura to 1L).norm() to 340,
            (bartenura to 2L).norm() to 302,
            (bartenura to 3L).norm() to 176,
            (bartenura to 4L).norm() to 179,
            (bartenura to 5L).norm() to 135,
            (bartenura to 5469L).norm() to 221,
            (bartenura to 5468L).norm() to 173,
            (bartenura to 5467L).norm() to 131,
            (bartenura to 5466L).norm() to 146,
            (bartenura to 5470L).norm() to 106,
            // Rashi-on-X ↔ Torah (needed by sibling-chain asymmetry guard)
            (5469L to 1L).norm() to 2426,
            (5468L to 2L).norm() to 2126,
            (5467L to 3L).norm() to 1536,
            (5466L to 4L).norm() to 900,
            (5470L to 5L).norm() to 900,
        )

        inferPrimaryBasesForEmptyDeclaredBookmeta(bookMeta, counts, Logger.withTag("test"))
        val seeded = bookMeta[bartenura]!!.baseTextBookIds
        torah.forEach { assertTrue(it in seeded, "Bartenura primary base should include $it") }

        applyLinkDensitySiblingChaining(bookMeta, counts, Logger.withTag("test"))
        val finalBases = bookMeta[bartenura]!!.baseTextBookIds
        // Bartenura's ratio to Rashi-on-Gen is 221/340 ≈ 0.65, well below the
        // 0.8 threshold. It is a Torah commentary that cites Rashi, NOT a
        // super-commentary on Rashi. The chain must therefore exclude
        // Rashi-on-X from its base set, and Rashi-on-X must not declare
        // Bartenura either.
        rashiOn.values.forEach {
            assertFalse(it in finalBases, "Bartenura must NOT chain to Rashi-on-X ($it) — citation, not super-commentary")
            assertFalse(bartenura in bookMeta[it]!!.baseTextBookIds, "Rashi $it shouldn't chain to Bartenura")
        }
    }

    private fun Pair<Long, Long>.norm(): Pair<Long, Long> =
        if (first < second) this else second to first
}
