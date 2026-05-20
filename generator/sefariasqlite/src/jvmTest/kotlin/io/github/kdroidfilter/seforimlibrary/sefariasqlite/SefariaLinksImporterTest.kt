package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SefariaLinksImporterTest {
    // Sefaria's schema base_text_titles is the source of truth: when the
    // target's metadata explicitly says "I depend on the source book", we
    // must trust it regardless of weaker signals like isBaseBook or rank.
    @Test
    fun explicitBaseTextTitlesMatchOverridesOtherSignals() {
        // Source (id=10) is the declared base text of target (id=20).
        // Note both are flagged isBaseBook=true and have ranks — those weaker
        // signals would point the wrong way, but base_text_titles wins.
        val sourceMeta = BookMeta(
            isBaseBook = true,
            categoryLevel = 1,
            priorityRank = 50,
        )
        val targetMeta = BookMeta(
            isBaseBook = true,
            categoryLevel = 1,
            priorityRank = 10,
            dependence = Dependence.COMMENTARY,
            baseTextBookIds = setOf(10L),
        )

        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 10L,
            targetBookId = 20L,
            sourceMeta = sourceMeta,
            targetMeta = targetMeta
        )

        assertEquals(ConnectionType.COMMENTARY, forward)
        assertEquals(ConnectionType.SOURCE, reverse)
    }

    @Test
    fun explicitBaseTextTitlesMatchSwapsDirectionWhenCsvIsInverted() {
        // Same setup but CSV had source/target swapped — the swap must flip back.
        val baseMeta = BookMeta(isBaseBook = true, categoryLevel = 1, priorityRank = 5)
        val depMeta = BookMeta(
            isBaseBook = false,
            categoryLevel = 1,
            priorityRank = null,
            dependence = Dependence.COMMENTARY,
            baseTextBookIds = setOf(10L),
        )

        // sourceId=20 (dep), targetId=10 (base) — the dep cites the base.
        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 20L,
            targetBookId = 10L,
            sourceMeta = depMeta,
            targetMeta = baseMeta
        )

        assertEquals(ConnectionType.SOURCE, forward)
        assertEquals(ConnectionType.COMMENTARY, reverse)
    }

    // When base_text_titles couldn't be resolved (e.g. the base book is not in
    // our DB), fall back to schema dependence flag.
    @Test
    fun dependenceAsymmetryFallbackWhenBaseTextTitlesAbsent() {
        val baseMeta = BookMeta(isBaseBook = false, categoryLevel = 1, priorityRank = null)
        val depMeta = BookMeta(
            isBaseBook = false,
            categoryLevel = 1,
            priorityRank = null,
            dependence = Dependence.COMMENTARY,
            baseTextBookIds = emptySet(),
        )

        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 10L,
            targetBookId = 20L,
            sourceMeta = baseMeta,
            targetMeta = depMeta
        )

        assertEquals(ConnectionType.COMMENTARY, forward)
        assertEquals(ConnectionType.SOURCE, reverse)
    }

    @Test
    fun isBaseBookFallbackWhenSchemaSilent() {
        // No dependence on either side, no base_text_titles — old curated flag still works.
        val baseMeta = BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = null)
        val depMeta = BookMeta(isBaseBook = false, categoryLevel = 2, priorityRank = null)

        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 1L,
            targetBookId = 2L,
            sourceMeta = baseMeta,
            targetMeta = depMeta
        )

        assertEquals(ConnectionType.COMMENTARY, forward)
        assertEquals(ConnectionType.SOURCE, reverse)
    }

    // ───── inferConnectionTypeFromSchema (empty CSV Conection Type) ─────

    @Test
    fun inferReturnsDependenceTypeWhenTargetDeclaresSourceAsBase() {
        val baseMeta = BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0)
        val depMeta = BookMeta(
            isBaseBook = false,
            categoryLevel = 2,
            priorityRank = null,
            dependence = Dependence.COMMENTARY,
            baseTextBookIds = setOf(10L),
        )

        val inferred = inferConnectionTypeFromSchema(
            srcBookId = 10L, tgtBookId = 20L,
            srcMeta = baseMeta, tgtMeta = depMeta,
        )

        assertEquals(ConnectionType.COMMENTARY, inferred)
    }

    @Test
    fun inferReturnsTargumWhenDependantIsATargum() {
        val baseMeta = BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0)
        val targumMeta = BookMeta(
            isBaseBook = false,
            categoryLevel = 2,
            priorityRank = null,
            dependence = Dependence.TARGUM,
            baseTextBookIds = setOf(10L),
        )

        val inferred = inferConnectionTypeFromSchema(
            srcBookId = 10L, tgtBookId = 30L,
            srcMeta = baseMeta, tgtMeta = targumMeta,
        )

        assertEquals(ConnectionType.TARGUM, inferred)
    }

    @Test
    fun inferReturnsNullWhenNeitherSideDeclaresTheOther() {
        // Lateral citation between two independent books — keep as OTHER.
        val a = BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 1)
        val b = BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 2)

        val inferred = inferConnectionTypeFromSchema(
            srcBookId = 10L, tgtBookId = 20L,
            srcMeta = a, tgtMeta = b,
        )

        assertNull(inferred)
    }

    @Test
    fun inferReturnsNullWhenBothSidesClaimDependence() {
        // Pathological: both list each other. Ambiguous — caller falls back.
        val a = BookMeta(
            isBaseBook = false, categoryLevel = 0, priorityRank = null,
            dependence = Dependence.COMMENTARY, baseTextBookIds = setOf(20L),
        )
        val b = BookMeta(
            isBaseBook = false, categoryLevel = 0, priorityRank = null,
            dependence = Dependence.COMMENTARY, baseTextBookIds = setOf(10L),
        )

        val inferred = inferConnectionTypeFromSchema(
            srcBookId = 10L, tgtBookId = 20L,
            srcMeta = a, tgtMeta = b,
        )

        assertNull(inferred)
    }

    @Test
    fun lateralCitationBetweenTwoPrimariesIsDowngradedToOther() {
        // Both books are primary in Sefaria's model (`dependence: null`) and
        // neither is in the curated priority list (`isBaseBook=false`,
        // `priorityRank=null`). CSV labels the cross-citation "commentary"
        // (e.g. Sod Yesharim ↔ Zohar) but no structural signal supports the
        // claim — they're independent Chassidic / Kabbalah works that
        // cross-reference each other.
        val a = BookMeta(isBaseBook = false, categoryLevel = 1, priorityRank = null)
        val b = BookMeta(isBaseBook = false, categoryLevel = 1, priorityRank = null)

        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 10L,
            targetBookId = 20L,
            sourceMeta = a,
            targetMeta = b,
        )

        assertEquals(ConnectionType.OTHER, forward)
        assertEquals(ConnectionType.OTHER, reverse)
    }

    @Test
    fun lateralCitationBetweenTwoDependantsIsDowngradedToOther() {
        // Two dependants that share no base text in their declared metadata
        // and no other directional signal — Bartenura on Torah ↔ Rashi on
        // Genesis is the canonical real-world case. The CSV labels the link
        // COMMENTARY but neither book is the base of the other. Without
        // downgrade, the link appears as a "source" entry on Rashi's reader.
        val a = BookMeta(
            isBaseBook = false, categoryLevel = 1, priorityRank = null,
            dependence = Dependence.COMMENTARY, baseTextBookIds = setOf(99L),
        )
        val b = BookMeta(
            isBaseBook = false, categoryLevel = 1, priorityRank = null,
            dependence = Dependence.COMMENTARY, baseTextBookIds = setOf(99L),
        )

        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 10L,
            targetBookId = 20L,
            sourceMeta = a,
            targetMeta = b,
        )

        assertEquals(ConnectionType.OTHER, forward)
        assertEquals(ConnectionType.OTHER, reverse)
    }

    @Test
    fun higherPriorityBookIsTreatedAsPrimary() {
        val sourceMeta = BookMeta(isBaseBook = true, categoryLevel = 2, priorityRank = 5)
        val targetMeta = BookMeta(isBaseBook = true, categoryLevel = 1, priorityRank = 20)

        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 1L,
            targetBookId = 2L,
            sourceMeta = sourceMeta,
            targetMeta = targetMeta
        )

        assertEquals(ConnectionType.COMMENTARY, forward)
        assertEquals(ConnectionType.SOURCE, reverse)
    }

    @Test
    fun lowerPriorityBookBecomesSecondary() {
        val sourceMeta = BookMeta(isBaseBook = true, categoryLevel = 2, priorityRank = 50)
        val targetMeta = BookMeta(isBaseBook = true, categoryLevel = 1, priorityRank = 10)

        val (forward, reverse) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.COMMENTARY,
            sourceBookId = 1L,
            targetBookId = 2L,
            sourceMeta = sourceMeta,
            targetMeta = targetMeta
        )

        assertEquals(ConnectionType.SOURCE, forward)
        assertEquals(ConnectionType.COMMENTARY, reverse)
    }
}
