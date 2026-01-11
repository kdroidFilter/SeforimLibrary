package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import kotlin.test.Test
import kotlin.test.assertEquals

class SefariaLinksImporterTest {
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
