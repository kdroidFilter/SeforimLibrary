package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import kotlin.test.Test
import kotlin.test.assertEquals

class OtzariaLinkOrientationTest {
    @Test
    fun sourceLinkIsFlippedIntoACommentaryFromTheBase() {
        // קרן אורה על זבחים (book 2, line 21) → זבחים (book 1, line 7), typed "source"
        val link = canonicalOtzariaLink(2, 200, 21, 1, 100, 7, ConnectionType.SOURCE)

        assertEquals(Link(sourceBookId = 1, targetBookId = 2, sourceLineId = 100, targetLineId = 200, targetLineIndex = 21, connectionType = ConnectionType.COMMENTARY), link)
    }

    @Test
    fun otherLinksKeepTheirOrientation() {
        val link = canonicalOtzariaLink(1, 100, 7, 2, 200, 21, ConnectionType.COMMENTARY)

        assertEquals(Link(sourceBookId = 1, targetBookId = 2, sourceLineId = 100, targetLineId = 200, targetLineIndex = 21, connectionType = ConnectionType.COMMENTARY), link)
    }
}
