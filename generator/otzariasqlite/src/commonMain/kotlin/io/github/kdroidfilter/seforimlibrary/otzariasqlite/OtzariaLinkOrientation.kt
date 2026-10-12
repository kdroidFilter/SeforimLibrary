package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link

/**
 * Builds the stored form of an Otzaria link read from `<source book>_links.json`.
 *
 * Otzaria often lists a commentary's links only in the commentary's own file, typed
 * `source` ("the target is my source"), e.g. קרן אורה על זבחים → זבחים, with nothing
 * in זבחים_links.json. Stored links are always base → dependant (see [Link]), so a
 * `source` link is flipped into a COMMENTARY link from the base line. Stored as is,
 * neither the commentary panel nor the virtual SOURCE view reads it.
 *
 * The returned link has no id yet.
 */
internal fun canonicalOtzariaLink(
    sourceBookId: Long,
    sourceLineId: Long,
    sourceLineIndex: Int,
    targetBookId: Long,
    targetLineId: Long,
    targetLineIndex: Int,
    connectionType: ConnectionType,
): Link =
    if (connectionType == ConnectionType.SOURCE) {
        Link(
            sourceBookId = targetBookId,
            targetBookId = sourceBookId,
            sourceLineId = targetLineId,
            targetLineId = sourceLineId,
            targetLineIndex = sourceLineIndex,
            connectionType = ConnectionType.COMMENTARY,
        )
    } else {
        Link(
            sourceBookId = sourceBookId,
            targetBookId = targetBookId,
            sourceLineId = sourceLineId,
            targetLineId = targetLineId,
            targetLineIndex = targetLineIndex,
            connectionType = connectionType,
        )
    }
