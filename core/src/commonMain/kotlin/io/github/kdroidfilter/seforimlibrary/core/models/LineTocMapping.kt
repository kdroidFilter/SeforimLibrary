package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Mapping between lines and table of contents entries
 *
 * @property lineId The identifier of the line
 * @property tocEntryId The identifier of the table of contents entry
 */
@Serializable
data class LineTocMapping(
    val lineId: Long,
    val tocEntryId: Long
)
