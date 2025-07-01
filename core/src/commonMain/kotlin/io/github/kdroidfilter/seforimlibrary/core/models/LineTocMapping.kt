package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Mapping entre lignes et entrées de table des matières
 */
@Serializable
data class LineTocMapping(
    val lineId: Long,
    val tocEntryId: Long
)
