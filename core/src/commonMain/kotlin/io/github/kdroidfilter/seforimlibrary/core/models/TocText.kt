package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a text used in table of contents entries
 *
 * @property id The unique identifier of the TOC text
 * @property text The content of the TOC text
 */
@Serializable
data class TocText(
    val id: Long = 0,
    val text: String
)
