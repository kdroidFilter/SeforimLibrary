package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a publication date
 *
 * @property id The unique identifier of the publication date
 * @property date The publication date as a string
 */
@Serializable
data class PubDate(
    val id: Long = 0,
    val date: String
)
