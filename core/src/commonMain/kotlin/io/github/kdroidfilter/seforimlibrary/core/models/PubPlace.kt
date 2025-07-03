package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a publication place
 *
 * @property id The unique identifier of the publication place
 * @property name The name of the publication place
 */
@Serializable
data class PubPlace(
    val id: Long = 0,
    val name: String
)
