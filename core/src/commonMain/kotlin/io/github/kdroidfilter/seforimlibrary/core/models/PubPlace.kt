package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a publication place
 */
@Serializable
data class PubPlace(
    val id: Long = 0,
    val name: String
)