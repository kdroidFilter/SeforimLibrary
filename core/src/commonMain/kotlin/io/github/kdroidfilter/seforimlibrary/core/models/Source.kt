package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a content source/provider entry.
 */
@Serializable
data class Source(
    val id: Long = 0,
    val name: String
)

