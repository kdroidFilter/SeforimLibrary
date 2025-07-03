package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a publication date
 */
@Serializable
data class PubDate(
    val id: Long = 0,
    val date: String
)