package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a book author in the library
 *
 * @property id The unique identifier of the author
 * @property name The name of the author
 */
@Serializable
data class Author(
    val id: Long = 0,
    val name: String
)
