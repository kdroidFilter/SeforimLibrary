package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a topic (keyword) associated with books in the library
 *
 * @property id The unique identifier of the topic
 * @property name The name of the topic
 */
@Serializable
data class Topic(
    val id: Long = 0,
    val name: String
)
