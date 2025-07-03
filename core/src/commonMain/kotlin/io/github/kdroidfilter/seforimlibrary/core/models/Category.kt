package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a category in the library hierarchy
 *
 * @property id The unique identifier of the category
 * @property parentId The identifier of the parent category, or null if this is a root category
 * @property title The title of the category
 * @property level The level of the category in the hierarchy (0 for root categories)
 */
@Serializable
data class Category(
    val id: Long = 0,
    val parentId: Long? = null,
    val title: String,
    val level: Int = 0
)
