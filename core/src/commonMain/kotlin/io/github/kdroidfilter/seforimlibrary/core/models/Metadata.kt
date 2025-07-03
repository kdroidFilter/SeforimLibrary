package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Book metadata from the JSON file
 *
 * @property title The title of the book
 * @property description The full description of the book
 * @property shortDescription A short description of the book
 * @property author The author of the book
 * @property extraTitles Alternative titles for the book
 * @property heShortDesc A short description in Hebrew
 * @property pubDate The publication date of the book
 * @property pubPlace The publication place of the book
 * @property order The display order of the book within its category
 */
@Serializable
data class BookMetadata(
    val title: String,
    val description: String? = null,
    val shortDescription: String? = null,
    val author: String? = null,
    val extraTitles: List<String>? = null,
    val heShortDesc: String? = null,
    val pubDate: String? = null,
    val pubPlace: String? = null,
    val order: Float? = null
)
