package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Métadonnées d'un livre depuis le fichier JSON
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
