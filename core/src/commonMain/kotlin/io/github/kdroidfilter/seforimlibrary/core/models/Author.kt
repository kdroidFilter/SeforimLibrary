package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Représente un auteur de livres dans la bibliothèque
 */
@Serializable
data class Author(
    val id: Long = 0,
    val name: String
)