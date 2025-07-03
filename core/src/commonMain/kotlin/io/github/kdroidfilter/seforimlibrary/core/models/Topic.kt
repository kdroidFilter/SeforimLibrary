package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Représente un topic (mot-clé) associé à des livres dans la bibliothèque
 */
@Serializable
data class Topic(
    val id: Long = 0,
    val name: String
)