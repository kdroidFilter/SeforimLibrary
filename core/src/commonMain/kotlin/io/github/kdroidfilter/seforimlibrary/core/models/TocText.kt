package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Représente un texte utilisé dans les entrées de la table des matières
 */
@Serializable
data class TocText(
    val id: Long = 0,
    val text: String
)