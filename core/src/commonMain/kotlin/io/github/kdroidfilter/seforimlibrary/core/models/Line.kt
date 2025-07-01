package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Représente une ligne individuelle d'un livre
 */
@Serializable
data class Line(
    val id: Long = 0,
    val bookId: Long,
    val lineIndex: Int,
    val content: String,        // Contenu HTML original
    val plainText: String      // Texte nettoyé pour recherche
    // tocEntryId est maintenant géré via la table lineTocMapping
)