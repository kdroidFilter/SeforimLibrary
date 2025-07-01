package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Résultat de recherche
 */
@Serializable
data class SearchResult(
    val bookId: Long,
    val bookTitle: String,
    val lineId: Long,
    val lineIndex: Int,
    val snippet: String,  // Extrait avec surbrillance
    val rank: Double     // Score de pertinence
)