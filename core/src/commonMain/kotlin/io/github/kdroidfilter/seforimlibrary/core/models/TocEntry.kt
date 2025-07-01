package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Entrée de la table des matières
 */
@Serializable
data class TocEntry(
    val id: Long = 0,
    val bookId: Long,
    val parentId: Long? = null,
    val text: String,
    val level: Int,
    val lineId: Long = 0,
    val lineIndex: Int,
    val order: Int,
    val path: String  // Chemin hiérarchique (ex: "1.2.3")
)
