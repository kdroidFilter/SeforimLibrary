package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Lien entre deux textes (commentaire, référence, etc.)
 */
@Serializable
data class Link(
    val id: Long = 0,
    val sourceBookId: Long,
    val targetBookId: Long,
    val sourceLineId: Long,
    val targetLineId: Long,
    val connectionType: ConnectionType
)

@Serializable
enum class ConnectionType {
    COMMENTARY, TARGUM, REFERENCE, OTHER;

    companion object {
        fun fromString(value: String): ConnectionType = when (value.lowercase()) {
            "commentary" -> COMMENTARY
            "targum" -> TARGUM
            "reference" -> REFERENCE
            else -> OTHER
        }
    }
}
