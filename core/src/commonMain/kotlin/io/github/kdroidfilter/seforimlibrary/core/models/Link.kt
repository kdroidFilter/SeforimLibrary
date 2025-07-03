package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Link between two texts (commentary, reference, etc.)
 *
 * @property id The unique identifier of the link
 * @property sourceBookId The identifier of the source book
 * @property targetBookId The identifier of the target book
 * @property sourceLineId The identifier of the source line
 * @property targetLineId The identifier of the target line
 * @property connectionType The type of connection between the texts
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

/**
 * Types of connections between texts
 *
 * @property COMMENTARY A commentary on the source text
 * @property TARGUM A translation of the source text
 * @property REFERENCE A reference to the source text
 * @property OTHER Any other type of connection
 */
@Serializable
enum class ConnectionType {
    COMMENTARY, TARGUM, REFERENCE, OTHER;

    companion object {
        /**
         * Creates a ConnectionType from a string value
         *
         * @param value The string representation of the connection type
         * @return The corresponding ConnectionType, or OTHER if not recognized
         */
        fun fromString(value: String): ConnectionType = when (value.lowercase()) {
            "commentary" -> COMMENTARY
            "targum" -> TARGUM
            "reference" -> REFERENCE
            else -> OTHER
        }
    }
}
