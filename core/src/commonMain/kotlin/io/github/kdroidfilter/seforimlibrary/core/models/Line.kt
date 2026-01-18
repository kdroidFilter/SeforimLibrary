package io.github.kdroidfilter.seforimlibrary.core.models

import androidx.compose.runtime.Stable
import kotlinx.serialization.Serializable

/**
 * Represents an individual line of a book
 *
 * @property id The unique identifier of the line
 * @property bookId The identifier of the book this line belongs to
 * @property lineIndex The index of the line within the book
 * @property content The original HTML content of the line
 * @property heRef The Hebrew reference/citation (e.g., "בראשית א, א")
 */
@Stable
@Serializable
data class Line(
    val id: Long = 0,
    val bookId: Long,
    val lineIndex: Int,
    val content: String,
    val heRef: String? = null
)
