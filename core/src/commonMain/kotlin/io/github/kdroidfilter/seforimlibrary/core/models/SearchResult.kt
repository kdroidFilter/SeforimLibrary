package io.github.kdroidfilter.seforimlibrary.core.models

import androidx.compose.runtime.Stable
import kotlinx.serialization.Serializable

/**
 * Search result
 *
 * @property bookId The identifier of the book containing the result
 * @property bookTitle The title of the book containing the result
 * @property lineId The identifier of the line containing the result
 * @property lineIndex The index of the line containing the result
 * @property snippet The text excerpt with highlighting
 * @property rank The relevance score of the result
 */
@Stable
@Serializable
data class SearchResult(
    val bookId: Long,
    val bookTitle: String,
    val lineId: Long,
    val lineIndex: Int,
    val snippet: String,
    val rank: Double
)
