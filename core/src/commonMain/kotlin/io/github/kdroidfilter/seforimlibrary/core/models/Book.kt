package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Représente un livre dans la bibliothèque
 */
@Serializable
data class Book(
    val id: Long = 0,
    val categoryId: Long,
    val title: String,
    val extraTitles: List<String> = emptyList(),
    val author: String? = null,
    val heShortDesc: String? = null,
    val pubDate: String? = null,
    val pubPlace: String? = null,
    val order: Float = 999f,
    val topics: String = "",
    val path: String,
    val bookType: BookType,
    val totalLines: Int = 0,
    val createdAt: Long = System.currentTimeMillis()
)

@Serializable
enum class BookType {
    TEXT, PDF
}
