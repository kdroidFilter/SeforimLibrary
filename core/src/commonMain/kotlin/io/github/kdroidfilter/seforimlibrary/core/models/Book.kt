package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Represents a book in the library
 *
 * @property id The unique identifier of the book
 * @property categoryId The identifier of the category this book belongs to
 * @property title The title of the book
 * @property authors The list of authors of this book
 * @property topics The list of topics associated with this book
 * @property pubPlaces The list of publication places for this book
 * @property pubDates The list of publication dates for this book
 * @property heShortDesc A short description of the book in Hebrew
 * @property order The display order of the book within its category
 * @property totalLines The total number of lines in the book
 */
@Serializable
data class Book(
    val id: Long = 0,
    val categoryId: Long,
    val title: String,
    val authors: List<Author> = emptyList(),
    val topics: List<Topic> = emptyList(),
    val pubPlaces: List<PubPlace> = emptyList(),
    val pubDates: List<PubDate> = emptyList(),
    val heShortDesc: String? = null,
    val order: Float = 999f,
    val totalLines: Int = 0,
    val hasTargumConnection: Boolean = false,
    val hasReferenceConnection: Boolean = false,
    val hasCommentaryConnection: Boolean = false,
    val hasOtherConnection: Boolean = false,
)
