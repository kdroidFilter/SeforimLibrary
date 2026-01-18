package io.github.kdroidfilter.seforimlibrary.core.models

import androidx.compose.runtime.Stable
import kotlinx.serialization.Serializable

/**
 * Represents a book in the library
 *
 * @property id The unique identifier of the book
 * @property categoryId The identifier of the category this book belongs to
 * @property title The title of the book
 * @property sourceId The identifier of the source this book originates from
 * @property authors The list of authors of this book
 * @property topics The list of topics associated with this book
 * @property pubPlaces The list of publication places for this book
 * @property pubDates The list of publication dates for this book
 * @property heShortDesc A short description of the book in Hebrew
 * @property order The display order of the book within its category
 * @property totalLines The total number of lines in the book
 * @property hasAltStructures Indicates if the book has alternative TOC structures (e.g., Parasha)
 * @property hasTeamim Indicates if the book contains biblical cantillation marks (teamim)
 * @property hasNekudot Indicates if the book contains vowel points (nikud/nekudot)
 */
@Stable
@Serializable
data class Book(
    val id: Long = 0,
    val categoryId: Long,
    val sourceId: Long,
    val title: String,
    val authors: List<Author> = emptyList(),
    val topics: List<Topic> = emptyList(),
    val pubPlaces: List<PubPlace> = emptyList(),
    val pubDates: List<PubDate> = emptyList(),
    val heShortDesc: String? = null,
    // Optional notes content: when a companion file named "הערות על <title>" exists,
    // its content is attached here instead of being inserted as a separate book.
    val notesContent: String? = null,
    val order: Float = 999f,
    val totalLines: Int = 0,
    val isBaseBook: Boolean = false,
    val hasTargumConnection: Boolean = false,
    val hasReferenceConnection: Boolean = false,
    val hasSourceConnection: Boolean = false,
    val hasCommentaryConnection: Boolean = false,
    val hasOtherConnection: Boolean = false,
    val hasAltStructures: Boolean = false,
    val hasTeamim: Boolean = false,
    val hasNekudot: Boolean = false,
)
