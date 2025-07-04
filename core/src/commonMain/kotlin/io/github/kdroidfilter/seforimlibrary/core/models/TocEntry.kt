package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Table of contents entry
 *
 * @property id The unique identifier of the TOC entry
 * @property bookId The identifier of the book this TOC entry belongs to
 * @property parentId The identifier of the parent TOC entry, or null if this is a root entry
 * @property textId The identifier of the associated text in the tocText table
 * @property text The text of the TOC entry (for compatibility with existing code)
 * @property level The level of the TOC entry in the hierarchy
 * @property lineId The identifier of the associated line, or null if not linked to a specific line
 */
@Serializable
data class TocEntry(
    val id: Long = 0,
    val bookId: Long,
    val parentId: Long? = null,
    val textId: Long? = null,
    val text: String = "",
    val level: Int,
    val lineId: Long? = null
)
