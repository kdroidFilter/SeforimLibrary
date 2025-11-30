package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Describes an alternative TOC structure available for a book
 * (e.g., Parasha/Aliyah alongside the primary chapter/verse).
 */
@Serializable
data class AltTocStructure(
    val id: Long = 0,
    val bookId: Long,
    val key: String,
    val title: String? = null,
    val heTitle: String? = null
)

/**
 * Single entry inside an alternative TOC tree.
 */
@Serializable
data class AltTocEntry(
    val id: Long = 0,
    val structureId: Long,
    val parentId: Long? = null,
    val textId: Long? = null,
    val text: String = "",
    val level: Int,
    val lineId: Long? = null,
    val isLastChild: Boolean = false,
    val hasChildren: Boolean = false
)

/**
 * Mapping from a line to its owning alternative TOC entry (per structure).
 */
@Serializable
data class LineAltTocMapping(
    val lineId: Long,
    val structureId: Long,
    val altTocEntryId: Long
)
