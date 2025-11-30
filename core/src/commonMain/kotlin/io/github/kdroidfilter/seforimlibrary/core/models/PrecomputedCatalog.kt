@file:OptIn(ExperimentalSerializationApi::class)

package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoNumber

/**
 * Precomputed catalog tree structure optimized for fast loading.
 * This structure is generated once during database creation and serialized to a binary file.
 * The application loads this file at startup instead of querying the database.
 */
@Serializable
data class PrecomputedCatalog(
    @ProtoNumber(1) val rootCategories: List<CatalogCategory>,
    @ProtoNumber(2) val version: Int = 1,
    @ProtoNumber(3) val totalBooks: Int = 0,
    @ProtoNumber(4) val totalCategories: Int = 0
)

/**
 * Represents a category in the precomputed catalog tree.
 * Contains its books and subcategories in a hierarchical structure.
 */
@Serializable
data class CatalogCategory(
    @ProtoNumber(1) val id: Long,
    @ProtoNumber(2) val title: String,
    @ProtoNumber(3) val level: Int,
    @ProtoNumber(4) val parentId: Long? = null,
    @ProtoNumber(5) val books: List<CatalogBook>,
    @ProtoNumber(6) val subcategories: List<CatalogCategory>
)

/**
 * Simplified book representation for the catalog tree.
 * Contains only the essential information needed for navigation.
 */
@Serializable
data class CatalogBook(
    @ProtoNumber(1) val id: Long,
    @ProtoNumber(2) val title: String,
    @ProtoNumber(3) val categoryId: Long,
    @ProtoNumber(4) val order: Float = 999f,
    @ProtoNumber(5) val authors: List<String> = emptyList(),
    @ProtoNumber(6) val totalLines: Int = 0,
    @ProtoNumber(7) val isBaseBook: Boolean = false,
    @ProtoNumber(8) val hasTargumConnection: Boolean = false,
    @ProtoNumber(9) val hasReferenceConnection: Boolean = false,
    @ProtoNumber(10) val hasCommentaryConnection: Boolean = false,
    @ProtoNumber(11) val hasOtherConnection: Boolean = false,
    @ProtoNumber(12) val hasAltStructures: Boolean = false
)
