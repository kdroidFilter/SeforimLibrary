package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Représente une catégorie dans la hiérarchie de la bibliothèque
 */
@Serializable
data class Category(
    val id: Long = 0,
    val parentId: Long? = null,
    val title: String,
    val description: String = "",
    val shortDescription: String = "",
    val order: Int = 999,
    val path: String,
    val level: Int = 0,
    val createdAt: Long = System.currentTimeMillis()
)