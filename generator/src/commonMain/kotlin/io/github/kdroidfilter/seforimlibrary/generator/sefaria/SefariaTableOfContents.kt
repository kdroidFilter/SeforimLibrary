package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.io.File

/**
 * Lightweight parser for Sefaria's table_of_contents.json.
 * Provides Hebrew names and ordering for categories/books based on the TOC structure.
 */
class SefariaTableOfContents private constructor(
    private val categoryInfoByEnglishPath: Map<List<String>, CategoryInfo>,
    private val orderedCategoriesByHebrewPath: Map<List<String>, List<CategoryInfo>>,
    private val bookInfoByEnglishTitle: Map<String, BookInfo>,
    private val orderedBooksByHebrewPath: Map<List<String>, List<BookInfo>>
) {

    data class CategoryInfo(
        val englishPath: List<String>,
        val englishName: String,
        val hebrewName: String,
        val order: Float,
        val hebrewPath: List<String>,
        val position: Int
    )

    data class BookInfo(
        val englishTitle: String,
        val hebrewTitle: String,
        val order: Float,
        val parentHebrewPath: List<String>,
        val position: Int
    )

    /**
     * Returns the Hebrew path for a given English category path, or null if unknown.
     */
    fun hebrewCategoryPath(englishPath: List<String>): List<String>? {
        val hebrewPath = mutableListOf<String>()
        for (i in englishPath.indices) {
            val segmentPath = englishPath.take(i + 1)
            val info = categoryInfoByEnglishPath[segmentPath] ?: return null
            hebrewPath.add(info.hebrewName)
        }
        return hebrewPath
    }

    fun bookInfo(englishTitle: String): BookInfo? = bookInfoByEnglishTitle[englishTitle]

    fun orderedCategoryNames(parentHebrewPath: List<String>): List<String> =
        orderedCategoriesByHebrewPath[parentHebrewPath]?.map { it.hebrewName } ?: emptyList()

    fun orderedBookTitles(parentHebrewPath: List<String>): List<String> =
        orderedBooksByHebrewPath[parentHebrewPath]?.map { it.hebrewTitle } ?: emptyList()

    companion object {
        private val defaultJson = Json {
            ignoreUnknownKeys = true
            isLenient = true
        }

        fun fromFile(file: File, json: Json = defaultJson): SefariaTableOfContents {
            val entries = json.decodeFromString<List<RawTocEntry>>(file.readText())

            val categoryInfoByEnglishPath = mutableMapOf<List<String>, CategoryInfo>()
            val orderedCategoriesByHebrewPath = mutableMapOf<List<String>, MutableList<CategoryInfo>>()
            val bookInfoByEnglishTitle = mutableMapOf<String, BookInfo>()
            val orderedBooksByHebrewPath = mutableMapOf<List<String>, MutableList<BookInfo>>()

            fun ingestBook(entry: RawTocEntry, parentEnglishPath: List<String>, parentHebrewPath: List<String>, position: Int) {
                val englishTitle = entry.title ?: return
                val hebrewTitle = entry.heTitle ?: englishTitle
                val order = entry.order ?: (position + 1).toFloat()
                val info = BookInfo(
                    englishTitle = englishTitle,
                    hebrewTitle = hebrewTitle,
                    order = order,
                    parentHebrewPath = parentHebrewPath.toList(),
                    position = position
                )
                bookInfoByEnglishTitle.putIfAbsent(englishTitle, info)
                orderedBooksByHebrewPath.getOrPut(parentHebrewPath.toList()) { mutableListOf() }.add(info)
            }

            fun ingestCategory(entry: RawTocEntry, parentEnglishPath: List<String>, parentHebrewPath: List<String>, position: Int) {
                val english = entry.category ?: return
                val hebrew = entry.heCategory ?: english
                val order = entry.order ?: (position + 1).toFloat()
                val currentEnglishPath = parentEnglishPath + english
                val currentHebrewPath = parentHebrewPath + hebrew

                val info = CategoryInfo(
                    englishPath = currentEnglishPath,
                    englishName = english,
                    hebrewName = hebrew,
                    order = order,
                    hebrewPath = currentHebrewPath,
                    position = position
                )

                categoryInfoByEnglishPath[currentEnglishPath] = info
                orderedCategoriesByHebrewPath.getOrPut(parentHebrewPath.toList()) { mutableListOf() }.add(info)

                entry.contents.forEachIndexed { idx, child ->
                    when {
                        child.category != null -> ingestCategory(child, currentEnglishPath, currentHebrewPath, idx)
                        child.title != null -> ingestBook(child, currentEnglishPath, currentHebrewPath, idx)
                    }
                }
            }

            entries.forEachIndexed { idx, entry ->
                when {
                    entry.category != null -> ingestCategory(entry, emptyList(), emptyList(), idx)
                    entry.title != null -> ingestBook(entry, emptyList(), emptyList(), idx)
                }
            }

            val sortedCategories = orderedCategoriesByHebrewPath.mapValues { (_, list) ->
                list.sortedWith(
                    compareBy<CategoryInfo> { it.order.toFloat() }
                        .thenBy { it.position }
                        .thenBy { it.hebrewName })
            }

            val sortedBooks = orderedBooksByHebrewPath.mapValues { (_, list) ->
                list.sortedWith(
                    compareBy<BookInfo> { it.order.toFloat() }
                        .thenBy { it.position }
                        .thenBy { it.hebrewTitle })
            }

            return SefariaTableOfContents(
                categoryInfoByEnglishPath = categoryInfoByEnglishPath,
                orderedCategoriesByHebrewPath = sortedCategories,
                bookInfoByEnglishTitle = bookInfoByEnglishTitle,
                orderedBooksByHebrewPath = sortedBooks
            )
        }
    }

    @Serializable
    private data class RawTocEntry(
        val category: String? = null,
        val heCategory: String? = null,
        val title: String? = null,
        val heTitle: String? = null,
        val order: Float? = null,
        val contents: List<RawTocEntry> = emptyList()
    )
}
