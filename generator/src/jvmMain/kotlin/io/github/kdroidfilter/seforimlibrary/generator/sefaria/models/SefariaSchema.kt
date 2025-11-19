package io.github.kdroidfilter.seforimlibrary.generator.sefaria.models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

/**
 * Represents a Sefaria book schema JSON
 */
@Serializable
data class SefariaSchema(
    val title: String,
    val heTitle: String,
    val categories: List<String>,
    val schema: SchemaNode? = null,
    val authors: List<Author> = emptyList(),
    val enDesc: String? = null,
    val heDesc: String? = null,
    val enShortDesc: String? = null,
    val heShortDesc: String? = null,
    val compDate: List<Int>? = null,
    val compPlace: String? = null,
    val compPlaceString: LocalizedString? = null,
    val pubDate: List<Int>? = null,
    val pubPlace: String? = null,
    val pubPlaceString: LocalizedString? = null,
    val era: String? = null,
    val dependence: String? = null,
    @SerialName("base_text_titles")
    val baseTextTitles: List<BaseTextTitle>? = null,
    @SerialName("base_text_mapping")
    val baseTextMapping: String? = null,
    @SerialName("collective_title")
    val collectiveTitle: LocalizedString? = null,
    val titleVariants: List<String>? = null,
    val heTitleVariants: List<String>? = null
)

@Serializable
data class SchemaNode(
    val nodeType: String? = null,
    val depth: Int? = null,
    val addressTypes: List<String>? = null,
    val sectionNames: List<String>? = null,
    val lengths: List<Int>? = null,
    val titles: List<Title>? = null,
    val nodes: List<SchemaNode>? = null,
    val key: String? = null,
    val heTitle: String? = null,
    val enTitle: String? = null
)

@Serializable
data class Author(
    val en: String,
    val he: String,
    val slug: String
)

@Serializable
data class LocalizedString(
    val en: String,
    val he: String
)

@Serializable
data class BaseTextTitle(
    val en: String,
    val he: String
)

@Serializable
data class Title(
    val text: String,
    val lang: String,
    val primary: Boolean? = null
)

/**
 * Represents a merged.json file containing the actual text
 */
@Serializable
data class SefariaMergedText(
    val title: String,
    val language: String,
    val versionTitle: String,
    val versionSource: String,
    val text: JsonElement, // Can be array of strings, array of arrays, or object
    val schema: SchemaNode? = null,
    val sectionNames: List<String>? = null,
    val categories: List<String>? = null
)

/**
 * Represents a link from the CSV files
 */
data class SefariaLink(
    val citation1: String,
    val citation2: String,
    val connectionType: String,
    val text1: String,
    val text2: String,
    val category1: String,
    val category2: String
)
