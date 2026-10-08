package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readText

/** One author topic of `authors.json`, as dumped raw by SefariaExport. */
@Serializable
internal data class SefariaAuthorTopic(
    val slug: String,
    val heTitles: List<String> = emptyList(),
    val birthYear: Int? = null,
    val birthYearIsApprox: Boolean? = null,
    val deathYear: Int? = null,
    val deathYearIsApprox: Boolean? = null,
    val era: String? = null,
) {
    /** The Hebrew titles other than [name], trimmed and deduplicated. */
    fun aliasesOf(name: String): List<String> =
        heTitles.map { it.trim() }.filter { it.isNotEmpty() && it != name }.distinct()
}

/** Sefaria author topics keyed by slug, from `authors.json`. */
internal class SefariaAuthorTopics(topics: List<SefariaAuthorTopic>) {
    private val bySlug = topics.associateBy { it.slug }

    operator fun get(slug: String): SefariaAuthorTopic? = bySlug[slug]

    companion object {
        const val FILE_NAME = "authors.json"

        fun load(dbRoot: Path, json: Json): SefariaAuthorTopics {
            val file = dbRoot.resolve(FILE_NAME)
            require(file.exists()) {
                "$FILE_NAME missing from the Sefaria export at $dbRoot " +
                    "(produced by SefariaExport since PR #7)"
            }
            return SefariaAuthorTopics(json.decodeFromString<List<SefariaAuthorTopic>>(file.readText()))
        }
    }
}
