package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import kotlinx.serialization.Serializable

internal object SefariaImportTuning {
    const val LINE_BATCH_SIZE = 5_000
    const val LINK_BATCH_SIZE = 2_000
    const val FILE_PARALLELISM = 8
}

/**
 * Sefaria's `dependence` flag — the canonical signal for "this book is a
 * dependant text of another book". `null` means the book stands on its own
 * (Tanakh, Talmud, etc.).
 *
 * `OTHER_DEPENDANT` covers values present in the schema that don't have an
 * exact mapping (e.g. "Guides", "Sub-Commentary"). Treated identically to
 * COMMENTARY for orientation purposes — what matters is "dependant or not".
 */
internal enum class Dependence { COMMENTARY, TARGUM, MIDRASH, OTHER_DEPENDANT }

internal data class BookMeta(
    val isBaseBook: Boolean,
    val categoryLevel: Int,
    val priorityRank: Int?,
    // Schema-derived: dependant kind ("Commentary"/"Targum"/...), null = base book.
    val dependence: Dependence? = null,
    // All known base bookIds: starts from Sefaria's declared `base_text_titles`,
    // then gets extended by `inferPrimaryBasesForEmptyDeclaredBookmeta` and by
    // density-based sibling chaining. Used by the resolver to orient links.
    val baseTextBookIds: Set<Long> = emptySet(),
    // **Strict** subset of `baseTextBookIds` — only the bookIds that came from
    // Sefaria's own `base_text_titles` declaration in the schema. Inference and
    // density chaining never mutate this set. Used by the SOURCE virtual view
    // to boost Sefaria-confirmed bases above lateral citations (e.g. Mishnah
    // Avot above Tehillim for Nachalat Avot on Pirkei Avot).
    val sefariaDeclaredBaseTextBookIds: Set<Long> = emptySet(),
    // Schema-derived: Sefaria's `collective_title.en` — the commentator name shared
    // across all volumes of a multi-volume work (e.g. "Rashi" for "Rashi on Genesis",
    // "Rashi on Exodus"…). Used by the density chain to aggregate per-collective
    // signal so volume-level noise doesn't tip the per-pair ratio.
    val collectiveTitleEn: String? = null,
)

internal data class BookPayload(
    val heTitle: String,
    val enTitle: String,
    val categoriesHe: List<String>,
    val lines: List<String>,
    val refEntries: List<RefEntry>,
    val headings: List<Heading>,
    val authors: List<String>,
    val description: String?,
    val pubDates: List<PubDate>,
    val altStructures: List<AltStructurePayload>,
    // Schema metadata used for link orientation. baseTextTitleKeys holds the
    // *normalized* titles (en+he) of declared base texts; resolution to bookIds
    // happens in a second pass once all books have been inserted.
    val dependence: Dependence? = null,
    val baseTextTitleKeys: List<String> = emptyList(),
    val collectiveTitleEn: String? = null,
    // All Sefaria-known aliases for the book (titleVariants + heTitleVariants),
    // normalized. Indexed alongside the primary titles in normalizedTitleToBookId
    // so that title-pattern base parsing ("X on Y") can resolve "Y" to a bookId
    // when "Y" is a Sefaria-recognised alias (e.g. "Avot" → Pirkei Avot).
    val titleAliasKeys: List<String> = emptyList(),
)

internal data class RefEntry(
    val ref: String,
    val heRef: String,
    val path: String,
    val lineIndex: Int
)

internal data class Heading(
    val title: String,
    val level: Int,
    val lineIndex: Int
)

internal data class AltStructurePayload(
    val key: String,
    val title: String?,
    val heTitle: String?,
    val nodes: List<AltNodePayload>
)

internal data class AltNodePayload(
    val title: String?,
    val heTitle: String?,
    val wholeRef: String?,
    val refs: List<String>,
    val addressTypes: List<String>,
    val childLabel: String?,
    val addresses: List<Int>,
    val skippedAddresses: List<Int>,
    val startingAddress: String?,
    val offset: Int?,
    val children: List<AltNodePayload>
)

@Serializable
internal data class DefaultCommentatorsEntry(
    val book: String,
    val commentators: List<String>
)

@Serializable
internal data class DefaultTargumEntry(
    val book: String,
    val targumim: List<String>
)
