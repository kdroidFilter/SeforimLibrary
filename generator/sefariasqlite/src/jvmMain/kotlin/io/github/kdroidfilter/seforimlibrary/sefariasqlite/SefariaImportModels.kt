package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import kotlinx.serialization.Serializable

internal object SefariaImportTuning {
    const val LINE_BATCH_SIZE = 5_000
    const val LINK_BATCH_SIZE = 2_000
    const val FILE_PARALLELISM = 8
}

internal data class BookMeta(
    val isBaseBook: Boolean,
    val categoryLevel: Int
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
    val altStructures: List<AltStructurePayload>
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

