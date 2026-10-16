package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.index.IndexQuery
import io.github.kdroidfilter.seforimlibrary.search.index.KnnRequest
import io.github.kdroidfilter.seforimlibrary.search.index.SortField
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.cbor.Cbor

/**
 * The calls of a [io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndex] across the bridge between the iOS
 * app and the Java runtime it embeds: CBOR both ways, each index and cursor named by a handle.
 */
@Serializable
internal sealed interface BridgeRequest {
    @Serializable
    @SerialName("open")
    data class Open(
        val indexDir: String,
        val cacheQueries: Boolean,
    ) : BridgeRequest

    @Serializable
    @SerialName("close")
    data class Close(
        val index: Int,
    ) : BridgeRequest

    @Serializable
    @SerialName("analyze")
    data class Analyze(
        val index: Int,
        val text: String,
    ) : BridgeRequest

    @Serializable
    @SerialName("search")
    data class Search(
        val index: Int,
        val query: IndexQuery,
        val limit: Int,
        val fields: Set<String>,
        val sort: List<SortField>? = null,
    ) : BridgeRequest

    @Serializable
    @SerialName("openCursor")
    data class OpenCursor(
        val index: Int,
        val query: IndexQuery,
        val fields: Set<String>,
    ) : BridgeRequest

    @Serializable
    @SerialName("next")
    data class CursorNext(
        val cursor: Int,
        val limit: Int,
    ) : BridgeRequest

    @Serializable
    @SerialName("closeCursor")
    data class CloseCursor(
        val cursor: Int,
    ) : BridgeRequest

    @Serializable
    @SerialName("facets")
    data class Facets(
        val index: Int,
        val query: IndexQuery,
    ) : BridgeRequest

    @Serializable
    @SerialName("collectLongs")
    data class CollectLongs(
        val index: Int,
        val query: IndexQuery,
        val field: String,
    ) : BridgeRequest

    @Serializable
    @SerialName("count")
    data class Count(
        val index: Int,
        val query: IndexQuery,
    ) : BridgeRequest

    @Serializable
    @SerialName("vectorEncoding")
    data class VectorEncodingOf(
        val index: Int,
        val field: String,
    ) : BridgeRequest

    @Serializable
    @SerialName("knn")
    class Knn(
        val index: Int,
        val request: KnnRequest,
    ) : BridgeRequest
}

/**
 * The result of a call ([result]: CBOR of a type depending on the call, decoded by the caller; none for a call without
 * one), or the failure it raised ([error]).
 */
@Serializable
internal class BridgeResponse(
    val result: ByteArray? = null,
    val error: String? = null,
)

@OptIn(ExperimentalSerializationApi::class)
internal val BridgeCbor =
    Cbor {
        ignoreUnknownKeys = true
        // Byte arrays (results, byte vectors) as one CBOR byte string, not an array of one integer per byte
        alwaysUseByteString = true
    }
