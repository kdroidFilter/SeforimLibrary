package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.LruCache
import io.github.kdroidfilter.seforimlibrary.search.SearchFacets
import io.github.kdroidfilter.seforimlibrary.search.index.IndexCursor
import io.github.kdroidfilter.seforimlibrary.search.index.IndexDoc
import io.github.kdroidfilter.seforimlibrary.search.index.IndexPage
import io.github.kdroidfilter.seforimlibrary.search.index.IndexQuery
import io.github.kdroidfilter.seforimlibrary.search.index.KnnRequest
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndex
import io.github.kdroidfilter.seforimlibrary.search.index.SortField
import io.github.kdroidfilter.seforimlibrary.search.index.VectorEncoding
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.LongArraySerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull

/**
 * How the calls reach the Java runtime Lucene runs in: one [BridgeRequest] in, its [BridgeResponse] out, both UTF-8 JSON.
 * Thread-safe: calls may overlap.
 */
fun interface BridgeTransport {
    fun call(request: ByteArray): ByteArray
}

private const val ANALYZE_CACHE_SIZE = 64

/**
 * The [LuceneIndex] at [indexDir] where Lucene can't run in process: in the Java runtime an iOS app embeds, in the Java
 * process an Android app before 15 starts. Lucene runs there with the same implementation as on the JVM and Android 15+
 * (`LuceneIndexBackend`, behind `IndexBridgeServer`); this side only carries the calls, through [transport].
 *
 * @param cacheQueries see `LuceneIndexBackend`
 */
class BridgedLuceneIndex(
    private val transport: BridgeTransport,
    indexDir: String,
    cacheQueries: Boolean = false,
) : LuceneIndex {
    private val handle: Int = call(BridgeRequest.Open(indexDir, cacheQueries), Int.serializer())

    // Analysis depends on the text alone: the same query is analyzed by its facets, its session and its highlights
    private val analyzed = LruCache<String, List<String>>(ANALYZE_CACHE_SIZE)

    override fun analyze(text: String): List<String> =
        analyzed[text] ?: call(BridgeRequest.Analyze(handle, text), ListSerializer(String.serializer())).also { analyzed[text] = it }

    override fun search(
        query: IndexQuery,
        limit: Int,
        fields: Set<String>,
        sort: List<SortField>?,
    ): IndexPage = call(BridgeRequest.Search(handle, query, limit, fields, sort), IndexPage.serializer())

    override fun openCursor(
        query: IndexQuery,
        fields: Set<String>,
    ): IndexCursor {
        val cursor = call(BridgeRequest.OpenCursor(handle, query, fields), Int.serializer())
        return object : IndexCursor {
            private var closed = false

            override fun next(limit: Int): IndexPage = call(BridgeRequest.CursorNext(cursor, limit), IndexPage.serializer())

            override fun close() {
                if (closed) return
                closed = true
                call<Unit>(BridgeRequest.CloseCursor(cursor), null)
            }
        }
    }

    override fun facets(query: IndexQuery): SearchFacets = call(BridgeRequest.Facets(handle, query), SearchFacets.serializer())

    override fun collectLongs(
        query: IndexQuery,
        field: String,
    ): LongArray = call(BridgeRequest.CollectLongs(handle, query, field), LongArraySerializer())

    override fun count(query: IndexQuery): Int = call(BridgeRequest.Count(handle, query), Int.serializer())

    override fun vectorEncoding(field: String): VectorEncoding? =
        call(BridgeRequest.VectorEncodingOf(handle, field), VectorEncoding.serializer().nullable)

    override fun knn(request: KnnRequest): List<IndexDoc> = call(BridgeRequest.Knn(handle, request), ListSerializer(IndexDoc.serializer()))

    override fun close() {
        call<Unit>(BridgeRequest.Close(handle), null)
    }

    private fun <T> call(
        request: BridgeRequest,
        result: DeserializationStrategy<T>?,
    ): T {
        val bytes = transport.call(BridgeJson.encodeToString(BridgeRequest.serializer(), request).encodeToByteArray())
        val response = BridgeJson.decodeFromString(BridgeResponse.serializer(), bytes.decodeToString())
        response.error?.let { throw IllegalStateException("Lucene failed in the Java runtime: $it") }
        @Suppress("UNCHECKED_CAST")
        return if (result == null) Unit as T else BridgeJson.decodeFromJsonElement(result, response.result ?: JsonNull as JsonElement)
    }
}
