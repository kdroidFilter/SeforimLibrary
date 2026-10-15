package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.SearchFacets
import io.github.kdroidfilter.seforimlibrary.search.index.IndexCursor
import io.github.kdroidfilter.seforimlibrary.search.index.IndexDoc
import io.github.kdroidfilter.seforimlibrary.search.index.IndexPage
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndex
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndexBackend
import io.github.kdroidfilter.seforimlibrary.search.index.VectorEncoding
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.LongArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonNull
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * The Java side of the bridge (see `BridgedLuceneIndex`): runs the calls of an app's [LuceneIndex]es on
 * [LuceneIndexBackend]s, where Lucene can't run in the app itself: the iOS app calls [call] through JNI, from any
 * thread, in the Java runtime it embeds.
 */
object IndexBridgeServer {
    private val ids = AtomicInteger()
    private val indexes = ConcurrentHashMap<Int, LuceneIndex>()
    private val cursors = ConcurrentHashMap<Int, IndexCursor>()

    /** Runs the call [request] (a [BridgeRequest] as UTF-8 JSON) and returns its [BridgeResponse] (UTF-8 JSON). */
    @JvmStatic
    fun call(request: ByteArray): ByteArray {
        val response =
            try {
                BridgeResponse(result = run(BridgeJson.decodeFromString(BridgeRequest.serializer(), request.decodeToString())))
            } catch (e: Throwable) {
                BridgeResponse(error = e.stackTraceToString())
            }
        return BridgeJson.encodeToString(BridgeResponse.serializer(), response).encodeToByteArray()
    }

    private fun run(request: BridgeRequest) =
        when (request) {
            is BridgeRequest.Open -> {
                val id = ids.incrementAndGet()
                indexes[id] = LuceneIndexBackend(Path.of(request.indexDir), cacheQueries = request.cacheQueries)
                BridgeJson.encodeToJsonElement(Int.serializer(), id)
            }
            is BridgeRequest.Close -> {
                indexes.remove(request.index)?.close()
                JsonNull
            }
            is BridgeRequest.Analyze ->
                BridgeJson.encodeToJsonElement(ListSerializer(String.serializer()), index(request.index).analyze(request.text))
            is BridgeRequest.Search ->
                page(index(request.index).search(request.query, request.limit, request.fields, request.sort))
            is BridgeRequest.OpenCursor -> {
                val id = ids.incrementAndGet()
                cursors[id] = index(request.index).openCursor(request.query, request.fields)
                BridgeJson.encodeToJsonElement(Int.serializer(), id)
            }
            is BridgeRequest.CursorNext ->
                page(cursors[request.cursor]?.next(request.limit) ?: error("Unknown cursor ${request.cursor}"))
            is BridgeRequest.CloseCursor -> {
                cursors.remove(request.cursor)?.close()
                JsonNull
            }
            is BridgeRequest.Facets ->
                BridgeJson.encodeToJsonElement(SearchFacets.serializer(), index(request.index).facets(request.query))
            is BridgeRequest.CollectLongs ->
                BridgeJson.encodeToJsonElement(LongArraySerializer(), index(request.index).collectLongs(request.query, request.field))
            is BridgeRequest.Count ->
                BridgeJson.encodeToJsonElement(Int.serializer(), index(request.index).count(request.query))
            is BridgeRequest.VectorEncodingOf ->
                index(request.index).vectorEncoding(request.field)?.let { BridgeJson.encodeToJsonElement(VectorEncoding.serializer(), it) }
                    ?: JsonNull
            is BridgeRequest.Knn ->
                BridgeJson.encodeToJsonElement(ListSerializer(IndexDoc.serializer()), index(request.index).knn(request.request))
        }

    private fun index(id: Int): LuceneIndex = indexes[id] ?: error("Unknown index $id")

    private fun page(page: IndexPage) = BridgeJson.encodeToJsonElement(IndexPage.serializer(), page)
}
