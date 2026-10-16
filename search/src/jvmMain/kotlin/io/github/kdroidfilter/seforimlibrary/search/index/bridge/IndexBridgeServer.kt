package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.SearchFacets
import io.github.kdroidfilter.seforimlibrary.search.index.IndexCursor
import io.github.kdroidfilter.seforimlibrary.search.index.IndexDoc
import io.github.kdroidfilter.seforimlibrary.search.index.IndexPage
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndex
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndexBackend
import io.github.kdroidfilter.seforimlibrary.search.index.VectorEncoding
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.LongArraySerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
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
    private val cursors = ConcurrentHashMap<Int, OpenCursor>()

    // A cursor with the index it reads, closed with it. Its calls take turns: a page being read when the index
    // closes finishes before its searcher is released
    private class OpenCursor(
        val index: Int,
        private val cursor: IndexCursor,
    ) {
        private var closed = false

        @Synchronized
        fun next(limit: Int): IndexPage {
            check(!closed) { "Closed cursor" }
            return cursor.next(limit)
        }

        @Synchronized
        fun close() {
            if (closed) return
            closed = true
            cursor.close()
        }
    }

    /** Runs the call [request] (a [BridgeRequest] in CBOR) and returns its [BridgeResponse] (CBOR). */
    @JvmStatic
    fun call(request: ByteArray): ByteArray {
        val response =
            try {
                BridgeResponse(result = run(BridgeCbor.decodeFromByteArray(BridgeRequest.serializer(), request)))
            } catch (e: Throwable) {
                BridgeResponse(error = e.stackTraceToString())
            }
        return BridgeCbor.encodeToByteArray(BridgeResponse.serializer(), response)
    }

    private fun run(request: BridgeRequest): ByteArray? =
        when (request) {
            is BridgeRequest.Open -> {
                val id = ids.incrementAndGet()
                indexes[id] = LuceneIndexBackend(Path.of(request.indexDir), cacheQueries = request.cacheQueries)
                encode(Int.serializer(), id)
            }
            is BridgeRequest.Close -> {
                // The cursors left open hold the index's reader: closed first
                cursors.filterValues { it.index == request.index }.keys.forEach { cursors.remove(it)?.close() }
                indexes.remove(request.index)?.close()
                null
            }
            is BridgeRequest.Analyze ->
                encode(ListSerializer(String.serializer()), index(request.index).analyze(request.text))
            is BridgeRequest.Search ->
                page(index(request.index).search(request.query, request.limit, request.fields, request.sort))
            is BridgeRequest.OpenCursor -> {
                val id = ids.incrementAndGet()
                cursors[id] = OpenCursor(request.index, index(request.index).openCursor(request.query, request.fields))
                encode(Int.serializer(), id)
            }
            is BridgeRequest.CursorNext ->
                page(cursors[request.cursor]?.next(request.limit) ?: error("Unknown cursor ${request.cursor}"))
            is BridgeRequest.CloseCursor -> {
                cursors.remove(request.cursor)?.close()
                null
            }
            is BridgeRequest.Facets ->
                encode(SearchFacets.serializer(), index(request.index).facets(request.query))
            is BridgeRequest.CollectLongs ->
                encode(LongArraySerializer(), index(request.index).collectLongs(request.query, request.field))
            is BridgeRequest.Count ->
                encode(Int.serializer(), index(request.index).count(request.query))
            is BridgeRequest.VectorEncodingOf ->
                encode(VectorEncoding.serializer().nullable, index(request.index).vectorEncoding(request.field))
            is BridgeRequest.Knn ->
                encode(ListSerializer(IndexDoc.serializer()), index(request.index).knn(request.request))
        }

    private fun index(id: Int): LuceneIndex = indexes[id] ?: error("Unknown index $id")

    private fun page(page: IndexPage) = encode(IndexPage.serializer(), page)

    private fun <T> encode(
        serializer: KSerializer<T>,
        value: T,
    ): ByteArray = BridgeCbor.encodeToByteArray(serializer, value)
}
