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
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.FileDescriptor
import java.io.FileOutputStream
import java.io.PrintStream
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * The Java side of the bridge (see `BridgedLuceneIndex`): runs the calls of an app's [LuceneIndex]es on
 * [LuceneIndexBackend]s, where Lucene can't run in the app itself: the iOS app calls [call] through JNI, from any
 * thread, in the Java runtime it embeds; an Android app runs [main] in a Java process of its own.
 */
object IndexBridgeServer {
    /**
     * Serves the calls of standard input on standard output, until the input closes: each call is a frame (its id, its
     * length, the request), each answer a frame with the same id; calls run concurrently, answered as they complete.
     * Anything else printed goes to standard error, which keeps the output for the frames alone.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        val frames = DataOutputStream(BufferedOutputStream(FileOutputStream(FileDescriptor.out)))
        System.setOut(PrintStream(FileOutputStream(FileDescriptor.err), true))
        val input = DataInputStream(BufferedInputStream(System.`in`))
        val workers =
            Executors.newFixedThreadPool(maxOf(2, Runtime.getRuntime().availableProcessors())) { task ->
                Thread(task, "bridge-${threadIds.incrementAndGet()}").apply { isDaemon = true }
            }
        while (true) {
            val id: Int
            val request: ByteArray
            try {
                id = input.readInt()
                request = ByteArray(input.readInt()).also { input.readFully(it) }
            } catch (_: EOFException) {
                break
            }
            workers.execute {
                val response = call(request)
                synchronized(frames) {
                    frames.writeInt(id)
                    frames.writeInt(response.size)
                    frames.write(response)
                    frames.flush()
                }
            }
        }
        // The calls already read are answered before the process ends
        workers.shutdown()
        workers.awaitTermination(1, TimeUnit.MINUTES)
    }

    private val threadIds = AtomicInteger()
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
