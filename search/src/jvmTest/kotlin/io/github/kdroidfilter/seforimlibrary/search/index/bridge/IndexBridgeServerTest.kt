package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.index.IndexQuery
import kotlinx.serialization.builtins.serializer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class IndexBridgeServerTest {
    private val indexDir: Path =
        Files.createTempDirectory("bridge_index").also { dir ->
            IndexWriter(FSDirectory.open(dir), IndexWriterConfig(StandardAnalyzer())).use { writer ->
                repeat(3) { writer.addDocument(Document().apply { add(StringField("type", "line", Field.Store.YES)) }) }
            }
        }

    @OptIn(ExperimentalPathApi::class)
    @AfterTest
    fun cleanUp() {
        indexDir.deleteRecursively()
    }

    private fun call(request: BridgeRequest): BridgeResponse =
        BridgeCbor.decodeFromByteArray(
            BridgeResponse.serializer(),
            IndexBridgeServer.call(BridgeCbor.encodeToByteArray(BridgeRequest.serializer(), request)),
        )

    private fun callInt(request: BridgeRequest): Int = BridgeCbor.decodeFromByteArray(Int.serializer(), assertNotNull(call(request).result))

    @Test
    fun `a cursor reads its pages until closed`() {
        val index = callInt(BridgeRequest.Open(indexDir.toString(), cacheQueries = false))
        val cursor = callInt(BridgeRequest.OpenCursor(index, IndexQuery.MatchAll, emptySet()))

        assertNull(call(BridgeRequest.CursorNext(cursor, 2)).error)
        call(BridgeRequest.CloseCursor(cursor))
        assertTrue(call(BridgeRequest.CursorNext(cursor, 2)).error.orEmpty().contains("Unknown cursor"))
        call(BridgeRequest.Close(index))
    }

    @Test
    fun `closing an index closes the cursors left open on it`() {
        val index = callInt(BridgeRequest.Open(indexDir.toString(), cacheQueries = false))
        val other = callInt(BridgeRequest.Open(indexDir.toString(), cacheQueries = false))
        val cursor = callInt(BridgeRequest.OpenCursor(index, IndexQuery.MatchAll, emptySet()))
        val otherCursor = callInt(BridgeRequest.OpenCursor(other, IndexQuery.MatchAll, emptySet()))

        call(BridgeRequest.Close(index))

        assertTrue(call(BridgeRequest.CursorNext(cursor, 2)).error.orEmpty().contains("Unknown cursor"))
        // Another index's cursors are left alone
        val page = call(BridgeRequest.CursorNext(otherCursor, 5))
        assertNull(page.error)
        call(BridgeRequest.Close(other))
        assertEquals(null, call(BridgeRequest.Close(other)).error)
    }
}
