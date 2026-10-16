package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.index.IndexQuery
import io.github.kdroidfilter.seforimlibrary.search.index.KnnRequest
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndexBackend
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StoredField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/** The bridge's calls end to end, CBOR both ways: the bridged index answers as the in-process one. */
class BridgedLuceneIndexTest {
    private val texts = listOf("בראשית ברא אלהים", "ויאמר אלהים יהי אור", "ויהי ערב ויהי בקר")

    private val indexDir: Path =
        Files.createTempDirectory("bridged_index").also { dir ->
            IndexWriter(FSDirectory.open(dir), IndexWriterConfig(StandardAnalyzer())).use { writer ->
                texts.forEachIndexed { i, text ->
                    writer.addDocument(
                        Document().apply {
                            add(StringField("type", "line", Field.Store.NO))
                            add(TextField("text", text, Field.Store.YES))
                            add(StoredField("line_id", 100L + i))
                        },
                    )
                }
            }
        }

    private val local = LuceneIndexBackend(indexDir)
    private val bridged = BridgedLuceneIndex(BridgeTransport { IndexBridgeServer.call(it) }, indexDir.toString())

    @OptIn(ExperimentalPathApi::class)
    @AfterTest
    fun cleanUp() {
        bridged.close()
        local.close()
        indexDir.deleteRecursively()
    }

    private val elohim = IndexQuery.Term("text", "אלהים")

    @Test
    fun `pages carry Hebrew text, ids and scores`() {
        val fields = setOf("text", "line_id")
        val expected = local.search(elohim, 10, fields)
        val actual = bridged.search(elohim, 10, fields)

        assertEquals(expected.totalHits, actual.totalHits)
        assertEquals(expected.docs.map { it.strings["text"] }, actual.docs.map { it.strings["text"] })
        assertEquals(expected.docs.map { it.longs["line_id"] }, actual.docs.map { it.longs["line_id"] })
        assertEquals(expected.docs.map { it.score }, actual.docs.map { it.score })
    }

    @Test
    fun `cursors, analysis, counts and ids cross the bridge`() {
        assertEquals(local.analyze(texts[1]), bridged.analyze(texts[1]))
        assertEquals(2, bridged.count(elohim))
        assertContentEquals(local.collectLongs(elohim, "line_id").sorted(), bridged.collectLongs(elohim, "line_id").sorted())
        bridged.openCursor(IndexQuery.MatchAll, setOf("line_id")).use { cursor ->
            assertEquals(2, cursor.next(2).docs.size)
            assertEquals(1, cursor.next(2).docs.size)
        }
    }

    @Test
    fun `a null result crosses the bridge`() {
        assertNull(bridged.vectorEncoding("embedding"))
    }

    @Test
    fun `a KNN vector with a NaN is refused`() {
        assertFailsWith<IllegalArgumentException> { KnnRequest("embedding", 1, emptySet(), floatVector = floatArrayOf(0f, Float.NaN)) }
    }
}
