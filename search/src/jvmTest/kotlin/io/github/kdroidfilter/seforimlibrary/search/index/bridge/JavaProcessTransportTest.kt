package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.index.IndexQuery
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The Android path of the bridge, on the desktop: a Java process serving [IndexBridgeServer.main], started by a script
 * with the arguments of the Android launcher (`<java.home> <class path> <main class> [Java options...]`).
 */
class JavaProcessTransportTest {
    private val dir: Path = Files.createTempDirectory("process_bridge")
    private val indexDir: Path =
        dir.resolve("index").also { index ->
            IndexWriter(FSDirectory.open(index), IndexWriterConfig(StandardAnalyzer())).use { writer ->
                repeat(3) { writer.addDocument(Document().apply { add(StringField("type", "line", Field.Store.YES)) }) }
            }
        }
    private val launcher: File =
        dir.resolve("launcher.sh").toFile().apply {
            writeText("#!/bin/sh\njavaHome=$1; classPath=$2; main=$3; shift 3\nexec \"\$javaHome/bin/java\" -cp \"\$classPath\" \"$@\" \"\$main\"\n")
            setExecutable(true)
        }

    @OptIn(ExperimentalPathApi::class)
    @AfterTest
    fun cleanUp() {
        dir.deleteRecursively()
    }

    private fun transport() =
        JavaProcessTransport(
            launcher = launcher,
            javaHome = File(System.getProperty("java.home")),
            classPath = System.getProperty("java.class.path").split(File.pathSeparator).map(::File),
        )

    @Test
    fun `the index answers through the Java process`() {
        transport().use { transport ->
            BridgedLuceneIndex(transport, indexDir.toString()).use { index ->
                assertEquals(3, index.count(IndexQuery.MatchAll))
                assertEquals(2, index.search(IndexQuery.MatchAll, limit = 2, fields = setOf("type")).docs.size)
            }
        }
    }

    @Test
    fun `a process that died is started again on the next call`() {
        transport().use { transport ->
            BridgedLuceneIndex(transport, indexDir.toString()).use { index ->
                assertEquals(3, index.count(IndexQuery.MatchAll))
            }
            // Closing the transport ends its process; the next index starts a new one
            transport.close()
            BridgedLuceneIndex(transport, indexDir.toString()).use { index ->
                assertEquals(3, index.count(IndexQuery.MatchAll))
            }
        }
    }
}
