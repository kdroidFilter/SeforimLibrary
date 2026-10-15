package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Path
import kotlin.test.Test

/**
 * Dumps what the engine answers on a real index (-PindexDir=<seforim.db.lucene>) into build/parity/jvm.txt, for the
 * parity with Android and iOS (see SearchDump). Skipped without an index.
 */
class RealIndexParityTest {
    @Test
    fun dumpRealIndex() {
        val indexDir = System.getProperty("seforimIndexDir") ?: return
        val out = File(System.getProperty("parityOut") ?: "build/parity/jvm.txt")
        LuceneSearchEngine(Path.of(indexDir)).use { engine ->
            out.parentFile.mkdirs()
            out.writeText(runBlocking { SearchDump.dump(engine) })
        }
        println("Parity dump: ${out.absolutePath}")
    }
}
