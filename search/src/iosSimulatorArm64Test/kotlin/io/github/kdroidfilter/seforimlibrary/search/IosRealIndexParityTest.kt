package io.github.kdroidfilter.seforimlibrary.search

import io.github.kdroidfilter.seforimlibrary.search.index.bridge.EmbeddedJvm
import io.github.kdroidfilter.seforimlibrary.search.index.bridge.JvmLuceneIndex
import kotlinx.coroutines.runBlocking
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.writeString
import platform.Foundation.NSProcessInfo
import kotlin.time.measureTimedValue
import kotlin.test.Test

/**
 * The search on iOS, end to end in the simulator: Lucene in the embedded Java runtime, everything else here. Dumps what
 * it answers on a real index like RealIndexParityTest does on the JVM (SEFORIM_INDEX_DIR, SEFORIM_JVM_CLASSPATH and
 * SEFORIM_PARITY_OUT, passed by the iosSimulatorArm64Test task). Skipped without an index.
 */
class IosRealIndexParityTest {
    @Test
    fun dumpRealIndex() {
        val env = NSProcessInfo.processInfo.environment
        val indexDir = env["SEFORIM_INDEX_DIR"] as String? ?: return
        val classPath = (env["SEFORIM_JVM_CLASSPATH"] as String).split(':')
        val (jvm, startup) = measureTimedValue { EmbeddedJvm.start(classPath, listOf("-Xmx768m") + (env["SEFORIM_JVM_OPTIONS"] as String?)?.split(' ').orEmpty()) }
        println("Java runtime started in $startup")
        val dictionary = indexDir.substringBeforeLast('/') + "/lexical.db"
        val engine =
            LexicalSearchEngine(
                index = JvmLuceneIndex(jvm, indexDir),
                dictionary = lazy { MagicDictionaryIndex.load(HebrewTextUtils::normalizeHebrew, dictionary) },
            )
        engine.use {
            val (dump, took) = measureTimedValue { runBlocking { SearchDump.dump(it) } }
            println("Parity dump in $took")
            val out = Path(env["SEFORIM_PARITY_OUT"] as String)
            SystemFileSystem.sink(out).buffered().use { sink -> sink.writeString(dump) }
            println("Parity dump: $out")
        }
    }
}
