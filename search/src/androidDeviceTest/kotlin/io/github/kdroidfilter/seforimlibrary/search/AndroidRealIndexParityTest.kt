package io.github.kdroidfilter.seforimlibrary.search

import android.os.SystemClock
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndexBackend
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File

/**
 * The search on ART (Android 15+), with Lucene in process: dumps what it answers on a real index, like
 * RealIndexParityTest on the JVM. The index and lexical.db go in the test app's files dir, under seforim/ (adb root, push,
 * chown to the app); the dump comes back as parity-android.txt there. Skipped without an index.
 */
@RunWith(AndroidJUnit4::class)
class AndroidRealIndexParityTest {
    @Test
    fun dumpRealIndex() {
        val files = InstrumentationRegistry.getInstrumentation().targetContext.filesDir
        val indexDir = File(files, "seforim/seforim.db.lucene")
        println("Parity index: $indexDir exists=${indexDir.exists()} files=${indexDir.list()?.size}")
        if (!indexDir.isDirectory) return
        val arguments = InstrumentationRegistry.getArguments()
        arguments.getString("luceneQueryCache")?.let { System.setProperty("seforim.lucene.queryCache", it) }
        val engine =
            LexicalSearchEngine(
                index = LuceneIndexBackend(indexDir.toPath()),
                dictionary = lazy { MagicDictionaryIndex.load(HebrewTextUtils::normalizeHebrew, File(indexDir.parentFile, "lexical.db").path) },
            )
        val start = SystemClock.elapsedRealtime()
        val dump = engine.use { runBlocking { SearchDump.dump(it) } }
        println("Parity dump in ${SystemClock.elapsedRealtime() - start} ms")
        File(files, "parity-android.txt").writeText(dump)
    }
}
