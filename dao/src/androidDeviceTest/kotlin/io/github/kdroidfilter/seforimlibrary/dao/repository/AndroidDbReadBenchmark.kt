package io.github.kdroidfilter.seforimlibrary.dao.repository

import android.util.Log
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import app.cash.sqldelight.driver.android.AndroidSqliteDriver
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File

/**
 * [DbReadWorkload] on Android, through SQLDelight's Android driver, on a database pushed to the app's external files
 * directory. Runs only with the instrumentation argument benchDb (the file's name there); `phase` runs one phase, the
 * host dropping the page cache before (an app can't: posix_fadvise isn't public on Android):
 *
 *   adb push seforim.db /sdcard/Android/data/io.github.kdroidfilter.seforimlibrary.dao.test/files/
 *   adb root && adb shell 'echo 3 > /proc/sys/vm/drop_caches'
 *   adb shell am instrument -w -e benchDb seforim.db -e phase 0 \
 *     io.github.kdroidfilter.seforimlibrary.dao.test/androidx.test.runner.AndroidJUnitRunner
 */
@RunWith(AndroidJUnit4::class)
class AndroidDbReadBenchmark {
    @Test
    fun benchmark() {
        val name = InstrumentationRegistry.getArguments().getString("benchDb") ?: return
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val file = File(context.getExternalFilesDir(null), name)
        check(file.exists()) { "No database at $file" }
        val driver = AndroidSqliteDriver(SeforimDb.Schema, context, file.absolutePath)
        val repository = SeforimRepository(file.absolutePath, driver)
        report("BENCH ${file.name} ${file.length() / 1_048_576} MB on Android ${android.os.Build.VERSION.RELEASE}")
        val phases = InstrumentationRegistry.getArguments().getString("phase")?.let { setOf(it.toInt()) }
        runBlocking { DbReadWorkload(driver, repository, {}, ::readBytes).run(phases, ::report) }
        driver.close()
    }

    private fun report(line: String) {
        Log.i("DbReadBenchmark", line)
        println(line)
    }

    private fun readBytes(): Long =
        runCatching {
            File("/proc/self/io").readLines().first { it.startsWith("read_bytes:") }.substringAfter(':').trim().toLong()
        }.getOrDefault(-1)
}
