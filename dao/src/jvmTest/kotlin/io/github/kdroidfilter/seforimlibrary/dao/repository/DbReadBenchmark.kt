package io.github.kdroidfilter.seforimlibrary.dao.repository

import app.cash.sqldelight.db.QueryResult
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test

/**
 * [DbReadWorkload] on the desktop, through the app's driver. Runs only with BENCH_DB set (a database the test may open
 * read-write); BENCH_PROFILE=android applies Android's SQLite memory (16 MB cache, no map).
 *
 *   BENCH_DB=/path/seforim.db ./gradlew :dao:jvmTest --tests '*DbReadBenchmark*' --rerun -i
 */
class DbReadBenchmark {
    @Test
    fun benchmark() {
        val path = System.getenv("BENCH_DB") ?: return
        val android = System.getenv("BENCH_PROFILE") == "android"
        // The app's driver: one persistent connection, prepared statements cached
        val driver = PersistentSqliteDriver("jdbc:sqlite:$path")
        val repository = SeforimRepository(path, driver)
        if (android) {
            driver.execute(null, "PRAGMA cache_size=-16000", 0)
            driver.executeQuery(null, "PRAGMA mmap_size=0", { QueryResult.Value(it.next().value) }, 0)
        }
        println("BENCH ${File(path).name} ${File(path).length() / 1_048_576} MB profile=${if (android) "android" else "desktop"}")
        runBlocking {
            DbReadWorkload(driver, repository, { dropFromPageCache(path) }, ::readBytes).run(report = ::println)
        }
        driver.close()
    }

    // Bytes this process made the disk read (page cache misses)
    private fun readBytes(): Long =
        File("/proc/self/io").readLines().first { it.startsWith("read_bytes:") }.substringAfter(':').trim().toLong()

    // Also clears the simulated cache of a slow storage (SLOWIO_DROP, see COMPACT_DB.md)
    private fun dropFromPageCache(path: String) {
        val script =
            "import os,sys\nfd=os.open(sys.argv[1],os.O_RDONLY)\nos.fdatasync(fd)\nos.posix_fadvise(fd,0,0,os.POSIX_FADV_DONTNEED)\n" +
                "d=os.environ.get('SLOWIO_DROP')\nif d: open(d,'w').write('x')"
        ProcessBuilder("python3", "-c", script, path).inheritIO().start().waitFor()
    }
}
