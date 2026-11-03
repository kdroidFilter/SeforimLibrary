package io.github.kdroidfilter.seforimlibrary.dao.repository

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlDriver
import kotlin.random.Random
import kotlin.time.TimeSource

/**
 * The app's reads on a real database (opening books, jumping, scrolling, lines by id, commentaries), each phase timed cold
 * then warm with the bytes it read from the disk: the benchmarks of the JVM (DbReadBenchmark) and of Android
 * (AndroidDbReadBenchmark) run it.
 *
 * @param dropFromPageCache drops the database file from the OS page cache
 * @param readBytes the bytes this process made the disk read so far, or -1
 */
class DbReadWorkload(
    private val driver: SqlDriver,
    private val repository: SeforimRepository,
    private val dropFromPageCache: () -> Unit,
    private val readBytes: () -> Long,
) {
    /** Runs the [phases] given by index (all by default). */
    suspend fun run(phases: Set<Int>? = null, report: (String) -> Unit) {
        val maxLineId = driver.longs("SELECT MAX(id) FROM line").single()
        val books = driver.longs("SELECT id FROM book WHERE totalLines > 100 ORDER BY id")
        val random = Random(42)
        val openBooks = List(150) { books[random.nextInt(books.size)] }
        val pages = List(300) { books[random.nextInt(books.size)] to random.nextInt(0, 100) }
        val longBooks = driver.longs("SELECT id FROM book WHERE totalLines > 1500 ORDER BY id")
        val readBooks = List(20) { longBooks[random.nextInt(longBooks.size)] }
        val lineIds = List(3000) { random.nextLong(1, maxLineId + 1) }
        val commentaryLines = List(300) { random.nextLong(1, maxLineId + 1) }

        val all: List<Pair<String, suspend () -> Unit>> =
            listOf(
                "open book (char counts + first 50 lines)" to {
                    for (b in openBooks) {
                        repository.getBookCharCounts(b)
                        repository.getLines(b, 0, 49)
                    }
                },
                "jump (300 pages of 50 lines, random books)" to {
                    for ((b, start) in pages) repository.getLines(b, start, start + 49)
                },
                "scroll (20 books, 30 pages of 50 lines each)" to {
                    for (b in readBooks) for (page in 0 until 30) repository.getLines(b, page * 50, page * 50 + 49)
                },
                "random lines by id (3000)" to {
                    for (id in lineIds) repository.getLine(id)
                },
                "commentaries with texts (300 lines)" to {
                    for (id in commentaryLines) repository.getCommentariesForLines(listOf(id))
                },
            )
        for ((index, entry) in all.withIndex()) {
            if (phases != null && index !in phases) continue
            val (name, phase) = entry
            dropFromPageCache()
            val (coldMs, coldBytes) = measure(phase)
            val (warmMs, _) = measure(phase)
            report(
                "BENCH ${name.padEnd(42)} cold ${coldMs.toString().padStart(6)} ms " +
                    "${(coldBytes / 1_048_576.0).format1().padStart(7)} MB read | warm ${warmMs.toString().padStart(5)} ms",
            )
        }
    }

    private suspend fun measure(block: suspend () -> Unit): Pair<Long, Long> {
        val before = readBytes()
        val mark = TimeSource.Monotonic.markNow()
        block()
        val ms = mark.elapsedNow().inWholeMilliseconds
        return ms to if (before < 0) -1 else readBytes() - before
    }

    private fun Double.format1(): String = if (this < 0) "?" else ((this * 10).toLong() / 10.0).toString()

    private fun SqlDriver.longs(sql: String): List<Long> =
        executeQuery(null, sql, { c -> QueryResult.Value(buildList { while (c.next().value) add(c.getLong(0)!!) }) }, 0).value
}
