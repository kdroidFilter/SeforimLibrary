package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.CoroutineScope

/**
 * The text of [html] as snippet text is built from: tags dropped, special characters escaped as HTML entities (what
 * `Jsoup.clean(html, Safelist.none())` returns, which the JVM and Android keep using).
 */
internal expect fun escapedText(html: String): String

/** Runs [block] to completion on the calling thread, for the synchronous parts of the API that wait on I/O. */
internal expect fun <T> runBlockingIo(block: suspend CoroutineScope.() -> T): T

/**
 * A read-only SQLite database (the lexical dictionary). Safe to use from several threads at once.
 */
interface SqliteReader : AutoCloseable {
    /** The rows [sql] returns with [args] bound in order, each as its column names to their text values. */
    fun query(
        sql: String,
        args: List<String> = emptyList(),
    ): List<Map<String, String?>>
}

/** Opens the SQLite database at [path] read-only; throws when it can't be opened. */
expect fun openReadOnlySqlite(path: String): SqliteReader
