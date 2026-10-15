@file:OptIn(ExperimentalForeignApi::class)

package io.github.kdroidfilter.seforimlibrary.search

import io.github.kdroidfilter.seforimlibrary.search.sqlite3.SQLITE_OK
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.SQLITE_OPEN_FULLMUTEX
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.SQLITE_OPEN_READONLY
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.SQLITE_ROW
import cnames.structs.sqlite3
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_bind_text
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_close_v2
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_column_count
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_column_name
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_column_text
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_errmsg
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_finalize
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_open_v2
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_prepare_v2
import cnames.structs.sqlite3_stmt
import io.github.kdroidfilter.seforimlibrary.search.sqlite3.sqlite3_step
import kotlinx.cinterop.CFunction
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.toCPointer
import kotlinx.cinterop.toKString
import kotlinx.cinterop.value

/** Through the system SQLite, one connection in serialized mode: its queries are safe from any thread. */
actual fun openReadOnlySqlite(path: String): SqliteReader = NativeSqliteReader(path)

// SQLITE_TRANSIENT: SQLite copies the bound text (a C macro cinterop can't map)
private val TRANSIENT = (-1L).toCPointer<CFunction<(COpaquePointer?) -> Unit>>()

private class NativeSqliteReader(
    path: String,
) : SqliteReader {
    private val db: CPointer<sqlite3> =
        memScoped {
            val out = alloc<CPointerVar<sqlite3>>()
            val status = sqlite3_open_v2(path, out.ptr, SQLITE_OPEN_READONLY or SQLITE_OPEN_FULLMUTEX, null)
            val opened = out.value
            if (status != SQLITE_OK || opened == null) {
                val message = opened?.let { sqlite3_errmsg(it)?.toKString() }
                sqlite3_close_v2(opened)
                error("Can't open $path: ${message ?: status}")
            }
            opened
        }

    override fun query(
        sql: String,
        args: List<String>,
    ): List<Map<String, String?>> =
        memScoped {
            val out = alloc<CPointerVar<sqlite3_stmt>>()
            check(sqlite3_prepare_v2(db, sql, -1, out.ptr, null) == SQLITE_OK) { "SQLite: ${sqlite3_errmsg(db)?.toKString()}" }
            val stmt = out.value!!
            try {
                args.forEachIndexed { i, arg -> sqlite3_bind_text(stmt, i + 1, arg, -1, TRANSIENT) }
                val columns = (0 until sqlite3_column_count(stmt)).map { sqlite3_column_name(stmt, it)?.toKString() ?: "" }
                val rows = ArrayList<Map<String, String?>>()
                while (sqlite3_step(stmt) == SQLITE_ROW) {
                    rows += columns.indices.associate { columns[it] to sqlite3_column_text(stmt, it)?.reinterpret<kotlinx.cinterop.ByteVar>()?.toKString() }
                }
                rows
            } finally {
                sqlite3_finalize(stmt)
            }
        }

    override fun close() {
        sqlite3_close_v2(db)
    }
}
