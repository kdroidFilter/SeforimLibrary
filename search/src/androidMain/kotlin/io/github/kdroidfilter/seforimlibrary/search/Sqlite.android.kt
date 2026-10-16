package io.github.kdroidfilter.seforimlibrary.search

import android.database.sqlite.SQLiteDatabase

/** Through Android's SQLite: one connection, which serializes the queries of every thread. */
actual fun openReadOnlySqlite(path: String): SqliteReader = AndroidSqliteReader(path)

private class AndroidSqliteReader(
    path: String,
) : SqliteReader {
    private val db = SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READONLY or SQLiteDatabase.NO_LOCALIZED_COLLATORS)

    override fun query(
        sql: String,
        args: List<String>,
    ): List<Map<String, String?>> =
        db.rawQuery(sql, args.toTypedArray()).use { cursor ->
            val columns = cursor.columnNames
            val rows = ArrayList<Map<String, String?>>(cursor.count)
            while (cursor.moveToNext()) rows += columns.indices.associate { columns[it] to cursor.getString(it) }
            rows
        }

    override fun close() {
        if (db.isOpen) db.close()
    }
}
