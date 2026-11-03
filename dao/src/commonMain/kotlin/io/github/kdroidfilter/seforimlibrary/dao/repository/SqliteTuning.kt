package io.github.kdroidfilter.seforimlibrary.dao.repository

/** SQLite's page cache, in KiB. */
internal expect val SQLITE_CACHE_KIB: Long

/** How much of the database SQLite maps into memory, in bytes (0: none). */
internal expect val SQLITE_MMAP_BYTES: Long
