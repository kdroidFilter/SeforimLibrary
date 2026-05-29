package io.github.kdroidfilter.seforimlibrary.common.db

/**
 * Page size (in bytes) for every generated Seforim database.
 *
 * 16 KiB (instead of SQLite's 4 KiB default) lowers the B-tree depth, packs the
 * long HTML `content` rows with far fewer overflow pages, and yields better
 * sequential / mmap read throughput on the read-only reader corpus.
 *
 * Must be applied to a connection BEFORE the first table is created (a fresh DB)
 * or carried through a `VACUUM` / `VACUUM INTO`. Setting it on an already
 * populated database outside of a VACUUM is silently ignored by SQLite, and it
 * cannot be changed at all while the database is in WAL mode.
 */
const val SEFORIM_DB_PAGE_SIZE: Int = 16_384

/** `PRAGMA page_size` statement that sets the page size to [SEFORIM_DB_PAGE_SIZE]. */
const val SEFORIM_DB_PAGE_SIZE_PRAGMA: String = "PRAGMA page_size=$SEFORIM_DB_PAGE_SIZE"
