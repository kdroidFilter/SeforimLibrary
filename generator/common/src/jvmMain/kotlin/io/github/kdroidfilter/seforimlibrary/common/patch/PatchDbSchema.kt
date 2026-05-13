package io.github.kdroidfilter.seforimlibrary.common.patch

/**
 * DDL for the per-release `patch.db` artefact. Mirrors `DELTA_UPDATE_PLAN.md`
 * §5.3 — each `patch.db` contains:
 *
 *  - `patch_meta`     metadata (from_version, to_version, schema_version, …)
 *  - `migrations`     schema DDL executed BEFORE upserts
 *  - `blobs`          auxiliary payloads (catalog.pb, etc.) keyed by name
 *  - `upsert_<table>` one row per upserted row of the target table, with the
 *                     SAME columns + PK = `id`
 *  - `delete_<table>` one column `id` listing rows to remove from the target
 *
 * The producer writes a single `patch_global.db` (small tables + meta +
 * migrations + blobs) plus one `patch_book_<id>.db` per touched book.
 */
internal object PatchDbSchema {

    const val CURRENT_VERSION: Int = 1

    val statements: List<String> = listOf(
        """
        CREATE TABLE IF NOT EXISTS patch_meta (
            key   TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS migrations (
            version INTEGER PRIMARY KEY NOT NULL,
            sql     TEXT NOT NULL
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS blobs (
            name    TEXT PRIMARY KEY NOT NULL,
            content BLOB NOT NULL
        )
        """.trimIndent(),

        // ─── upsert_<table> tables: same shape as target tables ─────────────────
        // We mirror only the columns that the importer writes; computed/derived
        // columns are filled in by the apply step (or recomputed client-side).

        """
        CREATE TABLE IF NOT EXISTS upsert_book (
            id                       INTEGER PRIMARY KEY NOT NULL,
            categoryId               INTEGER NOT NULL,
            sourceId                 INTEGER NOT NULL,
            title                    TEXT NOT NULL,
            heRef                    TEXT,
            heShortDesc              TEXT,
            notesContent             TEXT,
            orderIndex               INTEGER NOT NULL DEFAULT 999,
            totalLines               INTEGER NOT NULL DEFAULT 0,
            isBaseBook               INTEGER NOT NULL DEFAULT 0,
            hasTargumConnection      INTEGER NOT NULL DEFAULT 0,
            hasReferenceConnection   INTEGER NOT NULL DEFAULT 0,
            hasSourceConnection      INTEGER NOT NULL DEFAULT 0,
            hasCommentaryConnection  INTEGER NOT NULL DEFAULT 0,
            hasOtherConnection       INTEGER NOT NULL DEFAULT 0,
            hasAltStructures         INTEGER NOT NULL DEFAULT 0,
            hasTeamim                INTEGER NOT NULL DEFAULT 0,
            hasNekudot               INTEGER NOT NULL DEFAULT 0
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS upsert_line (
            id          INTEGER PRIMARY KEY NOT NULL,
            bookId      INTEGER NOT NULL,
            lineIndex   INTEGER NOT NULL,
            content     TEXT NOT NULL,
            heRef       TEXT,
            tocEntryId  INTEGER,
            charCount   INTEGER NOT NULL DEFAULT 0
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS upsert_link (
            id                 INTEGER PRIMARY KEY NOT NULL,
            sourceBookId       INTEGER NOT NULL,
            targetBookId       INTEGER NOT NULL,
            sourceLineId       INTEGER NOT NULL,
            targetLineId       INTEGER NOT NULL,
            targetLineIndex    INTEGER NOT NULL,
            connectionTypeId   INTEGER NOT NULL
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS upsert_tocEntry (
            id           INTEGER PRIMARY KEY NOT NULL,
            bookId       INTEGER NOT NULL,
            parentId     INTEGER,
            textId       INTEGER NOT NULL,
            level        INTEGER NOT NULL,
            lineId       INTEGER,
            isLastChild  INTEGER NOT NULL DEFAULT 0,
            hasChildren  INTEGER NOT NULL DEFAULT 0
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS upsert_tocText (
            id   INTEGER PRIMARY KEY NOT NULL,
            text TEXT NOT NULL
        )
        """.trimIndent(),

        """
        CREATE TABLE IF NOT EXISTS upsert_category (
            id          INTEGER PRIMARY KEY NOT NULL,
            parentId    INTEGER,
            title       TEXT NOT NULL,
            level       INTEGER NOT NULL DEFAULT 0,
            orderIndex  INTEGER NOT NULL DEFAULT 999
        )
        """.trimIndent(),

        // Lookup tables — included for completeness.
        "CREATE TABLE IF NOT EXISTS upsert_source (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS upsert_author (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS upsert_topic  (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS upsert_pub_place (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS upsert_pub_date  (id INTEGER PRIMARY KEY NOT NULL, date TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS upsert_connection_type (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",

        """
        CREATE TABLE IF NOT EXISTS upsert_line_toc (
            lineId      INTEGER NOT NULL,
            tocEntryId  INTEGER NOT NULL,
            PRIMARY KEY (lineId, tocEntryId)
        )
        """.trimIndent(),

        // ─── delete_<table> tables: just the ids to drop ───────────────────────
        "CREATE TABLE IF NOT EXISTS delete_book      (id INTEGER PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS delete_line      (id INTEGER PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS delete_link      (id INTEGER PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS delete_tocEntry  (id INTEGER PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS delete_tocText   (id INTEGER PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS delete_category  (id INTEGER PRIMARY KEY)",
    )
}
