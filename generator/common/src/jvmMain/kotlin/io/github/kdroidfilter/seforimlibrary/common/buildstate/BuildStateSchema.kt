package io.github.kdroidfilter.seforimlibrary.common.buildstate

/**
 * DDL for the generator-side build_state database.
 *
 * This database is a generator-only artifact (never shipped to the client).
 * It memorises the mapping `natural_key -> stable id` for every entity type
 * across builds, so that subsequent builds can re-use the same primary keys
 * and produce minimal SQLite deltas.
 *
 * See DELTA_UPDATE_PLAN.md §3.4.
 */
internal object BuildStateSchema {

    const val CURRENT_VERSION: Int = 1

    /** Tables created in order — execute one statement at a time. */
    val statements: List<String> = listOf(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        )
        """.trimIndent(),

        // Per-table id counters. next_id is the value to hand out for a fresh natural key.
        """
        CREATE TABLE IF NOT EXISTS id_counters (
            table_name TEXT PRIMARY KEY NOT NULL,
            next_id    INTEGER NOT NULL
        )
        """.trimIndent(),

        // ─── Lookup tables (single string natural key) ─────────────────────────
        // Used for: source, author, topic, pub_place, pub_date, connection_type,
        //           category, tocText. Discriminated by `kind`.
        """
        CREATE TABLE IF NOT EXISTS id_lookup (
            kind         TEXT    NOT NULL,
            natural_key  TEXT    NOT NULL,
            id           INTEGER NOT NULL,
            PRIMARY KEY (kind, natural_key)
        )
        """.trimIndent(),
        "CREATE INDEX IF NOT EXISTS idx_id_lookup_kind_id ON id_lookup(kind, id)",

        // ─── Book ──────────────────────────────────────────────────────────────
        // Natural key: (source_name, canonical_he_title).
        """
        CREATE TABLE IF NOT EXISTS id_book (
            source_name           TEXT    NOT NULL,
            canonical_he_title    TEXT    NOT NULL,
            id                    INTEGER NOT NULL,
            PRIMARY KEY (source_name, canonical_he_title)
        )
        """.trimIndent(),
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_id_book_id ON id_book(id)",

        // ─── Line ──────────────────────────────────────────────────────────────
        // Natural key: (book_id, content_hash, occurrence_index).
        // content_hash is sha1 of the normalised content (20 bytes).
        """
        CREATE TABLE IF NOT EXISTS id_line (
            book_id          INTEGER NOT NULL,
            content_hash     BLOB    NOT NULL,
            occurrence_idx   INTEGER NOT NULL,
            id               INTEGER NOT NULL,
            PRIMARY KEY (book_id, content_hash, occurrence_idx)
        )
        """.trimIndent(),
        "CREATE INDEX IF NOT EXISTS idx_id_line_book ON id_line(book_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_id_line_id ON id_line(id)",

        // ─── TOC entry ─────────────────────────────────────────────────────────
        // Natural key: (book_id, ancestor_path) where ancestor_path is a slash-
        // separated chain of TocText ids from root to this entry.
        """
        CREATE TABLE IF NOT EXISTS id_toc_entry (
            book_id        INTEGER NOT NULL,
            ancestor_path  TEXT    NOT NULL,
            id             INTEGER NOT NULL,
            PRIMARY KEY (book_id, ancestor_path)
        )
        """.trimIndent(),
        "CREATE INDEX IF NOT EXISTS idx_id_toc_entry_book ON id_toc_entry(book_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_id_toc_entry_id ON id_toc_entry(id)",

        // ─── Alt TOC structure / entry ─────────────────────────────────────────
        // Natural key alt_structure: (book_id, key).
        """
        CREATE TABLE IF NOT EXISTS id_alt_toc_structure (
            book_id INTEGER NOT NULL,
            key     TEXT    NOT NULL,
            id      INTEGER NOT NULL,
            PRIMARY KEY (book_id, key)
        )
        """.trimIndent(),
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_id_alt_toc_struct_id ON id_alt_toc_structure(id)",

        // Natural key alt_entry: (structure_id, ancestor_path).
        """
        CREATE TABLE IF NOT EXISTS id_alt_toc_entry (
            structure_id   INTEGER NOT NULL,
            ancestor_path  TEXT    NOT NULL,
            id             INTEGER NOT NULL,
            PRIMARY KEY (structure_id, ancestor_path)
        )
        """.trimIndent(),
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_id_alt_toc_entry_id ON id_alt_toc_entry(id)",

        // ─── Link ──────────────────────────────────────────────────────────────
        // Natural key: (src_line_id, tgt_line_id, connection_type_id).
        // Stable by construction since line ids are stable.
        """
        CREATE TABLE IF NOT EXISTS id_link (
            src_line_id         INTEGER NOT NULL,
            tgt_line_id         INTEGER NOT NULL,
            connection_type_id  INTEGER NOT NULL,
            id                  INTEGER NOT NULL,
            PRIMARY KEY (src_line_id, tgt_line_id, connection_type_id)
        )
        """.trimIndent(),
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_id_link_id ON id_link(id)",

        // ─── Book aliases (rename detection — §4.5) ────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS book_aliases (
            old_source_name        TEXT    NOT NULL,
            old_canonical_he_title TEXT    NOT NULL,
            new_source_name        TEXT    NOT NULL,
            new_canonical_he_title TEXT    NOT NULL,
            detected_at_version    INTEGER NOT NULL,
            PRIMARY KEY (old_source_name, old_canonical_he_title)
        )
        """.trimIndent(),

        // ─── Per-book source content hashes (touched-book detection — §6.2) ────
        // `source_hash` is a 32-byte sha256 of the canonical source artefact for
        // the book (Sefaria: merged.json; Otzaria: txt manifest entry).
        // `last_seen_version` records the build version that last observed this
        // hash for the book — used to prune stale entries.
        """
        CREATE TABLE IF NOT EXISTS book_source_hashes (
            source_name           TEXT    NOT NULL,
            canonical_he_title    TEXT    NOT NULL,
            source_hash           BLOB    NOT NULL,
            last_seen_version     INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (source_name, canonical_he_title)
        )
        """.trimIndent(),
    )
}
