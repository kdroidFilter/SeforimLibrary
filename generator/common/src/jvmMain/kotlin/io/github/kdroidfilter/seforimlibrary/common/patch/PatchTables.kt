package io.github.kdroidfilter.seforimlibrary.common.patch

/**
 * Per-table patch configuration. One entry per seforim.db table the
 * producer/applier knows about.
 *
 *  - [name]       : table name (also used as the suffix for upsert_<name>
 *                   and delete_<name>).
 *  - [primaryKey] : ordered column list. Forms the SQLite ON CONFLICT(...)
 *                   target. For composite keys, ALL columns are part of the
 *                   delete_* table's PK.
 *  - [updatable]  : `true` if the table has non-PK columns to update on
 *                   conflict; `false` for pure junctions (PK == all cols).
 */
internal data class PatchTable(
    val name: String,
    val primaryKey: List<String>,
    val updatable: Boolean,
)

/**
 * Canonical table order — parents (referenced) come before children
 * (referencing) for upserts. The applier runs upserts in this order and
 * deletes in reverse. The "depends on" comment on each line tracks why
 * the ordering matters.
 *
 * The schema_meta table is special-cased: keyed by a TEXT `key` column,
 * still tracked here for completeness.
 */
internal val PATCH_TABLES_IN_FK_ORDER: List<PatchTable> = listOf(
    // Lookup / atomic tables — no FK in.
    PatchTable("source",             listOf("id"),       updatable = true),
    PatchTable("author",             listOf("id"),       updatable = true),
    PatchTable("topic",              listOf("id"),       updatable = true),
    PatchTable("pub_place",          listOf("id"),       updatable = true),
    PatchTable("pub_date",           listOf("id"),       updatable = true),
    PatchTable("connection_type",    listOf("id"),       updatable = true),
    PatchTable("tocText",            listOf("id"),       updatable = true),

    // Self-ref tree — categories. parentId FK is self → same table, OK.
    PatchTable("category",           listOf("id"),       updatable = true),
    PatchTable("category_closure",   listOf("ancestorId", "descendantId"), updatable = false),

    // Book — depends on category + source.
    PatchTable("book",               listOf("id"),       updatable = true),

    // Book-attribute junctions — depend on book + author/topic/pubPlace/pubDate.
    PatchTable("book_author",        listOf("bookId", "authorId"),    updatable = false),
    PatchTable("book_topic",         listOf("bookId", "topicId"),     updatable = false),
    PatchTable("book_pub_place",     listOf("bookId", "pubPlaceId"),  updatable = false),
    PatchTable("book_pub_date",      listOf("bookId", "pubDateId"),   updatable = false),
    PatchTable("book_acronym",       listOf("bookId", "term"),        updatable = false),

    // TOC. tocEntry FK to line (lineId) and line FK to tocEntry (tocEntryId)
    // form a cycle, broken at apply time with PRAGMA defer_foreign_keys = ON.
    PatchTable("tocEntry",           listOf("id"),       updatable = true),
    PatchTable("line",               listOf("id"),       updatable = true),
    PatchTable("line_toc",           listOf("lineId"),   updatable = true),

    // Links.
    PatchTable("link",               listOf("id"),       updatable = true),
    PatchTable("book_has_links",     listOf("bookId"),   updatable = true),

    // Alternative TOCs.
    PatchTable("alt_toc_structure",  listOf("id"),       updatable = true),
    PatchTable("alt_toc_entry",      listOf("id"),       updatable = true),
    PatchTable("line_alt_toc",       listOf("lineId", "structureId"), updatable = true),

    // Defaults (book → book references).
    PatchTable("default_commentator", listOf("bookId", "commentatorBookId"), updatable = true),
    PatchTable("default_targum",      listOf("bookId", "targumBookId"),      updatable = true),

    // Versioning. Keyed by a string `key` column.
    PatchTable("schema_meta",        listOf("key"),      updatable = true),
)
