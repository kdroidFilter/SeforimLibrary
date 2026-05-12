package io.github.kdroidfilter.seforimlibrary.common.buildstate

/**
 * Table identifiers tracked by IdAllocator / build_state.db.
 *
 * `tableName` is the canonical name stored in `id_counters.table_name`.
 * `lookupKind` (when non-null) is the discriminator used in the shared
 * `id_lookup` table for simple TEXT-keyed lookups.
 */
enum class IdTable(val tableName: String, val lookupKind: String?) {
    // Lookup tables — share the `id_lookup` storage.
    SOURCE("source", "source"),
    AUTHOR("author", "author"),
    TOPIC("topic", "topic"),
    PUB_PLACE("pub_place", "pub_place"),
    PUB_DATE("pub_date", "pub_date"),
    CONNECTION_TYPE("connection_type", "connection_type"),
    CATEGORY("category", "category"),
    TOC_TEXT("tocText", "toc_text"),

    // Composite-keyed tables — dedicated storage.
    BOOK("book", null),
    LINE("line", null),
    TOC_ENTRY("tocEntry", null),
    ALT_TOC_STRUCTURE("alt_toc_structure", null),
    ALT_TOC_ENTRY("alt_toc_entry", null),
    LINK("link", null),
    ;

    companion object {
        fun fromTableName(name: String): IdTable? = values().firstOrNull { it.tableName == name }
    }
}
