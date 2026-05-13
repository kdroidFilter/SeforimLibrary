package io.github.kdroidfilter.seforimlibrary.common.buildstate

/**
 * In-memory representation of build_state.db. Built by [BuildStateReader.read]
 * (empty if no previous build_state exists) and consumed by IdAllocator.
 *
 * All maps are keyed by the entity's natural key and resolve to its stable id.
 * The [counters] map carries `next_id` per table — the value to hand out for
 * the next freshly-discovered natural key.
 */
data class BuildStateSnapshot(
    val schemaVersion: Int,
    val meta: Map<String, String>,
    val counters: Map<IdTable, Long>,
    // Lookup tables: kind -> (naturalKey -> id)
    val lookups: Map<IdTable, Map<String, Long>>,
    // (sourceName, canonicalHeTitle) -> id
    val books: Map<BookKey, Long>,
    // (bookId, contentHash, occurrenceIdx) -> id
    val lines: Map<LineKey, Long>,
    // (bookId, ancestorPath) -> id
    val tocEntries: Map<TocEntryKey, Long>,
    // (bookId, key) -> id
    val altTocStructures: Map<AltTocStructureKey, Long>,
    // (structureId, ancestorPath) -> id
    val altTocEntries: Map<AltTocEntryKey, Long>,
    // (srcLineId, tgtLineId, connectionTypeId) -> id
    val links: Map<LinkKey, Long>,
    val bookAliases: List<BookAlias>,
    // Per-book sha256 of the source artefact (Phase 2 touched-book detection).
    val sourceHashes: Map<BookKey, BookSourceHash>,
) {
    companion object {
        fun empty(): BuildStateSnapshot = BuildStateSnapshot(
            schemaVersion = BuildStateSchema.CURRENT_VERSION,
            meta = emptyMap(),
            counters = emptyMap(),
            lookups = emptyMap(),
            books = emptyMap(),
            lines = emptyMap(),
            tocEntries = emptyMap(),
            altTocStructures = emptyMap(),
            altTocEntries = emptyMap(),
            links = emptyMap(),
            bookAliases = emptyList(),
            sourceHashes = emptyMap(),
        )
    }
}

/**
 * 32-byte sha256 of the canonical source artefact for a book, paired with the
 * build version that produced it. Compared structurally on [hash] in equality.
 */
class BookSourceHash(val hash: ByteArray, val lastSeenVersion: Int) {
    init {
        require(hash.size == 32) { "BookSourceHash must be 32 bytes (sha256), got ${hash.size}" }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is BookSourceHash) return false
        return lastSeenVersion == other.lastSeenVersion && hash.contentEquals(other.hash)
    }

    override fun hashCode(): Int {
        var result = hash.contentHashCode()
        result = 31 * result + lastSeenVersion
        return result
    }

    override fun toString(): String =
        "BookSourceHash(hash=${hash.joinToString("") { "%02x".format(it) }.take(16)}…, version=$lastSeenVersion)"
}

data class BookKey(val sourceName: String, val canonicalHeTitle: String)

/** [contentHash] is a 20-byte sha1 digest. Compared structurally via [contentEquals]. */
class LineKey(val bookId: Long, val contentHash: ByteArray, val occurrenceIdx: Int) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LineKey) return false
        return bookId == other.bookId &&
            occurrenceIdx == other.occurrenceIdx &&
            contentHash.contentEquals(other.contentHash)
    }

    override fun hashCode(): Int {
        var result = bookId.hashCode()
        result = 31 * result + contentHash.contentHashCode()
        result = 31 * result + occurrenceIdx
        return result
    }
}

data class TocEntryKey(val bookId: Long, val ancestorPath: String)
data class AltTocStructureKey(val bookId: Long, val key: String)
data class AltTocEntryKey(val structureId: Long, val ancestorPath: String)
data class LinkKey(val srcLineId: Long, val tgtLineId: Long, val connectionTypeId: Long)

data class BookAlias(
    val oldKey: BookKey,
    val newKey: BookKey,
    val detectedAtVersion: Int,
)
