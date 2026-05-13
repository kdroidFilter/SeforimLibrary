package io.github.kdroidfilter.seforimlibrary.common.ids

import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocStructure
import io.github.kdroidfilter.seforimlibrary.core.models.AltTocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.security.MessageDigest

/**
 * Glue between [IdAllocator] and [SeforimRepository]. Importers should use these
 * helpers rather than calling repo `insertXxx` directly so that ids are driven
 * by the allocator and remain stable across builds.
 *
 * Idempotent contract: calling any helper twice for the same natural key inserts
 * the row once and returns the same id. This relies on repo `*WithId` methods
 * using `ON CONFLICT DO NOTHING` for lookup tables, and on the existing
 * `entity.id > 0` short-circuit for book/line/tocEntry/altToc.
 *
 * See DELTA_UPDATE_PLAN.md §3.5.
 */
class IdAllocatorBindings(
    val allocator: IdAllocator,
    private val repo: SeforimRepository,
) {
    // ─── Lookup-table helpers ──────────────────────────────────────────────────

    suspend fun upsertSource(name: String): Long {
        val id = allocator.sourceId(name)
        repo.insertSourceWithId(id, name)
        return id
    }

    suspend fun upsertAuthor(name: String): Long {
        val id = allocator.authorId(name)
        repo.insertAuthorWithId(id, name)
        return id
    }

    suspend fun upsertTopic(name: String): Long {
        val id = allocator.topicId(name)
        repo.insertTopicWithId(id, name)
        return id
    }

    suspend fun upsertPubPlace(name: String): Long {
        val id = allocator.pubPlaceId(name)
        repo.insertPubPlaceWithId(id, name)
        return id
    }

    suspend fun upsertPubDate(date: String): Long {
        val id = allocator.pubDateId(date)
        repo.insertPubDateWithId(id, date)
        return id
    }

    suspend fun upsertConnectionType(name: String): Long {
        val id = allocator.connectionTypeId(name)
        repo.insertConnectionTypeWithId(id, name)
        return id
    }

    suspend fun upsertCategory(
        canonicalPath: String,
        parentId: Long?,
        title: String,
        level: Int,
        orderIndex: Int,
    ): Long {
        val id = allocator.categoryId(canonicalPath)
        repo.insertCategoryWithId(id, parentId, title, level, orderIndex)
        return id
    }

    suspend fun upsertTocText(text: String): Long {
        val id = allocator.tocTextId(text)
        repo.insertTocTextWithId(id, text)
        return id
    }

    // ─── Composite-keyed entities ──────────────────────────────────────────────

    /**
     * Assigns a stable id to [book] (mutating-friendly via `copy`) and inserts it.
     * Returns the id that ended up persisted.
     */
    suspend fun insertBookStable(sourceName: String, canonicalHeTitle: String, book: Book): Long {
        val id = allocator.bookId(sourceName, canonicalHeTitle)
        val withId = if (book.id == id) book else book.copy(id = id)
        repo.insertBook(withId)
        return id
    }

    suspend fun insertLineStable(line: Line, occurrenceIdx: Int = 0): Long {
        val id = allocator.lineId(line.bookId, normalisedContentHash(line.content), occurrenceIdx)
        val withId = if (line.id == id) line else line.copy(id = id)
        repo.insertLine(withId)
        return id
    }

    suspend fun insertTocEntryStable(entry: TocEntry, ancestorPath: String): Long {
        val id = allocator.tocEntryId(entry.bookId, ancestorPath)
        val withId = if (entry.id == id) entry else entry.copy(id = id)
        repo.insertTocEntry(withId)
        return id
    }

    suspend fun upsertAltTocStructureStable(structure: AltTocStructure): Long {
        val id = allocator.altTocStructureId(structure.bookId, structure.key)
        val withId = if (structure.id == id) structure else structure.copy(id = id)
        repo.upsertAltTocStructure(withId)
        return id
    }

    suspend fun insertAltTocEntryStable(entry: AltTocEntry, ancestorPath: String): Long {
        val id = allocator.altTocEntryId(entry.structureId, ancestorPath)
        val withId = if (entry.id == id) entry else entry.copy(id = id)
        repo.insertAltTocEntry(withId)
        return id
    }

    suspend fun insertLinkStable(
        sourceBookId: Long,
        targetBookId: Long,
        sourceLineId: Long,
        targetLineId: Long,
        targetLineIndex: Long,
        connectionTypeId: Long,
    ): Long {
        val id = allocator.linkId(sourceLineId, targetLineId, connectionTypeId)
        repo.insertLinkWithId(
            id = id,
            sourceBookId = sourceBookId,
            targetBookId = targetBookId,
            sourceLineId = sourceLineId,
            targetLineId = targetLineId,
            targetLineIndex = targetLineIndex,
            connectionTypeId = connectionTypeId,
        )
        return id
    }

    companion object {
        /**
         * Builds the 20-byte content-hash slot of a line's natural key.
         *
         * When a Sefaria-style stable citation reference is available (heRef
         * like "Genesis 1:1"), we hash `"REF:$ref"` so the natural key is
         * decoupled from the rendered content — Sefaria's pipeline auto-
         * generates verse prefixes `(א), (ב), …` that mutate the rendered
         * content of every following verse on a head-insert. By keying on
         * heRef we keep the line id stable across those reformatting passes
         * (see DELTA_UPDATE_PLAN.md §2.1 + PHASE1_VALIDATION.md Test C).
         *
         * Otzaria lines and Sefaria heading lines fall back to a raw content
         * hash since they have no stable citation address.
         */
        fun lineNaturalKeyHash(content: String, heRef: String?): ByteArray {
            val prefixed = if (heRef != null) "REF:$heRef" else "CT:$content"
            return MessageDigest.getInstance("SHA-1").digest(prefixed.toByteArray(Charsets.UTF_8))
        }

        /**
         * sha1 of the line content, used as part of the natural key for `line`.
         * Always returns 20 bytes — required by [IdAllocator.lineId].
         *
         * Phase 1 keeps this minimal (raw content); Phase 3 will swap in the
         * Lucene-style normalisation pipeline (HTML strip + nikud strip + ...).
         */
        fun normalisedContentHash(content: String): ByteArray =
            MessageDigest.getInstance("SHA-1").digest(content.toByteArray(Charsets.UTF_8))
    }
}
