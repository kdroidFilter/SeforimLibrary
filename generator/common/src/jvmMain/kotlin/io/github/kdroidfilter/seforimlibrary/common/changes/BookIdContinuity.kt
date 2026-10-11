package io.github.kdroidfilter.seforimlibrary.common.changes

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator

/**
 * Keeps book ids stable when an upstream library re-keys a book, i.e. moves it
 * to another source or fixes its title. A book's natural key is
 * `(sourceName, canonicalHeTitle)`, so either change would otherwise allocate a
 * fresh id and orphan every user note pointing at the book or its lines.
 *
 * Run [registerAliases] before the importer allocates any book id, with the
 * exact set of keys it is about to allocate. Run [findLostIds] once the import
 * is done to make sure no previous book silently changed id.
 */
class BookIdContinuity(
    private val allocator: IdAllocator,
    private val renameDetector: BookRenameDetector = BookRenameDetector(),
    private val logger: Logger = Logger.withTag("BookIdContinuity"),
) {

    /** A previous book whose id is no longer used while its title still is. */
    data class LostBook(val key: BookKey, val id: Long, val currentIds: Set<Long>)

    // Previous keys and ids, captured before the importer allocates anything.
    private val previous: Map<BookKey, Long> = allocator.bookIds()

    /**
     * Pairs every previous book missing from [currentKeys] with the new key that
     * replaces it, and registers the pair on the allocator:
     *  1. same normalised title, when the pairing is one-to-one (source moves,
     *     whitespace or quote fixes);
     *  2. otherwise line-hash Jaccard via [BookRenameDetector] (real renames).
     *
     * @param currentKeys every key the importer will allocate in this build.
     * @param inScope previous keys the importer owns (e.g. not Sefaria's).
     * @param currentLineHashes sha1 of each line of a new book, as hashed by the
     *   importer for its line natural keys; `null` when unavailable.
     * @return the registered aliases, old key to new key.
     */
    fun registerAliases(
        currentKeys: Set<BookKey>,
        inScope: (BookKey) -> Boolean,
        atVersion: Int,
        currentLineHashes: (BookKey) -> List<ByteArray>?,
    ): Map<BookKey, BookKey> {
        val takenIds = currentKeys.mapNotNullTo(HashSet()) { previous[it] }
        // An id may sit under several keys (earlier aliases): keep one entry per id.
        val orphans = previous.entries
            .filter { (key, id) -> key !in currentKeys && id !in takenIds && inScope(key) }
            .groupBy({ it.value }, { it.key })
        val added = currentKeys.filterTo(HashSet()) { it !in previous }
        if (orphans.isEmpty() || added.isEmpty()) return emptyMap()

        val aliases = LinkedHashMap<BookKey, BookKey>()

        // 1. One-to-one title matches.
        val orphanIdsByTitle = HashMap<String, MutableSet<Long>>()
        orphans.forEach { (id, keys) ->
            keys.forEach { orphanIdsByTitle.getOrPut(normalizeTitle(it.canonicalHeTitle)) { HashSet() } += id }
        }
        val addedByTitle = added.groupBy { normalizeTitle(it.canonicalHeTitle) }
        val matchedIds = HashSet<Long>()
        for ((title, newKeys) in addedByTitle) {
            val ids = orphanIdsByTitle[title] ?: continue
            if (ids.size != 1 || newKeys.size != 1) {
                logger.w { "Ambiguous title '$title': ${ids.size} previous ids, new keys $newKeys; trying line hashes" }
                continue
            }
            val id = ids.single()
            aliases[orphans.getValue(id).first()] = newKeys.single()
            matchedIds += id
        }

        // 2. Line-hash similarity for what is left.
        val remainingOrphans = orphans.filterKeys { it !in matchedIds }
        val remainingAdded = added - aliases.values.toSet()
        if (remainingOrphans.isNotEmpty() && remainingAdded.isNotEmpty()) {
            val previousHashes = allocator.lineHashesByBook(remainingOrphans.keys)
            val removed = remainingOrphans.mapNotNull { (id, keys) ->
                previousHashes[id]?.let { keys.first() to it.toLineHashSet() }
            }.toMap()
            val addedHashes = remainingAdded.mapNotNull { key ->
                currentLineHashes(key)?.let { key to it.toLineHashSet() }
            }.toMap()
            // Otzaria: only a match that is unique both ways keeps an id.
            renameDetector.detectUnique(removed, addedHashes).forEach { aliases[it.oldKey] = it.newKey }
        }

        aliases.forEach { (old, new) -> allocator.registerBookAlias(old, new, atVersion) }
        logger.i { "Registered ${aliases.size} book aliases (${matchedIds.size} by title)" }
        return aliases
    }

    /**
     * Previous books whose id is not in [currentBooks] while a current book has
     * the same normalised title: the book survived but changed id.
     * Books really removed upstream (title gone) are not reported.
     *
     * @param currentBooks id to title of every book in the produced DB.
     */
    fun findLostIds(currentBooks: Map<Long, String>): List<LostBook> {
        val currentIdsByTitle = HashMap<String, MutableSet<Long>>()
        currentBooks.forEach { (id, title) -> currentIdsByTitle.getOrPut(normalizeTitle(title)) { HashSet() } += id }
        val lostIds = previous.values.filterTo(HashSet()) { it !in currentBooks }
        return previous.mapNotNull { (key, id) ->
            if (id !in lostIds) return@mapNotNull null
            val currentIds = currentIdsByTitle[normalizeTitle(key.canonicalHeTitle)] ?: return@mapNotNull null
            LostBook(key, id, currentIds)
        }
    }

    private fun List<ByteArray>.toLineHashSet() =
        BookRenameDetector.LineHashSet(mapTo(HashSet()) { BookRenameDetector.ContentHash(it) })

    companion object {
        private val WHITESPACE = Regex("\\s+")

        /** Folds the title variations upstream fixes most often: spacing and quote marks. */
        fun normalizeTitle(title: String): String = title
            .replace('“', '"').replace('”', '"').replace('"', '״')
            .replace('‘', '\'').replace('’', '\'').replace('\'', '׳')
            .replace("׳׳", "״")
            .replace(WHITESPACE, " ")
            .trim()
    }
}
