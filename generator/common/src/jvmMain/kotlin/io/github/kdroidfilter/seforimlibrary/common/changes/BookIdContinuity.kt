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
     * @param inScope previous keys the importer owns (e.g. not Sefaria's). Otzaria: those
     *   of the previous build; any other previous key a current book has counts as new.
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
        // Otzaria: a key only an older build had is new to this one too.
        val added = currentKeys.filterTo(HashSet()) { it !in previous || !inScope(it) }
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

        aliases.forEach { (old, new) ->
            // Otzaria: the renamed book takes the key; the gone book's id is retired, not reused.
            previous[new]?.let { gone ->
                allocator.retireBookKey(new)
                logger.w { "Book $old renamed to $new, a key an older build gave id $gone; that id is retired" }
            }
            allocator.registerBookAlias(old, new, atVersion)
        }
        logger.i { "Registered ${aliases.size} book aliases (${matchedIds.size} by title)" }
        return aliases
    }

    /**
     * Otzaria: drops the alias of each of [currentKeys] that leads to a book [keepsId] gives the
     * id to: that key is a book of its own again, beside the book its alias leads to.
     */
    fun dropAliasesLeadingTo(currentKeys: Set<BookKey>, keepsId: (BookKey) -> Boolean) {
        for (key in currentKeys) {
            val target = allocator.aliasedBookKey(key) ?: continue
            if (!keepsId(target)) continue
            logger.w { "Book $key is back beside $target, the book its alias leads to; it takes a new id" }
            allocator.dropBookAlias(key)
        }
    }

    /**
     * Otzaria: for the Sefaria importer, whose [currentKeys] the Otzaria importer skips by exact
     * title. An alias leading to another source's book of the same title gives that id back (as
     * [registerAliases] does unless the title is ambiguous); one leading to a book Otzaria still
     * imports is dropped, so that book keeps the id.
     */
    fun settleCrossSourceAliases(currentKeys: Set<BookKey>, atVersion: Int) {
        val sources = currentKeys.mapTo(HashSet()) { it.sourceName }
        val titles = currentKeys.mapTo(HashSet()) { it.canonicalHeTitle }
        for (key in currentKeys) {
            val target = allocator.aliasedBookKey(key) ?: continue
            if (target.sourceName in sources) continue
            if (target.canonicalHeTitle == key.canonicalHeTitle &&
                allocator.peekBookId(key.sourceName, key.canonicalHeTitle) == null
            ) {
                logger.i { "Book $key takes back its id from $target, which it covers" }
                allocator.registerBookAlias(target, key, atVersion)
            }
        }
        // A book of a current title is skipped, so its id stays where the alias leads.
        dropAliasesLeadingTo(currentKeys) { it.sourceName !in sources && it.canonicalHeTitle !in titles }
    }

    /**
     * Previous books whose id is not in [currentBooks] while a current book has
     * the same normalised title: the book survived but changed id.
     * Books really removed upstream (title gone) are not reported.
     *
     * @param currentBooks id to title of every book in the produced DB.
     * @param inScope Otzaria: previous keys to check (the books the previous build had).
     */
    fun findLostIds(currentBooks: Map<Long, String>, inScope: (BookKey) -> Boolean = { true }): List<LostBook> {
        val currentIdsByTitle = HashMap<String, MutableSet<Long>>()
        currentBooks.forEach { (id, title) -> currentIdsByTitle.getOrPut(normalizeTitle(title)) { HashSet() } += id }
        val lostIds = previous.values.filterTo(HashSet()) { it !in currentBooks }
        return previous.mapNotNull { (key, id) ->
            if (id !in lostIds || !inScope(key)) return@mapNotNull null
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

/**
 * Otzaria: the Otzaria keys the previous import brought in, i.e. whose source hash it
 * recorded (it records one for every book it imports). The build state keeps every key
 * it ever saw, including dead keys whose title lives on under another id, so these are
 * told apart from the books gone since.
 */
fun IdAllocator.previousOtzariaImportKeys(sefariaSource: String, buildVersion: Int): Set<BookKey> {
    val previous = bookIds().keys.filter { it.sourceName != sefariaSource }
    val recorded = previous.mapNotNull { key -> previousSourceHash(key)?.let { key to it.lastSeenVersion } }
    check(previous.isEmpty() || recorded.isNotEmpty()) {
        "The build state holds ${previous.size} Otzaria books but no source hash for any of them " +
            "(files_manifest.json), so the books of the previous build are unknown"
    }
    val last = recorded.maxOfOrNull { it.second } ?: return emptySet()
    // Older builds' books are told apart by an older version only.
    check(last < buildVersion) {
        "Build version $buildVersion is not after the previous build's ($last), so its books are unknown"
    }
    return recorded.filter { it.second == last }.mapTo(HashSet()) { it.first }
}
