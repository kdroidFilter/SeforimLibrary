package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger

internal data class SefariaBlacklists(
    val authorKeys: Set<String>,
    val bookTitleKeys: Set<String>,
    val bookPathKeys: Set<String>
) {
    fun isEmpty(): Boolean = authorKeys.isEmpty() && bookTitleKeys.isEmpty() && bookPathKeys.isEmpty()

    companion object {
        val Empty = SefariaBlacklists(
            authorKeys = emptySet(),
            bookTitleKeys = emptySet(),
            bookPathKeys = emptySet()
        )
    }
}

internal data class BlacklistFilterResult(
    val payloads: List<BookPayload>,
    val skippedTotal: Int,
    val skippedByBook: Int,
    val skippedByAuthor: Int,
    val skippedBookExamples: List<String>,
    val skippedAuthorExamples: List<String>,
    val skippedNormalizedPaths: Set<String>
)

internal fun loadSefariaBlacklists(classLoader: ClassLoader?, logger: Logger): SefariaBlacklists {
    val authorEntries = loadBlacklistEntries(classLoader, "authors_blacklist.txt", logger)
    val bookEntries = loadBlacklistEntries(classLoader, "books_blacklist.txt", logger)

    val authorKeys = authorEntries.mapNotNull { normalizeTitleKey(it) }.toSet()
    val bookTitleKeys = bookEntries.mapNotNull { normalizeTitleKey(it) }.toSet()
    val bookPathKeys = bookEntries
        .asSequence()
        .filter { it.contains('/') || it.contains('\\') }
        .map { normalizePriorityEntry(it) }
        .filter { it.isNotBlank() }
        .toSet()

    return SefariaBlacklists(
        authorKeys = authorKeys,
        bookTitleKeys = bookTitleKeys,
        bookPathKeys = bookPathKeys
    )
}

internal fun filterBlacklistedPayloads(
    payloads: List<BookPayload>,
    blacklists: SefariaBlacklists
): BlacklistFilterResult {
    if (payloads.isEmpty() || blacklists.isEmpty()) {
        return BlacklistFilterResult(
            payloads = payloads,
            skippedTotal = 0,
            skippedByBook = 0,
            skippedByAuthor = 0,
            skippedBookExamples = emptyList(),
            skippedAuthorExamples = emptyList(),
            skippedNormalizedPaths = emptySet()
        )
    }

    var skippedTotal = 0
    var skippedByBook = 0
    var skippedByAuthor = 0
    val skippedBookExamples = ArrayList<String>(5)
    val skippedAuthorExamples = ArrayList<String>(5)
    val skippedNormalizedPaths = LinkedHashSet<String>()

    val filtered = payloads.filter { payload ->
        val bookBlacklisted = isBookBlacklisted(payload, blacklists)
        val authorBlacklisted = isAuthorBlacklisted(payload, blacklists)

        if (bookBlacklisted || authorBlacklisted) {
            skippedTotal++
            skippedNormalizedPaths += normalizedBookPath(payload.categoriesHe, payload.heTitle)

            if (bookBlacklisted) {
                skippedByBook++
                if (skippedBookExamples.size < 5) skippedBookExamples += payload.heTitle
            }
            if (authorBlacklisted) {
                skippedByAuthor++
                if (skippedAuthorExamples.size < 5) {
                    val author = payload.authors.firstOrNull().orEmpty()
                    skippedAuthorExamples += if (author.isBlank()) payload.heTitle else "${payload.heTitle} ($author)"
                }
            }

            false
        } else {
            true
        }
    }

    return BlacklistFilterResult(
        payloads = filtered,
        skippedTotal = skippedTotal,
        skippedByBook = skippedByBook,
        skippedByAuthor = skippedByAuthor,
        skippedBookExamples = skippedBookExamples,
        skippedAuthorExamples = skippedAuthorExamples,
        skippedNormalizedPaths = skippedNormalizedPaths
    )
}

private fun isBookBlacklisted(payload: BookPayload, blacklists: SefariaBlacklists): Boolean {
    if (blacklists.bookTitleKeys.isNotEmpty()) {
        normalizeTitleKey(payload.heTitle)?.let { if (it in blacklists.bookTitleKeys) return true }
        normalizeTitleKey(payload.enTitle)?.let { if (it in blacklists.bookTitleKeys) return true }
    }
    if (blacklists.bookPathKeys.isNotEmpty()) {
        val path = normalizedBookPath(payload.categoriesHe, payload.heTitle)
        if (path in blacklists.bookPathKeys) return true
    }
    return false
}

private fun isAuthorBlacklisted(payload: BookPayload, blacklists: SefariaBlacklists): Boolean {
    if (payload.authors.isEmpty() || blacklists.authorKeys.isEmpty()) return false
    return payload.authors.any { author ->
        normalizeTitleKey(author)?.let { it in blacklists.authorKeys } == true
    }
}

private fun loadBlacklistEntries(
    classLoader: ClassLoader?,
    resourceName: String,
    logger: Logger
): List<String> = try {
    val stream = sequenceOf(resourceName, "/$resourceName")
        .mapNotNull { name -> classLoader?.getResourceAsStream(name) }
        .firstOrNull()
        ?: return emptyList()

    stream.bufferedReader(Charsets.UTF_8).useLines { lines ->
        lines
            .map { raw -> raw.removePrefix("\uFEFF") }
            .map { raw -> raw.trim() }
            .filter { it.isNotEmpty() && !it.startsWith("#") }
            .map { unescapeBlacklistLine(it) }
            .toList()
    }
} catch (e: Exception) {
    logger.w(e) { "Unable to read $resourceName, continuing without it" }
    emptyList()
}

private fun unescapeBlacklistLine(value: String): String {
    return value
        .replace("\\\"", "\"")
        .replace("\\'", "'")
}

