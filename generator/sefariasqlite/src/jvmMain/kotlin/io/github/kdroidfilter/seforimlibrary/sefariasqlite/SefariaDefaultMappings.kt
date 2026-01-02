package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.serialization.json.Json

/**
 * Loads default commentators configuration from bundled JSON.
 *
 * @return a map keyed by normalized base-book title → ordered list of normalized commentator titles.
 */
internal fun loadDefaultCommentatorsConfig(
    classLoader: ClassLoader?,
    json: Json,
    logger: Logger
): Map<String, List<String>> = try {
    val stream = classLoader?.getResourceAsStream("default_commentators.json") ?: return emptyMap()
    val jsonText = stream.bufferedReader(Charsets.UTF_8).use { it.readText() }
    val entries = json.decodeFromString<List<DefaultCommentatorsEntry>>(jsonText)
    entries.mapNotNull { entry ->
        val bookKey = normalizeTitleKey(entry.book)
        if (bookKey.isNullOrBlank()) return@mapNotNull null
        val commentatorKeys = entry.commentators
            .mapNotNull { normalizeTitleKey(it) }
            .filter { it.isNotBlank() }
        if (commentatorKeys.isEmpty()) return@mapNotNull null
        bookKey to commentatorKeys
    }.toMap()
} catch (e: Exception) {
    logger.w(e) { "Unable to read default_commentators.json, continuing without default commentators" }
    emptyMap()
}

/**
 * Loads default targum configuration from bundled JSON.
 *
 * @return a map keyed by normalized base-book title → ordered list of normalized targum titles.
 */
internal fun loadDefaultTargumConfig(
    classLoader: ClassLoader?,
    json: Json,
    logger: Logger
): Map<String, List<String>> = try {
    val stream = classLoader?.getResourceAsStream("default_targumim.json") ?: return emptyMap()
    val jsonText = stream.bufferedReader(Charsets.UTF_8).use { it.readText() }
    val entries = json.decodeFromString<List<DefaultTargumEntry>>(jsonText)
    entries.mapNotNull { entry ->
        val bookKey = normalizeTitleKey(entry.book)
        if (bookKey.isNullOrBlank()) return@mapNotNull null
        val targumKeys = entry.targumim
            .mapNotNull { normalizeTitleKey(it) }
            .filter { it.isNotBlank() }
        if (targumKeys.isEmpty()) return@mapNotNull null
        bookKey to targumKeys
    }.toMap()
} catch (e: Exception) {
    logger.w(e) { "Unable to read default_targumim.json, continuing without default targumim" }
    emptyMap()
}

internal suspend fun applyDefaultCommentators(
    repository: SeforimRepository,
    logger: Logger,
    defaultsByBookKey: Map<String, List<String>>,
    normalizedTitleToBookId: Map<String, Long>
) {
    if (defaultsByBookKey.isEmpty()) return

    logger.i { "Applying default commentators for ${defaultsByBookKey.size} base books" }

    // Clear previous mappings for a clean regeneration
    repository.clearAllDefaultCommentators()

    var totalRows = 0

    defaultsByBookKey.forEach { (bookKey, commentatorKeys) ->
        val baseBookId = normalizedTitleToBookId[bookKey] ?: return@forEach

        val uniqueCommentatorIds = LinkedHashSet<Long>()
        commentatorKeys.forEach { commentatorKey ->
            val commentatorBookId = normalizedTitleToBookId[commentatorKey]
            if (commentatorBookId != null && commentatorBookId != baseBookId) {
                uniqueCommentatorIds += commentatorBookId
            }
        }

        if (uniqueCommentatorIds.isNotEmpty()) {
            repository.setDefaultCommentatorsForBook(baseBookId, uniqueCommentatorIds.toList())
            totalRows += uniqueCommentatorIds.size
        }
    }

    logger.i { "Inserted $totalRows default commentator rows" }
}

internal suspend fun applyDefaultTargumim(
    repository: SeforimRepository,
    logger: Logger,
    defaultsByBookKey: Map<String, List<String>>,
    normalizedTitleToBookId: Map<String, Long>
) {
    if (defaultsByBookKey.isEmpty()) return

    logger.i { "Applying default targumim for ${defaultsByBookKey.size} base books" }

    repository.clearAllDefaultTargum()

    var totalRows = 0

    defaultsByBookKey.forEach { (bookKey, targumKeys) ->
        val baseBookId = normalizedTitleToBookId[bookKey] ?: return@forEach

        val uniqueTargumIds = LinkedHashSet<Long>()
        targumKeys.forEach { targumKey ->
            val targumBookId = normalizedTitleToBookId[targumKey]
            if (targumBookId != null && targumBookId != baseBookId) {
                uniqueTargumIds += targumBookId
            }
        }

        if (uniqueTargumIds.isNotEmpty()) {
            repository.setDefaultTargumForBook(baseBookId, uniqueTargumIds.toList())
            totalRows += uniqueTargumIds.size
        }
    }

    logger.i { "Inserted $totalRows default targum rows" }
}

