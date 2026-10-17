package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem

/**
 * Streaming dictionary index backed by SQLite (tables: surface, variant, base).
 * Previously we loaded the entire dictionary into memory; now we stream lookups
 * on-demand with a small LRU cache while preserving the exact expansion shape
 * used by the search ranking logic.
 */
class MagicDictionaryIndex private constructor(
    private val norm: (String) -> String,
    private val db: SqliteReader,
) : AutoCloseable {
    /**
     * The forms a token expands to. [surface] holds every form (inflections, then the dictionary's variants, then
     * the base) as the ranking uses them; [inflections] only the base's inflected forms and the base itself: the
     * variants also hold loose associations (יום for הוא), so they don't count as the same word.
     */
    data class Expansion(
        val surface: List<String>,
        val variants: List<String>,
        val base: List<String>,
        val inflections: List<String> = emptyList(),
    )

    // Dictionaries since v0.4.2 list the extra lemmas of ambiguous surfaces (פנינו: פנה and פנים)
    private val lookupSql: String by lazy { if (db.hasTable(EXTRA_BASE_TABLE)) LOOKUP_WITH_EXTRA_BASES_SQL else LOOKUP_SQL }

    /**
     * Cache expansions per normalized token to avoid repeated DB hits. Lookups in progress are cached too, so
     * concurrent searches (facets and results start together) share them.
     */
    private val tokenCache = LruCache<String, CompletableDeferred<List<Expansion>>>(TOKEN_CACHE_SIZE)

    /**
     * Cache fully-normalized expansions per base id so repeated hits to the same base
     * avoid re-normalizing rows.
     */
    private val baseCache = LruCache<Long, Expansion>(BASE_CACHE_SIZE)

    /** Starts looking up [tokens] in parallel, so the following [expansionFor] calls find them ready. */
    fun prefetch(tokens: Collection<String>) {
        tokens.forEach { lookup(it, async = true) }
    }

    fun expansionsFor(tokens: List<String>): List<Expansion> = tokens.flatMap { expansionsForToken(it) }.distinct()

    fun expansionFor(token: String): Expansion? {
        val expansions = expansionsForToken(token)
        if (expansions.isEmpty()) return null

        val normalized = norm(token)
        // Strategy: prefer the expansion whose base matches the token
        val matchingBase =
            expansions.firstOrNull { exp ->
                exp.base.any { it == normalized }
            }
        if (matchingBase != null) return matchingBase

        // Then the entries the token truly is a form of: one reached only through a dictionary variant (loose
        // associations: לעולם to יום) isn't the token's paradigm, however large
        val ownEntries = expansions.filter { exp -> exp.inflections.any { it == normalized } }

        // The largest of them (more terms = more complete paradigm)
        return ownEntries.ifEmpty { expansions }.maxByOrNull { it.surface.size }
    }

    private fun expansionsForToken(token: String): List<Expansion> {
        val result = lookup(token, async = false)
        @Suppress("OPT_IN_USAGE")
        return if (result.isCompleted) result.getCompleted() else runBlockingIo { result.await() }
    }

    private fun lookup(
        token: String,
        async: Boolean,
    ): CompletableDeferred<List<Expansion>> {
        val normalized = norm(token)
        if (normalized.isEmpty()) return CompletableDeferred(emptyList())
        var created = false
        val result =
            tokenCache.getOrPut(normalized) {
                created = true
                CompletableDeferred()
            }
        if (!created) return result
        // fetchExpansions logs and skips its failures: an empty result is cached like any other
        val fetch = { result.complete(runCatching { fetchForToken(token, normalized) }.getOrDefault(emptyList())) }
        if (async) lookupScope.launch { fetch() } else fetch()
        return result
    }

    private fun fetchForToken(
        token: String,
        normalized: String,
    ): List<Expansion> {
        // Try raw, normalized, and final-form variants to match DB values.
        val candidates = buildLookupCandidates(token, normalized)
        val mergedByBase = LinkedHashMap<Long, Expansion>()

        for (candidate in candidates) {
            val fetched = fetchExpansions(candidate, normalized)
            for ((baseId, exp) in fetched) {
                val existing = mergedByBase[baseId]
                if (existing == null) {
                    mergedByBase[baseId] = exp
                } else {
                    val surfaces = (existing.surface + exp.surface).distinct()
                    val variants = (existing.variants + exp.variants).distinct()
                    val base = (existing.base + exp.base).distinct()
                    val inflections = (existing.inflections + exp.inflections).distinct()
                    mergedByBase[baseId] = Expansion(surfaces, variants, base, inflections)
                }
            }
        }
        return mergedByBase.values.toList()
    }

    /**
     * Fetch expansions for a token. Returns a list of (baseId, Expansion) pairs so callers can merge by base id.
     */
    private fun fetchExpansions(
        rawToken: String,
        normalizedToken: String,
    ): List<Pair<Long, Expansion>> {
        val expansions = mutableListOf<Pair<Long, Expansion>>()

        runCatching {
            val sql = lookupSql
            val rows = db.query(sql, List(sql.count { it == '?' }) { rawToken })
            val accum = LinkedHashMap<Long, BaseBucket>()
            for (row in rows) {
                val baseId = row["base_id"]?.toLongOrNull() ?: continue
                val bucket =
                    accum.getOrPut(baseId) {
                        BaseBucket(
                            baseRaw = row["base"] ?: "",
                            surfaces = linkedSetOf(),
                            variants = linkedSetOf(),
                        )
                    }
                row["surface"]?.let { bucket.surfaces += it }
                row["variant"]?.let { bucket.variants += it }
            }

            for ((baseId, bucket) in accum) {
                val cached = baseCache[baseId]
                if (cached != null) {
                    expansions += baseId to cached
                    continue
                }

                val surfaceN = bucket.surfaces.mapNotNull { v -> norm(v).takeIf { it.isNotEmpty() } }
                val variantsN = bucket.variants.mapNotNull { v -> norm(v).takeIf { it.isNotEmpty() } }
                val baseN =
                    norm(bucket.baseRaw).takeIf { it.isNotEmpty() }
                        ?: surfaceN.firstOrNull()
                        ?: normalizedToken

                val baseTerms = listOfNotNull(baseN.takeIf { it.isNotEmpty() })
                val allTerms = (surfaceN + variantsN + baseTerms).distinct()
                if (allTerms.isEmpty()) continue

                val exp =
                    Expansion(
                        surface = allTerms,
                        variants = emptyList(),
                        base = baseTerms,
                        inflections = (surfaceN + baseTerms).distinct(),
                    )

                baseCache[baseId] = exp
                expansions += baseId to exp
            }
        }.onFailure {
            logger.d { "[MagicDictionary] Failed to fetch expansions for '$rawToken' : ${it.message}" }
        }

        return expansions
    }

    /**
     * Load all surface forms whose base lemma directly from the underlying SQLite DB.
     * This is used for snippet highlighting of Hashem names, independent of token-level expansions.
     */
    fun loadHashemSurfaces(): List<String> {
        val terms = linkedSetOf<String>()
        runCatching {
            val sql =
                """
                SELECT s.value AS surface
                FROM surface s
                JOIN base b ON s.base_id = b.id
                WHERE b.value = 'יהוה'
                """.trimIndent()
            for (row in db.query(sql)) {
                val trimmed = row["surface"]?.trim() ?: continue
                if (trimmed.isNotEmpty()) terms += trimmed
            }
        }.onFailure {
            logger.d { "[MagicDictionary] Failed to load Hashem surfaces: ${it.message}" }
        }
        return terms.toList()
    }

    override fun close() {
        db.close()
    }

    companion object {
        private val logger = Logger.withTag("MagicDictionary")
        private const val TOKEN_CACHE_SIZE = 1024
        private const val BASE_CACHE_SIZE = 512
        private const val LOOKUP_PARALLELISM = 4

        // Parallel lookups of a query's words
        private val lookupScope = CoroutineScope(SupervisorJob() + Dispatchers.IO.limitedParallelism(LOOKUP_PARALLELISM))

        private val FINALS = mapOf('כ' to 'ך', 'מ' to 'ם', 'נ' to 'ן', 'פ' to 'ף', 'צ' to 'ץ')

        private val REQUIRED_TABLES = listOf("surface", "variant", "base", "surface_variant")

        /**
         * Load from SQLite DB (expected tables: surface(value, base_id), variant(value, surface_id), base(value)).
         * Uses streaming lookup to avoid holding the entire dictionary in memory.
         */
        fun load(
            norm: (String) -> String,
            candidate: String?,
        ): MagicDictionaryIndex? {
            if (candidate == null || !isRegularFile(candidate)) return null
            return runCatching {
                val db = openReadOnlySqlite(candidate)
                if (!db.hasRequiredTables()) {
                    db.close()
                    logger.d { "[MagicDictionary] Ignoring candidate $candidate because required tables are missing" }
                    return null
                }
                logger.d { "[MagicDictionary] Streaming lexical db from $candidate (lazy on-demand)" }
                MagicDictionaryIndex(norm, db)
            }.onFailure {
                logger.d { "[MagicDictionary] Failed to load from $candidate : ${it.message}" }
            }.getOrNull()
        }

        /**
         * Find the first candidate path that exists and contains the required tables.
         */
        fun findValidDictionary(candidates: List<String>): String? {
            for (candidate in candidates) {
                if (!isRegularFile(candidate)) continue
                val valid = runCatching { openReadOnlySqlite(candidate).use { it.hasRequiredTables() } }.getOrDefault(false)
                if (valid) {
                    logger.d { "[MagicDictionary] Using validated lexical db at $candidate" }
                    return candidate
                }
                logger.d { "[MagicDictionary] Candidate $candidate is present but missing required tables; skipping" }
            }
            return null
        }

        private fun isRegularFile(path: String): Boolean = SystemFileSystem.metadataOrNull(Path(path))?.isRegularFile == true

        private fun SqliteReader.hasRequiredTables(): Boolean =
            runCatching {
                val names =
                    query(
                        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN " +
                            REQUIRED_TABLES.joinToString(prefix = "(", postfix = ")") { "'$it'" },
                    ).mapNotNull { it["name"] }
                names.containsAll(REQUIRED_TABLES)
            }.getOrDefault(false)

        private fun SqliteReader.hasTable(name: String): Boolean =
            query("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = ?", listOf(name)).isNotEmpty()

        private const val LOOKUP_SQL = """
            WITH matches AS (
                SELECT s.base_id AS base_id FROM surface s WHERE s.value = ?
                UNION
                SELECT b.id FROM base b WHERE b.value = ?
                UNION
                SELECT s.base_id FROM variant v
                JOIN surface_variant sv ON sv.variant_id = v.id
                JOIN surface s ON sv.surface_id = s.id
                WHERE v.value = ?
            )
            SELECT b.id as base_id,
                   b.value as base,
                   s.value as surface,
                   v.value as variant
            FROM base b
            JOIN matches m ON m.base_id = b.id
            LEFT JOIN surface s ON s.base_id = b.id
            LEFT JOIN surface_variant sv ON sv.surface_id = s.id
            LEFT JOIN variant v ON sv.variant_id = v.id
        """

        private const val EXTRA_BASE_TABLE = "surface_extra_base"

        /**
         * [LOOKUP_SQL] plus the extra lemmas of ambiguous surfaces: a token matches every lemma it is a form of, and
         * such a surface counts among the forms of each. Its variants stay with its primary lemma only, as they hold
         * associations of that reading.
         */
        private const val LOOKUP_WITH_EXTRA_BASES_SQL = """
            WITH matches AS (
                SELECT s.base_id AS base_id FROM surface s WHERE s.value = ?
                UNION
                SELECT e.base_id FROM surface_extra_base e JOIN surface s ON s.id = e.surface_id WHERE s.value = ?
                UNION
                SELECT b.id FROM base b WHERE b.value = ?
                UNION
                SELECT s.base_id FROM variant v
                JOIN surface_variant sv ON sv.variant_id = v.id
                JOIN surface s ON sv.surface_id = s.id
                WHERE v.value = ?
            ),
            members AS (
                SELECT s.base_id AS base_id, s.id AS surface_id, 1 AS own
                FROM surface s WHERE s.base_id IN (SELECT base_id FROM matches)
                UNION ALL
                SELECT e.base_id, e.surface_id, 0
                FROM surface_extra_base e WHERE e.base_id IN (SELECT base_id FROM matches)
            )
            SELECT b.id as base_id,
                   b.value as base,
                   s.value as surface,
                   v.value as variant
            FROM base b
            JOIN matches m ON m.base_id = b.id
            LEFT JOIN members mb ON mb.base_id = b.id
            LEFT JOIN surface s ON s.id = mb.surface_id
            LEFT JOIN surface_variant sv ON sv.surface_id = s.id AND mb.own = 1
            LEFT JOIN variant v ON sv.variant_id = v.id
        """
    }

    private class BaseBucket(
        val baseRaw: String,
        val surfaces: MutableSet<String>,
        val variants: MutableSet<String>,
    )

    private fun buildLookupCandidates(
        rawToken: String,
        normalized: String,
    ): List<String> {
        fun applyFinalForm(t: String): String {
            if (t.isEmpty()) return t
            val last = t.last()
            val final = FINALS[last] ?: last
            return if (final == last) t else t.dropLast(1) + final
        }

        return listOf(
            rawToken,
            normalized,
            applyFinalForm(rawToken),
            applyFinalForm(normalized),
        ).filter { it.isNotBlank() }.distinct()
    }
}
