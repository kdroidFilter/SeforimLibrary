package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.search.index.BoolBuilder
import io.github.kdroidfilter.seforimlibrary.search.index.IndexCursor
import io.github.kdroidfilter.seforimlibrary.search.index.IndexDoc
import io.github.kdroidfilter.seforimlibrary.search.index.IndexQuery
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndex
import io.github.kdroidfilter.seforimlibrary.search.index.Occur
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.withContext
import kotlin.concurrent.Volatile

private val logger = Logger.withTag("LexicalSearchEngine")

// Hard cap on how many synonym/expansion terms we allow per token
private const val MAX_SYNONYM_TERMS_PER_TOKEN: Int = 32

// Global cap for boost queries built from dictionary expansions
private const val MAX_SYNONYM_BOOST_TERMS: Int = 256
private const val DEFAULT_NEAR = 5
private const val PLAN_CACHE_SIZE = 16

// Searches run by warmUp(): one and several words, common and rarer ones, a quoted phrase, the divine name (whose
// dictionary entry is the largest)
val WARMUP_QUERIES = listOf("ברוך ה׳ לעולם", "אמר רבי יוחנן", "תפילין", "\"קריאת שמע\"")
const val WARMUP_ROUNDS = 2
const val WARMUP_PAGE_SIZE = 25

// Single Hebrew letters, dropped from a query before its dictionary expansion
private val SINGLE_LETTERS =
    setOf("א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י", "כ", "ל", "מ", "נ", "ס", "ע", "פ", "צ", "ק", "ר", "ש", "ת")

private val WHITESPACE = Regex("\\s+")

// The stored fields a hit is read from
private val HIT_FIELDS = setOf("book_id", "book_title", "line_id", "line_index", "is_base_book", "order_index", "text_raw")

/**
 * The full-text search of Hebrew religious texts, with diacritics handling, dictionary expansion and fuzzy matching,
 * over a Lucene [index] (see [LuceneIndex]: in process on the JVM and Android, through the embedded Java runtime on
 * iOS). Hits are ranked by Lucene, then boosted for base books; their snippets come from [snippetProvider] (the
 * indexed text when there is none). Inflections come from the lexical [dictionary] when it is available.
 */
class LexicalSearchEngine(
    private val index: LuceneIndex,
    private val snippetProvider: SnippetProvider? = null,
    private val dictionary: Lazy<MagicDictionaryIndex?> = lazyOf(null),
) : SearchEngine {
    @Volatile
    private var closed = false

    // A dictionary that finished loading after close() is closed by its first reader: close() couldn't see it
    private val magicDict: MagicDictionaryIndex?
        get() {
            val dict = dictionary.value
            if (closed) {
                dict?.close()
                return null
            }
            return dict
        }

    // --- SearchEngine interface implementation ---

    override fun openSession(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean,
    ): SearchSession? {
        val context = buildSearchContext(query, near, bookFilter, categoryFilter, bookIds, lineIds, baseBookOnly) ?: return null
        return LexicalSearchSession(context, index.openCursor(context.query, HIT_FIELDS))
    }

    override suspend fun attachSnippets(
        hits: List<LineHit>,
        query: String,
        near: Int,
    ): List<LineHit> {
        if (hits.isEmpty()) return hits
        val context = buildSearchContext(query, near, null, null, null, null) ?: return hits
        val sources = withContext(Dispatchers.IO) { snippetSources(hits.map { LineSnippetInfo(it.lineId, it.bookId, it.lineIndex) }) }
        return hits.map { hit ->
            val source = sources[hit.lineId] ?: SnippetSource(hit.rawText, 0, hit.rawText.length)
            hit.copy(
                snippet = SnippetBuilder.build(source.text, context.highlightPlan, source.lineStart, source.lineEnd),
                rawText = source.text,
                lineStart = source.lineStart,
                lineEnd = source.lineEnd,
            )
        }
    }

    override suspend fun warmUp() {
        withContext(Dispatchers.IO) { hashemHighlightTerms }
        // A few searches down the whole path (facets, ranking, snippets) load the index structures and, on the JVM,
        // get the hot code compiled before the user's first search
        repeat(WARMUP_ROUNDS) {
            for (query in WARMUP_QUERIES) {
                computeFacets(query, baseBookOnly = true)
                openSession(query, baseBookOnly = true)?.use { it.nextPage(WARMUP_PAGE_SIZE) }
            }
        }
    }

    override fun searchBooksByTitlePrefix(
        query: String,
        limit: Int,
    ): List<Long> {
        val q = HebrewTextUtils.normalizeHebrew(query)
        if (q.isBlank()) return emptyList()
        val tokens = q.split(WHITESPACE).map { it.trim() }.filter { it.isNotEmpty() }
        if (tokens.isEmpty()) return emptyList()

        val titleQuery =
            BoolBuilder()
                // Restrict to book_title docs
                .add(IndexQuery.Term("type", "book_title"), Occur.FILTER)
                .apply {
                    // prefix on analyzed 'title'
                    tokens.forEach { tok -> add(IndexQuery.Prefix("title", tok), Occur.MUST) }
                }.build()
        val ids = LinkedHashSet<Long>()
        for (doc in index.search(titleQuery, limit, setOf("book_id")).docs) {
            doc.longs["book_id"]?.let { ids.add(it) }
        }
        return ids.toList()
    }

    override fun buildSnippet(
        rawText: String,
        query: String,
        near: Int,
    ): String = SnippetBuilder.build(SnippetSources.clean(rawText), planFor(query, near))

    override fun highlightRanges(
        text: String,
        query: String,
    ): List<IntRange> = mergedRanges(text, planFor(query, DEFAULT_NEAR).highlights(text))

    override fun rangeSnippet(
        text: String,
        ranges: List<IntRange>,
        lineStart: Int,
        lineEnd: Int,
    ): String = SnippetBuilder.build(text, ranges.map { Occurrence(it.first, it.last + 1, 0, EvidenceLevel.LITERAL) }, lineStart, lineEnd)

    // The highlight plans of the latest queries: a results page and its preview ask for the same one many times
    private val planCache = LruCache<String, HighlightPlan>(PLAN_CACHE_SIZE)

    private fun planFor(
        query: String,
        near: Int,
    ): HighlightPlan {
        planCache[query]?.let { return it }
        val plan = buildSearchContext(query, near, null, null, null, null)?.highlightPlan ?: HighlightPlan.EMPTY
        planCache[query] = plan
        return plan
    }

    /**
     * What a query highlights: each word with its inflections, from the dictionary entries it truly is a form of and
     * that aren't blacklisted for it. With ה׳ in the query, the lone ה is the divine name.
     */
    private fun buildHighlightPlan(
        tokens: List<String>,
        hasHashem: Boolean,
        exactPhrases: List<List<String>>,
    ): HighlightPlan {
        val forms =
            tokens.distinct().map { token ->
                if (hasHashem && highlightKey(token) == "ה") {
                    return@map TokenForms(token, extraLiterals = hashemHighlightTerms)
                }
                val key = highlightKey(token)
                val inflections =
                    magicDict
                        ?.expansionsFor(listOf(token))
                        .orEmpty()
                        .filter { exp -> exp.inflections.any { highlightKey(it) == key } && !HallucinationBlacklist.rejects(token, exp) }
                        .flatMap { it.inflections }
                TokenForms(token, inflections)
            }
        val phrases =
            exactPhrases
                .map { it.joinToString(" ") }
                .filter { it.isNotBlank() }
        return HighlightPlan.build(forms, phrases)
    }

    override fun findInBookCandidates(
        query: String,
        bookId: Long,
    ): LongArray? {
        val norm = HebrewTextUtils.normalizeHebrew(query)
        if (norm.isBlank()) return LongArray(0)
        val bookLines =
            BoolBuilder()
                .add(IndexQuery.Term("type", "line"), Occur.FILTER)
                .add(IndexQuery.IntExact("book_id", bookId.toInt()), Occur.FILTER)
                .build()
        // Nothing to filter on: every line would be a candidate, cheaper scanned from the DB
        val clauses = literalPresenceClauses(index.analyze(norm))
        if (clauses.isEmpty()) return null
        val builder = BoolBuilder().add(bookLines, Occur.FILTER)
        clauses.forEach { builder.add(it, Occur.FILTER) }

        // Only line_id is read per hit: no scoring, no other stored field
        val ids = index.collectLongs(builder.build(), "line_id")
        // No candidate may mean a book the index lacks (older index, book added later): unknown, not "no match"
        return if (ids.isEmpty() && index.count(bookLines) == 0) null else ids
    }

    /**
     * Filters every line holding the analyzed [tokens] as one literal substring satisfies. The
     * substring may start and end mid-word: the first token is only a word suffix, the last a
     * word prefix, the inner ones whole words. A first or lone token is matched anywhere in a
     * word through its 4-grams, so one under 4 letters can't filter (a leading wildcard would
     * scan the whole term dictionary); a last one under 3 neither, its prefix being too common.
     */
    private fun literalPresenceClauses(tokens: List<String>): List<IndexQuery> =
        tokens.mapIndexedNotNull { i, token ->
            when {
                i in 1 until tokens.lastIndex -> IndexQuery.Term("text", token)
                i > 0 && token.length >= 3 -> IndexQuery.Prefix("text", token)
                else -> buildNgramPresenceForToken(token)
            }
        }

    override fun close() {
        closed = true
        index.close()
        // The engine owns its dictionary (and its SQLite connections) once loaded; one still loading is closed by
        // magicDict
        if (dictionary.isInitialized()) dictionary.value?.close()
    }

    override fun computeFacets(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean,
    ): SearchFacets? {
        val context =
            buildSearchContext(query, near, bookFilter, categoryFilter, bookIds, lineIds, baseBookOnly)
                ?: return null
        return index.facets(context.matchQuery)
    }

    // --- Inner SearchSession class ---

    private inner class LexicalSearchSession(
        private val context: SearchContext,
        private val cursor: IndexCursor,
    ) : SearchSession {
        private var finished = false
        private var totalHitsValue: Long? = null

        override suspend fun nextPage(
            limit: Int,
            snippets: Boolean,
        ): SearchPage? =
            withContext(Dispatchers.IO) {
                if (finished) return@withContext null
                val page = cursor.next(limit)
                if (totalHitsValue == null) totalHitsValue = page.totalHits
                if (page.docs.isEmpty()) {
                    finished = true
                    return@withContext null
                }
                val hits = mapDocs(page.docs, context, snippets)
                val isLast = page.docs.size < limit
                if (isLast) finished = true
                SearchPage(
                    hits = hits,
                    totalHits = totalHitsValue ?: hits.size.toLong(),
                    isLastPage = isLast,
                )
            }

        override fun close() {
            cursor.close()
        }
    }

    // --- Additional public search methods ---

    suspend fun searchAllText(
        rawQuery: String,
        near: Int = 5,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> = doSearch(rawQuery, near, limit, offset, bookFilter = null, categoryFilter = null)

    suspend fun searchInBook(
        rawQuery: String,
        near: Int,
        bookId: Long,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> = doSearch(rawQuery, near, limit, offset, bookFilter = bookId, categoryFilter = null)

    suspend fun searchInCategory(
        rawQuery: String,
        near: Int,
        categoryId: Long,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> = doSearch(rawQuery, near, limit, offset, bookFilter = null, categoryFilter = categoryId)

    suspend fun searchInBooks(
        rawQuery: String,
        near: Int,
        bookIds: Collection<Long>,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> {
        if (bookIds.isEmpty()) return emptyList()
        return doSearch(rawQuery, near, limit, offset, bookFilter = null, categoryFilter = null, bookIds = bookIds)
    }

    // --- Private implementation ---

    private class SearchContext(
        val query: IndexQuery,
        // The required clauses only: the same matches as [query], without the ranking-only clauses (fuzzy, n-grams,
        // synonym boosts) that cost a rewrite but can't change the hit set
        val matchQuery: IndexQuery,
        private val plan: Lazy<HighlightPlan>,
    ) {
        val highlightPlan: HighlightPlan get() = plan.value
    }

    private fun buildSearchContext(
        rawQuery: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean = false,
    ): SearchContext? {
        // Split the raw query into exact phrases (wrapped in ASCII double quotes) and free text.
        // Quoted segments are matched verbatim WITHOUT dictionary expansion (Google-style).
        val parsed = SearchQueryParser.parse(rawQuery)
        val norm = HebrewTextUtils.normalizeHebrew(parsed.freeText)
        val exactPhrasesNorm =
            parsed.exactPhrases
                .map { HebrewTextUtils.normalizeHebrew(it) }
                .filter { it.isNotBlank() }
        // Analyzed once, for both the phrase clauses and the highlights (a bridge call on iOS)
        val exactPhraseTokens = exactPhrasesNorm.map { index.analyze(it) }

        if (norm.isBlank() && exactPhrasesNorm.isEmpty()) return null

        val analyzedRaw = if (norm.isBlank()) emptyList() else index.analyze(norm)

        // Check if the original query contained ה׳ (Hashem) before normalization
        val hasHashem = rawQuery.contains("ה׳") || rawQuery.contains("ה'")

        // Filter out single Hebrew letters and stop words BEFORE dictionary expansion
        // BUT preserve "ה" if the original query had "ה׳" (Hashem)
        val analyzedStd =
            analyzedRaw.filter { token ->
                // Special case: if query has ה׳, keep "ה" token
                if (token == "ה" && hasHashem) return@filter true
                // Preserve numeric tokens (e.g., "6") so they can expand via MagicDictionary
                if (token.any { it.isDigit() }) return@filter true

                token.length >= 2 && token !in SINGLE_LETTERS
            }

        logger.d { "[DEBUG] Original query had Hashem (ה׳): $hasHashem" }
        logger.d { "[DEBUG] Analyzed tokens: $analyzedStd" }

        // Get all possible expansions for each token (a token can belong to multiple bases)
        // These expansions are used for SEARCH - we keep all of them for better recall
        magicDict?.prefetch(analyzedStd)
        val tokenExpansions: Map<String, List<MagicDictionaryIndex.Expansion>> =
            analyzedStd.associateWith { token ->
                // Get best expansion (prefers matching base, then largest)
                val expansion = magicDict?.expansionFor(token) ?: return@associateWith emptyList()
                listOf(expansion)
            }

        // What the snippets highlight: built on first use (facets need none)
        val highlightPlan = lazy { buildHighlightPlan(analyzedStd, hasHashem, exactPhraseTokens) }

        val builder = BoolBuilder()
        // Mirrors every required clause of builder (the facets need only the hit set)
        val matchBuilder = BoolBuilder()

        fun require(
            query: IndexQuery,
            occur: Occur,
        ) {
            builder.add(query, occur)
            matchBuilder.add(query, occur)
        }
        require(IndexQuery.Term("type", "line"), Occur.FILTER)
        if (bookFilter != null) require(IndexQuery.IntExact("book_id", bookFilter.toInt()), Occur.FILTER)
        if (categoryFilter != null) require(IndexQuery.IntExact("category_id", categoryFilter.toInt()), Occur.FILTER)
        // Filter by base books only (is_base_book = 1) when baseBookOnly is true
        if (baseBookOnly) require(IndexQuery.IntExact("is_base_book", 1), Occur.FILTER)
        val bookIdsArray = bookIds?.map { it.toInt() }?.toIntArray()
        if (bookIdsArray != null && bookIdsArray.isNotEmpty()) {
            require(IndexQuery.IntSet("book_id", bookIdsArray), Occur.FILTER)
        }
        val lineIdsArray = lineIds?.map { it.toInt() }?.toIntArray()
        if (lineIdsArray != null && lineIdsArray.isNotEmpty()) {
            require(IndexQuery.IntSet("line_id", lineIdsArray), Occur.FILTER)
        }
        // Quoted phrases: each must appear verbatim as an exact, in-order, adjacent phrase.
        var exactClausesAdded = 0
        for ((phrase, tokens) in exactPhrasesNorm.zip(exactPhraseTokens)) {
            val exactQuery = buildExactPhraseQuery(phrase, tokens) ?: continue
            require(exactQuery, Occur.MUST)
            exactClausesAdded++
            logger.d { "[DEBUG] Added exact-phrase MUST for: \"$phrase\"" }
        }

        // A fully-quoted query that produced no usable phrase clause (e.g. just punctuation)
        // must not fall through to a match-all query.
        if (norm.isBlank() && exactClausesAdded == 0) return null

        // Free (unquoted) text: dictionary-aware ranking + presence filtering.
        if (norm.isNotBlank()) {
            val rankedQuery = buildExpandedQuery(norm, near, analyzedRaw, analyzedStd, tokenExpansions)
            val mustAllTokensQuery: IndexQuery? = buildPresenceFilterForTokens(analyzedStd, near, tokenExpansions)
            val phraseQuery: IndexQuery? = buildSynonymPhraseQuery(analyzedStd, tokenExpansions, near)
            if (mustAllTokensQuery != null) {
                require(mustAllTokensQuery, Occur.FILTER)
            }
            if (phraseQuery != null && analyzedStd.size >= 2) {
                if (near == 0) require(phraseQuery, Occur.MUST) else builder.add(phraseQuery, Occur.SHOULD)
            }
            builder.add(rankedQuery, Occur.SHOULD)
        }

        return SearchContext(
            query = builder.build(),
            matchQuery = matchBuilder.build(),
            plan = highlightPlan,
        )
    }

    // suspend because it calls snippetProvider.getSnippetSources();
    // callers are responsible for providing Dispatchers.IO context.
    private suspend fun mapDocs(
        docs: List<IndexDoc>,
        context: SearchContext,
        snippets: Boolean = true,
    ): List<LineHit> {
        if (docs.isEmpty()) return emptyList()

        // First pass: extract metadata from index
        class DocMeta(
            val score: Float,
            val bookId: Long,
            val bookTitle: String,
            val lineId: Long,
            val lineIndex: Int,
            val isBaseBook: Boolean,
            val orderIndex: Int,
            // from text_raw field, may be empty if not stored
            val indexedRaw: String,
        )

        val docMetas =
            docs.map { doc ->
                DocMeta(
                    score = doc.score,
                    bookId = doc.longs.getValue("book_id"),
                    bookTitle = doc.strings["book_title"] ?: "",
                    lineId = doc.longs.getValue("line_id"),
                    lineIndex = doc.longs.getValue("line_index").toInt(),
                    isBaseBook = doc.longs["is_base_book"]?.toInt() == 1,
                    orderIndex = doc.longs["order_index"]?.toInt() ?: 999,
                    indexedRaw = doc.strings["text_raw"] ?: "",
                )
            }

        val snippetSources: Map<Long, SnippetSource> =
            if (snippets) snippetSources(docMetas.map { LineSnippetInfo(it.lineId, it.bookId, it.lineIndex) }) else emptyMap()

        val hits =
            docMetas.map { meta ->
                val source = snippetSources[meta.lineId] ?: SnippetSource(meta.indexedRaw, 0, meta.indexedRaw.length)
                val raw = source.text
                val baseScore = meta.score

                // Calculate boost: lower orderIndex = higher boost (only for base books)
                val boostedScore =
                    if (meta.isBaseBook) {
                        // Formula: boost = baseScore * (1 + (120 - orderIndex) / 60)
                        // orderIndex 1 gets ~3x boost, orderIndex 50 gets ~2.2x boost, orderIndex 100+ gets ~1.3x boost
                        val boostFactor = 1.0f + (120 - meta.orderIndex).coerceAtLeast(0) / 60.0f
                        baseScore * boostFactor
                    } else {
                        baseScore
                    }

                val snippet = if (snippets) SnippetBuilder.build(raw, context.highlightPlan, source.lineStart, source.lineEnd) else ""
                LineHit(
                    bookId = meta.bookId,
                    bookTitle = meta.bookTitle,
                    lineId = meta.lineId,
                    lineIndex = meta.lineIndex,
                    snippet = snippet,
                    score = boostedScore,
                    rawText = raw,
                    isBaseBook = meta.isBaseBook,
                )
            }
        // Re-sort by boosted score (descending)
        return hits.sortedByDescending { it.score }
    }

    // Snippet sources: from the provider if available, otherwise none (the indexed text_raw fallback)
    private suspend fun snippetSources(lines: List<LineSnippetInfo>): Map<Long, SnippetSource> =
        snippetProvider?.getSnippetSources(lines) ?: emptyMap()

    private suspend fun doSearch(
        rawQuery: String,
        near: Int,
        limit: Int,
        offset: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>? = null,
    ): List<LineHit> {
        val context = buildSearchContext(rawQuery, near, bookFilter, categoryFilter, bookIds, null) ?: return emptyList()
        return withContext(Dispatchers.IO) {
            val top = index.search(context.query, offset + limit, HIT_FIELDS)
            mapDocs(top.docs.drop(offset), context)
        }
    }

    private fun buildNgramPresenceForToken(token: String): IndexQuery? {
        if (token.length < 4) return null
        val grams = (0..token.length - 4).map { token.substring(it, it + 4) }.distinct()
        if (grams.isEmpty()) return null
        return BoolBuilder().apply { grams.forEach { add(IndexQuery.Term("text_ng4", it), Occur.MUST) } }.build()
    }

    private fun buildPresenceFilterForTokens(
        tokens: List<String>,
        near: Int,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>,
    ): IndexQuery? {
        if (tokens.isEmpty()) return null
        val outer = BoolBuilder()
        for (t in tokens) {
            val expansions = expansionsByToken[t] ?: emptyList()
            val synonymTerms = buildLimitedTermsForToken(t, expansions)
            val ngram = if (near > 0) buildNgramPresenceForToken(t) else null
            val clause =
                BoolBuilder()
                    .apply {
                        add(IndexQuery.Term("text", t), Occur.SHOULD)
                        if (ngram != null) add(ngram, Occur.SHOULD)
                        for (term in synonymTerms) {
                            if (term != t) add(IndexQuery.Term("text", term), Occur.SHOULD)
                        }
                    }.build()
            outer.add(clause, Occur.MUST)
        }
        return outer.build()
    }

    /**
     * Builds a strict, verbatim phrase query for a quoted segment: terms must appear in the
     * given order and be adjacent (slop = 0), with NO dictionary expansion, fuzzy matching or
     * n-gram substring matching. A single-token segment becomes an exact term query.
     *
     * Nikud and final letters are still normalised (the segment is normalised by the caller and
     * tokenised like the index) so that the phrase matches the indexed form.
     */
    private fun buildExactPhraseQuery(
        phraseNorm: String,
        tokens: List<String>,
    ): IndexQuery? {
        if (phraseNorm.isBlank() || tokens.isEmpty()) return null
        return IndexQuery.AnalyzedPhrase("text", phraseNorm, 0)
    }

    private fun expansionTerms(expansions: List<MagicDictionaryIndex.Expansion>): Triple<List<String>, List<String>, List<String>> {
        val surfaceTerms = LinkedHashSet<String>()
        val variantTerms = LinkedHashSet<String>()
        val baseTerms = LinkedHashSet<String>()
        for (exp in expansions) {
            surfaceTerms.addAll(exp.surface)
            variantTerms.addAll(exp.variants)
            baseTerms.addAll(exp.base)
        }
        return Triple(
            surfaceTerms.take(MAX_SYNONYM_BOOST_TERMS),
            variantTerms.take(MAX_SYNONYM_BOOST_TERMS),
            baseTerms.take(MAX_SYNONYM_BOOST_TERMS),
        )
    }

    private fun buildMagicBoostQuery(expansions: List<MagicDictionaryIndex.Expansion>): IndexQuery? {
        if (expansions.isEmpty()) return null
        val (surfaces, variants, bases) = expansionTerms(expansions)
        val b = BoolBuilder()
        for (s in surfaces) b.add(IndexQuery.Boost(IndexQuery.Term("text", s), 2.0f), Occur.SHOULD)
        for (v in variants) b.add(IndexQuery.Boost(IndexQuery.Term("text", v), 1.5f), Occur.SHOULD)
        for (ba in bases) b.add(IndexQuery.Boost(IndexQuery.Term("text", ba), 1.0f), Occur.SHOULD)
        return b.build()
    }

    private fun buildSynonymBoostQuery(expansions: List<MagicDictionaryIndex.Expansion>): IndexQuery? {
        if (expansions.isEmpty()) return null
        val (surfaces, variants, bases) = expansionTerms(expansions)
        val b = BoolBuilder()
        for (term in surfaces + variants + bases) b.add(IndexQuery.Term("text", term), Occur.SHOULD)
        return b.build()
    }

    private fun buildSynonymPhrases(
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>,
    ): List<Pair<IndexQuery, Float>> {
        if (tokens.isEmpty()) return emptyList()
        val termExpansions = buildTermAlternativesForTokens(tokens, expansionsByToken)
        return listOf(
            IndexQuery.MultiPhrase("text", termExpansions, 0) to 50.0f,
            IndexQuery.MultiPhrase("text", termExpansions, 3) to 20.0f,
            IndexQuery.MultiPhrase("text", termExpansions, 8) to 5.0f,
        )
    }

    private fun buildSynonymPhraseQuery(
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>,
        near: Int,
    ): IndexQuery? {
        if (tokens.isEmpty()) return null
        return IndexQuery.MultiPhrase("text", buildTermAlternativesForTokens(tokens, expansionsByToken), near)
    }

    private fun buildNgram4Query(norm: String): IndexQuery? {
        val tokens = norm.split(WHITESPACE).map { it.trim() }.filter { it.length >= 4 }
        if (tokens.isEmpty()) return null
        val uniq = tokens.flatMap { t -> (0..t.length - 4).map { t.substring(it, it + 4) } }.distinct()
        if (uniq.isEmpty()) return null
        return BoolBuilder().apply { uniq.forEach { add(IndexQuery.Term("text_ng4", it), Occur.MUST) } }.build()
    }

    private fun buildExpandedQuery(
        norm: String,
        near: Int,
        analyzed: List<String>,
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>,
    ): IndexQuery {
        val base = IndexQuery.AnalyzedPhrase("text", norm, near)
        val allExpansions = expansionsByToken.values.flatten()
        val builder = BoolBuilder()
        builder.add(base, Occur.SHOULD)
        for ((query, boost) in buildSynonymPhrases(tokens, expansionsByToken)) {
            builder.add(IndexQuery.Boost(query, boost), Occur.SHOULD)
        }
        buildNgram4Query(norm)?.let { builder.add(it, Occur.SHOULD) }
        buildFuzzyQuery(norm, near, analyzed)?.let { builder.add(it, Occur.SHOULD) }
        buildMagicBoostQuery(allExpansions)?.let { builder.add(it, Occur.SHOULD) }
        buildSynonymBoostQuery(allExpansions)?.let { builder.add(it, Occur.SHOULD) }
        return builder.build()
    }

    private fun buildFuzzyQuery(
        norm: String,
        near: Int,
        analyzed: List<String>,
    ): IndexQuery? {
        if (near == 0) return null
        if (norm.length < 4) return null
        val tokens = analyzed.filter { it.length >= 4 }
        if (tokens.isEmpty()) return null
        return BoolBuilder().apply { tokens.distinct().forEach { add(IndexQuery.Fuzzy("text", it, 1), Occur.MUST) } }.build()
    }

    private fun buildLimitedTermsForToken(
        token: String,
        expansions: List<MagicDictionaryIndex.Expansion>,
    ): List<String> {
        if (expansions.isEmpty()) return listOf(token)

        val baseTerms = expansions.flatMap { it.base }.distinct()
        val otherTerms = expansions.flatMap { it.surface + it.variants }.distinct()

        val ordered = LinkedHashSet<String>()
        if (token.isNotBlank()) ordered += token
        baseTerms.forEach { ordered += it }
        otherTerms.forEach { ordered += it }
        return ordered.take(MAX_SYNONYM_TERMS_PER_TOKEN)
    }

    private fun buildTermAlternativesForTokens(
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>,
    ): List<List<String>> = tokens.map { token -> buildLimitedTermsForToken(token, expansionsByToken[token] ?: emptyList()) }

    // The divine name's forms never change: read once from the dictionary, not on every search
    private val hashemHighlightTerms: List<String> by lazy { readHashemHighlightTerms() }

    private fun readHashemHighlightTerms(): List<String> {
        val dict = magicDict ?: return emptyList()
        val raw = dict.loadHashemSurfaces()
        if (raw.isEmpty()) return emptyList()

        val terms = linkedSetOf<String>()
        raw.forEach { value ->
            val trimmed = value.trim()
            if (trimmed.isEmpty()) return@forEach
            terms += trimmed
            val stripped = HebrewTextUtils.stripDiacritics(trimmed).trim()
            if (stripped.isNotEmpty()) terms += stripped
            val normalized = HebrewTextUtils.normalizeHebrew(trimmed).trim()
            if (normalized.isNotEmpty()) terms += normalized
        }
        return terms.toList()
    }
}
