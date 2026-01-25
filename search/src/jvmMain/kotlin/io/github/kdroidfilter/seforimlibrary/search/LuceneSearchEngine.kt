package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.StoredFields
import org.apache.lucene.index.Term
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BoostQuery
import org.apache.lucene.search.Collector
import org.apache.lucene.search.FuzzyQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.PrefixQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.Scorable
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.ScoreMode
import org.apache.lucene.search.TermQuery
import org.apache.lucene.util.QueryBuilder
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.document.IntPoint
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.Closeable
import java.nio.file.Path

/**
 * Lucene-based implementation of SearchEngine for full-text search.
 * Supports Hebrew text with diacritics handling, dictionary expansion, and fuzzy matching.
 */
class LuceneSearchEngine(
    private val indexDir: Path,
    private val snippetProvider: SnippetProvider? = null,
    private val analyzer: Analyzer = StandardAnalyzer(),
    private val dictionaryPath: Path? = null
) : SearchEngine {

    companion object {
        private val logger = Logger.withTag("LuceneSearchEngine")
        // Hard cap on how many synonym/expansion terms we allow per token
        private const val MAX_SYNONYM_TERMS_PER_TOKEN: Int = 32
        // Global cap for boost queries built from dictionary expansions
        private const val MAX_SYNONYM_BOOST_TERMS: Int = 256
        // Constants for snippet source building (must match indexer)
        private const val SNIPPET_NEIGHBOR_WINDOW = 4
        private const val SNIPPET_MIN_LENGTH = 280

        /**
         * Blacklist of hallucinated dictionary mappings.
         * Loaded from resources/hallucination_blacklist.tsv
         * Key: normalized token, Value: set of incorrect base forms to reject
         *
         * TODO: This is a temporary workaround. The proper fix is to correct the
         *       hallucinated mappings directly in the lexical dictionary (lexical.db)
         *       so this blacklist becomes unnecessary.
         */
        private val HALLUCINATION_BLACKLIST: Map<String, Set<String>> by lazy {
            loadHallucinationBlacklist()
        }

        private fun loadHallucinationBlacklist(): Map<String, Set<String>> {
            val result = mutableMapOf<String, MutableSet<String>>()
            try {
                val inputStream = LuceneSearchEngine::class.java.getResourceAsStream("/hallucination_blacklist.tsv")
                if (inputStream == null) {
                    logger.w { "hallucination_blacklist.tsv not found in resources" }
                    return emptyMap()
                }
                inputStream.bufferedReader().useLines { lines ->
                    lines.forEach { line ->
                        val trimmed = line.trim()
                        if (trimmed.isEmpty() || trimmed.startsWith("#")) return@forEach
                        val parts = trimmed.split("\t")
                        if (parts.size >= 2) {
                            val token = HebrewTextUtils.normalizeHebrew(parts[0])
                            val base = HebrewTextUtils.normalizeHebrew(parts[1])
                            result.getOrPut(token) { mutableSetOf() }.add(base)
                        }
                    }
                }
                logger.d { "Loaded ${result.size} hallucination blacklist entries" }
            } catch (e: Exception) {
                logger.e(e) { "Failed to load hallucination blacklist" }
            }
            return result
        }

        /**
         * Check if an expansion should be rejected based on the hallucination blacklist.
         */
        private fun isHallucinatedExpansion(token: String, expansion: MagicDictionaryIndex.Expansion): Boolean {
            val normalizedToken = HebrewTextUtils.normalizeHebrew(token)
            val blacklistedBases = HALLUCINATION_BLACKLIST[normalizedToken] ?: return false
            return expansion.base.any { base ->
                val normalizedBase = HebrewTextUtils.normalizeHebrew(base)
                blacklistedBases.contains(normalizedBase)
            }
        }
    }

    // Open Lucene directory lazily to avoid any I/O at app startup
    private val dir by lazy { FSDirectory.open(indexDir) }

    private val stdAnalyzer: Analyzer by lazy { analyzer }
    private val magicDict: MagicDictionaryIndex? by lazy {
        val candidates = listOfNotNull(
            dictionaryPath,
            System.getProperty("magicDict")?.let { Path.of(it) },
            System.getenv("SEFORIM_MAGIC_DICT")?.let { Path.of(it) },
            indexDir.resolveSibling("lexical.db"),
            indexDir.resolveSibling("seforim.db").resolveSibling("lexical.db"),
            Path.of("SeforimLibrary/SeforimMagicIndexer/magicindexer/build/db/lexical.db")
        ).distinct()
        val firstExisting = MagicDictionaryIndex.findValidDictionary(candidates)
        if (firstExisting == null) {
            logger.d {
                "[MagicDictionary] Missing lexical.db; search will run without dictionary expansions. " +
                    "Provide -DmagicDict=/path/lexical.db or SEFORIM_MAGIC_DICT. Checked: " +
                    candidates.joinToString()
            }
            return@lazy null
        }
        logger.d { "[MagicDictionary] Loading lexical db from $firstExisting" }
        val loaded = MagicDictionaryIndex.load(HebrewTextUtils::normalizeHebrew, firstExisting)
        if (loaded == null) {
            logger.d {
                "[MagicDictionary] Failed to load lexical db at $firstExisting; " +
                    "continuing without dictionary expansions"
            }
        }
        loaded
    }

    private inline fun <T> withSearcher(block: (IndexSearcher) -> T): T {
        DirectoryReader.open(dir).use { reader ->
            val searcher = IndexSearcher(reader)
            return block(searcher)
        }
    }

    // --- SearchEngine interface implementation ---

    override fun openSession(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?
    ): SearchSession? {
        val context = buildSearchContext(query, near, bookFilter, categoryFilter, bookIds, lineIds) ?: return null
        val reader = DirectoryReader.open(dir)
        return LuceneSearchSession(context.query, context.anchorTerms, context.highlightTerms, reader)
    }

    override fun searchBooksByTitlePrefix(query: String, limit: Int): List<Long> {
        val q = HebrewTextUtils.normalizeHebrew(query)
        if (q.isBlank()) return emptyList()
        val tokens = q.split("\\s+".toRegex()).map { it.trim() }.filter { it.isNotEmpty() }
        if (tokens.isEmpty()) return emptyList()

        return withSearcher { searcher ->
            val must = BooleanQuery.Builder()
            // Restrict to book_title docs
            must.add(TermQuery(Term("type", "book_title")), BooleanClause.Occur.FILTER)
            tokens.forEach { tok ->
                // prefix on analyzed 'title'
                must.add(PrefixQuery(Term("title", tok)), BooleanClause.Occur.MUST)
            }
            val luceneQuery = must.build()
            val top = searcher.search(luceneQuery, limit)
            val stored: StoredFields = searcher.storedFields()
            val ids = LinkedHashSet<Long>()
            for (sd in top.scoreDocs) {
                val doc = stored.document(sd.doc)
                val id = doc.getField("book_id")?.numericValue()?.toLong()
                if (id != null) ids.add(id)
            }
            ids.toList().take(limit)
        }
    }

    override fun buildSnippet(rawText: String, query: String, near: Int): String {
        val norm = HebrewTextUtils.normalizeHebrew(query)
        if (norm.isBlank()) return Jsoup.clean(rawText, Safelist.none())
        val rawClean = Jsoup.clean(rawText, Safelist.none())
        val analyzedStd = (analyzeToTerms(stdAnalyzer, norm) ?: emptyList())
        val hasHashem = query.contains("ה׳") || query.contains("ה'")
        val hashemTerms = if (hasHashem) loadHashemHighlightTerms() else emptyList()
        val highlightTerms = filterTermsForHighlight(
            analyzedStd + buildNgramTerms(analyzedStd, gram = 4) + hashemTerms
        )
        val anchorTerms = buildAnchorTerms(norm, highlightTerms)
        return buildSnippetInternal(rawClean, anchorTerms, highlightTerms)
    }

    override fun buildHighlightTerms(query: String): List<String> {
        val norm = HebrewTextUtils.normalizeHebrew(query)
        if (norm.isBlank()) return emptyList()

        val analyzedRaw = analyzeToTerms(stdAnalyzer, norm) ?: emptyList()
        val hasHashem = query.contains("ה׳") || query.contains("ה'")

        // Filter single letters and stop words (same logic as buildSearchContext)
        val analyzedStd = analyzedRaw.filter { token ->
            if (token == "ה" && hasHashem) return@filter true
            if (token.any { it.isDigit() }) return@filter true
            token.length >= 2 && token !in setOf(
                "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י", "כ", "ל", "מ",
                "נ", "ס", "ע", "פ", "צ", "ק", "ר", "ש", "ת",
            )
        }

        // Get dictionary expansions
        val tokenExpansions: Map<String, List<MagicDictionaryIndex.Expansion>> =
            analyzedStd.associateWith { token ->
                val expansion = magicDict?.expansionFor(token) ?: return@associateWith emptyList()
                listOf(expansion)
            }

        // Filter hallucinations for highlighting
        val tokenExpansionsForHighlight = tokenExpansions.mapValues { (token, exps) ->
            exps.filter { exp -> !isHallucinatedExpansion(token, exp) }
        }

        // Build expanded terms (filter 2-letter from expansions only)
        val allExpansionsForHighlight = tokenExpansionsForHighlight.values.flatten()
        val expandedTerms = allExpansionsForHighlight
            .flatMap { it.surface + it.variants + it.base }
            .filter { it.length > 2 }
            .distinct()

        val ngramTerms = buildNgramTerms(analyzedStd, gram = 4)
        val hashemTerms = if (hasHashem) loadHashemHighlightTerms() else emptyList()

        return filterTermsForHighlight(analyzedStd + expandedTerms + ngramTerms + hashemTerms)
    }

    override fun close() {
        // Directory is closed automatically when readers are closed
    }

    override fun computeFacets(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?
    ): SearchFacets? {
        val context = buildSearchContext(query, near, bookFilter, categoryFilter, bookIds, lineIds)
            ?: return null

        return withSearcher { searcher ->
            val categoryCounts = mutableMapOf<Long, Int>()
            val bookCounts = mutableMapOf<Long, Int>()
            var totalHits = 0L

            // Lightweight collector that only reads stored fields for aggregation
            val collector = object : Collector {
                override fun getLeafCollector(leafContext: LeafReaderContext): LeafCollector {
                    val storedFields = leafContext.reader().storedFields()

                    return object : LeafCollector {
                        override fun setScorer(scorer: Scorable) {
                            // No scoring needed for facet counting
                        }

                        override fun collect(doc: Int) {
                            totalHits++
                            val luceneDoc = storedFields.document(doc)

                            // Book count
                            val bookId = luceneDoc.getField("book_id")?.numericValue()?.toLong()
                            if (bookId != null) {
                                bookCounts[bookId] = (bookCounts[bookId] ?: 0) + 1
                            }

                            // Category counts from ancestors (stored as comma-separated string)
                            val ancestorStr = luceneDoc.getField("ancestor_category_ids")?.stringValue() ?: ""
                            if (ancestorStr.isNotEmpty()) {
                                for (idStr in ancestorStr.split(",")) {
                                    val catId = idStr.trim().toLongOrNull() ?: continue
                                    categoryCounts[catId] = (categoryCounts[catId] ?: 0) + 1
                                }
                            }
                        }
                    }
                }

                override fun scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
            }

            searcher.search(context.query, collector)

            SearchFacets(
                totalHits = totalHits,
                categoryCounts = categoryCounts.toMap(),
                bookCounts = bookCounts.toMap()
            )
        }
    }

    // --- Inner SearchSession class ---

    inner class LuceneSearchSession internal constructor(
        private val query: Query,
        private val anchorTerms: List<String>,
        private val highlightTerms: List<String>,
        private val reader: DirectoryReader
    ) : SearchSession {
        private val searcher = IndexSearcher(reader)
        private var after: ScoreDoc? = null
        private var finished = false
        private var totalHitsValue: Long? = null

        override fun nextPage(limit: Int): SearchPage? {
            if (finished) return null
            val top = searcher.searchAfter(after, query, limit)
            if (totalHitsValue == null) totalHitsValue = top.totalHits?.value
            if (top.scoreDocs.isEmpty()) {
                finished = true
                return null
            }
            val stored = searcher.storedFields()
            val hits = mapScoreDocs(stored, top.scoreDocs.toList(), anchorTerms, highlightTerms)
            after = top.scoreDocs.last()
            val isLast = top.scoreDocs.size < limit
            if (isLast) finished = true
            return SearchPage(
                hits = hits,
                totalHits = totalHitsValue ?: hits.size.toLong(),
                isLastPage = isLast
            )
        }

        override fun close() {
            reader.close()
        }
    }

    // --- Additional public search methods ---

    fun searchAllText(rawQuery: String, near: Int = 5, limit: Int, offset: Int = 0): List<LineHit> =
        doSearch(rawQuery, near, limit, offset, bookFilter = null, categoryFilter = null)

    fun searchInBook(rawQuery: String, near: Int, bookId: Long, limit: Int, offset: Int = 0): List<LineHit> =
        doSearch(rawQuery, near, limit, offset, bookFilter = bookId, categoryFilter = null)

    fun searchInCategory(rawQuery: String, near: Int, categoryId: Long, limit: Int, offset: Int = 0): List<LineHit> =
        doSearch(rawQuery, near, limit, offset, bookFilter = null, categoryFilter = categoryId)

    fun searchInBooks(rawQuery: String, near: Int, bookIds: Collection<Long>, limit: Int, offset: Int = 0): List<LineHit> =
        doSearchInBooks(rawQuery, near, limit, offset, bookIds)

    // --- Private implementation ---

    private data class SearchContext(
        val query: Query,
        val anchorTerms: List<String>,
        val highlightTerms: List<String>
    )

    private fun buildSearchContext(
        rawQuery: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?
    ): SearchContext? {
        val norm = HebrewTextUtils.normalizeHebrew(rawQuery)
        if (norm.isBlank()) return null

        val analyzedRaw = analyzeToTerms(stdAnalyzer, norm) ?: emptyList()

        // Check if the original query contained ה׳ (Hashem) before normalization
        val hasHashem = rawQuery.contains("ה׳") || rawQuery.contains("ה'")

        // Filter out single Hebrew letters and stop words BEFORE dictionary expansion
        // BUT preserve "ה" if the original query had "ה׳" (Hashem)
        val analyzedStd = analyzedRaw.filter { token ->
            // Special case: if query has ה׳, keep "ה" token
            if (token == "ה" && hasHashem) return@filter true
            // Preserve numeric tokens (e.g., "6") so they can expand via MagicDictionary
            if (token.any { it.isDigit() }) return@filter true

            token.length >= 2 && token !in setOf(
                "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י", "כ", "ל", "מ",
                "נ", "ס", "ע", "פ", "צ", "ק", "ר", "ש", "ת",
            )
        }

        logger.d { "[DEBUG] Original query had Hashem (ה׳): $hasHashem" }
        logger.d { "[DEBUG] Analyzed tokens: $analyzedStd" }

        // Get all possible expansions for each token (a token can belong to multiple bases)
        // These expansions are used for SEARCH - we keep all of them for better recall
        val tokenExpansions: Map<String, List<MagicDictionaryIndex.Expansion>> =
            analyzedStd.associateWith { token ->
                // Get best expansion (prefers matching base, then largest)
                val expansion = magicDict?.expansionFor(token) ?: return@associateWith emptyList()
                listOf(expansion)
            }
        tokenExpansions.forEach { (token, exps) ->
            exps.forEach { exp ->
                logger.d { "[DEBUG] Token '$token' -> expansion: surface=${exp.surface.take(10)}..., variants=${exp.variants.take(10)}..., base=${exp.base}" }
            }
        }

        // For HIGHLIGHTING, filter out hallucinated expansions to avoid highlighting unrelated words
        val tokenExpansionsForHighlight: Map<String, List<MagicDictionaryIndex.Expansion>> =
            tokenExpansions.mapValues { (token, exps) ->
                exps.filter { exp ->
                    val isHallucination = isHallucinatedExpansion(token, exp)
                    if (isHallucination) {
                        logger.d { "[DEBUG] Token '$token' -> BLOCKED for highlight (hallucination): base=${exp.base}" }
                    }
                    !isHallucination
                }
            }

        val allExpansionsForHighlight = tokenExpansionsForHighlight.values.flatten()
        // Filter out 2-letter terms from dictionary expansions for highlighting
        // (2-letter words should only be highlighted if explicitly in the query)
        val expandedTerms = allExpansionsForHighlight
            .flatMap { it.surface + it.variants + it.base }
            .filter { it.length > 2 }
            .distinct()
        // Add 4-gram terms used in the query (matches text_ng4 clauses) so highlighting can
        // reflect matches that were found via the n-gram branch.
        val ngramTerms = buildNgramTerms(analyzedStd, gram = 4)
        // For highlighting/snippets, use the actual query tokens plus the concrete
        // terms that the search query uses (expansions + n-grams), and if the query
        // mentions Hashem explicitly, also include dictionary-based variants of the
        // divine name from the lexical DB
        val hashemTerms = if (hasHashem) loadHashemHighlightTerms() else emptyList()
        val highlightTerms = filterTermsForHighlight(analyzedStd + expandedTerms + ngramTerms + hashemTerms)
        val anchorTerms = buildAnchorTerms(norm, highlightTerms)

        val rankedQuery = buildExpandedQuery(norm, near, analyzedStd, tokenExpansions)
        val mustAllTokensQuery: Query? = buildPresenceFilterForTokens(analyzedStd, near, tokenExpansions)
        val phraseQuery: Query? = buildSynonymPhraseQuery(analyzedStd, tokenExpansions, near)

        val builder = BooleanQuery.Builder()
        builder.add(TermQuery(Term("type", "line")), BooleanClause.Occur.FILTER)
        if (bookFilter != null) builder.add(IntPoint.newExactQuery("book_id", bookFilter.toInt()), BooleanClause.Occur.FILTER)
        if (categoryFilter != null) builder.add(IntPoint.newExactQuery("category_id", categoryFilter.toInt()), BooleanClause.Occur.FILTER)
        val bookIdsArray = bookIds?.map { it.toInt() }?.toIntArray()
        if (bookIdsArray != null && bookIdsArray.isNotEmpty()) {
            builder.add(IntPoint.newSetQuery("book_id", *bookIdsArray), BooleanClause.Occur.FILTER)
        }
        val lineIdsArray = lineIds?.map { it.toInt() }?.toIntArray()
        if (lineIdsArray != null && lineIdsArray.isNotEmpty()) {
            builder.add(IntPoint.newSetQuery("line_id", *lineIdsArray), BooleanClause.Occur.FILTER)
        }
        if (mustAllTokensQuery != null) {
            builder.add(mustAllTokensQuery, BooleanClause.Occur.FILTER)
            logger.d { "[DEBUG] Added mustAllTokensQuery as FILTER" }
        }
        val analyzedCount = analyzedStd.size
        if (phraseQuery != null && analyzedCount >= 2) {
            val occur = if (near == 0) BooleanClause.Occur.MUST else BooleanClause.Occur.SHOULD
            builder.add(phraseQuery, occur)
            logger.d { "[DEBUG] Added phraseQuery with occur=$occur, near=$near" }
        }
        builder.add(rankedQuery, BooleanClause.Occur.SHOULD)
        logger.d { "[DEBUG] Added rankedQuery as SHOULD" }

        val finalQuery = builder.build()
        logger.d { "[DEBUG] Final query: $finalQuery" }

        return SearchContext(
            query = finalQuery,
            anchorTerms = anchorTerms,
            highlightTerms = highlightTerms
        )
    }

    private fun mapScoreDocs(
        stored: StoredFields,
        scoreDocs: List<ScoreDoc>,
        anchorTerms: List<String>,
        highlightTerms: List<String>
    ): List<LineHit> {
        if (scoreDocs.isEmpty()) return emptyList()

        // First pass: extract metadata from index
        data class DocMeta(
            val sd: ScoreDoc,
            val bookId: Long,
            val bookTitle: String,
            val lineId: Long,
            val lineIndex: Int,
            val isBaseBook: Boolean,
            val orderIndex: Int,
            val indexedRaw: String // from text_raw field, may be empty if not stored
        )

        val docMetas = scoreDocs.map { sd ->
            val doc = stored.document(sd.doc)
            DocMeta(
                sd = sd,
                bookId = doc.getField("book_id").numericValue().toLong(),
                bookTitle = doc.getField("book_title").stringValue() ?: "",
                lineId = doc.getField("line_id").numericValue().toLong(),
                lineIndex = doc.getField("line_index").numericValue().toInt(),
                isBaseBook = doc.getField("is_base_book")?.numericValue()?.toInt() == 1,
                orderIndex = doc.getField("order_index")?.numericValue()?.toInt() ?: 999,
                indexedRaw = doc.getField("text_raw")?.stringValue() ?: ""
            )
        }

        // Get snippet sources: from provider if available, otherwise from index
        val snippetSources: Map<Long, String> = if (snippetProvider != null) {
            val lineInfos = docMetas.map { LineSnippetInfo(it.lineId, it.bookId, it.lineIndex) }
            snippetProvider.getSnippetSources(lineInfos)
        } else {
            // Fallback to indexed text_raw
            docMetas.associate { it.lineId to it.indexedRaw }
        }

        val hits = docMetas.map { meta ->
            val raw = snippetSources[meta.lineId] ?: meta.indexedRaw
            val baseScore = meta.sd.score

            // Calculate boost: lower orderIndex = higher boost (only for base books)
            val boostedScore = if (meta.isBaseBook) {
                // Formula: boost = baseScore * (1 + (120 - orderIndex) / 60)
                // orderIndex 1 gets ~3x boost, orderIndex 50 gets ~2.2x boost, orderIndex 100+ gets ~1.3x boost
                val boostFactor = 1.0f + (120 - meta.orderIndex).coerceAtLeast(0) / 60.0f
                baseScore * boostFactor
            } else {
                baseScore
            }

            val snippet = buildSnippetInternal(raw, anchorTerms, highlightTerms)
            LineHit(
                bookId = meta.bookId,
                bookTitle = meta.bookTitle,
                lineId = meta.lineId,
                lineIndex = meta.lineIndex,
                snippet = snippet,
                score = boostedScore,
                rawText = raw
            )
        }
        // Re-sort by boosted score (descending)
        return hits.sortedByDescending { it.score }
    }

    private fun doSearch(
        rawQuery: String,
        near: Int,
        limit: Int,
        offset: Int,
        bookFilter: Long?,
        categoryFilter: Long?
    ): List<LineHit> {
        val context = buildSearchContext(rawQuery, near, bookFilter, categoryFilter, null, null) ?: return emptyList()
        return withSearcher { searcher ->
            val top = searcher.search(context.query, offset + limit)
            val stored: StoredFields = searcher.storedFields()
            val sliced = top.scoreDocs.drop(offset)
            mapScoreDocs(stored, sliced, context.anchorTerms, context.highlightTerms)
        }
    }

    private fun doSearchInBooks(
        rawQuery: String,
        near: Int,
        limit: Int,
        offset: Int,
        bookIds: Collection<Long>
    ): List<LineHit> {
        if (bookIds.isEmpty()) return emptyList()
        val context = buildSearchContext(rawQuery, near, bookFilter = null, categoryFilter = null, bookIds = bookIds, lineIds = null) ?: return emptyList()
        return withSearcher { searcher ->
            val top = searcher.search(context.query, offset + limit)
            val stored: StoredFields = searcher.storedFields()
            val sliced = top.scoreDocs.drop(offset)
            mapScoreDocs(stored, sliced, context.anchorTerms, context.highlightTerms)
        }
    }

    private fun analyzeToTerms(analyzer: Analyzer, text: String): List<String>? = try {
        val out = mutableListOf<String>()
        val ts: TokenStream = analyzer.tokenStream("text", text)
        val termAtt = ts.addAttribute(CharTermAttribute::class.java)
        ts.reset()
        while (ts.incrementToken()) {
            val t = termAtt.toString()
            if (t.isNotBlank()) out += t
        }
        ts.end(); ts.close()
        out
    } catch (_: Exception) { null }

    private fun buildNgramPresenceForToken(token: String): Query? {
        if (token.length < 4) return null
        val grams = mutableListOf<String>()
        var i = 0
        val L = token.length
        while (i + 4 <= L) {
            grams += token.substring(i, i + 4)
            i += 1
        }
        if (grams.isEmpty()) return null
        val b = BooleanQuery.Builder()
        for (g in grams.distinct()) {
            b.add(TermQuery(Term("text_ng4", g)), BooleanClause.Occur.MUST)
        }
        return b.build()
    }

    private fun buildPresenceFilterForTokens(
        tokens: List<String>,
        near: Int,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>
    ): Query? {
        if (tokens.isEmpty()) return null
        val outer = BooleanQuery.Builder()
        for (t in tokens) {
            val expansions = expansionsByToken[t] ?: emptyList()
            val synonymTerms = buildLimitedTermsForToken(t, expansions)
            val ngram = if (near > 0) buildNgramPresenceForToken(t) else null
            val clause = BooleanQuery.Builder().apply {
                add(TermQuery(Term("text", t)), BooleanClause.Occur.SHOULD)
                if (ngram != null) add(ngram, BooleanClause.Occur.SHOULD)
                for (term in synonymTerms) {
                    if (term != t) {
                        add(TermQuery(Term("text", term)), BooleanClause.Occur.SHOULD)
                    }
                }
            }.build()
            outer.add(clause, BooleanClause.Occur.MUST)
        }
        return outer.build()
    }

    private fun buildHebrewStdQuery(norm: String, near: Int): Query {
        val qb = QueryBuilder(stdAnalyzer)
        val phrase = qb.createPhraseQuery("text", norm, near)
        if (phrase != null) return phrase
        val bool = qb.createBooleanQuery("text", norm, BooleanClause.Occur.MUST)
        return bool ?: BooleanQuery.Builder().build()
    }

    private fun buildMagicBoostQuery(expansions: List<MagicDictionaryIndex.Expansion>): Query? {
        if (expansions.isEmpty()) return null
        val surfaceTerms = LinkedHashSet<String>()
        val variantTerms = LinkedHashSet<String>()
        val baseTerms = LinkedHashSet<String>()
        for (exp in expansions) {
            surfaceTerms.addAll(exp.surface)
            variantTerms.addAll(exp.variants)
            baseTerms.addAll(exp.base)
        }

        val limitedSurfaces = surfaceTerms.take(MAX_SYNONYM_BOOST_TERMS)
        val limitedVariants = variantTerms.take(MAX_SYNONYM_BOOST_TERMS)
        val limitedBases = baseTerms.take(MAX_SYNONYM_BOOST_TERMS)
        if (surfaceTerms.size > limitedSurfaces.size ||
            variantTerms.size > limitedVariants.size ||
            baseTerms.size > limitedBases.size
        ) {
            logger.d {
                "[DEBUG] Capped magic boost terms: " +
                    "surface=${surfaceTerms.size}->${limitedSurfaces.size}, " +
                    "variants=${variantTerms.size}->${limitedVariants.size}, " +
                    "base=${baseTerms.size}->${limitedBases.size}"
            }
        }

        val b = BooleanQuery.Builder()
        for (s in limitedSurfaces) {
            b.add(BoostQuery(TermQuery(Term("text", s)), 2.0f), BooleanClause.Occur.SHOULD)
        }
        for (v in limitedVariants) {
            b.add(BoostQuery(TermQuery(Term("text", v)), 1.5f), BooleanClause.Occur.SHOULD)
        }
        for (ba in limitedBases) {
            b.add(BoostQuery(TermQuery(Term("text", ba)), 1.0f), BooleanClause.Occur.SHOULD)
        }
        return b.build()
    }

    private fun buildSynonymBoostQuery(expansions: List<MagicDictionaryIndex.Expansion>): Query? {
        if (expansions.isEmpty()) return null
        val surfaceTerms = LinkedHashSet<String>()
        val variantTerms = LinkedHashSet<String>()
        val baseTerms = LinkedHashSet<String>()
        for (exp in expansions) {
            surfaceTerms.addAll(exp.surface)
            variantTerms.addAll(exp.variants)
            baseTerms.addAll(exp.base)
        }

        val limitedSurfaces = surfaceTerms.take(MAX_SYNONYM_BOOST_TERMS)
        val limitedVariants = variantTerms.take(MAX_SYNONYM_BOOST_TERMS)
        val limitedBases = baseTerms.take(MAX_SYNONYM_BOOST_TERMS)
        if (surfaceTerms.size > limitedSurfaces.size ||
            variantTerms.size > limitedVariants.size ||
            baseTerms.size > limitedBases.size
        ) {
            logger.d {
                "[DEBUG] Capped synonym boost terms: " +
                    "surface=${surfaceTerms.size}->${limitedSurfaces.size}, " +
                    "variants=${variantTerms.size}->${limitedVariants.size}, " +
                    "base=${baseTerms.size}->${limitedBases.size}"
            }
        }

        val b = BooleanQuery.Builder()
        for (s in limitedSurfaces) {
            b.add(TermQuery(Term("text", s)), BooleanClause.Occur.SHOULD)
        }
        for (v in limitedVariants) {
            b.add(TermQuery(Term("text", v)), BooleanClause.Occur.SHOULD)
        }
        for (ba in limitedBases) {
            b.add(TermQuery(Term("text", ba)), BooleanClause.Occur.SHOULD)
        }
        return b.build()
    }

    private fun buildSynonymPhrases(
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>
    ): List<Pair<Query, Float>> {
        if (tokens.isEmpty()) return emptyList()
        val termExpansions = buildTermAlternativesForTokens(tokens, expansionsByToken)
        logger.d { "[DEBUG] buildSynonymPhrases - termExpansions sizes: ${termExpansions.map { it.size }}" }
        fun buildMultiPhrase(slop: Int): Query {
            val builder = org.apache.lucene.search.MultiPhraseQuery.Builder()
            builder.setSlop(slop)
            var pos = 0
            for (alts in termExpansions) {
                builder.add(alts.map { Term("text", it) }.toTypedArray(), pos)
                pos++
            }
            return builder.build()
        }
        return listOf(
            buildMultiPhrase(0) to 50.0f,
            buildMultiPhrase(3) to 20.0f,
            buildMultiPhrase(8) to 5.0f
        )
    }

    private fun buildSynonymPhraseQuery(
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>,
        near: Int
    ): Query? {
        if (tokens.isEmpty()) return null
        val termExpansions = buildTermAlternativesForTokens(tokens, expansionsByToken)
        val builder = org.apache.lucene.search.MultiPhraseQuery.Builder()
        builder.setSlop(near)
        var position = 0
        for (alts in termExpansions) {
            builder.add(alts.map { Term("text", it) }.toTypedArray(), position)
            position++
        }
        return builder.build()
    }

    private fun buildNgram4Query(norm: String): Query? {
        val tokens = norm.split("\\s+".toRegex()).map { it.trim() }.filter { it.length >= 4 }
        if (tokens.isEmpty()) return null
        val grams = mutableListOf<String>()
        for (t in tokens) {
            val L = t.length
            var i = 0
            while (i + 4 <= L) {
                grams += t.substring(i, i + 4)
                i += 1
            }
        }
        val uniq = grams.distinct()
        if (uniq.isEmpty()) return null
        val b = BooleanQuery.Builder()
        for (g in uniq) {
            b.add(TermQuery(Term("text_ng4", g)), BooleanClause.Occur.MUST)
        }
        return b.build()
    }

    private fun buildExpandedQuery(
        norm: String,
        near: Int,
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>
    ): Query {
        val base = buildHebrewStdQuery(norm, near)
        val allExpansions = expansionsByToken.values.flatten()
        val synonymPhrases = buildSynonymPhrases(tokens, expansionsByToken)
        val ngram = buildNgram4Query(norm)
        val fuzzy = buildFuzzyQuery(norm, near)
        val builder = BooleanQuery.Builder()
        builder.add(base, BooleanClause.Occur.SHOULD)
        for ((query, boost) in synonymPhrases) {
            builder.add(BoostQuery(query, boost), BooleanClause.Occur.SHOULD)
        }
        if (ngram != null) builder.add(ngram, BooleanClause.Occur.SHOULD)
        if (fuzzy != null) builder.add(fuzzy, BooleanClause.Occur.SHOULD)
        val magic = buildMagicBoostQuery(allExpansions)
        if (magic != null) builder.add(magic, BooleanClause.Occur.SHOULD)
        val synonymBoost = buildSynonymBoostQuery(allExpansions)
        if (synonymBoost != null) builder.add(synonymBoost, BooleanClause.Occur.SHOULD)
        return builder.build()
    }

    private fun buildFuzzyQuery(norm: String, near: Int): Query? {
        if (near == 0) return null
        if (norm.length < 4) return null
        val tokens = analyzeToTerms(stdAnalyzer, norm)?.filter { it.length >= 4 } ?: emptyList()
        if (tokens.isEmpty()) return null
        val b = BooleanQuery.Builder()
        for (t in tokens.distinct()) {
            b.add(FuzzyQuery(Term("text", t), 1), BooleanClause.Occur.MUST)
        }
        return b.build()
    }

    private fun buildAnchorTerms(normQuery: String, analyzedTerms: List<String>): List<String> {
        val qTokens = normQuery.split("\\s+".toRegex())
            .map { it.trim() }
            .filter { it.isNotEmpty() }
        val combined = (qTokens + analyzedTerms.map { it.trimEnd('$') })
        val filtered = filterTermsForHighlight(combined)
        if (filtered.isNotEmpty()) return filtered
        val qFiltered = filterTermsForHighlight(qTokens)
        return qFiltered.ifEmpty { qTokens }
    }

    private fun filterTermsForHighlight(terms: List<String>): List<String> {
        if (terms.isEmpty()) return emptyList()

        fun useful(t: String): Boolean {
            val s = t.trim()
            if (s.isEmpty()) return false
            if (s.length < 2) return false
            if (s.none { it.isLetterOrDigit() }) return false
            return true
        }
        return terms
            .map { it.trim() }
            .filter { useful(it) }
            .distinct()
            .sortedByDescending { it.length }
    }

    private fun buildSnippetInternal(raw: String, anchorTerms: List<String>, highlightTerms: List<String>, context: Int = 220): String {
        if (raw.isEmpty()) return ""
        val (plain, mapToOrig) = HebrewTextUtils.stripDiacriticsWithMap(raw)
        val hasDiacritics = plain.length != raw.length
        val effContext = if (hasDiacritics) maxOf(context, 360) else context
        val plainSearch = HebrewTextUtils.replaceFinalsWithBase(plain)

        // Find best anchor position: where most terms cluster together
        // Optimized: limit occurrences per term, early exit on perfect score
        var plainIdx = 0
        var plainLen = anchorTerms.firstOrNull()?.length ?: 0

        if (anchorTerms.isNotEmpty()) {
            val maxOccPerTerm = 5 // Limit occurrences per term for perf
            val positions = mutableListOf<Pair<Int, String>>() // (position, term)

            for (term in anchorTerms) {
                if (term.isEmpty()) continue
                var from = 0
                var count = 0
                while (from <= plainSearch.length - term.length && count < maxOccPerTerm) {
                    val idx = plainSearch.indexOf(term, startIndex = from)
                    if (idx == -1) break
                    positions.add(idx to term)
                    from = idx + 1
                    count++
                }
            }

            if (positions.isNotEmpty()) {
                val maxPossibleScore = anchorTerms.size
                var bestScore = 0

                for ((pos, term) in positions) {
                    // Count unique terms in window around this position
                    val windowStart = pos - effContext
                    val windowEnd = pos + term.length + effContext
                    var uniqueTerms = 0
                    val seen = mutableSetOf<String>()
                    for ((p, t) in positions) {
                        if (p in windowStart..windowEnd && seen.add(t)) uniqueTerms++
                    }
                    val score = uniqueTerms * 100 + term.length

                    if (score > bestScore) {
                        bestScore = score
                        plainIdx = pos
                        plainLen = term.length
                        // Early exit if we found all terms clustered
                        if (uniqueTerms >= maxPossibleScore) break
                    }
                }
            }
        }
        val plainStart = (plainIdx - effContext).coerceAtLeast(0)
        val plainEnd = (plainIdx + plainLen + effContext).coerceAtMost(plain.length)
        val origStart = HebrewTextUtils.mapToOrigIndex(mapToOrig, plainStart)
        val origEnd = HebrewTextUtils.mapToOrigIndex(mapToOrig, plainEnd).coerceAtMost(raw.length)

        val base = raw.substring(origStart, origEnd)
        val basePlain = plain.substring(plainStart, plainEnd)
        val basePlainSearch = HebrewTextUtils.replaceFinalsWithBase(basePlain)
        val baseMap = IntArray(plainEnd - plainStart) { idx ->
            (mapToOrig[plainStart + idx] - origStart).coerceIn(0, base.length.coerceAtLeast(1) - 1)
        }

        val pool = (highlightTerms + highlightTerms.map { it.trimEnd('$') }).distinct().filter { it.isNotBlank() }
        val intervals = mutableListOf<IntRange>()
        val basePlainLower = basePlainSearch.lowercase()

        fun isWordBoundary(text: String, index: Int): Boolean {
            if (index < 0 || index >= text.length) return true
            val ch = text[index]
            return ch.isWhitespace() || !ch.isLetterOrDigit()
        }

        for (term in pool) {
            if (term.isEmpty()) continue
            val t = term.lowercase()
            var from = 0
            while (from <= basePlainLower.length - t.length && t.isNotEmpty()) {
                val idx = basePlainLower.indexOf(t, startIndex = from)
                if (idx == -1) break

                val isAtWordStart = isWordBoundary(basePlainLower, idx - 1)
                val isAtWordEnd = isWordBoundary(basePlainLower, idx + t.length)
                val isWholeWord = isAtWordStart && isAtWordEnd
                val shouldHighlight = isWholeWord

                if (shouldHighlight) {
                    val startOrig = HebrewTextUtils.mapToOrigIndex(baseMap, idx)
                    val endOrig = HebrewTextUtils.mapToOrigIndex(baseMap, (idx + t.length - 1)) + 1
                    if (startOrig in 0 until endOrig && endOrig <= base.length) {
                        intervals += (startOrig until endOrig)
                    }
                }
                from = idx + 1
            }
        }

        val merged = mergeIntervals(intervals.sortedBy { it.first })
        val highlighted = insertBoldTags(base, merged)
        val prefix = if (origStart > 0) "..." else ""
        val suffix = if (origEnd < raw.length) "..." else ""
        return prefix + highlighted + suffix
    }

    private fun mergeIntervals(ranges: List<IntRange>): List<IntRange> {
        if (ranges.isEmpty()) return ranges
        val out = mutableListOf<IntRange>()
        var cur = ranges[0]
        for (i in 1 until ranges.size) {
            val r = ranges[i]
            if (r.first <= cur.last + 1) {
                cur = cur.first .. maxOf(cur.last, r.last)
            } else {
                out += cur
                cur = r
            }
        }
        out += cur
        return out
    }

    private fun insertBoldTags(text: String, intervals: List<IntRange>): String {
        if (intervals.isEmpty()) return text
        val sb = StringBuilder(text)
        for (r in intervals.asReversed()) {
            val start = r.first.coerceIn(0, sb.length)
            val end = (r.last + 1).coerceIn(0, sb.length)
            if (end > start) {
                sb.insert(end, "</b>")
                sb.insert(start, "<b>")
            }
        }
        return sb.toString()
    }

    private fun buildNgramTerms(tokens: List<String>, gram: Int = 4): List<String> {
        if (gram <= 0) return emptyList()
        val out = mutableListOf<String>()
        tokens.forEach { t ->
            val trimmed = t.trim()
            if (trimmed.length >= gram) {
                var i = 0
                while (i + gram <= trimmed.length) {
                    out += trimmed.substring(i, i + gram)
                    i += 1
                }
            }
        }
        return out.distinct()
    }

    private fun buildLimitedTermsForToken(
        token: String,
        expansions: List<MagicDictionaryIndex.Expansion>
    ): List<String> {
        if (expansions.isEmpty()) return listOf(token)

        val baseTerms = expansions.flatMap { it.base }.distinct()
        val otherTerms = expansions.flatMap { it.surface + it.variants }.distinct()

        val ordered = LinkedHashSet<String>()
        if (token.isNotBlank()) {
            ordered += token
        }
        baseTerms.forEach { ordered += it }
        otherTerms.forEach { ordered += it }

        val totalSize = ordered.size
        val limited = ordered.take(MAX_SYNONYM_TERMS_PER_TOKEN)
        if (totalSize > limited.size) {
            logger.d {
                "[DEBUG] Capped synonym terms for token '$token' from $totalSize to ${limited.size}"
            }
        }
        return limited
    }

    private fun buildTermAlternativesForTokens(
        tokens: List<String>,
        expansionsByToken: Map<String, List<MagicDictionaryIndex.Expansion>>
    ): List<List<String>> {
        if (tokens.isEmpty()) return emptyList()
        return tokens.map { token ->
            val expansions = expansionsByToken[token] ?: emptyList()
            buildLimitedTermsForToken(token, expansions)
        }
    }

    private fun loadHashemHighlightTerms(): List<String> {
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

        val out = terms.toList()
        logger.d { "[DEBUG] Hashem highlight terms from lexical DB: ${out.take(20)}..." }
        return out
    }
}
