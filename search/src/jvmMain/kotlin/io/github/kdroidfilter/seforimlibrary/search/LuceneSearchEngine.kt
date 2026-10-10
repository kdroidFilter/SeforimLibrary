package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.DocValuesType
import org.apache.lucene.index.FieldInfo
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.StoredFieldVisitor
import org.apache.lucene.index.StoredFields
import org.apache.lucene.index.Term
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BoostQuery
import org.apache.lucene.search.Collector
import org.apache.lucene.search.CollectorManager
import org.apache.lucene.search.FuzzyQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.PrefixQuery
import org.apache.lucene.search.PointRangeQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.QueryCachingPolicy
import org.apache.lucene.search.Scorable
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.ReferenceManager
import org.apache.lucene.search.ScoreMode
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.search.SearcherManager
import org.apache.lucene.search.TermQuery
import org.apache.lucene.util.QueryBuilder
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.document.IntPoint
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * Lucene-based implementation of SearchEngine for full-text search.
 * Supports Hebrew text with diacritics handling, dictionary expansion, and fuzzy matching.
 */
private val logger = Logger.withTag("LuceneSearchEngine")
// Hard cap on how many synonym/expansion terms we allow per token
private const val MAX_SYNONYM_TERMS_PER_TOKEN: Int = 32
// Global cap for boost queries built from dictionary expansions
private const val MAX_SYNONYM_BOOST_TERMS: Int = 256
// Constants for snippet source building (must match indexer)
private const val SNIPPET_NEIGHBOR_WINDOW = 4
private const val SNIPPET_MIN_LENGTH = 280
private const val FIELD_BOOK_ID = "book_id"
private const val FIELD_ANCESTOR_CATEGORY_IDS = "ancestor_category_ids"
// Searches run by warmUp(): one and several words, common and rarer ones, a quoted phrase, the divine name (whose
// dictionary entry is the largest)
val WARMUP_QUERIES = listOf("ברוך ה׳ לעולם", "אמר רבי יוחנן", "תפילין", "\"קריאת שמע\"")
const val WARMUP_ROUNDS = 2
const val WARMUP_PAGE_SIZE = 25

// Smallest partition worth a task of its own when a segment is split across cores
private const val MIN_DOCS_PER_SLICE = 100_000

/**
 * Searchers that split the index, even a single segment, into one partition per core and search them concurrently:
 * a merged index is one big segment, which a plain searcher scores on one thread.
 */
private object PartitionedSearcherFactory : SearcherFactory() {
    private val cores = Runtime.getRuntime().availableProcessors()
    private val executor: ExecutorService by lazy {
        val ids = AtomicInteger()
        Executors.newFixedThreadPool(cores) { task ->
            Thread(task, "lucene-search-${ids.incrementAndGet()}").apply { isDaemon = true }
        }
    }

    override fun newSearcher(reader: IndexReader, previousReader: IndexReader?): IndexSearcher {
        val searcher = if (cores == 1) {
            IndexSearcher(reader)
        } else {
            val docsPerSlice = maxOf(MIN_DOCS_PER_SLICE, reader.maxDoc() / cores + 1)
            object : IndexSearcher(reader, executor) {
                override fun slices(leaves: List<LeafReaderContext>): Array<LeafSlice> =
                    slices(leaves, docsPerSlice, Int.MAX_VALUE, true)
            }
        }
        return searcher.apply { queryCachingPolicy = StableFiltersCachingPolicy }
    }
}

/**
 * Caches only the filters every search repeats (line documents, base books). Lucene's default policy also caches any
 * clause seen twice, and each search builds its word filters twice (facets, then results): it would then turn them
 * into bitsets over the whole index for a query that won't come back.
 */
private object StableFiltersCachingPolicy : QueryCachingPolicy {
    override fun onUse(query: Query) = Unit

    override fun shouldCache(query: Query): Boolean = when (query) {
        is TermQuery -> query.term.field() == "type"
        is PointRangeQuery -> query.field == "is_base_book"
        else -> false
    }
}

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

class LuceneSearchEngine(
    private val indexDir: Path,
    private val snippetProvider: SnippetProvider? = null,
    private val analyzer: Analyzer = StandardAnalyzer(),
    private val dictionaryPath: Path? = null
) : SearchEngine {

    // Open Lucene directory lazily to avoid any I/O at app startup.
    // GraalVM native image does not support MMapDirectory (Panama foreign downcalls), use NIOFSDirectory instead.
    private val dir by lazy {
        if (System.getProperty("org.graalvm.nativeimage.imagecode") != null) {
            NIOFSDirectory(indexDir)
        } else {
            FSDirectory.open(indexDir)
        }
    }

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

    // One reader shared by every search: opening one per search re-reads the term index of each segment and maps,
    // then unmaps, all the files. Refreshed when the index changes on disk (delta updates write into it).
    private val searcherManagerLazy = lazy {
        SearcherManager(dir, PartitionedSearcherFactory).apply {
            addListener(
                object : ReferenceManager.RefreshListener {
                    override fun beforeRefresh() = Unit

                    override fun afterRefresh(didRefresh: Boolean) {
                        if (didRefresh) bookAncestors.clear()
                    }
                }
            )
        }
    }

    private val searcherManager: SearcherManager by searcherManagerLazy

    // Ancestor category ids per book, for the facets: every line of a book shares them
    private val bookAncestors = ConcurrentHashMap<Long, LongArray>()

    private fun acquireSearcher(): IndexSearcher {
        val manager = searcherManager
        manager.maybeRefresh()
        return manager.acquire()
    }

    private inline fun <T> withSearcher(block: (IndexSearcher) -> T): T {
        val searcher = acquireSearcher()
        try {
            return block(searcher)
        } finally {
            searcherManager.release(searcher)
        }
    }

    // --- SearchEngine interface implementation ---

    override fun openSession(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean
    ): SearchSession? {
        val context = buildSearchContext(query, near, bookFilter, categoryFilter, bookIds, lineIds, baseBookOnly) ?: return null
        return LuceneSearchSession(context.query, context.snippetTerms, acquireSearcher())
    }

    override suspend fun attachSnippets(hits: List<LineHit>, query: String, near: Int): List<LineHit> {
        if (hits.isEmpty()) return hits
        val context = buildSearchContext(query, near, null, null, null, null) ?: return hits
        val sources = withContext(Dispatchers.IO) { snippetSources(hits.map { LineSnippetInfo(it.lineId, it.bookId, it.lineIndex) }) }
        return hits.map { hit ->
            val raw = sources[hit.lineId] ?: hit.rawText
            hit.copy(snippet = SnippetBuilder.build(raw, context.snippetTerms), rawText = raw)
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
            ids.toList()
        }
    }

    override fun buildSnippet(rawText: String, query: String, near: Int): String {
        val parsed = SearchQueryParser.parse(query)
        val norm = HebrewTextUtils.normalizeHebrew(parsed.freeText)
        val exactPhrasesNorm = parsed.exactPhrases
            .map { HebrewTextUtils.normalizeHebrew(it) }
            .filter { it.isNotBlank() }
        if (norm.isBlank() && exactPhrasesNorm.isEmpty()) return Jsoup.clean(rawText, Safelist.none())
        val rawClean = Jsoup.clean(rawText, Safelist.none())
        val analyzedStd = (analyzeToTerms(stdAnalyzer, norm) ?: emptyList())
        val hasHashem = query.contains("ה׳") || query.contains("ה'")
        val hashemTerms = if (hasHashem) loadHashemHighlightTerms() else emptyList()
        // Verbatim tokens from the quoted phrases must be highlighted too (no expansion).
        val exactPhraseTokens = exactPhrasesNorm.flatMap { analyzeToTerms(stdAnalyzer, it) ?: emptyList() }
        val highlightTerms = filterTermsForHighlight(
            analyzedStd + buildNgramTerms(analyzedStd, gram = 4) + hashemTerms + exactPhraseTokens
        )
        val anchorBasis = (sequenceOf(norm) + exactPhrasesNorm.asSequence())
            .filter { it.isNotBlank() }
            .joinToString(" ")
        val anchorTerms = buildAnchorTerms(anchorBasis, highlightTerms)
        return SnippetBuilder.build(rawClean, SnippetTerms(anchorTerms, highlightTerms))
    }

    override fun findInBookCandidates(query: String, bookId: Long): LongArray? {
        val norm = HebrewTextUtils.normalizeHebrew(query)
        if (norm.isBlank()) return LongArray(0)
        val bookLines = BooleanQuery.Builder()
            .add(TermQuery(Term("type", "line")), BooleanClause.Occur.FILTER)
            .add(IntPoint.newExactQuery("book_id", bookId.toInt()), BooleanClause.Occur.FILTER)
            .build()
        // Nothing to filter on: every line would be a candidate, cheaper scanned from the DB
        val clauses = literalPresenceClauses(analyzeToTerms(stdAnalyzer, norm).orEmpty())
        if (clauses.isEmpty()) return null
        val builder = BooleanQuery.Builder().add(bookLines, BooleanClause.Occur.FILTER)
        clauses.forEach { builder.add(it, BooleanClause.Occur.FILTER) }

        return withSearcher { searcher ->
            // Only line_id is read per hit: no scoring, no other stored field. One collector per slice, as the
            // slices may run concurrently
            class LineIdCollector : Collector {
                var ids = LongArray(256)
                var count = 0

                override fun getLeafCollector(leafContext: LeafReaderContext): LeafCollector {
                    val storedFields = leafContext.reader().storedFields()
                    return object : LeafCollector {
                        override fun setScorer(scorer: Scorable) = Unit

                        override fun collect(doc: Int) {
                            val lineId = storedFields.document(doc, setOf("line_id"))
                                .getField("line_id")?.numericValue()?.toLong() ?: return
                            if (count == ids.size) ids = ids.copyOf(count * 2)
                            ids[count++] = lineId
                        }
                    }
                }

                override fun scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
            }
            val ids = searcher.search(
                builder.build(),
                object : CollectorManager<LineIdCollector, LongArray> {
                    override fun newCollector() = LineIdCollector()

                    override fun reduce(collectors: Collection<LineIdCollector>): LongArray {
                        val out = LongArray(collectors.sumOf { it.count })
                        var at = 0
                        for (c in collectors) {
                            c.ids.copyInto(out, at, 0, c.count)
                            at += c.count
                        }
                        return out
                    }
                }
            )
            val count = ids.size
            // No candidate may mean a book the index lacks (older index, book added later): unknown, not "no match"
            if (count == 0 && searcher.count(bookLines) == 0) null else ids
        }
    }

    /**
     * Filters every line holding the analyzed [tokens] as one literal substring satisfies. The
     * substring may start and end mid-word: the first token is only a word suffix, the last a
     * word prefix, the inner ones whole words. A first or lone token is matched anywhere in a
     * word through its 4-grams, so one under 4 letters can't filter (a leading wildcard would
     * scan the whole term dictionary); a last one under 3 neither, its prefix being too common.
     */
    private fun literalPresenceClauses(tokens: List<String>): List<Query> =
        tokens.mapIndexedNotNull { i, token ->
            when {
                i in 1 until tokens.lastIndex -> TermQuery(Term("text", token))
                i > 0 && token.length >= 3 -> PrefixQuery(Term("text", token))
                else -> buildNgramPresenceForToken(token)
            }
        }

    override fun close() {
        if (searcherManagerLazy.isInitialized()) runCatching { searcherManager.close() }
    }

    override fun computeFacets(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean
    ): SearchFacets? {
        val context = buildSearchContext(query, near, bookFilter, categoryFilter, bookIds, lineIds, baseBookOnly)
            ?: return null

        return withSearcher { searcher ->
            // Only book ids are read per hit (doc values when the index has them, else the first stored field);
            // category counts follow from the book counts, as every line of a book shares its ancestors. One
            // collector per slice, as the slices may run concurrently
            class FacetCollector : Collector {
                val bookCounts = HashMap<Long, Int>()
                var totalHits = 0L

                override fun getLeafCollector(leafContext: LeafReaderContext): LeafCollector {
                    val reader = leafContext.reader()
                    val storedFields = reader.storedFields()
                    val bookIdValues: NumericDocValues? =
                        reader.fieldInfos.fieldInfo(FIELD_BOOK_ID)
                            ?.takeIf { it.docValuesType == DocValuesType.NUMERIC }
                            ?.let { reader.getNumericDocValues(FIELD_BOOK_ID) }
                    // Lines of a book are mostly contiguous: count runs, then fold them into the map
                    var runBook = -1L
                    var runCount = 0

                    fun flushRun() {
                        if (runCount > 0) bookCounts.merge(runBook, runCount, Int::plus)
                        runCount = 0
                    }

                    return object : LeafCollector {
                        override fun setScorer(scorer: Scorable) = Unit

                        override fun collect(doc: Int) {
                            totalHits++
                            val bookId =
                                if (bookIdValues != null && bookIdValues.advanceExact(doc)) {
                                    bookIdValues.longValue()
                                } else {
                                    readStoredBookId(storedFields, doc) ?: return
                                }
                            if (bookId != runBook) {
                                flushRun()
                                runBook = bookId
                                if (!bookAncestors.containsKey(bookId)) loadAncestors(storedFields, doc, bookId)
                            }
                            runCount++
                        }

                        override fun finish() = flushRun()
                    }
                }

                override fun scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
            }

            searcher.search(
                context.matchQuery,
                object : CollectorManager<FacetCollector, SearchFacets> {
                    override fun newCollector() = FacetCollector()

                    override fun reduce(collectors: Collection<FacetCollector>): SearchFacets {
                        val bookCounts = HashMap<Long, Int>()
                        collectors.forEach { c -> c.bookCounts.forEach { (book, n) -> bookCounts.merge(book, n, Int::plus) } }
                        val categoryCounts = HashMap<Long, Int>()
                        for ((bookId, count) in bookCounts) {
                            bookAncestors[bookId]?.forEach { categoryCounts.merge(it, count, Int::plus) }
                        }
                        return SearchFacets(
                            totalHits = collectors.sumOf { it.totalHits },
                            categoryCounts = categoryCounts,
                            bookCounts = bookCounts
                        )
                    }
                }
            )
        }
    }

    private fun readStoredBookId(storedFields: StoredFields, doc: Int): Long? {
        var bookId: Long? = null
        storedFields.document(
            doc,
            object : StoredFieldVisitor() {
                override fun needsField(fieldInfo: FieldInfo): Status = when {
                    bookId != null -> Status.STOP
                    fieldInfo.name == FIELD_BOOK_ID -> Status.YES
                    else -> Status.NO
                }

                override fun longField(fieldInfo: FieldInfo, value: Long) {
                    bookId = value
                }

                override fun intField(fieldInfo: FieldInfo, value: Int) {
                    bookId = value.toLong()
                }
            }
        )
        return bookId
    }

    private fun loadAncestors(storedFields: StoredFields, doc: Int, bookId: Long) {
        val ancestors = storedFields.document(doc, setOf(FIELD_ANCESTOR_CATEGORY_IDS))
            .getField(FIELD_ANCESTOR_CATEGORY_IDS)?.stringValue()
            ?.split(",")
            ?.mapNotNull { it.trim().toLongOrNull() }
            .orEmpty()
        // Lines added by delta updates lack the field: try again on the book's next run
        if (ancestors.isNotEmpty()) bookAncestors[bookId] = ancestors.toLongArray()
    }

    // --- Inner SearchSession class ---

    inner class LuceneSearchSession internal constructor(
        private val query: Query,
        private val snippetTerms: SnippetTerms,
        private val searcher: IndexSearcher
    ) : SearchSession {
        private var after: ScoreDoc? = null
        private var finished = false
        private var totalHitsValue: Long? = null

        override suspend fun nextPage(limit: Int, snippets: Boolean): SearchPage? = withContext(Dispatchers.IO) {
            if (finished) return@withContext null
            val top = searcher.searchAfter(after, query, limit)
            if (totalHitsValue == null) totalHitsValue = top.totalHits?.value
            if (top.scoreDocs.isEmpty()) {
                finished = true
                return@withContext null
            }
            val stored = searcher.storedFields()
            val hits = mapScoreDocs(stored, top.scoreDocs.asList(), snippetTerms, snippets)
            after = top.scoreDocs.last()
            val isLast = top.scoreDocs.size < limit
            if (isLast) finished = true
            SearchPage(
                hits = hits,
                totalHits = totalHitsValue ?: hits.size.toLong(),
                isLastPage = isLast
            )
        }

        override fun close() {
            searcherManager.release(searcher)
        }
    }

    // --- Additional public search methods ---

    suspend fun searchAllText(rawQuery: String, near: Int = 5, limit: Int, offset: Int = 0): List<LineHit> =
        doSearch(rawQuery, near, limit, offset, bookFilter = null, categoryFilter = null)

    suspend fun searchInBook(rawQuery: String, near: Int, bookId: Long, limit: Int, offset: Int = 0): List<LineHit> =
        doSearch(rawQuery, near, limit, offset, bookFilter = bookId, categoryFilter = null)

    suspend fun searchInCategory(rawQuery: String, near: Int, categoryId: Long, limit: Int, offset: Int = 0): List<LineHit> =
        doSearch(rawQuery, near, limit, offset, bookFilter = null, categoryFilter = categoryId)

    suspend fun searchInBooks(rawQuery: String, near: Int, bookIds: Collection<Long>, limit: Int, offset: Int = 0): List<LineHit> =
        doSearchInBooks(rawQuery, near, limit, offset, bookIds)

    // --- Private implementation ---

    private class SearchContext(
        val query: Query,
        // The required clauses only: the same matches as [query], without the ranking-only clauses (fuzzy, n-grams,
        // synonym boosts) that cost a rewrite but can't change the hit set
        val matchQuery: Query,
        val anchorTerms: List<String>,
        val highlightTerms: List<String>
    ) {
        val snippetTerms by lazy { SnippetTerms(anchorTerms, highlightTerms) }
    }

    private fun buildSearchContext(
        rawQuery: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean = false
    ): SearchContext? {
        // Split the raw query into exact phrases (wrapped in ASCII double quotes) and free text.
        // Quoted segments are matched verbatim WITHOUT dictionary expansion (Google-style).
        val parsed = SearchQueryParser.parse(rawQuery)
        val norm = HebrewTextUtils.normalizeHebrew(parsed.freeText)
        val exactPhrasesNorm = parsed.exactPhrases
            .map { HebrewTextUtils.normalizeHebrew(it) }
            .filter { it.isNotBlank() }

        if (norm.isBlank() && exactPhrasesNorm.isEmpty()) return null

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
        magicDict?.prefetch(analyzedStd)
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
        // Verbatim tokens from the quoted phrases must be highlighted too (no expansion).
        val exactPhraseTokens = exactPhrasesNorm.flatMap { analyzeToTerms(stdAnalyzer, it) ?: emptyList() }
        val highlightTerms = filterTermsForHighlight(
            analyzedStd + expandedTerms + ngramTerms + hashemTerms + exactPhraseTokens
        )
        val anchorBasis = (sequenceOf(norm) + exactPhrasesNorm.asSequence())
            .filter { it.isNotBlank() }
            .joinToString(" ")
        val anchorTerms = buildAnchorTerms(anchorBasis, highlightTerms)

        val builder = BooleanQuery.Builder()
        // Mirrors every required clause of builder (the facets need only the hit set)
        val matchBuilder = BooleanQuery.Builder()
        fun require(query: Query, occur: BooleanClause.Occur) {
            builder.add(query, occur)
            matchBuilder.add(query, occur)
        }
        require(TermQuery(Term("type", "line")), BooleanClause.Occur.FILTER)
        if (bookFilter != null) require(IntPoint.newExactQuery("book_id", bookFilter.toInt()), BooleanClause.Occur.FILTER)
        if (categoryFilter != null) require(IntPoint.newExactQuery("category_id", categoryFilter.toInt()), BooleanClause.Occur.FILTER)
        // Filter by base books only (is_base_book = 1) when baseBookOnly is true
        if (baseBookOnly) require(IntPoint.newExactQuery("is_base_book", 1), BooleanClause.Occur.FILTER)
        val bookIdsArray = bookIds?.map { it.toInt() }?.toIntArray()
        if (bookIdsArray != null && bookIdsArray.isNotEmpty()) {
            require(IntPoint.newSetQuery("book_id", *bookIdsArray), BooleanClause.Occur.FILTER)
        }
        val lineIdsArray = lineIds?.map { it.toInt() }?.toIntArray()
        if (lineIdsArray != null && lineIdsArray.isNotEmpty()) {
            require(IntPoint.newSetQuery("line_id", *lineIdsArray), BooleanClause.Occur.FILTER)
        }
        // Quoted phrases: each must appear verbatim as an exact, in-order, adjacent phrase.
        var exactClausesAdded = 0
        for (phrase in exactPhrasesNorm) {
            val exactQuery = buildExactPhraseQuery(phrase) ?: continue
            require(exactQuery, BooleanClause.Occur.MUST)
            exactClausesAdded++
            logger.d { "[DEBUG] Added exact-phrase MUST for: \"$phrase\"" }
        }

        // A fully-quoted query that produced no usable phrase clause (e.g. just punctuation)
        // must not fall through to a match-all query.
        if (norm.isBlank() && exactClausesAdded == 0) return null

        // Free (unquoted) text: dictionary-aware ranking + presence filtering.
        if (norm.isNotBlank()) {
            val rankedQuery = buildExpandedQuery(norm, near, analyzedStd, tokenExpansions)
            val mustAllTokensQuery: Query? = buildPresenceFilterForTokens(analyzedStd, near, tokenExpansions)
            val phraseQuery: Query? = buildSynonymPhraseQuery(analyzedStd, tokenExpansions, near)
            if (mustAllTokensQuery != null) {
                require(mustAllTokensQuery, BooleanClause.Occur.FILTER)
                logger.d { "[DEBUG] Added mustAllTokensQuery as FILTER" }
            }
            if (phraseQuery != null && analyzedStd.size >= 2) {
                if (near == 0) require(phraseQuery, BooleanClause.Occur.MUST) else builder.add(phraseQuery, BooleanClause.Occur.SHOULD)
                logger.d { "[DEBUG] Added phraseQuery, near=$near" }
            }
            builder.add(rankedQuery, BooleanClause.Occur.SHOULD)
            logger.d { "[DEBUG] Added rankedQuery as SHOULD" }
        }

        val finalQuery = builder.build()
        logger.d { "[DEBUG] Final query: $finalQuery" }

        return SearchContext(
            query = finalQuery,
            matchQuery = matchBuilder.build(),
            anchorTerms = anchorTerms,
            highlightTerms = highlightTerms
        )
    }

    // suspend because it calls snippetProvider.getSnippetSources();
    // callers are responsible for providing Dispatchers.IO context.
    private suspend fun mapScoreDocs(
        stored: StoredFields,
        scoreDocs: List<ScoreDoc>,
        snippetTerms: SnippetTerms,
        snippets: Boolean = true
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

        val snippetSources: Map<Long, String> =
            if (snippets) snippetSources(docMetas.map { LineSnippetInfo(it.lineId, it.bookId, it.lineIndex) }) else emptyMap()

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

            val snippet = if (snippets) SnippetBuilder.build(raw, snippetTerms) else ""
            LineHit(
                bookId = meta.bookId,
                bookTitle = meta.bookTitle,
                lineId = meta.lineId,
                lineIndex = meta.lineIndex,
                snippet = snippet,
                score = boostedScore,
                rawText = raw,
                isBaseBook = meta.isBaseBook
            )
        }
        // Re-sort by boosted score (descending)
        return hits.sortedByDescending { it.score }
    }

    // Snippet sources: from the provider if available, otherwise none (the indexed text_raw fallback)
    private suspend fun snippetSources(lines: List<LineSnippetInfo>): Map<Long, String> =
        snippetProvider?.getSnippetSources(lines) ?: emptyMap()

    private suspend fun doSearch(
        rawQuery: String,
        near: Int,
        limit: Int,
        offset: Int,
        bookFilter: Long?,
        categoryFilter: Long?
    ): List<LineHit> {
        val context = buildSearchContext(rawQuery, near, bookFilter, categoryFilter, null, null) ?: return emptyList()
        return withContext(Dispatchers.IO) {
            withSearcher { searcher ->
                val top = searcher.search(context.query, offset + limit)
                val stored: StoredFields = searcher.storedFields()
                val sliced = top.scoreDocs.drop(offset)
                mapScoreDocs(stored, sliced, context.snippetTerms)
            }
        }
    }

    private suspend fun doSearchInBooks(
        rawQuery: String,
        near: Int,
        limit: Int,
        offset: Int,
        bookIds: Collection<Long>
    ): List<LineHit> {
        if (bookIds.isEmpty()) return emptyList()
        val context = buildSearchContext(rawQuery, near, bookFilter = null, categoryFilter = null, bookIds = bookIds, lineIds = null) ?: return emptyList()
        return withContext(Dispatchers.IO) {
            withSearcher { searcher ->
                val top = searcher.search(context.query, offset + limit)
                val stored: StoredFields = searcher.storedFields()
                val sliced = top.scoreDocs.drop(offset)
                mapScoreDocs(stored, sliced, context.snippetTerms)
            }
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

    /**
     * Builds a strict, verbatim phrase query for a quoted segment: terms must appear in the
     * given order and be adjacent (slop = 0), with NO dictionary expansion, fuzzy matching or
     * n-gram substring matching. A single-token segment becomes an exact [TermQuery].
     *
     * Nikud and final letters are still normalised (the segment is normalised by the caller and
     * tokenised through [stdAnalyzer]) so that the phrase matches the indexed form.
     */
    private fun buildExactPhraseQuery(phraseNorm: String): Query? {
        if (phraseNorm.isBlank()) return null
        val qb = QueryBuilder(stdAnalyzer)
        qb.createPhraseQuery("text", phraseNorm, 0)?.let { return it }
        return qb.createBooleanQuery("text", phraseNorm, BooleanClause.Occur.MUST)
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

    // The divine name's forms never change: read once from the dictionary, not on every search
    private val hashemHighlightTerms: List<String> by lazy { readHashemHighlightTerms() }

    private fun loadHashemHighlightTerms(): List<String> = hashemHighlightTerms

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

        val out = terms.toList()
        logger.d { "[DEBUG] Hashem highlight terms from lexical DB: ${out.take(20)}..." }
        return out
    }
}
