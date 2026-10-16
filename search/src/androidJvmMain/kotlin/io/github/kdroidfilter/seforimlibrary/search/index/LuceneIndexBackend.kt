package io.github.kdroidfilter.seforimlibrary.search.index

import io.github.kdroidfilter.seforimlibrary.search.SearchFacets
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.document.IntPoint
import org.apache.lucene.index.DocValuesType
import org.apache.lucene.index.FieldInfo
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.ReaderUtil
import org.apache.lucene.index.StoredFieldVisitor
import org.apache.lucene.index.StoredFields
import org.apache.lucene.index.Term
import org.apache.lucene.index.VectorSimilarityFunction
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BoostQuery
import org.apache.lucene.search.Collector
import org.apache.lucene.search.CollectorManager
import org.apache.lucene.search.FuzzyQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.KnnByteVectorQuery
import org.apache.lucene.search.KnnFloatVectorQuery
import org.apache.lucene.search.LRUQueryCache
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.search.MultiPhraseQuery
import org.apache.lucene.search.PointRangeQuery
import org.apache.lucene.search.PrefixQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.QueryCachingPolicy
import org.apache.lucene.search.ReferenceManager
import org.apache.lucene.search.Scorable
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.ScoreMode
import org.apache.lucene.search.SearcherFactory
import org.apache.lucene.search.SearcherManager
import org.apache.lucene.search.Sort
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.TopDocs
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.util.QueryBuilder
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import org.apache.lucene.index.VectorEncoding as LuceneVectorEncoding
import org.apache.lucene.search.SortField as LuceneSortField

private const val FIELD_BOOK_ID = "book_id"
private const val FIELD_ANCESTOR_CATEGORY_IDS = "ancestor_category_ids"

// Smallest partition worth a task of its own when a segment is split across cores
private const val MIN_DOCS_PER_SLICE = 100_000

/**
 * The [LuceneIndex] of a Lucene directory, read with Lucene in this process: on the JVM, on Android (ART, API 35+),
 * and inside the Java runtime the iOS app embeds (see `IndexBridgeServer`).
 *
 * @param indexDir the index directory
 * @param cacheQueries whether to cache any query seen often (small indexes: the lookup index); else only the filters
 *   every search repeats
 * @param analyzer the analyzer the index was written with
 */
class LuceneIndexBackend(
    private val indexDir: Path,
    private val cacheQueries: Boolean = false,
    private val analyzer: Analyzer = StandardAnalyzer(),
) : LuceneIndex {
    // Open lazily to avoid any I/O at app startup
    private val dir: Directory by lazy { openDirectory(indexDir) }

    // One reader shared by every search: opening one per search re-reads the term index of each segment and maps,
    // then unmaps, all the files. Refreshed when the index changes on disk (delta updates write into it).
    private val searcherManagerLazy =
        lazy {
            SearcherManager(dir, searcherFactory()).apply {
                addListener(
                    object : ReferenceManager.RefreshListener {
                        override fun beforeRefresh() = Unit

                        override fun afterRefresh(didRefresh: Boolean) {
                            if (didRefresh) bookAncestors.clear()
                        }
                    },
                )
            }
        }
    private val searcherManager: SearcherManager by searcherManagerLazy

    // Ancestor category ids per book, for the facets: every line of a book shares them
    private val bookAncestors = ConcurrentHashMap<Long, LongArray>()

    private fun searcherFactory(): SearcherFactory =
        if (!queryCacheEnabled) {
            // Lucene's query cache changes how the filters run, and with it the last digits of the scores: off, the
            // same search ranks the same every time (the parity tests across platforms)
            object : SearcherFactory() {
                override fun newSearcher(
                    reader: IndexReader,
                    previousReader: IndexReader?,
                ): IndexSearcher = PartitionedSearcherFactory.newSearcher(reader, previousReader).apply { queryCache = null }
            }
        } else if (!cacheQueries) {
            PartitionedSearcherFactory
        } else {
            object : SearcherFactory() {
                override fun newSearcher(
                    reader: IndexReader,
                    previousReader: IndexReader?,
                ): IndexSearcher =
                    IndexSearcher(reader).apply {
                        queryCache = LRUQueryCache(1024, 64L * 1024 * 1024) // up to 64MiB cache
                        queryCachingPolicy = UsageTrackingQueryCachingPolicy()
                    }
            }
        }

    private fun acquire(): IndexSearcher {
        val manager = searcherManager
        manager.maybeRefresh()
        return manager.acquire()
    }

    private inline fun <T> withSearcher(block: (IndexSearcher) -> T): T {
        val searcher = acquire()
        try {
            return block(searcher)
        } finally {
            searcherManager.release(searcher)
        }
    }

    override fun analyze(text: String): List<String> =
        try {
            val out = mutableListOf<String>()
            analyzer.tokenStream("text", text).use { ts ->
                val termAtt = ts.addAttribute(CharTermAttribute::class.java)
                ts.reset()
                while (ts.incrementToken()) {
                    val t = termAtt.toString()
                    if (t.isNotBlank()) out += t
                }
                ts.end()
            }
            out
        } catch (_: Exception) {
            emptyList()
        }

    override fun search(
        query: IndexQuery,
        limit: Int,
        fields: Set<String>,
        sort: List<SortField>?,
    ): IndexPage =
        withSearcher { searcher ->
            val luceneQuery = toLucene(query)
            val top =
                if (sort.isNullOrEmpty()) {
                    searcher.search(luceneQuery, limit)
                } else {
                    searcher.search(luceneQuery, limit, Sort(*sort.map { it.toLucene() }.toTypedArray()))
                }
            top.toPage(searcher.storedFields(), fields)
        }

    override fun openCursor(
        query: IndexQuery,
        fields: Set<String>,
    ): IndexCursor = Cursor(toLucene(query), fields, acquire())

    private inner class Cursor(
        private val query: Query,
        private val fields: Set<String>,
        private val searcher: IndexSearcher,
    ) : IndexCursor {
        private var after: ScoreDoc? = null
        private var totalHits: Long? = null
        private var closed = false

        override fun next(limit: Int): IndexPage {
            val top = searcher.searchAfter(after, query, limit)
            if (totalHits == null) totalHits = top.totalHits.value()
            top.scoreDocs.lastOrNull()?.let { after = it }
            return IndexPage(readDocs(searcher.storedFields(), top.scoreDocs.asList(), fields), totalHits ?: 0)
        }

        override fun close() {
            if (closed) return
            closed = true
            searcherManager.release(searcher)
        }
    }

    override fun facets(query: IndexQuery): SearchFacets =
        withSearcher { searcher ->
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
                        reader.fieldInfos
                            .fieldInfo(FIELD_BOOK_ID)
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
                toLucene(query),
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
                            bookCounts = bookCounts,
                        )
                    }
                },
            )
        }

    override fun collectLongs(
        query: IndexQuery,
        field: String,
    ): LongArray =
        withSearcher { searcher ->
            // One collector per slice, as the slices may run concurrently
            class LongCollector : Collector {
                var values = LongArray(256)
                var count = 0

                override fun getLeafCollector(leafContext: LeafReaderContext): LeafCollector {
                    val storedFields = leafContext.reader().storedFields()
                    val wanted = setOf(field)
                    return object : LeafCollector {
                        override fun setScorer(scorer: Scorable) = Unit

                        override fun collect(doc: Int) {
                            val value =
                                storedFields
                                    .document(doc, wanted)
                                    .getField(field)
                                    ?.numericValue()
                                    ?.toLong() ?: return
                            if (count == values.size) values = values.copyOf(count * 2)
                            values[count++] = value
                        }
                    }
                }

                override fun scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
            }
            searcher.search(
                toLucene(query),
                object : CollectorManager<LongCollector, LongArray> {
                    override fun newCollector() = LongCollector()

                    override fun reduce(collectors: Collection<LongCollector>): LongArray {
                        val out = LongArray(collectors.sumOf { it.count })
                        var at = 0
                        for (c in collectors) {
                            c.values.copyInto(out, at, 0, c.count)
                            at += c.count
                        }
                        return out
                    }
                },
            )
        }

    override fun count(query: IndexQuery): Int = withSearcher { it.count(toLucene(query)) }

    override fun vectorEncoding(field: String): VectorEncoding? =
        withSearcher { searcher ->
            searcher.indexReader
                .leaves()
                .firstNotNullOfOrNull { it.reader().fieldInfos.fieldInfo(field) }
                ?.let { if (it.vectorEncoding == LuceneVectorEncoding.BYTE) VectorEncoding.BYTE else VectorEncoding.FLOAT32 }
        }

    override fun knn(request: KnnRequest): List<IndexDoc> =
        withSearcher { searcher ->
            val filter = request.filter?.let { toLucene(it) }
            val byteVector = request.byteVector
            val floatVector = request.floatVector
            val top: List<ScoreDoc> =
                when {
                    byteVector != null ->
                        searcher.search(KnnByteVectorQuery(request.field, byteVector, request.k, filter), request.k).scoreDocs.asList()
                    floatVector != null && request.oversample > 1 ->
                        rescoredKnn(searcher, request.field, floatVector, request.k, request.oversample, filter)
                    floatVector != null ->
                        searcher.search(KnnFloatVectorQuery(request.field, floatVector, request.k, filter), request.k).scoreDocs.asList()
                    else -> error("KnnRequest without a vector")
                }
            readDocs(searcher.storedFields(), top, request.fields)
        }

    /**
     * KNN over a field quantized by Lucene (float vectors stored next to their int8 copy): the graph search ranks on the
     * approximate int8 similarity, so it gathers [oversample] times more candidates and keeps the [k] best by their
     * exact float similarity. Empty when the field holds no eligible document.
     */
    private fun rescoredKnn(
        searcher: IndexSearcher,
        field: String,
        query: FloatArray,
        k: Int,
        oversample: Int,
        filter: Query?,
    ): List<ScoreDoc> {
        val candidates = searcher.search(KnnFloatVectorQuery(field, query, k * oversample, filter), k * oversample).scoreDocs
        if (candidates.isEmpty()) return emptyList()
        val leaves = searcher.indexReader.leaves()
        val rescored = ArrayList<ScoreDoc>(candidates.size)
        // By segment, in doc order: the vector iterators only move forward
        for ((leafIndex, docs) in candidates.groupBy { ReaderUtil.subIndex(it.doc, leaves) }) {
            val leaf = leaves[leafIndex]
            val values = leaf.reader().getFloatVectorValues(field) ?: continue
            val iterator = values.iterator()
            for (doc in docs.map { it.doc }.sorted()) {
                if (iterator.advance(doc - leaf.docBase) != doc - leaf.docBase) continue
                rescored += ScoreDoc(doc, VectorSimilarityFunction.COSINE.compare(query, values.vectorValue(iterator.index())))
            }
        }
        return rescored.sortedByDescending { it.score }.take(k)
    }

    override fun close() {
        if (searcherManagerLazy.isInitialized()) runCatching { searcherManager.close() }
    }

    // --- Lucene mapping ---

    private fun toLucene(query: IndexQuery): Query =
        when (query) {
            is IndexQuery.Term -> TermQuery(Term(query.field, query.text))
            is IndexQuery.Prefix -> PrefixQuery(Term(query.field, query.text))
            is IndexQuery.Fuzzy -> FuzzyQuery(Term(query.field, query.text), query.maxEdits)
            is IndexQuery.Bool ->
                BooleanQuery
                    .Builder()
                    .apply { query.clauses.forEach { add(toLucene(it.query), it.occur.toLucene()) } }
                    .build()
            is IndexQuery.Boost -> BoostQuery(toLucene(query.query), query.boost)
            is IndexQuery.MultiPhrase ->
                MultiPhraseQuery
                    .Builder()
                    .apply {
                        setSlop(query.slop)
                        query.positions.forEachIndexed { position, alternatives ->
                            add(alternatives.map { Term(query.field, it) }.toTypedArray(), position)
                        }
                    }.build()
            is IndexQuery.AnalyzedPhrase -> {
                val builder = QueryBuilder(analyzer)
                builder.createPhraseQuery(query.field, query.text, query.slop)
                    ?: builder.createBooleanQuery(query.field, query.text, BooleanClause.Occur.MUST)
                    ?: BooleanQuery.Builder().build()
            }
            is IndexQuery.IntExact -> IntPoint.newExactQuery(query.field, query.value)
            is IndexQuery.IntSet -> IntPoint.newSetQuery(query.field, *query.values)
            IndexQuery.MatchAll -> MatchAllDocsQuery()
        }

    private fun Occur.toLucene(): BooleanClause.Occur =
        when (this) {
            Occur.MUST -> BooleanClause.Occur.MUST
            Occur.SHOULD -> BooleanClause.Occur.SHOULD
            Occur.FILTER -> BooleanClause.Occur.FILTER
            Occur.MUST_NOT -> BooleanClause.Occur.MUST_NOT
        }

    private fun SortField.toLucene(): LuceneSortField =
        LuceneSortField(
            field,
            when (type) {
                SortField.Type.INT -> LuceneSortField.Type.INT
                SortField.Type.LONG -> LuceneSortField.Type.LONG
            },
            reverse,
        )

    private fun TopDocs.toPage(
        stored: StoredFields,
        fields: Set<String>,
    ): IndexPage = IndexPage(readDocs(stored, scoreDocs.asList(), fields), totalHits.value())

    private fun readDocs(
        stored: StoredFields,
        scoreDocs: List<ScoreDoc>,
        fields: Set<String>,
    ): List<IndexDoc> =
        scoreDocs.map { sd ->
            val longs = HashMap<String, Long>()
            val strings = HashMap<String, String>()
            if (fields.isNotEmpty()) {
                for (field in stored.document(sd.doc, fields)) {
                    val number = field.numericValue()
                    if (number != null) {
                        longs[field.name()] = number.toLong()
                    } else {
                        field.stringValue()?.let { strings[field.name()] = it }
                    }
                }
            }
            // A sorted search has no score (NaN): 0 instead, the same on every platform
            IndexDoc(if (sd.score.isNaN()) 0f else sd.score, longs, strings)
        }

    private fun readStoredBookId(
        storedFields: StoredFields,
        doc: Int,
    ): Long? {
        var bookId: Long? = null
        storedFields.document(
            doc,
            object : StoredFieldVisitor() {
                override fun needsField(fieldInfo: FieldInfo): Status =
                    when {
                        bookId != null -> Status.STOP
                        fieldInfo.name == FIELD_BOOK_ID -> Status.YES
                        else -> Status.NO
                    }

                override fun longField(
                    fieldInfo: FieldInfo,
                    value: Long,
                ) {
                    bookId = value
                }

                override fun intField(
                    fieldInfo: FieldInfo,
                    value: Int,
                ) {
                    bookId = value.toLong()
                }
            },
        )
        return bookId
    }

    private fun loadAncestors(
        storedFields: StoredFields,
        doc: Int,
        bookId: Long,
    ) {
        val ancestors =
            storedFields
                .document(doc, setOf(FIELD_ANCESTOR_CATEGORY_IDS))
                .getField(FIELD_ANCESTOR_CATEGORY_IDS)
                ?.stringValue()
                ?.split(",")
                ?.mapNotNull { it.trim().toLongOrNull() }
                .orEmpty()
        // Lines added by delta updates lack the field: try again on the book's next run
        if (ancestors.isNotEmpty()) bookAncestors[bookId] = ancestors.toLongArray()
    }

    companion object {
        /**
         * The directory of [path]: memory-mapped where Lucene can (HotSpot), read through NIO where its Panama-based
         * mapping isn't available (GraalVM native image, Android, the iOS Java runtime).
         */
        fun openDirectory(path: Path): Directory = if (mmapAvailable) FSDirectory.open(path) else NIOFSDirectory(path)

        private val queryCacheEnabled: Boolean = System.getProperty("seforim.lucene.queryCache") != "off"

        private val mmapAvailable: Boolean by lazy {
            System.getProperty("org.graalvm.nativeimage.imagecode") == null &&
                System.getProperty("java.vm.vendor")?.contains("Android") != true &&
                System.getProperty("os.name") != "Darwin" &&
                System.getProperty("java.vm.name")?.contains("Zero") != true
        }
    }
}

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

    override fun newSearcher(
        reader: IndexReader,
        previousReader: IndexReader?,
    ): IndexSearcher {
        val searcher =
            if (cores == 1) {
                IndexSearcher(reader)
            } else {
                val docsPerSlice = maxOf(MIN_DOCS_PER_SLICE, reader.maxDoc() / cores + 1)
                object : IndexSearcher(reader, executor) {
                    override fun slices(leaves: List<LeafReaderContext>): Array<LeafSlice> = slices(leaves, docsPerSlice, Int.MAX_VALUE, true)
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

    override fun shouldCache(query: Query): Boolean =
        when (query) {
            is TermQuery -> query.term.field() == "type"
            is PointRangeQuery -> query.field == "is_base_book"
            else -> false
        }
}
