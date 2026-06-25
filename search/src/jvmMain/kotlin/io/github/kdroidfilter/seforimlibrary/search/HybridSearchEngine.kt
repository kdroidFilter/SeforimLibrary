package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.nio.file.Files
import java.nio.file.Path

/**
 * Hybrid search = lexical (BM25 + MagicDictionary, [LuceneSearchEngine]) fused with
 * dense semantic search (v4 embedding + [VectorSearcher]) via Reciprocal Rank Fusion.
 *
 *   RRF(doc) = Σ 1/(K + rank_in_list)
 *
 * Implements [SearchEngine] so the app uses it transparently: [openSession] runs the
 * fused search (the session pages over the in-memory fused list); all other methods
 * (facets, snippets, title prefix) delegate to the lexical engine.
 *
 * Falls back to pure lexical when: the model/vector index is absent, OR the filters
 * are not supported by the dense index (categoryFilter / lineIds). Book / base-book
 * filters ARE supported by both paths.
 *
 * Dense-only hits (lines the lexical path missed) are turned into full [LineHit]s via
 * [resolveLine], supplied by the caller (fetches title + text + snippet by line id).
 */
class HybridSearchEngine(
    private val lexical: LuceneSearchEngine,
    private val modelDir: Path?,
    private val indexDir: Path,
    private val rrfK: Int = 60,
    private val candidates: Int = 150,
    private val resolveLine: suspend (lineId: Long, bookId: Long, query: String) -> LineHit?,
) : SearchEngine {

    private val logger = Logger.withTag("HybridSearch")

    // The embedder (heavy OrtSession) + vector searcher are loaded LAZILY on the first
    // dense search, off the main thread (see [ensureDense]) — so the first query just
    // shows the normal search spinner instead of freezing the UI while the model loads.
    @Volatile private var embedder: SeforimEmbedder? = null
    @Volatile private var vectorSearcher: VectorSearcher? = null
    @Volatile private var denseTried = false
    private val denseMutex = Mutex()

    // Cheap, no-load check: are the model + vector index even present? Decides whether to
    // take the dense path (then load lazily); the actual OrtSession is built in fuse().
    private val denseConfigured: Boolean =
        indexDir != null && Files.isDirectory(indexDir) && SeforimEmbedder.isAvailable(modelDir)

    val denseEnabled: Boolean get() = embedder != null && vectorSearcher != null

    /** Load the embedder + vector searcher once, on a background thread. Idempotent. */
    private suspend fun ensureDense() {
        if (denseTried) return
        denseMutex.withLock {
            if (denseTried) return
            withContext(Dispatchers.IO) {
                val emb = SeforimEmbedder.tryLoad(modelDir)
                val vs = if (emb != null && indexDir != null && Files.isDirectory(indexDir)) {
                    runCatching { VectorSearcher(indexDir) }.getOrNull()
                } else null
                if (vs != null) {
                    embedder = emb; vectorSearcher = vs
                    logger.i { "dense ready (vector index $indexDir)" }
                } else {
                    runCatching { emb?.close() }
                    logger.i { "dense unavailable -> lexical only" }
                }
            }
            denseTried = true
        }
    }

    override fun openSession(
        query: String,
        near: Int,
        bookFilter: Long?,
        categoryFilter: Long?,
        bookIds: Collection<Long>?,
        lineIds: Collection<Long>?,
        baseBookOnly: Boolean,
    ): SearchSession? {
        if (query.isBlank()) return null
        // Dense index supports book / base-book filters only. For category/line filters
        // (or when dense isn't configured), use pure lexical to stay correct. The model
        // itself is loaded lazily in fuse() — denseConfigured is a cheap no-load check.
        val denseOk = denseConfigured && categoryFilter == null && lineIds == null
        if (!denseOk) {
            return lexical.openSession(query, near, bookFilter, categoryFilter, bookIds, lineIds, baseBookOnly)
        }
        val effBookIds = bookIds ?: bookFilter?.let { listOf(it) }
        return HybridSession(query, near, effBookIds, baseBookOnly)
    }

    override fun searchBooksByTitlePrefix(query: String, limit: Int): List<Long> =
        lexical.searchBooksByTitlePrefix(query, limit)

    override fun buildSnippet(rawText: String, query: String, near: Int): String =
        lexical.buildSnippet(rawText, query, near)

    override fun buildHighlightTerms(query: String): List<String> =
        lexical.buildHighlightTerms(query)

    override fun computeFacets(
        query: String, near: Int, bookFilter: Long?, categoryFilter: Long?,
        bookIds: Collection<Long>?, lineIds: Collection<Long>?, baseBookOnly: Boolean,
    ): SearchFacets? = lexical.computeFacets(query, near, bookFilter, categoryFilter, bookIds, lineIds, baseBookOnly)

    override fun close() {
        runCatching { vectorSearcher?.close() }
        runCatching { embedder?.close() }
        runCatching { lexical.close() }
    }

    /** Lexical page + dense KNN, fused by RRF, resolved to full LineHits. */
    private suspend fun fuse(query: String, near: Int, bookIds: Collection<Long>?, baseOnly: Boolean): List<LineHit> {
        ensureDense()   // first call loads the model off-main (covered by the search spinner)
        val lexHits = lexical.openSession(query, near = near, bookIds = bookIds, baseBookOnly = baseOnly)
            ?.use { it.nextPage(candidates)?.hits ?: emptyList() } ?: emptyList()

        // Dense failed to load -> lexical-only result (still correct, just no semantic recall).
        val emb = embedder
        val vs = vectorSearcher
        if (emb == null || vs == null) return lexHits

        val denseHits = withContext(Dispatchers.Default) {
            val qVec = emb.embed(query)
            vs.search(qVec, candidates, baseOnly, bookIds)
        }

        val rrf = HashMap<Long, Double>()
        val bookOf = HashMap<Long, Long>()
        lexHits.forEachIndexed { rank, h ->
            rrf.merge(h.lineId, 1.0 / (rrfK + rank + 1), Double::plus); bookOf[h.lineId] = h.bookId
        }
        denseHits.forEachIndexed { rank, h ->
            rrf.merge(h.lineId, 1.0 / (rrfK + rank + 1), Double::plus); bookOf.putIfAbsent(h.lineId, h.bookId)
        }
        val lexById = lexHits.associateBy { it.lineId }
        val ordered = rrf.entries.sortedByDescending { it.value }
        val out = ArrayList<LineHit>(ordered.size)
        for ((lineId, score) in ordered) {
            val hit = lexById[lineId] ?: resolveLine(lineId, bookOf[lineId] ?: -1L, query)
            if (hit != null) out += hit.copy(score = score.toFloat())
        }
        return out
    }

    private inner class HybridSession(
        private val query: String,
        private val near: Int,
        private val bookIds: Collection<Long>?,
        private val baseOnly: Boolean,
    ) : SearchSession {
        private var fused: List<LineHit>? = null
        private var offset = 0

        override suspend fun nextPage(limit: Int): SearchPage? {
            val all = fused ?: fuse(query, near, bookIds, baseOnly).also { fused = it }
            if (offset >= all.size) return null
            val end = minOf(offset + limit, all.size)
            val slice = all.subList(offset, end)
            offset = end
            return SearchPage(hits = slice, totalHits = all.size.toLong(), isLastPage = offset >= all.size)
        }

        override fun close() {}
    }

    companion object {
        private val logger = Logger.withTag("HybridSearch")

        fun create(
            lexical: LuceneSearchEngine,
            indexDir: Path,
            modelDir: Path? = null,
            resolveLine: suspend (Long, Long, String) -> LineHit?,
        ): HybridSearchEngine {
            // ONE index: dense vectors live in the SAME Lucene index as the text
            // (seforim.db.lucene) — no separate vector index. The embedder + searcher
            // load lazily on the first dense search (no UI freeze).
            return HybridSearchEngine(lexical, modelDir, indexDir, resolveLine = resolveLine)
        }
    }
}
