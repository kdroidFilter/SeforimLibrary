package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
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
        Files.isDirectory(indexDir) && SeforimEmbedder.isAvailable(modelDir)

    val denseEnabled: Boolean get() = embedder != null && vectorSearcher != null

    /** Load the embedder + vector searcher once, on a background thread. Idempotent. */
    private suspend fun ensureDense() {
        if (denseTried) return
        denseMutex.withLock {
            if (denseTried) return
            withContext(Dispatchers.IO) {
                val emb = SeforimEmbedder.tryLoad(modelDir)
                val vs = if (emb != null && Files.isDirectory(indexDir)) {
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

    /**
     * Picks the passage of [text] closest in meaning to [query], using the SAME dense
     * encoder as semantic search — so the highlight reflects meaning instead of scattering
     * dictionary word matches. Returns the winning passage verbatim, or null when dense is
     * unavailable or the text isn't worth localizing (a single short clause).
     */
    override suspend fun semanticSpan(query: String, text: String): String? {
        if (query.isBlank() || text.isBlank()) return null
        ensureDense()
        val emb = embedder ?: return null
        val clauses = splitClauses(text)
        if (clauses.size < 2) return null
        return withContext(Dispatchers.Default) {
            val qVec = emb.embed(query)
            clauses
                .map { it to cosine(qVec, emb.embed(it)) }
                .maxByOrNull { it.second }
                ?.first
        }
    }

    /**
     * Embedding-based find-in-page in one book: dense KNN over the index scoped to [bookId].
     * Reuses the line vectors already in the index, so this is just the query embedding + a
     * filtered KNN — the per-line passage is computed by the caller on the displayed text.
     */
    override suspend fun denseReady(): Boolean {
        ensureDense()
        return denseEnabled
    }

    override suspend fun semanticFind(query: String, bookId: Long, limit: Int): List<Long> {
        if (query.isBlank()) return emptyList()
        ensureDense()
        val emb = embedder ?: return emptyList()
        val vs = vectorSearcher ?: return emptyList()
        return withContext(Dispatchers.Default) {
            vs.search(emb.embed(query), limit, baseBookOnly = false, bookIds = listOf(bookId)).map { it.lineId }
        }
    }

    override fun computeFacets(
        query: String, near: Int, bookFilter: Long?, categoryFilter: Long?,
        bookIds: Collection<Long>?, lineIds: Collection<Long>?, baseBookOnly: Boolean,
    ): SearchFacets? = lexical.computeFacets(query, near, bookFilter, categoryFilter, bookIds, lineIds, baseBookOnly)

    override fun close() {
        runCatching { vectorSearcher?.close() }
        runCatching { embedder?.close() }
        runCatching { lexical.close() }
    }

    /** The fused, RRF-ordered hits plus the ids that the lexical path matched (so the dense-only
     *  ones can get a meaning-based snippet lazily — their lexical snippet is meaningless). */
    private class FusedResult(val hits: List<LineHit>, val lexicalIds: Set<Long>)

    /** Lexical page + dense KNN, fused by RRF, resolved to full LineHits. */
    private suspend fun fuse(query: String, near: Int, bookIds: Collection<Long>?, baseOnly: Boolean): FusedResult {
        ensureDense()   // first call loads the model off-main (covered by the search spinner)
        val lexHits = lexical.openSession(query, near = near, bookIds = bookIds, baseBookOnly = baseOnly)
            ?.use { it.nextPage(candidates)?.hits ?: emptyList() } ?: emptyList()

        // Dense failed to load -> lexical-only result (still correct, just no semantic recall).
        val emb = embedder
        val vs = vectorSearcher
        if (emb == null || vs == null) return FusedResult(lexHits, lexHits.map { it.lineId }.toSet())

        // Don't let a dense failure sink the whole search: degrade to lexical and log. This
        // also catches native-image gaps (e.g. a missing Lucene KNN vectors-format SPI entry).
        val denseHits = try {
            withContext(Dispatchers.Default) {
                val qVec = emb.embed(query)
                vs.search(qVec, candidates, baseOnly, bookIds)
            }
        } catch (e: Exception) {
            logger.w(e) { "dense KNN failed; falling back to lexical-only" }
            return FusedResult(lexHits, lexHits.map { it.lineId }.toSet())
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
        return FusedResult(out, lexById.keys)
    }

    /**
     * Snippet for a dense-only hit: the lexical builder can't anchor (the query words aren't in
     * the text) and, fed the passage, it tokenizes and bolds each word separately — so short
     * function words (ואם, או…) get highlighted on their own. Instead, locate the passage closest
     * in meaning and bold it as ONE contiguous span inside a context window. Null -> keep lexical.
     */
    private suspend fun semanticSnippet(query: String, rawText: String, near: Int): String? {
        // Jsoup.clean returns HTML-escaped text; substrings stay escaped, so we splice <b> in
        // directly (same convention as the lexical snippet builder).
        val clean = Jsoup.clean(rawText, Safelist.none())
        val passage = semanticSpan(query, clean) ?: return null
        val idx = clean.indexOf(passage)
        if (idx < 0) return lexical.buildSnippet(rawText, passage, near)
        val from = (idx - SNIPPET_CONTEXT).coerceAtLeast(0)
        val to = (idx + passage.length + SNIPPET_CONTEXT).coerceAtMost(clean.length)
        return buildString {
            if (from > 0) append("…")
            append(clean, from, idx)
            append("<b>").append(passage).append("</b>")
            append(clean, idx + passage.length, to)
            if (to < clean.length) append("…")
        }
    }

    private inner class HybridSession(
        private val query: String,
        private val near: Int,
        private val bookIds: Collection<Long>?,
        private val baseOnly: Boolean,
    ) : SearchSession {
        private var fused: FusedResult? = null
        private var offset = 0

        override suspend fun nextPage(limit: Int): SearchPage? {
            val all = fused ?: fuse(query, near, bookIds, baseOnly).also { fused = it }
            if (offset >= all.hits.size) return null
            val end = minOf(offset + limit, all.hits.size)
            // Re-snippet only the dense-only hits on THIS page (bounded work) so semantic
            // results show the passage that actually matched, not the line's opening words.
            val slice = all.hits.subList(offset, end).map { hit ->
                if (hit.lineId in all.lexicalIds) {
                    hit
                } else {
                    semanticSnippet(query, hit.rawText, near)?.let { hit.copy(snippet = it) } ?: hit
                }
            }
            offset = end
            return SearchPage(hits = slice, totalHits = all.hits.size.toLong(), isLastPage = offset >= all.hits.size)
        }

        override fun close() {}
    }

    /**
     * Splits [text] into passage candidates: first on strong clause delimiters (incl. the
     * Hebrew sof-pasuk ׃), then sliding word windows over any segment too long to localize.
     * Substrings are verbatim slices of [text] (original spacing preserved) so the caller
     * can match them back diacritic-insensitively. Tiny fragments are dropped to keep a
     * highlight that is a *passage*, not a stray word.
     */
    private fun splitClauses(text: String): List<String> {
        val delimiters = charArrayOf('.', '!', '?', ':', ';', '׃', '\n')
        val segments = ArrayList<IntRange>()
        var start = 0
        for (i in text.indices) {
            if (text[i] in delimiters) {
                if (i > start) segments += start..i
                start = i + 1
            }
        }
        if (start < text.length) segments += start until text.length

        val out = ArrayList<String>()
        for (seg in segments) {
            val sub = text.substring(seg.first, seg.last + 1)
            val wordRanges = WORD_RE.findAll(sub).map { it.range }.toList()
            if (wordRanges.size < MIN_PASSAGE_WORDS) continue
            if (wordRanges.size <= MAX_PASSAGE_WORDS) {
                out += sub.substring(wordRanges.first().first, wordRanges.last().last + 1)
            } else {
                // Segment too long to localize: slide word windows over the original slice.
                var w = 0
                while (w < wordRanges.size) {
                    val from = wordRanges[w].first
                    val toIdx = minOf(w + MAX_PASSAGE_WORDS - 1, wordRanges.size - 1)
                    out += sub.substring(from, wordRanges[toIdx].last + 1)
                    if (toIdx == wordRanges.size - 1) break
                    w += WINDOW_STRIDE_WORDS
                }
            }
        }
        return if (out.isEmpty()) listOf(text.trim()) else out
    }

    private fun cosine(a: FloatArray, b: FloatArray): Float {
        var s = 0f
        val n = minOf(a.size, b.size)
        for (i in 0 until n) s += a[i] * b[i]
        return s
    }

    companion object {
        private val logger = Logger.withTag("HybridSearch")

        private val WORD_RE = Regex("\\S+")
        private const val MIN_PASSAGE_WORDS = 3
        private const val MAX_PASSAGE_WORDS = 14
        private const val WINDOW_STRIDE_WORDS = 8
        private const val SNIPPET_CONTEXT = 90 // chars of context kept on each side of the passage

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
