package io.github.kdroidfilter.seforimlibrary.searchindex.lucene

import io.github.kdroidfilter.seforimlibrary.searchindex.TextIndexWriter
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.KnnVectorsFormat
import org.apache.lucene.codecs.lucene104.Lucene104Codec
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.IntPoint
import org.apache.lucene.document.KnnByteVectorField
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.StoredField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.VectorSimilarityFunction
import org.apache.lucene.store.FSDirectory
import java.nio.file.Path
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.math.abs
import kotlin.math.roundToInt

/**
 * Lucene-backed implementation of [TextIndexWriter].
 * Stores two kinds of documents in the same index:
 *  - type=line: searchable book line (text field analyzed) + stored metadata
 *  - type=book_title: terms for title/acronym suggestions (analyzed field 'title')
 */
class LuceneTextIndexWriter(
    indexDir: Path,
    analyzer: Analyzer = StandardAnalyzer(),
    private val indexHebrewField: Boolean = false,
    private val indexPrimaryText: Boolean = true,
    // Optional dense embeddings: if provided, each line doc also gets an int8
    // KnnByteVectorField("vec", COSINE) -> SINGLE index holding text + vectors.
    private val vectorProvider: ((Long) -> FloatArray?)? = null,
    // Merge to a single segment on close: one HNSW graph and one term dictionary to search instead of dozens
    private val forceMerge: Boolean = true,
) : TextIndexWriter {
    companion object Fields {
        const val FIELD_TYPE = "type"
        const val TYPE_LINE = "line"
        const val TYPE_BOOK_TITLE = "book_title"
        // Base-book lines get their own vector field: the default search is restricted to base books (~4% of the
        // lines), which a filter on one shared graph serves badly; a search over all lines queries both fields
        const val FIELD_VEC = "vec"
        const val FIELD_VEC_BASE = "vec_base"

        const val FIELD_BOOK_ID = "book_id"
        const val FIELD_CATEGORY_ID = "category_id"
        const val FIELD_ANCESTOR_CATEGORY_IDS = "ancestor_category_ids"
        const val FIELD_BOOK_TITLE = "book_title"
        const val FIELD_LINE_ID = "line_id"
        const val FIELD_LINE_INDEX = "line_index"
        const val FIELD_TEXT = "text"
        // FIELD_TEXT_RAW removed - snippet source is now fetched from DB at query time
        const val FIELD_TEXT_HE = "text_he"
        const val FIELD_TEXT_NG4 = "text_ng4"
        const val FIELD_TITLE = "title" // analyzed suggestion term
        const val FIELD_ORDER_INDEX = "order_index"
        const val FIELD_IS_BASE_BOOK = "is_base_book"
    }

    private val dir = FSDirectory.open(indexDir)
    private val writer: IndexWriter
    private val mergeWorkers = Runtime.getRuntime().availableProcessors()
    private val mergeExecutor: ExecutorService = Executors.newFixedThreadPool(mergeWorkers)

    init {
        // Plain HNSW over byte vectors: the vectors are quantized to int8 before indexing, so the index holds only
        // the bytes (Lucene's quantized formats would keep the raw floats next to them, 4x the size)
        val vectorsFormat = Lucene99HnswVectorsFormat(16, 100, mergeWorkers, mergeExecutor)
        val cfg = IndexWriterConfig(analyzer).apply {
            openMode = IndexWriterConfig.OpenMode.CREATE
            codec = object : Lucene104Codec() {
                override fun getKnnVectorsFormatForField(field: String): KnnVectorsFormat = vectorsFormat
            }
            // Fewer, larger flushed segments: every merge of vector segments rebuilds a graph
            ramBufferSizeMB = 1024.0
        }
        writer = IndexWriter(dir, cfg)
    }

    override fun addLine(
        bookId: Long,
        bookTitle: String,
        categoryId: Long,
        ancestorCategoryIds: List<Long>,
        lineId: Long,
        lineIndex: Int,
        normalizedText: String,
        rawPlainText: String?,
        normalizedTextHebrew: String?,
        orderIndex: Int,
        isBaseBook: Boolean
    ) {
        val doc = Document().apply {
            add(StringField(FIELD_TYPE, TYPE_LINE, Field.Store.NO))

            add(StoredField(FIELD_BOOK_ID, bookId))
            add(IntPoint(FIELD_BOOK_ID, bookId.toInt()))
            // Doc values: the search facets count hits per book without reading stored fields
            add(NumericDocValuesField(FIELD_BOOK_ID, bookId))
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, bookTitle))

            // Index ancestor category IDs for efficient filtering and retrieval
            // IntPoint for filtering (multi-valued)
            for (ancestorId in ancestorCategoryIds) {
                add(IntPoint(FIELD_ANCESTOR_CATEGORY_IDS, ancestorId.toInt()))
            }
            // StoredField for retrieval (comma-separated)
            add(StoredField(FIELD_ANCESTOR_CATEGORY_IDS, ancestorCategoryIds.joinToString(",")))

            add(StoredField(FIELD_LINE_ID, lineId))
            add(IntPoint(FIELD_LINE_ID, lineId.toInt()))
            add(StoredField(FIELD_LINE_INDEX, lineIndex))
            add(IntPoint(FIELD_LINE_INDEX, lineIndex))

            // Store order_index and is_base_book for boosting
            add(StoredField(FIELD_ORDER_INDEX, orderIndex))
            add(IntPoint(FIELD_ORDER_INDEX, orderIndex))
            add(StoredField(FIELD_IS_BASE_BOOK, if (isBaseBook) 1 else 0))
            add(IntPoint(FIELD_IS_BASE_BOOK, if (isBaseBook) 1 else 0))

            if (indexPrimaryText) {
                add(TextField(FIELD_TEXT, normalizedText, Field.Store.NO))
            }
            // Optional secondary text field
            val hebText = normalizedTextHebrew ?: if (indexHebrewField) normalizedText else null
            hebText?.let { add(TextField(FIELD_TEXT_HE, it, Field.Store.NO)) }
            // Index 4-gram tokens for substring search (per-field analyzer applies NGram filter)
            add(TextField(FIELD_TEXT_NG4, normalizedText, Field.Store.NO))
            // rawPlainText is no longer stored - snippet source is fetched from DB at query time

            // Dense embedding (single fused index): attach the line's int8 vector if available.
            vectorProvider?.invoke(lineId)?.let { vec ->
                val field = if (isBaseBook) FIELD_VEC_BASE else FIELD_VEC
                add(KnnByteVectorField(field, quantizeInt8(vec), VectorSimilarityFunction.COSINE))
            }
        }
        writer.addDocument(doc)
    }

    override fun addBookTitleTerm(bookId: Long, categoryId: Long, displayTitle: String, term: String) {
        val doc = Document().apply {
            add(StringField(FIELD_TYPE, TYPE_BOOK_TITLE, Field.Store.NO))
            add(StoredField(FIELD_BOOK_ID, bookId))
            add(IntPoint(FIELD_BOOK_ID, bookId.toInt()))
            // Same schema as the line documents (Lucene requires one per field)
            add(NumericDocValuesField(FIELD_BOOK_ID, bookId))
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, displayTitle))
            add(TextField(FIELD_TITLE, term, Field.Store.NO))
        }
        writer.addDocument(doc)
    }

    override fun deleteLineById(lineId: Long) {
        // line_id is indexed as IntPoint (point field), so target it with an
        // exact-match point query. Deletes every line-document whose
        // line_id equals this value — typically exactly one.
        writer.deleteDocuments(IntPoint.newExactQuery(FIELD_LINE_ID, lineId.toInt()))
    }

    override fun commit() {
        writer.commit()
    }

    // Scales each vector so its largest component maps to 127: COSINE ignores the scale, so every vector gets the
    // full int8 range (cosine to the float vector >= 0.9999 on the corpus)
    private fun quantizeInt8(vec: FloatArray): ByteArray {
        val max = vec.maxOf { abs(it) }.takeIf { it > 0f } ?: return ByteArray(vec.size)
        return ByteArray(vec.size) { i -> (vec[i] * 127f / max).roundToInt().toByte() }
    }

    override fun close() {
        try {
            if (forceMerge) {
                writer.forceMerge(1)
                writer.commit()
            }
            writer.close()
        } finally {
            mergeExecutor.shutdown()
            dir.close()
        }
    }
}
