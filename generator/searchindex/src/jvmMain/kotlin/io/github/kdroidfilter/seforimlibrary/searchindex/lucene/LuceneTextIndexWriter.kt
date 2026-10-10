package io.github.kdroidfilter.seforimlibrary.searchindex.lucene

import io.github.kdroidfilter.seforimlibrary.searchindex.TextIndexWriter
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.KnnVectorsFormat
import org.apache.lucene.codecs.lucene104.Lucene104Codec
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.IntPoint
import org.apache.lucene.document.KnnFloatVectorField
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.StoredField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.quantization.QuantizedByteVectorValues
import java.nio.file.Path
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

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
    // Optional dense embeddings: if provided, each line doc also gets a
    // KnnFloatVectorField("vec", COSINE) -> SINGLE index holding text + vectors.
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
        // int8 vectors (optimized scalar quantization): the graph search reads a quarter of the float bytes, so the
        // hot part of the index fits the RAM of a modest machine; the raw floats stay on disk for the merges
        val vectorsFormat = Lucene104HnswScalarQuantizedVectorsFormat(
            QuantizedByteVectorValues.ScalarEncoding.UNSIGNED_BYTE,
            16,
            100,
            mergeWorkers,
            mergeExecutor,
        )
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

            // Dense embedding (single fused index): attach the line's vector if available.
            vectorProvider?.invoke(lineId)?.let { vec ->
                val field = if (isBaseBook) FIELD_VEC_BASE else FIELD_VEC
                add(KnnFloatVectorField(field, vec, org.apache.lucene.index.VectorSimilarityFunction.COSINE))
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
