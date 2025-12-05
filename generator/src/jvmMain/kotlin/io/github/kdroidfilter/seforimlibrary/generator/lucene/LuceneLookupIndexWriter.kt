package io.github.kdroidfilter.seforimlibrary.generator.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import org.apache.lucene.store.FSDirectory
import java.nio.file.Path

class LuceneLookupIndexWriter(indexDir: Path, analyzer: Analyzer = StandardAnalyzer()) : LookupIndexWriter {
    companion object F {
        const val FIELD_TYPE = "type"
        const val TYPE_BOOK = "book"
        const val TYPE_TOC = "toc"

        const val FIELD_BOOK_ID = "book_id"
        const val FIELD_CATEGORY_ID = "category_id"
        const val FIELD_BOOK_TITLE = "book_title" // stored
        const val FIELD_Q = "q" // analyzed text for lookup (title/acronyms/toc)
        const val FIELD_IS_BASE_BOOK = "is_base_book"
        const val FIELD_ORDER_INDEX = "order_index"

        private const val BASE_BOOK_TERM_MULTIPLIER = 120

        const val FIELD_TOC_ID = "toc_id"
        const val FIELD_TOC_TEXT = "toc_text" // stored
        const val FIELD_TOC_LEVEL = "toc_level"
    }

    private val dir = FSDirectory.open(indexDir)
    private val writer: IndexWriter

    init {
        val cfg = IndexWriterConfig(analyzer).apply {
            // Ensure base books receive lower docIDs so they win ties in constant-score lookups
            indexSort = Sort(
                SortField(FIELD_IS_BASE_BOOK, SortField.Type.INT, true),
                SortField(FIELD_ORDER_INDEX, SortField.Type.INT)
            )
        }
        writer = IndexWriter(dir, cfg)
    }

    override fun addBook(
        bookId: Long,
        categoryId: Long,
        displayTitle: String,
        terms: Collection<String>,
        isBaseBook: Boolean,
        orderIndex: Int?
    ) {
        val boostRepeats = if (isBaseBook) BASE_BOOK_TERM_MULTIPLIER else 1
        val order = orderIndex ?: Int.MAX_VALUE
        val doc = Document().apply {
            add(StringField(FIELD_TYPE, TYPE_BOOK, Field.Store.NO))
            add(StoredField(FIELD_BOOK_ID, bookId))
            add(IntPoint(FIELD_BOOK_ID, bookId.toInt()))
            add(NumericDocValuesField(FIELD_BOOK_ID, bookId))
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, displayTitle))
            add(StoredField(FIELD_IS_BASE_BOOK, if (isBaseBook) 1 else 0))
            add(IntPoint(FIELD_IS_BASE_BOOK, if (isBaseBook) 1 else 0))
            add(NumericDocValuesField(FIELD_IS_BASE_BOOK, if (isBaseBook) 1L else 0L))
            add(StoredField(FIELD_ORDER_INDEX, order))
            add(IntPoint(FIELD_ORDER_INDEX, order))
            add(NumericDocValuesField(FIELD_ORDER_INDEX, order.toLong()))
            // Index all terms into a single analyzed field for prefix queries
            repeat(boostRepeats) {
                terms.forEach { t -> add(TextField(FIELD_Q, t, Field.Store.NO)) }
            }
        }
        writer.addDocument(doc)
    }

    override fun addToc(
        tocId: Long,
        bookId: Long,
        categoryId: Long,
        bookTitle: String,
        text: String,
        level: Int
    ) {
        val doc = Document().apply {
            add(StringField(FIELD_TYPE, TYPE_TOC, Field.Store.NO))
            add(StoredField(FIELD_TOC_ID, tocId))
            add(IntPoint(FIELD_TOC_ID, tocId.toInt()))
            add(StoredField(FIELD_BOOK_ID, bookId))
            add(IntPoint(FIELD_BOOK_ID, bookId.toInt()))
            add(NumericDocValuesField(FIELD_BOOK_ID, bookId))
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, bookTitle))
            add(NumericDocValuesField(FIELD_IS_BASE_BOOK, 0L))
            add(NumericDocValuesField(FIELD_ORDER_INDEX, Int.MAX_VALUE.toLong()))
            add(StoredField(FIELD_TOC_TEXT, text))
            add(StoredField(FIELD_TOC_LEVEL, level))
            add(TextField(FIELD_Q, text, Field.Store.NO))
        }
        writer.addDocument(doc)
    }

    override fun commit() {
        writer.commit()
    }

    override fun close() {
        writer.close(); dir.close()
    }
}
