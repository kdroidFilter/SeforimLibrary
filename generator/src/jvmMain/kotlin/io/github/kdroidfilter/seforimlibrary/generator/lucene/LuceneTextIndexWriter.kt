package io.github.kdroidfilter.seforimlibrary.generator.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.IntPoint
import org.apache.lucene.document.StoredField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.FSDirectory
import java.nio.file.Path

/**
 * Lucene-backed implementation of [TextIndexWriter].
 * Stores two kinds of documents in the same index:
 *  - type=line: searchable book line (text field analyzed) + stored metadata
 *  - type=book_title: terms for title/acronym suggestions (analyzed field 'title')
 */
class LuceneTextIndexWriter(indexDir: Path, analyzer: Analyzer = StandardAnalyzer()) : TextIndexWriter {
    companion object Fields {
        const val FIELD_TYPE = "type"
        const val TYPE_LINE = "line"
        const val TYPE_BOOK_TITLE = "book_title"

        const val FIELD_BOOK_ID = "book_id"
        const val FIELD_CATEGORY_ID = "category_id"
        const val FIELD_BOOK_TITLE = "book_title"
        const val FIELD_LINE_ID = "line_id"
        const val FIELD_LINE_INDEX = "line_index"
        const val FIELD_TEXT = "text"
        const val FIELD_TEXT_RAW = "text_raw"
        const val FIELD_TITLE = "title" // analyzed suggestion term
    }

    private val dir = FSDirectory.open(indexDir)
    private val writer: IndexWriter

    init {
        val cfg = IndexWriterConfig(analyzer)
        writer = IndexWriter(dir, cfg)
    }

    override fun addLine(
        bookId: Long,
        bookTitle: String,
        categoryId: Long,
        lineId: Long,
        lineIndex: Int,
        normalizedText: String,
        rawPlainText: String?
    ) {
        val doc = Document().apply {
            add(StringField(FIELD_TYPE, TYPE_LINE, Field.Store.NO))

            add(StoredField(FIELD_BOOK_ID, bookId))
            add(IntPoint(FIELD_BOOK_ID, bookId.toInt()))
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, bookTitle))

            add(StoredField(FIELD_LINE_ID, lineId))
            add(IntPoint(FIELD_LINE_ID, lineId.toInt()))
            add(StoredField(FIELD_LINE_INDEX, lineIndex))
            add(IntPoint(FIELD_LINE_INDEX, lineIndex))

            add(TextField(FIELD_TEXT, normalizedText, Field.Store.NO))
            rawPlainText?.let { add(StoredField(FIELD_TEXT_RAW, it)) }
        }
        writer.addDocument(doc)
    }

    override fun addBookTitleTerm(bookId: Long, categoryId: Long, displayTitle: String, term: String) {
        val doc = Document().apply {
            add(StringField(FIELD_TYPE, TYPE_BOOK_TITLE, Field.Store.NO))
            add(StoredField(FIELD_BOOK_ID, bookId))
            add(IntPoint(FIELD_BOOK_ID, bookId.toInt()))
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, displayTitle))
            add(TextField(FIELD_TITLE, term, Field.Store.NO))
        }
        writer.addDocument(doc)
    }

    override fun commit() {
        writer.commit()
    }

    override fun close() {
        writer.close()
        dir.close()
    }
}

