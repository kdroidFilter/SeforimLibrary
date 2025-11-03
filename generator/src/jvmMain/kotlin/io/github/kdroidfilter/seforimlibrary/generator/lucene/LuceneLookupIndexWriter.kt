package io.github.kdroidfilter.seforimlibrary.generator.lucene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
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

        const val FIELD_TOC_ID = "toc_id"
        const val FIELD_TOC_TEXT = "toc_text" // stored
        const val FIELD_TOC_LEVEL = "toc_level"
    }

    private val dir = FSDirectory.open(indexDir)
    private val writer: IndexWriter

    init {
        val cfg = IndexWriterConfig(analyzer)
        writer = IndexWriter(dir, cfg)
    }

    override fun addBook(bookId: Long, categoryId: Long, displayTitle: String, terms: Collection<String>) {
        val doc = Document().apply {
            add(StringField(FIELD_TYPE, TYPE_BOOK, Field.Store.NO))
            add(StoredField(FIELD_BOOK_ID, bookId))
            add(IntPoint(FIELD_BOOK_ID, bookId.toInt()))
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, displayTitle))
            // Index all terms into a single analyzed field for prefix queries
            terms.forEach { t -> add(TextField(FIELD_Q, t, Field.Store.NO)) }
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
            add(StoredField(FIELD_CATEGORY_ID, categoryId))
            add(IntPoint(FIELD_CATEGORY_ID, categoryId.toInt()))
            add(StoredField(FIELD_BOOK_TITLE, bookTitle))
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

