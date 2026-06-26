package io.github.kdroidfilter.seforimlibrary.search

import org.apache.lucene.document.IntPoint
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.KnnFloatVectorQuery
import org.apache.lucene.search.Query
import org.apache.lucene.store.FSDirectory
import java.io.Closeable
import java.nio.file.Path

/** A dense (semantic) hit from the fused Lucene index. */
data class DenseHit(val lineId: Long, val bookId: Long, val score: Float)

/**
 * Runs a filtered KNN query over the SINGLE fused Lucene index (text fields +
 * dense `KnnFloatVectorField` per line, built by the generator's text index
 * writer). Filters (base-books / specific books) are applied as a pre-filter so
 * the KNN only considers eligible documents.
 *
 * Returns line ids that are joined back to the DB by the caller.
 */
class VectorSearcher(indexDir: Path) : Closeable {
    private val dir = FSDirectory.open(indexDir)

    private fun filterQuery(baseBookOnly: Boolean, bookIds: Collection<Long>?): Query? {
        val b = BooleanQuery.Builder()
        var any = false
        if (baseBookOnly) {
            // Field name matches the fused text index (LuceneTextIndexWriter.FIELD_IS_BASE_BOOK).
            b.add(IntPoint.newExactQuery("is_base_book", 1), BooleanClause.Occur.FILTER); any = true
        }
        if (!bookIds.isNullOrEmpty()) {
            b.add(IntPoint.newSetQuery("book_id", *bookIds.map { it.toInt() }.toIntArray()), BooleanClause.Occur.FILTER); any = true
        }
        return if (any) b.build() else null
    }

    fun search(query: FloatArray, k: Int, baseBookOnly: Boolean = false, bookIds: Collection<Long>? = null): List<DenseHit> {
        DirectoryReader.open(dir).use { reader ->
            val searcher = IndexSearcher(reader)
            val knn = KnnFloatVectorQuery("vec", query, k, filterQuery(baseBookOnly, bookIds))
            val top = searcher.search(knn, k)
            val stored = searcher.storedFields()
            return top.scoreDocs.map { sd ->
                val d = stored.document(sd.doc)
                DenseHit(
                    lineId = d.getField("line_id").numericValue().toLong(),
                    bookId = d.getField("book_id").numericValue().toLong(),
                    score = sd.score,
                )
            }
        }
    }

    override fun close() = dir.close()
}
