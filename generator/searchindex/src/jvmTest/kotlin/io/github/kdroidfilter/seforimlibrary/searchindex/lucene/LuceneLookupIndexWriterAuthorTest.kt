package io.github.kdroidfilter.seforimlibrary.searchindex.lucene

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.PrefixQuery
import org.apache.lucene.search.Sort
import org.apache.lucene.search.SortField
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.FSDirectory
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals

class LuceneLookupIndexWriterAuthorTest {
    @Test
    fun authorsAreFoundByAliasPrefixAndRankedByBookCount() {
        val dir = Files.createTempDirectory("lookup-authors")
        LuceneLookupIndexWriter(dir).use { lookup ->
            lookup.addBook(1, 1, "רעקא אחר", listOf("רעקא אחר"))
            lookup.addAuthor(113, "עקיבא איגר", listOf("עקיבא איגר", "רעקא"), bookCount = 144)
            lookup.addAuthor(500, "רעקב", listOf("רעקב"), bookCount = 1)
            lookup.commit()
        }
        FSDirectory.open(dir).use { fs ->
            DirectoryReader.open(fs).use { reader ->
                val searcher = IndexSearcher(reader)
                val query = BooleanQuery.Builder()
                    .add(TermQuery(Term("type", "author")), BooleanClause.Occur.FILTER)
                    .add(PrefixQuery(Term("q", "רעק")), BooleanClause.Occur.MUST)
                    .build()
                val sort = Sort(SortField("book_count", SortField.Type.LONG, true))
                val ids = searcher.search(query, 10, sort).scoreDocs.map {
                    searcher.storedFields().document(it.doc).getField("author_id").numericValue().toLong()
                }
                assertEquals(listOf(113L, 500L), ids)
            }
        }
    }
}
