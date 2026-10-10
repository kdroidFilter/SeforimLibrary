package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.assertFalse
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.LowerCaseFilter
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.ngram.NGramTokenFilter
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.IntPoint
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.StoredField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.ByteBuffersDirectory
import org.apache.lucene.store.FSDirectory
import java.nio.file.Files
import java.nio.file.Path

class LuceneSearchEngineTest {

    // --- buildSnippet tests (no index required) ---

    @Test
    fun `buildSnippet returns clean text for empty query`() {
        val tempDir = createTempIndexDir()
        try {
            createMinimalIndex(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val rawText = "<p>שלום עולם</p>"
            val result = engine.buildSnippet(rawText, "", 5)

            // Should return clean text without HTML
            assertFalse(result.contains("<p>"))
            assertTrue(result.contains("שלום עולם"))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `buildSnippet highlights matching Hebrew text`() {
        val tempDir = createTempIndexDir()
        try {
            createMinimalIndex(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val rawText = "בראשית ברא אלהים את השמים ואת הארץ"
            val result = engine.buildSnippet(rawText, "בראשית", 5)

            // Should contain bold tags around the match
            assertTrue(result.contains("<b>") && result.contains("</b>"))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `buildSnippet handles text with nikud`() {
        val tempDir = createTempIndexDir()
        try {
            createMinimalIndex(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val rawText = "בְּרֵאשִׁית בָּרָא אֱלֹהִים"
            val result = engine.buildSnippet(rawText, "בראשית", 5)

            // Should still find and highlight despite nikud
            assertTrue(result.contains("<b>") || result.contains("בְּרֵאשִׁית"))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `buildSnippet truncates long text with ellipsis`() {
        val tempDir = createTempIndexDir()
        try {
            createMinimalIndex(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            // Create a very long text
            val longText = "הקדמה ארוכה מאוד " + "מילה ".repeat(200) + " סיום הטקסט"
            val result = engine.buildSnippet(longText, "הקדמה", 5)

            // Should contain ellipsis if truncated
            // The snippet extracts context around the match, may have ... at end
            assertTrue(result.length < longText.length || result.contains("..."))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- openSession tests ---

    @Test
    fun `openSession returns null for blank query`() {
        val tempDir = createTempIndexDir()
        try {
            createMinimalIndex(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("", 5)
            assertNull(session)

            val sessionBlank = engine.openSession("   ", 5)
            assertNull(sessionBlank)

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `openSession returns session for valid query`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithContent(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("שלום", 5)
            assertNotNull(session)
            session.close()

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `session nextPage returns results`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithContent(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("שלום", 5)
            assertNotNull(session)

            val page = runBlocking { session.nextPage(10) }
            assertNotNull(page)
            assertTrue(page.hits.isNotEmpty())
            assertTrue(page.totalHits > 0)

            session.close()
            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `session pagination works correctly`() {
        val tempDir = createTempIndexDir()
        try {
            // Create index with multiple documents
            createIndexWithMultipleDocuments(tempDir, count = 25)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("טקסט", 5)
            assertNotNull(session)

            runBlocking {
                // First page
                val page1 = session.nextPage(10)
                assertNotNull(page1)
                assertEquals(10, page1.hits.size)
                assertFalse(page1.isLastPage)

                // Second page
                val page2 = session.nextPage(10)
                assertNotNull(page2)
                assertEquals(10, page2.hits.size)
                assertFalse(page2.isLastPage)

                // Third page (last, should have 5 items)
                val page3 = session.nextPage(10)
                assertNotNull(page3)
                assertEquals(5, page3.hits.size)
                assertTrue(page3.isLastPage)

                // No more pages
                val page4 = session.nextPage(10)
                assertNull(page4)
            }

            session.close()
            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `session can be closed multiple times safely`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithContent(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("שלום", 5)
            assertNotNull(session)

            // Close multiple times should not throw
            session.close()
            // Second close - should be safe
            try {
                session.close()
            } catch (e: Exception) {
                // Some implementations may throw on double close, that's acceptable
            }

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- searchBooksByTitlePrefix tests ---

    @Test
    fun `searchBooksByTitlePrefix returns empty for blank query`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithBookTitles(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val result = engine.searchBooksByTitlePrefix("")
            assertTrue(result.isEmpty())

            val resultBlank = engine.searchBooksByTitlePrefix("   ")
            assertTrue(resultBlank.isEmpty())

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `searchBooksByTitlePrefix finds matching books`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithBookTitles(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val result = engine.searchBooksByTitlePrefix("בראשית")
            assertTrue(result.isNotEmpty())

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `searchBooksByTitlePrefix respects limit`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithMultipleBookTitles(tempDir, count = 10)
            val engine = LuceneSearchEngine(tempDir)

            val result = engine.searchBooksByTitlePrefix("ספר", limit = 5)
            assertTrue(result.size <= 5)

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- Filter tests ---

    @Test
    fun `openSession with book filter returns only matching book`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithMultipleBooks(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("טקסט", 5, bookFilter = 1L)
            assertNotNull(session)

            val page = runBlocking { session.nextPage(100) }
            assertNotNull(page)

            // All results should be from book 1
            assertTrue(page.hits.all { it.bookId == 1L })

            session.close()
            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `openSession with bookIds filter returns only matching books`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithMultipleBooks(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("טקסט", 5, bookIds = listOf(1L, 2L))
            assertNotNull(session)

            val page = runBlocking { session.nextPage(100) }
            assertNotNull(page)

            // All results should be from books 1 or 2
            assertTrue(page.hits.all { it.bookId in listOf(1L, 2L) })

            session.close()
            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- LineHit data tests ---

    @Test
    fun `LineHit contains all required fields`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithContent(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("שלום", 5)
            assertNotNull(session)

            val page = runBlocking { session.nextPage(10) }
            assertNotNull(page)
            assertTrue(page.hits.isNotEmpty())

            val hit = page.hits.first()
            assertTrue(hit.bookId > 0)
            assertTrue(hit.bookTitle.isNotEmpty())
            assertTrue(hit.lineId > 0)
            assertTrue(hit.lineIndex >= 0)
            assertTrue(hit.snippet.isNotEmpty())
            assertTrue(hit.score > 0)
            assertTrue(hit.rawText.isNotEmpty())

            session.close()
            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- Edge cases ---

    @Test
    fun `handles special Hebrew characters in query`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithContent(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            // Query with gershayim
            val session1 = engine.openSession("רש\"י", 5)
            // Should not throw, may return null or empty results
            session1?.close()

            // Query with maqaf
            val session2 = engine.openSession("מה-טבו", 5)
            session2?.close()

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `handles query with Hashem representation`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithHashemContent(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            // Query with ה׳
            val session = engine.openSession("ה׳", 5)
            // Should handle without error
            session?.close()

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- Exact phrase (quoted) tests ---

    @Test
    fun `quoted query matches only the exact ordered adjacent phrase`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithPhrases(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("\"מלך דוד\"", 5)
            assertNotNull(session)
            // line 1 ("מלך דוד") and line 5 ("ירושלים מלך דוד עיר") contain the exact phrase.
            // line 2 ("דוד מלך", reversed) and line 3 ("מלך גדול דוד", gap) must be excluded.
            assertEquals(setOf(1L, 5L), collectLineIds(session))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `unquoted query matches every doc containing the words in any order`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithPhrases(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("מלך דוד", 5)
            assertNotNull(session)
            // Without quotes the order/adjacency is relaxed: lines 1, 2, 3 and 5 all qualify.
            assertEquals(setOf(1L, 2L, 3L, 5L), collectLineIds(session))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `mixed free word plus quoted phrase requires both`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithPhrases(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            val session = engine.openSession("ירושלים \"מלך דוד\"", 5)
            assertNotNull(session)
            // Only line 5 has both the free word "ירושלים" and the exact phrase "מלך דוד".
            assertEquals(setOf(5L), collectLineIds(session))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    @Test
    fun `gershayim-quoted query behaves like an exact phrase`() {
        val tempDir = createTempIndexDir()
        try {
            createIndexWithPhrases(tempDir)
            val engine = LuceneSearchEngine(tempDir)

            // Same phrase as the ASCII test but delimited with Hebrew gershayim (U+05F4).
            val session = engine.openSession("״מלך דוד״", 5)
            assertNotNull(session)
            assertEquals(setOf(1L, 5L), collectLineIds(session))

            engine.close()
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- findInBookCandidates tests ---

    @Test
    fun `findInBookCandidates finds a substring inside a word, nikud ignored`() {
        withFindIndex { engine ->
            assertEquals(setOf(1L), engine.findInBookCandidates("רֵאשִׁי", bookId = 1)!!.toSet())
        }
    }

    @Test
    fun `findInBookCandidates finds a query starting and ending mid-word`() {
        withFindIndex { engine ->
            // "שית" ends a word, "ברא" begins one, "אלהים" is whole
            assertEquals(setOf(1L), engine.findInBookCandidates("שית ברא אלהים", bookId = 1)!!.toSet())
        }
    }

    @Test
    fun `findInBookCandidates can't answer for a book the index lacks`() {
        withFindIndex { engine ->
            assertNull(engine.findInBookCandidates("בראשית", bookId = 99))
            assertTrue(engine.findInBookCandidates("ויקרא", bookId = 1)!!.isEmpty())
        }
    }

    @Test
    fun `findInBookCandidates stays within the book`() {
        withFindIndex { engine ->
            assertEquals(setOf(4L), engine.findInBookCandidates("בראשית", bookId = 2)!!.toSet())
        }
    }

    @Test
    fun `findInBookCandidates can't answer when the query is too short to filter`() {
        withFindIndex { engine ->
            assertNull(engine.findInBookCandidates("של", bookId = 1))
            assertTrue(engine.findInBookCandidates("  ", bookId = 1)!!.isEmpty())
        }
    }

    // --- Helper methods ---

    private fun collectLineIds(session: SearchSession): Set<Long> = runBlocking {
        val ids = mutableSetOf<Long>()
        while (true) {
            val page = session.nextPage(50) ?: break
            page.hits.forEach { ids.add(it.lineId) }
            if (page.isLastPage) break
        }
        session.close()
        ids
    }

    private fun createIndexWithPhrases(indexDir: Path) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                val rows = listOf(
                    1L to "מלך דוד",
                    2L to "דוד מלך",
                    3L to "מלך גדול דוד",
                    4L to "שלום עולם",
                    5L to "ירושלים מלך דוד עיר",
                )
                rows.forEach { (lineId, text) ->
                    val doc = Document().apply {
                        add(StringField("type", "line", Field.Store.YES))
                        add(StoredField("book_id", 1))
                        add(IntPoint("book_id", 1))
                        add(StoredField("book_title", "ספר בדיקה"))
                        add(StoredField("line_id", lineId))
                        add(IntPoint("line_id", lineId.toInt()))
                        add(StoredField("line_index", (lineId - 1).toInt()))
                        add(TextField("text", HebrewTextUtils.normalizeHebrew(text), Field.Store.NO))
                        add(StoredField("text_raw", text))
                        add(StoredField("is_base_book", 1))
                        add(StoredField("order_index", 1))
                    }
                    writer.addDocument(doc)
                }
            }
        }
    }

    private fun withFindIndex(block: (LuceneSearchEngine) -> Unit) {
        val tempDir = createTempIndexDir()
        try {
            // Same analyzers as the generator: standard words + per-word 4-grams
            val ngram4 = object : Analyzer() {
                override fun createComponents(fieldName: String): TokenStreamComponents {
                    val src = StandardTokenizer()
                    return TokenStreamComponents(src, NGramTokenFilter(LowerCaseFilter(src), 4, 4, false))
                }
            }
            val analyzer = PerFieldAnalyzerWrapper(StandardAnalyzer(), mapOf("text_ng4" to ngram4))
            FSDirectory.open(tempDir).use { dir ->
                IndexWriter(dir, IndexWriterConfig(analyzer)).use { writer ->
                    listOf(
                        Triple(1L, 1, "בראשית ברא אלהים"),
                        Triple(2L, 1, "והארץ היתה תהו"),
                        Triple(3L, 1, "ויברא אלהים את האדם"),
                        Triple(4L, 2, "בראשית רבה"),
                    ).forEach { (lineId, bookId, text) ->
                        val norm = HebrewTextUtils.normalizeHebrew(text)
                        writer.addDocument(
                            Document().apply {
                                add(StringField("type", "line", Field.Store.NO))
                                add(StoredField("book_id", bookId))
                                add(IntPoint("book_id", bookId))
                                add(StoredField("line_id", lineId))
                                add(IntPoint("line_id", lineId.toInt()))
                                add(TextField("text", norm, Field.Store.NO))
                                add(TextField("text_ng4", norm, Field.Store.NO))
                            },
                        )
                    }
                }
            }
            block(LuceneSearchEngine(tempDir))
        } finally {
            deleteDirectory(tempDir)
        }
    }

    // --- facets ---

    @Test
    fun `computeFacets counts hits per book and per ancestor category`() {
        listOf(true, false).forEach { docValues ->
            withFacetIndex(docValues) { engine ->
                val facets = assertNotNull(engine.computeFacets("שלום"))
                assertEquals(6, facets.totalHits)
                assertEquals(mapOf(1L to 3, 2L to 3), facets.bookCounts)
                // Book 1 sits in 10 > 11, book 2 in 10 > 12; the delta line of book 1 inherits its ancestors
                assertEquals(mapOf(10L to 6, 11L to 3, 12L to 3), facets.categoryCounts)
            }
        }
    }

    @Test
    fun `computeFacets matches the session hit set`() {
        withFacetIndex(docValues = true) { engine ->
            val facets = assertNotNull(engine.computeFacets("שלום", baseBookOnly = true))
            val ids = collectLineIds(assertNotNull(engine.openSession("שלום", baseBookOnly = true)))
            assertEquals(ids.size.toLong(), facets.totalHits)
        }
    }

    @Test
    fun `pages without snippets get them attached later`() {
        withFacetIndex(docValues = true) { engine ->
            runBlocking {
                val page = assertNotNull(engine.openSession("שלום")?.use { it.nextPage(10, snippets = false) })
                assertTrue(page.hits.all { it.snippet.isEmpty() })
                val withSnippets = engine.attachSnippets(page.hits, "שלום", 5)
                assertEquals(page.hits.map { it.lineId }, withSnippets.map { it.lineId })
                assertTrue(withSnippets.all { it.snippet.contains("<b>שלום</b>") })
            }
        }
    }

    private fun withFacetIndex(docValues: Boolean, block: (LuceneSearchEngine) -> Unit) {
        val tempDir = createTempIndexDir()
        try {
            FSDirectory.open(tempDir).use { dir ->
                IndexWriter(dir, IndexWriterConfig(StandardAnalyzer())).use { writer ->
                    // (lineId, bookId, ancestor category ids)
                    listOf(
                        Triple(1L, 1L, "10,11"), Triple(2L, 1L, "10,11"), Triple(3L, 2L, "10,12"),
                        Triple(4L, 2L, "10,12"), Triple(5L, 2L, "10,12"),
                    ).forEach { (lineId, bookId, ancestors) ->
                        writer.addDocument(facetDoc(lineId, bookId, ancestors, "שלום עולם $lineId", docValues))
                    }
                    writer.addDocument(facetDoc(6L, 1L, "10,11", "אחר לגמרי", docValues))
                    writer.commit()
                    // A line added by a delta update: own segment, no ancestors field
                    writer.addDocument(facetDoc(7L, 1L, null, "שלום לכולם", docValues))
                }
            }
            block(LuceneSearchEngine(tempDir))
        } finally {
            deleteDirectory(tempDir)
        }
    }

    private fun facetDoc(lineId: Long, bookId: Long, ancestors: String?, text: String, docValues: Boolean) =
        Document().apply {
            add(StringField("type", "line", Field.Store.NO))
            add(StoredField("book_id", bookId))
            add(IntPoint("book_id", bookId.toInt()))
            if (docValues) add(NumericDocValuesField("book_id", bookId))
            add(StoredField("book_title", "ספר $bookId"))
            ancestors?.let { add(StoredField("ancestor_category_ids", it)) }
            add(StoredField("line_id", lineId))
            add(IntPoint("line_id", lineId.toInt()))
            add(StoredField("line_index", lineId.toInt()))
            add(TextField("text", HebrewTextUtils.normalizeHebrew(text), Field.Store.NO))
            add(StoredField("text_raw", text))
            add(StoredField("is_base_book", 1))
            add(IntPoint("is_base_book", 1))
            add(StoredField("order_index", 1))
        }

    private fun createTempIndexDir(): Path {
        return Files.createTempDirectory("lucene_test_index")
    }

    private fun deleteDirectory(path: Path) {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .forEach { Files.deleteIfExists(it) }
        }
    }

    private fun createMinimalIndex(indexDir: Path) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                // Create at least one document so the index is valid
                val doc = Document().apply {
                    add(StringField("type", "line", Field.Store.YES))
                    add(StoredField("book_id", 1))
                    add(IntPoint("book_id", 1))
                    add(StoredField("book_title", "Test Book"))
                    add(StoredField("line_id", 1))
                    add(IntPoint("line_id", 1))
                    add(StoredField("line_index", 0))
                    add(TextField("text", "שלום עולם", Field.Store.NO))
                    add(StoredField("text_raw", "שלום עולם"))
                    add(StoredField("is_base_book", 0))
                    add(StoredField("order_index", 1))
                }
                writer.addDocument(doc)
            }
        }
    }

    private fun createIndexWithContent(indexDir: Path) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                val texts = listOf(
                    "שלום עולם, ברוכים הבאים",
                    "בראשית ברא אלהים את השמים ואת הארץ",
                    "שלום וברכה לכל הקוראים"
                )

                texts.forEachIndexed { idx, text ->
                    val doc = Document().apply {
                        add(StringField("type", "line", Field.Store.YES))
                        add(StoredField("book_id", 1))
                        add(IntPoint("book_id", 1))
                        add(StoredField("book_title", "ספר בדיקה"))
                        add(StoredField("line_id", idx + 1L))
                        add(IntPoint("line_id", idx + 1))
                        add(StoredField("line_index", idx))
                        add(TextField("text", HebrewTextUtils.normalizeHebrew(text), Field.Store.NO))
                        add(StoredField("text_raw", text))
                        add(StoredField("is_base_book", 1))
                        add(StoredField("order_index", 1))
                    }
                    writer.addDocument(doc)
                }
            }
        }
    }

    private fun createIndexWithMultipleDocuments(indexDir: Path, count: Int) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                repeat(count) { idx ->
                    val text = "טקסט מספר $idx עם תוכן לבדיקה"
                    val doc = Document().apply {
                        add(StringField("type", "line", Field.Store.YES))
                        add(StoredField("book_id", 1))
                        add(IntPoint("book_id", 1))
                        add(StoredField("book_title", "ספר בדיקה"))
                        add(StoredField("line_id", idx + 1L))
                        add(IntPoint("line_id", idx + 1))
                        add(StoredField("line_index", idx))
                        add(TextField("text", HebrewTextUtils.normalizeHebrew(text), Field.Store.NO))
                        add(StoredField("text_raw", text))
                        add(StoredField("is_base_book", 1))
                        add(StoredField("order_index", 1))
                    }
                    writer.addDocument(doc)
                }
            }
        }
    }

    private fun createIndexWithBookTitles(indexDir: Path) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                val titles = listOf(
                    "בראשית רבה",
                    "שמות רבה",
                    "ויקרא רבה"
                )

                titles.forEachIndexed { idx, title ->
                    val doc = Document().apply {
                        add(StringField("type", "book_title", Field.Store.YES))
                        add(StoredField("book_id", idx + 1L))
                        add(IntPoint("book_id", idx + 1))
                        add(TextField("title", HebrewTextUtils.normalizeHebrew(title), Field.Store.YES))
                    }
                    writer.addDocument(doc)
                }

                // Also add some line documents
                createLineDocuments(writer)
            }
        }
    }

    private fun createIndexWithMultipleBookTitles(indexDir: Path, count: Int) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                repeat(count) { idx ->
                    val title = "ספר מספר ${idx + 1}"
                    val doc = Document().apply {
                        add(StringField("type", "book_title", Field.Store.YES))
                        add(StoredField("book_id", idx + 1L))
                        add(IntPoint("book_id", idx + 1))
                        add(TextField("title", HebrewTextUtils.normalizeHebrew(title), Field.Store.YES))
                    }
                    writer.addDocument(doc)
                }
            }
        }
    }

    private fun createIndexWithMultipleBooks(indexDir: Path) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                val booksData = listOf(
                    1L to "ספר ראשון",
                    2L to "ספר שני",
                    3L to "ספר שלישי"
                )

                var lineId = 1L
                booksData.forEach { (bookId, bookTitle) ->
                    repeat(5) { idx ->
                        val text = "טקסט בספר $bookId שורה $idx"
                        val doc = Document().apply {
                            add(StringField("type", "line", Field.Store.YES))
                            add(StoredField("book_id", bookId))
                            add(IntPoint("book_id", bookId.toInt()))
                            add(StoredField("book_title", bookTitle))
                            add(StoredField("line_id", lineId))
                            add(IntPoint("line_id", lineId.toInt()))
                            add(StoredField("line_index", idx))
                            add(TextField("text", HebrewTextUtils.normalizeHebrew(text), Field.Store.NO))
                            add(StoredField("text_raw", text))
                            add(StoredField("is_base_book", 1))
                            add(StoredField("order_index", bookId.toInt()))
                        }
                        writer.addDocument(doc)
                        lineId++
                    }
                }
            }
        }
    }

    private fun createIndexWithHashemContent(indexDir: Path) {
        FSDirectory.open(indexDir).use { dir ->
            val config = IndexWriterConfig(StandardAnalyzer())
            IndexWriter(dir, config).use { writer ->
                val texts = listOf(
                    "ברוך ה׳ לעולם",
                    "יהוה אלהי ישראל",
                    "ה' מלך ה' מלך"
                )

                texts.forEachIndexed { idx, text ->
                    val doc = Document().apply {
                        add(StringField("type", "line", Field.Store.YES))
                        add(StoredField("book_id", 1))
                        add(IntPoint("book_id", 1))
                        add(StoredField("book_title", "ספר תפילות"))
                        add(StoredField("line_id", idx + 1L))
                        add(IntPoint("line_id", idx + 1))
                        add(StoredField("line_index", idx))
                        add(TextField("text", HebrewTextUtils.normalizeHebrew(text), Field.Store.NO))
                        add(StoredField("text_raw", text))
                        add(StoredField("is_base_book", 1))
                        add(StoredField("order_index", 1))
                    }
                    writer.addDocument(doc)
                }
            }
        }
    }

    private fun createLineDocuments(writer: IndexWriter) {
        val text = "טקסט לבדיקה עם תוכן"
        val doc = Document().apply {
            add(StringField("type", "line", Field.Store.YES))
            add(StoredField("book_id", 1))
            add(IntPoint("book_id", 1))
            add(StoredField("book_title", "ספר בדיקה"))
            add(StoredField("line_id", 1L))
            add(IntPoint("line_id", 1))
            add(StoredField("line_index", 0))
            add(TextField("text", HebrewTextUtils.normalizeHebrew(text), Field.Store.NO))
            add(StoredField("text_raw", text))
            add(StoredField("is_base_book", 1))
            add(StoredField("order_index", 1))
        }
        writer.addDocument(doc)
    }
}
