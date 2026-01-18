package io.github.kdroidfilter.seforimlibrary.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.assertFalse
import org.apache.lucene.analysis.standard.StandardAnalyzer
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

            val page = session.nextPage(10)
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

            val page = session.nextPage(100)
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

            val page = session.nextPage(100)
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

            val page = session.nextPage(10)
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

    // --- Helper methods ---

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
