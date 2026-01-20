package io.github.kdroidfilter.seforimlibrary.dao.repository

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Integration tests for [SeforimRepository].
 * Uses an in-memory SQLite database for isolation and speed.
 */
class SeforimRepositoryIntegrationTest {
    private lateinit var driver: JdbcSqliteDriver
    private lateinit var repository: SeforimRepository

    @BeforeTest
    fun setup() {
        driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        repository = SeforimRepository(":memory:", driver)
    }

    @AfterTest
    fun tearDown() {
        driver.close()
    }

    // ==================== Source Tests ====================

    @Test
    fun `insertSource creates new source and returns id`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        assertTrue(sourceId > 0)
    }

    @Test
    fun `insertSource returns existing id for duplicate source`() = runBlocking {
        val firstId = repository.insertSource("Sefaria")
        val secondId = repository.insertSource("Sefaria")
        assertEquals(firstId, secondId)
    }

    @Test
    fun `getSourceByName returns source when exists`() = runBlocking {
        val sourceName = "Otzaria"
        repository.insertSource(sourceName)

        val source = repository.getSourceByName(sourceName)

        assertNotNull(source)
        assertEquals(sourceName, source.name)
    }

    @Test
    fun `getSourceByName returns null when not exists`() = runBlocking {
        val source = repository.getSourceByName("NonExistent")
        assertNull(source)
    }

    // ==================== Category Tests ====================

    @Test
    fun `insertCategory creates root category`() = runBlocking {
        val category = Category(
            parentId = null,
            title = "Torah",
            level = 0,
            order = 1
        )

        val categoryId = repository.insertCategory(category)

        assertTrue(categoryId > 0)
    }

    @Test
    fun `insertCategory creates child category with parent`() = runBlocking {
        val parentCategory = Category(parentId = null, title = "Torah", level = 0, order = 1)
        val parentId = repository.insertCategory(parentCategory)

        val childCategory = Category(parentId = parentId, title = "Bereshit", level = 1, order = 1)
        val childId = repository.insertCategory(childCategory)

        assertTrue(childId > 0)
        assertNotEquals(parentId, childId)
    }

    @Test
    fun `insertCategory returns existing id for duplicate in same parent`() = runBlocking {
        val category = Category(parentId = null, title = "Torah", level = 0, order = 1)
        val firstId = repository.insertCategory(category)
        val secondId = repository.insertCategory(category)

        assertEquals(firstId, secondId)
    }

    @Test
    fun `getRootCategories returns only root categories`() = runBlocking {
        // Insert root categories
        repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        repository.insertCategory(Category(parentId = null, title = "Neviim", level = 0, order = 2))

        // Insert a child category
        val parentId = repository.insertCategory(Category(parentId = null, title = "Ketuvim", level = 0, order = 3))
        repository.insertCategory(Category(parentId = parentId, title = "Tehillim", level = 1, order = 1))

        val rootCategories = repository.getRootCategories()

        assertEquals(3, rootCategories.size)
        assertTrue(rootCategories.all { it.parentId == null })
    }

    @Test
    fun `getSubcategories returns children of parent`() = runBlocking {
        val parentId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        repository.insertCategory(Category(parentId = parentId, title = "Bereshit", level = 1, order = 1))
        repository.insertCategory(Category(parentId = parentId, title = "Shemot", level = 1, order = 2))
        repository.insertCategory(Category(parentId = parentId, title = "Vayikra", level = 1, order = 3))

        val subcategories = repository.getCategoryChildren(parentId)

        assertEquals(3, subcategories.size)
        assertTrue(subcategories.all { cat -> cat.parentId == parentId })
    }

    @Test
    fun `getCategory returns category by id`() = runBlocking {
        val title = "Torah"
        val categoryId = repository.insertCategory(Category(parentId = null, title = title, level = 0, order = 1))

        val category = repository.getCategory(categoryId)

        assertNotNull(category)
        assertEquals(title, category.title)
        assertEquals(categoryId, category.id)
    }

    @Test
    fun `getCategory returns null for non-existent id`() = runBlocking {
        val category = repository.getCategory(99999L)
        assertNull(category)
    }

    // ==================== Book Tests ====================

    @Test
    fun `insertBook creates new book`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))

        val book = Book(
            categoryId = categoryId,
            sourceId = sourceId,
            title = "Bereshit",
            order = 1f
        )
        val bookId = repository.insertBook(book)

        assertTrue(bookId > 0)
    }

    @Test
    fun `getBook returns book with all metadata`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val book = Book(
            categoryId = categoryId,
            sourceId = sourceId,
            title = "Bereshit",
            order = 1f,
            heShortDesc = "בראשית"
        )
        val bookId = repository.insertBook(book)

        val retrievedBook = repository.getBook(bookId)

        assertNotNull(retrievedBook)
        assertEquals("Bereshit", retrievedBook.title)
        assertEquals("בראשית", retrievedBook.heShortDesc)
        assertEquals(categoryId, retrievedBook.categoryId)
    }

    @Test
    fun `getBook returns null for non-existent id`() = runBlocking {
        val book = repository.getBook(99999L)
        assertNull(book)
    }

    @Test
    fun `getBookCore returns lightweight book data`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Shemot", order = 2f)
        )

        val book = repository.getBookCore(bookId)

        assertNotNull(book)
        assertEquals("Shemot", book.title)
    }

    @Test
    fun `getBooksByCategory returns books in category`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))

        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f))
        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Shemot", order = 2f))
        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Vayikra", order = 3f))

        val books = repository.getBooksByCategory(categoryId)

        assertEquals(3, books.size)
        assertTrue(books.all { it.categoryId == categoryId })
    }

    @Test
    fun `updateBookTotalLines updates line count`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        repository.updateBookTotalLines(bookId, 1533)

        val book = repository.getBook(bookId)
        assertNotNull(book)
        assertEquals(1533, book.totalLines)
    }

    // ==================== Line Tests ====================

    @Test
    fun `insertLine creates new line`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        val line = Line(
            bookId = bookId,
            lineIndex = 0,
            content = "<p>בראשית ברא אלהים את השמים ואת הארץ</p>"
        )
        val lineId = repository.insertLine(line)

        assertTrue(lineId > 0)
    }

    @Test
    fun `getLine returns line by id`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )
        val content = "<p>בראשית ברא אלהים</p>"
        val lineId = repository.insertLine(Line(bookId = bookId, lineIndex = 0, content = content))

        val line = repository.getLine(lineId)

        assertNotNull(line)
        assertEquals(content, line.content)
        assertEquals(bookId, line.bookId)
    }

    @Test
    fun `getLine returns null for non-existent id`() = runBlocking {
        val line = repository.getLine(99999L)
        assertNull(line)
    }

    @Test
    fun `getLines returns lines for book within range`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        repository.insertLine(Line(bookId = bookId, lineIndex = 0, content = "Line 0"))
        repository.insertLine(Line(bookId = bookId, lineIndex = 1, content = "Line 1"))
        repository.insertLine(Line(bookId = bookId, lineIndex = 2, content = "Line 2"))

        val lines = repository.getLines(bookId, startIndex = 0, endIndex = 3)

        assertEquals(3, lines.size)
        assertTrue(lines.all { it.bookId == bookId })
    }

    @Test
    fun `getLines returns paginated lines`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        // Insert 10 lines
        repeat(10) { i ->
            repository.insertLine(Line(bookId = bookId, lineIndex = i, content = "Line $i"))
        }

        // Get first range (lineIndex 0-4)
        val firstRange = repository.getLines(bookId, startIndex = 0, endIndex = 4)
        assertTrue(firstRange.isNotEmpty())
        assertTrue(firstRange.all { it.lineIndex < 5 })

        // Get second range (lineIndex 5-9)
        val secondRange = repository.getLines(bookId, startIndex = 5, endIndex = 9)
        assertTrue(secondRange.isNotEmpty())
        assertTrue(secondRange.all { it.lineIndex >= 5 })

        // Verify no overlap by line index
        val firstRangeIndices = firstRange.map { it.lineIndex }.toSet()
        val secondRangeIndices = secondRange.map { it.lineIndex }.toSet()
        assertTrue(firstRangeIndices.intersect(secondRangeIndices).isEmpty())
    }

    // ==================== TOC Tests ====================

    @Test
    fun `insertTocEntry creates new toc entry`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        val tocEntry = TocEntry(
            bookId = bookId,
            parentId = null,
            text = "Chapter 1",
            level = 0,
            hasChildren = false
        )
        val tocId = repository.insertTocEntry(tocEntry)

        assertTrue(tocId > 0)
    }

    @Test
    fun `getTocEntriesForBook returns entries for book`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        // Insert entries
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = null, text = "Chapter 1", level = 0, hasChildren = true))
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = null, text = "Chapter 2", level = 0, hasChildren = true))

        val entries = repository.getTocEntriesForBook(bookId)

        assertEquals(2, entries.size)
        assertTrue(entries.all { it.bookId == bookId })
    }

    @Test
    fun `getTocChildren returns children of parent entry`() = runBlocking {
        val sourceId = repository.insertSource("Sefaria")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bookId = repository.insertBook(
            Book(categoryId = categoryId, sourceId = sourceId, title = "Bereshit", order = 1f)
        )

        val parentId = repository.insertTocEntry(
            TocEntry(bookId = bookId, parentId = null, text = "Chapter 1", level = 0, hasChildren = true)
        )
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = parentId, text = "Verse 1", level = 1, hasChildren = false))
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = parentId, text = "Verse 2", level = 1, hasChildren = false))
        repository.insertTocEntry(TocEntry(bookId = bookId, parentId = parentId, text = "Verse 3", level = 1, hasChildren = false))

        val children = repository.getTocChildren(parentId)

        assertEquals(3, children.size)
        assertTrue(children.all { it.parentId == parentId })
    }

    // ==================== Category Closure Tests ====================

    @Test
    fun `rebuildCategoryClosure creates self and ancestor pairs`() = runBlocking {
        // Create a hierarchy: Torah -> Bereshit -> Chapters
        val torahId = repository.insertCategory(Category(parentId = null, title = "Torah", level = 0, order = 1))
        val bereshitId = repository.insertCategory(Category(parentId = torahId, title = "Bereshit", level = 1, order = 1))
        val chaptersId = repository.insertCategory(Category(parentId = bereshitId, title = "Chapters", level = 2, order = 1))

        repository.rebuildCategoryClosure()

        // Verify hierarchy works by getting books under ancestor category
        val sourceId = repository.insertSource("Test")
        repository.insertBook(Book(categoryId = chaptersId, sourceId = sourceId, title = "Test Book", order = 1f))

        val booksUnderTorah = repository.getBooksUnderCategoryTree(torahId)
        assertEquals(1, booksUnderTorah.size)
    }

    // ==================== Transaction Tests ====================

    @Test
    fun `runInTransaction executes block and returns result`() = runBlocking {
        val result = repository.runInTransaction {
            val sourceId = repository.insertSource("Test")
            val categoryId = repository.insertCategory(Category(parentId = null, title = "Test", level = 0, order = 1))
            repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Test Book", order = 1f))
        }

        assertTrue(result > 0)
    }

    // ==================== Max ID Tests ====================

    @Test
    fun `getMaxBookId returns 0 when no books exist`() = runBlocking {
        val maxId = repository.getMaxBookId()
        assertEquals(0L, maxId)
    }

    @Test
    fun `getMaxBookId returns highest book id`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Test", level = 0, order = 1))

        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book 1", order = 1f))
        repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book 2", order = 2f))
        val lastBookId = repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book 3", order = 3f))

        val maxId = repository.getMaxBookId()

        assertEquals(lastBookId, maxId)
    }

    @Test
    fun `getMaxLineId returns 0 when no lines exist`() = runBlocking {
        val maxId = repository.getMaxLineId()
        assertEquals(0L, maxId)
    }

    @Test
    fun `getMaxLineId returns highest line id`() = runBlocking {
        val sourceId = repository.insertSource("Test")
        val categoryId = repository.insertCategory(Category(parentId = null, title = "Test", level = 0, order = 1))
        val bookId = repository.insertBook(Book(categoryId = categoryId, sourceId = sourceId, title = "Book", order = 1f))

        repository.insertLine(Line(bookId = bookId, lineIndex = 0, content = "Line 1"))
        repository.insertLine(Line(bookId = bookId, lineIndex = 1, content = "Line 2"))
        val lastLineId = repository.insertLine(Line(bookId = bookId, lineIndex = 2, content = "Line 3"))

        val maxId = repository.getMaxLineId()

        assertEquals(lastLineId, maxId)
    }
}
