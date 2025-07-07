package sample.app

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Search
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentaryWithText
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentatorInfo
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.async
import kotlinx.coroutines.launch

@Composable
expect fun getDatabasePath(): String

@Composable
expect fun getRepository(): SeforimRepository

@Composable
expect fun DatabaseSelectionButtonIfAvailable()

data class BookState(
    val book: Book? = null,
    val lines: List<Line> = emptyList(),
    val commentaries: List<CommentaryWithText> = emptyList(),
    val toc: List<TocEntry> = emptyList(),
    val selectedLine: Line? = null,
    val isLoading: Boolean = false,
    val isTocLoading: Boolean = false
)

data class PopupState(
    val show: Boolean = false,
    val book: Book? = null,
    val lines: List<Line> = emptyList(),
    val commentaries: List<CommentaryWithText> = emptyList(),
    val commentators: List<CommentatorInfo> = emptyList(),
    val selectedLine: Line? = null
)

@Composable
fun App() {
    Logger.setMinSeverity(co.touchlab.kermit.Severity.Error)
    val repository = getRepository()
    val scope = rememberCoroutineScope()

    // Category tree state
    var rootCategories by remember { mutableStateOf<List<Category>>(emptyList()) }
    var selectedCategory by remember { mutableStateOf<Category?>(null) }
    var expandedCategories by remember { mutableStateOf(setOf<Long>()) }
    var categoryChildren by remember { mutableStateOf<Map<Long, List<Category>>>(emptyMap()) }
    var booksInCategory by remember { mutableStateOf<Set<Book>>(emptySet()) }

    // Book state
    var bookState by remember { mutableStateOf(BookState()) }

    // TOC state
    var expandedTocEntries by remember { mutableStateOf(setOf<Long>()) }
    var tocChildren by remember { mutableStateOf<Map<Long, List<TocEntry>>>(emptyMap()) }

    // Popup states
    var bookPopup by remember { mutableStateOf(PopupState()) }
    var showSearchPopup by remember { mutableStateOf(false) }

    // Load root categories on startup
    LaunchedEffect(repository) {
        rootCategories = repository.getRootCategories()
    }

    // Helper functions
    fun getAllDescendantIds(entryId: Long, childrenMap: Map<Long, List<TocEntry>>): Set<Long> {
        val result = mutableSetOf<Long>()
        childrenMap[entryId]?.forEach { child ->
            result.add(child.id)
            result.addAll(getAllDescendantIds(child.id, childrenMap))
        }
        return result
    }

    fun loadBook(book: Book) {
        bookState = bookState.copy(
            book = book,
            selectedLine = null,
            isLoading = true,
            isTocLoading = true
        )

        scope.launch {
            try {
                // Load book data in parallel
                val linesDeferred = async { repository.getLines(book.id, 0, 30) }
                val tocDeferred = async {
                    repository.getBookRootToc(book.id).ifEmpty {
                        repository.getBookToc(book.id)
                    }
                }

                val lines = linesDeferred.await()
                val toc = tocDeferred.await()

                // Load commentaries for first 5 lines
                val commentaries = if (lines.isNotEmpty()) {
                    repository.getCommentariesForLines(lines.take(5).map { it.id })
                } else emptyList()

                bookState = bookState.copy(
                    lines = lines,
                    commentaries = commentaries,
                    toc = toc,
                    isLoading = false,
                    isTocLoading = false
                )

                // Auto-expand first TOC entry if exists
                if (toc.isNotEmpty()) {
                    val firstEntry = toc.first()
                    expandedTocEntries = setOf(firstEntry.id)
                    val children = repository.getTocChildren(firstEntry.id)
                    tocChildren = mapOf(firstEntry.id to children)
                }
            } catch (e: Exception) {
                bookState = bookState.copy(isLoading = false, isTocLoading = false)
                Logger.e { "Error loading book: ${e.message}" }
            }
        }
    }

    fun selectLine(line: Line) {
        bookState = bookState.copy(selectedLine = line)
        scope.launch {
            bookState = bookState.copy(
                commentaries = repository.getCommentariesForLines(listOf(line.id))
            )
        }
    }

    fun loadPopupBook(commentary: CommentaryWithText) {
        scope.launch {
            val targetBook = repository.getBook(commentary.link.targetBookId)
            if (targetBook != null) {
                val targetLine = repository.getLine(commentary.link.targetLineId)

                val (lines, selectedLine) = if (targetLine != null) {
                    val startIndex = maxOf(0, targetLine.lineIndex - 25)
                    val endIndex = targetLine.lineIndex + 25
                    repository.getLines(targetBook.id, startIndex, endIndex) to targetLine
                } else {
                    repository.getLines(targetBook.id, 0, 100) to null
                }

                val commentaries = if (lines.isNotEmpty()) {
                    repository.getCommentariesForLines(lines.map { it.id })
                } else emptyList()

                val commentators = repository.getAvailableCommentators(targetBook.id)

                bookPopup = PopupState(
                    show = true,
                    book = targetBook,
                    lines = lines,
                    commentaries = commentaries,
                    commentators = commentators,
                    selectedLine = selectedLine
                )
            }
        }
    }

    AppTheme {
        Row(modifier = Modifier.fillMaxSize()) {
            // Column 1: Book tree
            Box(
                modifier = Modifier
                    .weight(0.25f)
                    .fillMaxHeight()
                    .background(MaterialTheme.colorScheme.surface)
                    .padding(8.dp)
            ) {
                CategoryBookTree(
                    rootCategories = rootCategories,
                    expandedCategories = expandedCategories,
                    categoryChildren = categoryChildren,
                    booksInCategory = booksInCategory,
                    selectedCategory = selectedCategory,
                    selectedBook = bookState.book,
                    onCategoryClick = { category ->
                        selectedCategory = category

                        if (expandedCategories.contains(category.id)) {
                            expandedCategories -= category.id
                        } else {
                            expandedCategories += category.id

                            scope.launch {
                                try {
                                    val childrenDeferred = async {
                                        if (!categoryChildren.containsKey(category.id)) {
                                            repository.getCategoryChildren(category.id)
                                        } else emptyList()
                                    }
                                    val booksDeferred = async { repository.getBooksByCategory(category.id) }

                                    val children = childrenDeferred.await()
                                    val books = booksDeferred.await()

                                    if (children.isNotEmpty()) {
                                        categoryChildren += category.id to children
                                    }
                                    if (books.isNotEmpty()) {
                                        booksInCategory += books
                                    }
                                } catch (e: Exception) {
                                    Logger.e { "Error loading category: ${e.message}" }
                                }
                            }
                        }
                    },
                    onBookClick = ::loadBook
                )
            }

            // Column 2: TOC
            Box(
                modifier = Modifier
                    .weight(0.20f)
                    .fillMaxHeight()
                    .background(MaterialTheme.colorScheme.surface.copy(alpha = 0.7f))
                    .padding(8.dp)
            ) {
                if (bookState.book != null) {
                    Column {
                        Text(
                            text = "תוכן עניינים", // Table of Contents
                            fontWeight = FontWeight.Bold,
                            fontSize = 16.sp,
                            modifier = Modifier.padding(bottom = 8.dp)
                        )

                        if (bookState.isTocLoading) {
                            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                                    CircularProgressIndicator(modifier = Modifier.size(24.dp))
                                    Spacer(modifier = Modifier.height(4.dp))
                                    Text("טוען תוכן עניינים...", fontSize = 12.sp) // Loading table of contents...
                                }
                            }
                        } else {
                            TocView(
                                tocEntries = bookState.toc,
                                expandedEntries = expandedTocEntries,
                                childrenMap = tocChildren,
                                onEntryClick = { tocEntry ->
                                    tocEntry.lineId?.let { lineId ->
                                        scope.launch {
                                            val line = repository.getLine(lineId)
                                            if (line != null) {
                                                if (!bookState.lines.any { it.id == lineId }) {
                                                    val startIndex = maxOf(0, line.lineIndex - 25)
                                                    val endIndex = line.lineIndex + 25
                                                    bookState = bookState.copy(
                                                        lines = repository.getLines(bookState.book!!.id, startIndex, endIndex)
                                                    )
                                                }
                                                selectLine(line)
                                            }
                                        }
                                    }
                                },
                                onEntryExpand = { tocEntry ->
                                    val isExpanded = expandedTocEntries.contains(tocEntry.id)

                                    if (isExpanded) {
                                        val descendants = getAllDescendantIds(tocEntry.id, tocChildren)
                                        expandedTocEntries = expandedTocEntries - tocEntry.id - descendants
                                    } else {
                                        expandedTocEntries += tocEntry.id

                                        if (!tocChildren.containsKey(tocEntry.id)) {
                                            scope.launch {
                                                val children = repository.getTocChildren(tocEntry.id)
                                                tocChildren += tocEntry.id to children

                                                // Recursively check for grandchildren
                                                suspend fun loadChildrenRecursively(entries: List<TocEntry>) {
                                                    entries.forEach { entry ->
                                                        if (!tocChildren.containsKey(entry.id)) {
                                                            val entryChildren = repository.getTocChildren(entry.id)
                                                            tocChildren += entry.id to entryChildren
                                                            if (entryChildren.isNotEmpty()) {
                                                                loadChildrenRecursively(entryChildren)
                                                            }
                                                        }
                                                    }
                                                }

                                                if (children.isNotEmpty()) {
                                                    loadChildrenRecursively(children)
                                                }
                                            }
                                        }
                                    }
                                }
                            )
                        }
                    }
                } else {
                    Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                        Text("בחר ספר כדי לצפות בתוכן העניינים שלו") // Select a book to view its table of contents
                    }
                }
            }

            // Column 3: Book content and comments
            Column(
                modifier = Modifier
                    .weight(0.65f)
                    .fillMaxHeight()
                    .padding(8.dp)
            ) {
                if (bookState.book != null) {
                    val hasComments = bookState.selectedLine?.let { line ->
                        bookState.commentaries.any { it.link.sourceLineId == line.id }
                    } ?: false

                    // Book content
                    Box(
                        modifier = Modifier
                            .weight(if (hasComments) 0.5f else 1f)
                            .fillMaxWidth()
                    ) {
                        if (bookState.isLoading) {
                            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                                    CircularProgressIndicator()
                                    Spacer(modifier = Modifier.height(8.dp))
                                    Text("טוען תוכן הספר...") // Loading book content...
                                }
                            }
                        } else {
                            BookContentView(
                                book = bookState.book!!,
                                lines = bookState.lines,
                                selectedLine = bookState.selectedLine,
                                onLineSelected = ::selectLine
                            )
                        }
                    }

                    // Comments view (only if has comments)
                    if (hasComments) {
                        Box(
                            modifier = Modifier
                                .weight(0.5f)
                                .fillMaxWidth()
                                .background(MaterialTheme.colorScheme.surface.copy(alpha = 0.5f))
                        ) {
                            LineCommentsView(
                                selectedLine = bookState.selectedLine,
                                commentaries = bookState.commentaries,
                                onCommentClick = ::loadPopupBook
                            )
                        }
                    }
                }
            }
        }

        // Book popup
        if (bookPopup.show && bookPopup.book != null) {
            BookPopup(
                book = bookPopup.book!!,
                lines = bookPopup.lines,
                commentaries = bookPopup.commentaries,
                commentators = bookPopup.commentators,
                selectedLine = bookPopup.selectedLine,
                onDismiss = { bookPopup = PopupState() }
            )
        }

        // FAB for search and database
        Box(modifier = Modifier.fillMaxSize()) {
            Row(
                modifier = Modifier
                    .align(Alignment.TopEnd)
                    .padding(16.dp),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                DatabaseSelectionButtonIfAvailable()
                IconButton(onClick = { showSearchPopup = true }) {
                    Icon(Icons.Default.Search, contentDescription = "Search")
                }
            }
        }

        // Search popup
        if (showSearchPopup) {
            SearchPopup(
                repository = repository,
                onDismiss = { showSearchPopup = false },
                onResultClick = { searchResult ->
                    scope.launch {
                        val book = repository.getBook(searchResult.bookId)
                        if (book != null) {
                            selectedCategory = repository.getCategory(book.categoryId)
                            loadBook(book)

                            val line = repository.getLine(searchResult.lineId)
                            if (line != null) {
                                val startIndex = maxOf(0, line.lineIndex - 5)
                                val endIndex = line.lineIndex + 5
                                bookState = bookState.copy(
                                    lines = repository.getLines(book.id, startIndex, endIndex)
                                )
                                selectLine(line)
                            }

                            showSearchPopup = false
                        }
                    }
                }
            )
        }
    }
}