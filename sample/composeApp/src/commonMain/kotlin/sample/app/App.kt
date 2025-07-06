package sample.app

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.material.*
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Search
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.material.lightColors
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentaryWithText
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentatorInfo
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.launch
import javax.print.attribute.standard.Severity
import sample.app.BookPopup

@Composable
expect fun getDatabasePath(): String

@Composable
expect fun getRepository(): SeforimRepository

@Composable
expect fun DatabaseSelectionButtonIfAvailable()

@Composable
fun App() {
    Logger.setMinSeverity(co.touchlab.kermit.Severity.Error)
    val repository = getRepository()
    val coroutineScope = rememberCoroutineScope()

    // State for the UI
    var rootCategories by remember { mutableStateOf<List<Category>>(emptyList()) }
    var selectedCategory by remember { mutableStateOf<Category?>(null) }
    var expandedCategories by remember { mutableStateOf(setOf<Long>()) }
    var categoryChildren by remember { mutableStateOf<Map<Long, List<Category>>>(emptyMap()) }

    var booksInCategory by remember { mutableStateOf<List<Book>>(emptyList()) }
    var selectedBook by remember { mutableStateOf<Book?>(null) }

    var bookLines by remember { mutableStateOf<List<Line>>(emptyList()) }
    var bookCommentaries by remember { mutableStateOf<List<CommentaryWithText>>(emptyList()) }
    var commentators by remember { mutableStateOf<List<CommentatorInfo>>(emptyList()) }
    var bookToc by remember { mutableStateOf<List<TocEntry>>(emptyList()) }
    var expandedTocEntries by remember { mutableStateOf(setOf<Long>()) }
    var tocChildren by remember { mutableStateOf<Map<Long, List<TocEntry>>>(emptyMap()) }

    // State for selected line and its comments
    var selectedLine by remember { mutableStateOf<Line?>(null) }

    // State for popup book display
    var showBookPopup by remember { mutableStateOf(false) }
    var popupBook by remember { mutableStateOf<Book?>(null) }
    var popupBookLines by remember { mutableStateOf<List<Line>>(emptyList()) }
    var popupBookCommentaries by remember { mutableStateOf<List<CommentaryWithText>>(emptyList()) }
    var popupCommentators by remember { mutableStateOf<List<CommentatorInfo>>(emptyList()) }

    // State for search popup
    var showSearchPopup by remember { mutableStateOf(false) }

    // Load root categories on startup
    LaunchedEffect(repository) {
        rootCategories = repository.getRootCategories()
    }

    // Define monochromatic color palette
    val monochromeColors = lightColors(
        primary = Color(0xFF333333),
        primaryVariant = Color(0xFF555555),
        secondary = Color(0xFF666666),
        secondaryVariant = Color(0xFF888888),
        background = Color.White,
        surface = Color(0xFFF5F5F5),
        error = Color(0xFF555555),
        onPrimary = Color.White,
        onSecondary = Color.White,
        onBackground = Color(0xFF333333),
        onSurface = Color(0xFF333333),
        onError = Color.White
    )

    MaterialTheme(
        colors = monochromeColors
    ) {

        // Main content area - 4 vertical columns
        Row(modifier = Modifier.fillMaxSize()) {
            // First column - Book tree
            Box(
                modifier = Modifier
                    .weight(0.25f)
                    .fillMaxHeight()
                    .background(MaterialTheme.colors.surface)
                    .padding(8.dp)
            ) {
                CategoryBookTree(
                    rootCategories = rootCategories,
                    expandedCategories = expandedCategories,
                    categoryChildren = categoryChildren,
                    booksInCategory = booksInCategory,
                    selectedCategory = selectedCategory,
                    selectedBook = selectedBook,
                    onCategoryClick = { category: Category ->
                        selectedCategory = category

                        // Toggle expanded state
                        expandedCategories = if (expandedCategories.contains(category.id)) {
                            expandedCategories - category.id
                        } else {
                            expandedCategories + category.id
                        }

                        // Load children if expanded
                        if (expandedCategories.contains(category.id)) {
                            coroutineScope.launch {
                                // Recursive function to load all category children without depth limitation
                                suspend fun loadCategoryChildren(categoryId: Long) {
                                    val children = repository.getCategoryChildren(categoryId)
                                    if (children.isNotEmpty()) {
                                        // Update the children map with the new children
                                        categoryChildren = categoryChildren + Pair(categoryId, children)

                                        // Recursively load children of children
                                        for (child in children) {
                                            // Only load if not already loaded
                                            if (!categoryChildren.containsKey(child.id)) {
                                                loadCategoryChildren(child.id)
                                            }
                                        }
                                    }
                                }

                                // Load all levels of categories
                                loadCategoryChildren(category.id)

                                // Load books for this category and all its children
                                suspend fun loadBooksForCategory(categoryId: Long) {
                                    // Load books for this category
                                    val books = repository.getBooksByCategory(categoryId)
                                    if (books.isNotEmpty()) {
                                        booksInCategory = booksInCategory + books
                                    }

                                    // Recursively load books for child categories
                                    val children = categoryChildren[categoryId] ?: emptyList()
                                    for (child in children) {
                                        loadBooksForCategory(child.id)
                                    }
                                }

                                // Start loading books from the selected category
                                booksInCategory = emptyList() // Clear previous books
                                loadBooksForCategory(category.id)
                            }
                        }
                    },
                    onBookClick = { book: Book ->
                        selectedBook = book
                        selectedLine = null // Reset selected line when changing books

                        // Load book content and commentaries
                        coroutineScope.launch {
                            // Load first 100 lines of the book
                            bookLines = repository.getLines(book.id, 0, 100)

                            // Load commentators for this book
                            commentators = repository.getAvailableCommentators(book.id)

                            // Load commentaries for the first few lines
                            if (bookLines.isNotEmpty()) {
                                val lineIds = bookLines.map { it.id }
                                bookCommentaries = repository.getCommentariesForLines(lineIds)
                            }

                            // Load TOC data for the book
                            bookToc = repository.getBookRootToc(book.id)

                            // Pre-load children for all root TOC entries
                            val rootEntryIds = bookToc.map { it.id }.toSet()
                            expandedTocEntries = rootEntryIds

                            // Load all TOC levels recursively without depth limitation
                            val childrenMap = mutableMapOf<Long, List<TocEntry>>()

                            // Recursive function to load all TOC children without depth limitation
                            suspend fun loadTocChildren(entries: List<TocEntry>) {
                                for (entry in entries) {
                                    val children = repository.getTocChildren(entry.id)
                                    if (children.isNotEmpty()) {
                                        childrenMap[entry.id] = children
                                        // Recursively load children of children
                                        loadTocChildren(children)
                                    }
                                }
                            }

                            // Load all levels of TOC entries
                            loadTocChildren(bookToc)

                            tocChildren = childrenMap
                        }
                    }
                )
            }

            // Second column - TOC
            Box(
                modifier = Modifier
                    .weight(0.20f)
                    .fillMaxHeight()
                    .background(MaterialTheme.colors.surface.copy(alpha = 0.7f))
                    .padding(8.dp)
            ) {
                if (selectedBook != null) {
                    Column {
                        // TOC header
                        Text(
                            text = "תוכן עניינים",
                            fontWeight = FontWeight.Bold,
                            fontSize = 16.sp,
                            modifier = Modifier.padding(bottom = 8.dp)
                        )

                        // TOC content
                        TocView(
                            tocEntries = bookToc,
                            expandedEntries = expandedTocEntries,
                            childrenMap = tocChildren,
                            onEntryClick = { tocEntry ->
                                // Navigate to the line associated with this TOC entry
                                coroutineScope.launch {
                                    // If the TOC entry has a lineId, navigate directly to that line
                                    tocEntry.lineId?.let { lineId ->
                                        // Get the line directly by its ID
                                        val line = repository.getLine(lineId)

                                        if (line != null) {
                                            // Make sure the line is in the bookLines list
                                            if (!bookLines.any { it.id == lineId }) {
                                                // If we need to add the line to the list, we'll load a small section around it
                                                // to provide context, but we're not using the "50" calculation anymore
                                                val startIndex = maxOf(0, line.lineIndex - 5)
                                                val endIndex = line.lineIndex + 5
                                                bookLines = repository.getLines(selectedBook!!.id, startIndex, endIndex)
                                            }

                                            // Set the selected line directly
                                            selectedLine = line

                                            // Load commentaries for this line
                                            bookCommentaries = repository.getCommentariesForLines(listOf(lineId))
                                        }
                                    }
                                }
                            },
                            onEntryExpand = { tocEntry ->
                                // Toggle expanded state
                                expandedTocEntries = if (expandedTocEntries.contains(tocEntry.id)) {
                                    expandedTocEntries - tocEntry.id
                                } else {
                                    expandedTocEntries + tocEntry.id
                                }

                                // Load children if expanded and not already loaded
                                if (expandedTocEntries.contains(tocEntry.id) && !tocChildren.containsKey(tocEntry.id)) {
                                    coroutineScope.launch {
                                        // Recursive function to load all TOC children without depth limitation
                                        suspend fun loadTocChildrenOnExpand(entry: TocEntry) {
                                            val children = repository.getTocChildren(entry.id)
                                            if (children.isNotEmpty()) {
                                                // Update the children map with the new children
                                                tocChildren = tocChildren + Pair(entry.id, children)

                                                // Recursively load children of children
                                                for (child in children) {
                                                    // Only load if not already loaded
                                                    if (!tocChildren.containsKey(child.id)) {
                                                        loadTocChildrenOnExpand(child)
                                                    }
                                                }
                                            }
                                        }

                                        // Load all levels when expanding
                                        loadTocChildrenOnExpand(tocEntry)
                                    }
                                }
                            }
                        )
                    }
                } else {
                    Box(
                        modifier = Modifier.fillMaxSize(),
                        contentAlignment = Alignment.Center
                    ) {
                        Text("בחר ספר כדי לצפות בתוכן העניינים שלו")
                    }
                }
            }

            // Third column - Book content and Line comments combined
            Column(
                modifier = Modifier
                    .weight(0.65f)
                    .fillMaxHeight()
                    .padding(8.dp)
            ) {
                if (selectedBook != null) {
                    // Check if there are comments for the selected line
                    val hasComments = selectedLine?.let { line ->
                        bookCommentaries.any { it.link.sourceLineId == line.id }
                    } ?: false

                    // Adjust weight based on whether there are comments
                    Box(
                        modifier = Modifier
                            .weight(if (hasComments) 0.5f else 1f)
                            .fillMaxWidth()
                    ) {
                        BookContentView(
                            book = selectedBook!!,
                            lines = bookLines,
                            selectedLine = selectedLine,
                            onLineSelected = { line ->
                                selectedLine = line
                                // Load commentaries for this line
                                coroutineScope.launch {
                                    bookCommentaries = repository.getCommentariesForLines(listOf(line.id))
                                }
                            }
                        )
                    }

                    // Only show comments view if there are comments for the selected line
                    if (hasComments) {
                        Box(
                            modifier = Modifier
                                .weight(0.5f)
                                .fillMaxWidth()
                                .background(MaterialTheme.colors.surface.copy(alpha = 0.5f))
                        ) {
                            LineCommentsView(
                                selectedLine = selectedLine,
                                commentaries = bookCommentaries,
                                onCommentClick = { commentary ->
                                    // Load the target book and its content
                                    coroutineScope.launch {
                                        // Get the book
                                        val targetBook = repository.getBook(commentary.link.targetBookId)
                                        if (targetBook != null) {
                                            popupBook = targetBook

                                            // Load the first 100 lines of the book
                                            popupBookLines = repository.getLines(targetBook.id, 0, 100)

                                            // Load commentaries for the first few lines
                                            if (popupBookLines.isNotEmpty()) {
                                                val lineIds = popupBookLines.map { it.id }
                                                popupBookCommentaries = repository.getCommentariesForLines(lineIds)
                                            }

                                            // Load commentators for this book
                                            popupCommentators = repository.getAvailableCommentators(targetBook.id)

                                            // Show the popup
                                            showBookPopup = true
                                        }
                                    }
                                }
                            )
                        }
                    }
                }
            }
        }

        // Show book popup if needed
        if (showBookPopup && popupBook != null) {
            BookPopup(
                book = popupBook!!,
                lines = popupBookLines,
                commentaries = popupBookCommentaries,
                commentators = popupCommentators,
                onDismiss = { showBookPopup = false }
            )
        }

        // Floating action buttons for search and database selection
        Box(modifier = Modifier.fillMaxSize()) {
            Row(
                modifier = Modifier
                    .align(Alignment.TopEnd)
                    .padding(16.dp),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                // Database selection button (only shown on desktop)
                DatabaseSelectionButtonIfAvailable()

                // Search button
                IconButton(onClick = { showSearchPopup = true }) {
                    Icon(
                        imageVector = Icons.Default.Search,
                        contentDescription = "Search"
                    )
                }
            }
        }

        // Show search popup if needed
        if (showSearchPopup) {
            SearchPopup(
                repository = repository,
                onDismiss = { showSearchPopup = false },
                onResultClick = { searchResult ->
                    // When a search result is clicked, load the book and navigate to the line
                    coroutineScope.launch {
                        // Get the book
                        val book = repository.getBook(searchResult.bookId)
                        if (book != null) {
                            // Set as selected book
                            selectedBook = book
                            selectedCategory = repository.getCategory(book.categoryId)

                            // Get the line
                            val line = repository.getLine(searchResult.lineId)
                            if (line != null) {
                                // Load a section of lines around the selected line
                                val startIndex = maxOf(0, line.lineIndex - 5)
                                val endIndex = line.lineIndex + 5
                                bookLines = repository.getLines(book.id, startIndex, endIndex)

                                // Set the selected line
                                selectedLine = line

                                // Load commentaries for this line
                                bookCommentaries = repository.getCommentariesForLines(listOf(line.id))

                                // Load TOC data for the book
                                bookToc = repository.getBookRootToc(book.id)

                                // Close the search popup
                                showSearchPopup = false
                            }
                        }
                    }
                }
            )
        }
    }
}
