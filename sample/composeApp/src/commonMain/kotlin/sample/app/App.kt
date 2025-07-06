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

@Composable
fun App() {
    Logger.setMinSeverity(co.touchlab.kermit.Severity.Error)
    val repository = getRepository()
    val coroutineScope = rememberCoroutineScope()

    // Helper function to get all descendant IDs recursively
    fun getAllDescendantIds(entryId: Long, childrenMap: Map<Long, List<TocEntry>>): Set<Long> {
        val result = mutableSetOf<Long>()
        val children = childrenMap[entryId] ?: return result

        for (child in children) {
            result.add(child.id)
            result.addAll(getAllDescendantIds(child.id, childrenMap))
        }

        return result
    }

    // State for the UI
    var rootCategories by remember { mutableStateOf<List<Category>>(emptyList()) }
    var selectedCategory by remember { mutableStateOf<Category?>(null) }
    var expandedCategories by remember { mutableStateOf(setOf<Long>()) }
    var categoryChildren by remember { mutableStateOf<Map<Long, List<Category>>>(emptyMap()) }

    var booksInCategory by remember { mutableStateOf<Set<Book>>(emptySet()) }
    var selectedBook by remember { mutableStateOf<Book?>(null) }

    var bookLines by remember { mutableStateOf<List<Line>>(emptyList()) }
    var bookCommentaries by remember { mutableStateOf<List<CommentaryWithText>>(emptyList()) }
    var bookToc by remember { mutableStateOf<List<TocEntry>>(emptyList()) }
    var expandedTocEntries by remember { mutableStateOf(setOf<Long>()) }
    var tocChildren by remember { mutableStateOf<Map<Long, List<TocEntry>>>(emptyMap()) }

    // State for selected line and its comments
    var selectedLine by remember { mutableStateOf<Line?>(null) }

    // ✅ États de chargement
    var isLoadingBook by remember { mutableStateOf(false) }
    var isLoadingToc by remember { mutableStateOf(false) }

    // State for popup book display
    var showBookPopup by remember { mutableStateOf(false) }
    var popupBook by remember { mutableStateOf<Book?>(null) }
    var popupBookLines by remember { mutableStateOf<List<Line>>(emptyList()) }
    var popupBookCommentaries by remember { mutableStateOf<List<CommentaryWithText>>(emptyList()) }
    var popupCommentators by remember { mutableStateOf<List<CommentatorInfo>>(emptyList()) }
    var popupSelectedLine by remember { mutableStateOf<Line?>(null) }

    // State for search popup
    var showSearchPopup by remember { mutableStateOf(false) }

    // Load root categories on startup
    LaunchedEffect(repository) {
        rootCategories = repository.getRootCategories()
    }


    AppTheme {

        // Main content area - 4 vertical columns
        Row(modifier = Modifier.fillMaxSize()) {
            // First column - Book tree
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
                    selectedBook = selectedBook,
                    onCategoryClick = { category: Category ->
                        selectedCategory = category

                        expandedCategories = if (expandedCategories.contains(category.id)) {
                            expandedCategories - category.id
                        } else {
                            expandedCategories + category.id
                        }

                        if (expandedCategories.contains(category.id)) {
                            coroutineScope.launch {
                                try {
                                    // Charger enfants et livres en parallèle
                                    val childrenDeferred = async<List<Category>> {
                                        if (!categoryChildren.containsKey(category.id)) {
                                            repository.getCategoryChildren(category.id)
                                        } else emptyList()
                                    }

                                    val booksDeferred = async<List<Book>> {
                                        repository.getBooksByCategory(category.id)
                                    }

                                    val children = childrenDeferred.await()
                                    val books = booksDeferred.await()

                                    if (children.isNotEmpty()) {
                                        categoryChildren = categoryChildren + Pair(category.id, children)
                                    }

                                    if (books.isNotEmpty()) {
                                        // Using a Set automatically prevents duplicates
                                        booksInCategory = booksInCategory + books
                                    }


                                } catch (e: Exception) {
                                    Logger.e { "Erreur lors du chargement de la catégorie: ${e.message}" }
                                }
                            }
                        } else {
                        }
                    },
                    onBookClick = { book: Book ->
                        selectedBook = book
                        selectedLine = null
                        isLoadingBook = true
                        isLoadingToc = true

                        coroutineScope.launch {
                            try {
                                // Paralléliser les opérations
                                val bookDataDeferred = async { 
                                    repository.getLines(book.id, 0, 30)
                                }
                                val rootTocDeferred = async { repository.getBookRootToc(book.id) }

                                // Charger le contenu principal
                                bookLines = bookDataDeferred.await()
                                isLoadingBook = false  // ✅ Livre chargé


                                // Charger commentaires pour les 5 premières lignes seulement
                                if (bookLines.isNotEmpty()) {
                                    bookCommentaries = repository.getCommentariesForLines(
                                        bookLines.take(5).map { it.id }
                                    )
                                }

                                // Charger uniquement la TOC racine
                                bookToc = rootTocDeferred.await()

                                // Si la TOC racine est vide, essayer de charger toutes les entrées TOC
                                if (bookToc.isEmpty()) {
                                    bookToc = repository.getBookToc(book.id)
                                }

                                // Développer automatiquement la première entrée TOC (titre du livre)
                                // Les autres enfants seront chargés uniquement lorsque l'utilisateur clique sur une entrée
                                val expandedEntries = mutableSetOf<Long>()
                                // Si la TOC n'est pas vide, ajouter la première entrée à la liste des entrées développées
                                if (bookToc.isNotEmpty()) {
                                    expandedEntries.add(bookToc.first().id)
                                }
                                val childrenMap = mutableMapOf<Long, List<TocEntry>>()

                                tocChildren = childrenMap
                                expandedTocEntries = expandedEntries

                                // Si la TOC n'est pas vide, charger les enfants de la première entrée
                                if (bookToc.isNotEmpty()) {
                                    val firstEntry = bookToc.first()
                                    val children = repository.getTocChildren(firstEntry.id)

                                    // Toujours mettre à jour la carte des enfants, même si children est vide
                                    // Cela nous permet de savoir qu'on a vérifié les enfants pour cette entrée
                                    tocChildren = tocChildren + Pair(firstEntry.id, children)

                                    if (children.isNotEmpty()) {
                                        // Fonction récursive pour vérifier les enfants à tous les niveaux
                                        suspend fun checkChildrenRecursively(entries: List<TocEntry>, currentMap: MutableMap<Long, List<TocEntry>>) {
                                            for (entry in entries) {
                                                if (!currentMap.containsKey(entry.id)) {
                                                    val entryChildren = repository.getTocChildren(entry.id)
                                                    // Toujours ajouter l'entrée à la carte, même si entryChildren est vide
                                                    currentMap[entry.id] = entryChildren

                                                    // Vérifier récursivement les enfants de cette entrée
                                                    if (entryChildren.isNotEmpty()) {
                                                        checkChildrenRecursively(entryChildren, currentMap)
                                                    }
                                                }
                                            }
                                        }

                                        // Vérifier si chaque enfant a ses propres enfants et tous les descendants
                                        val updatedChildrenMap = tocChildren.toMutableMap()
                                        checkChildrenRecursively(children, updatedChildrenMap)
                                        tocChildren = updatedChildrenMap
                                    }
                                }

                                isLoadingToc = false  // ✅ TOC chargé

                            } catch (e: Exception) {
                                isLoadingBook = false
                                isLoadingToc = false
                                Logger.e { "Erreur lors du chargement du livre: ${e.message}" }
                            }
                        }
                    }
                )
            }

            // Second column - TOC
            Box(
                modifier = Modifier
                    .weight(0.20f)
                    .fillMaxHeight()
                    .background(MaterialTheme.colorScheme.surface.copy(alpha = 0.7f))
                    .padding(8.dp)
            ) {
                if (selectedBook != null) {
                    Column {
                        Text(
                            text = "תוכן עניינים",
                            fontWeight = FontWeight.Bold,
                            fontSize = 16.sp,
                            modifier = Modifier.padding(bottom = 8.dp)
                        )

                        if (isLoadingToc) {
                            // ✅ Indicateur de chargement pour TOC
                            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                                    CircularProgressIndicator(modifier = Modifier.size(24.dp))
                                    Spacer(modifier = Modifier.height(4.dp))
                                    Text("טוען תוכן עניינים...", fontSize = 12.sp)
                                }
                            }
                        } else {
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
                                                    // If we need to add the line to the list, we'll load a larger section around it
                                                    // to match the window size in BookContentView.kt (25 lines before and 25 lines after)
                                                    val startIndex = maxOf(0, line.lineIndex - 25)
                                                    val endIndex = line.lineIndex + 25
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
                                    val isCurrentlyExpanded = expandedTocEntries.contains(tocEntry.id)

                                    if (isCurrentlyExpanded) {
                                        // If currently expanded, collapse this entry and all its descendants
                                        val descendantIds = getAllDescendantIds(tocEntry.id, tocChildren)
                                        expandedTocEntries = expandedTocEntries - tocEntry.id - descendantIds
                                    } else {
                                        // If currently collapsed, expand this entry
                                        expandedTocEntries = expandedTocEntries + tocEntry.id

                                        // Load children if not already loaded
                                        if (!tocChildren.containsKey(tocEntry.id)) {
                                            coroutineScope.launch {
                                                // Load immediate children of the expanded entry
                                                val children = repository.getTocChildren(tocEntry.id)

                                                // Always update the children map, even if children is empty
                                                // This allows us to track that we've checked for children for this entry
                                                tocChildren = tocChildren + Pair(tocEntry.id, children)

                                                if (children.isNotEmpty()) {
                                                    // We don't automatically expand children anymore, 
                                                    // we just load the first level of children
                                                    // and let the user click on them to expand further

                                                    // Recursive function to check for children at all levels
                                                    suspend fun checkChildrenRecursively(entries: List<TocEntry>, currentMap: MutableMap<Long, List<TocEntry>>) {
                                                        for (entry in entries) {
                                                            if (!currentMap.containsKey(entry.id)) {
                                                                val entryChildren = repository.getTocChildren(entry.id)
                                                                // Always add the entry to the map, even if children is empty
                                                                // This allows us to track that we've checked for children for this entry
                                                                currentMap[entry.id] = entryChildren

                                                                // Recursively check children of this entry
                                                                if (entryChildren.isNotEmpty()) {
                                                                    checkChildrenRecursively(entryChildren, currentMap)
                                                                }
                                                            }
                                                        }
                                                    }

                                                    // Check if each child has children of its own and all descendants
                                                    // This helps us determine which entries should show the expand/collapse icon
                                                    val updatedChildrenMap = tocChildren.toMutableMap()
                                                    checkChildrenRecursively(children, updatedChildrenMap)
                                                    tocChildren = updatedChildrenMap
                                                }
                                            }
                                        }
                                    }
                                }
                            )
                        }
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
                        if (isLoadingBook) {
                            // ✅ Indicateur de chargement
                            Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                                Column(horizontalAlignment = Alignment.CenterHorizontally) {
                                    CircularProgressIndicator()
                                    Spacer(modifier = Modifier.height(8.dp))
                                    Text("טוען תוכן הספר...")
                                }
                            }
                        } else {
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
                    }

                    // Only show comments view if there are comments for the selected line
                    if (hasComments) {
                        Box(
                            modifier = Modifier
                                .weight(0.5f)
                                .fillMaxWidth()
                                .background(MaterialTheme.colorScheme.surface.copy(alpha = 0.5f))
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

                                            // Get the target line
                                            val targetLine = repository.getLine(commentary.link.targetLineId)

                                            if (targetLine != null) {
                                                // Load a section of lines around the target line (25 lines before and after)
                                                val startIndex = maxOf(0, targetLine.lineIndex - 25)
                                                val endIndex = targetLine.lineIndex + 25
                                                popupBookLines = repository.getLines(targetBook.id, startIndex, endIndex)

                                                // Set the selected line in the popup
                                                popupSelectedLine = targetLine

                                                // Load commentaries for the visible lines
                                                if (popupBookLines.isNotEmpty()) {
                                                    val lineIds = popupBookLines.map { it.id }
                                                    popupBookCommentaries = repository.getCommentariesForLines(lineIds)
                                                }
                                            } else {
                                                // Fallback to loading the first 100 lines if target line not found
                                                popupBookLines = repository.getLines(targetBook.id, 0, 100)

                                                // Load commentaries for the first few lines
                                                if (popupBookLines.isNotEmpty()) {
                                                    val lineIds = popupBookLines.map { it.id }
                                                    popupBookCommentaries = repository.getCommentariesForLines(lineIds)
                                                }
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
                selectedLine = popupSelectedLine,
                onDismiss = { 
                    showBookPopup = false 
                    popupSelectedLine = null  // Reset selected line when closing popup
                }
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
