package sample.app

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Search
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.key.Key
import androidx.compose.ui.input.key.key
import androidx.compose.ui.input.key.onKeyEvent
import androidx.compose.ui.text.input.TextFieldValue
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import com.mohamedrejeb.richeditor.model.rememberRichTextState
import com.mohamedrejeb.richeditor.ui.material.RichText
import io.github.kdroidfilter.seforimlibrary.core.models.SearchResult
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.launch

/**
 * A popup dialog that allows searching across all books and displays the results.
 *
 * @param repository The repository used for searching
 * @param onDismiss Callback when the popup is dismissed
 * @param onResultClick Callback when a search result is clicked
 */
@Composable
fun SearchPopup(
    repository: SeforimRepository,
    onDismiss: () -> Unit,
    onResultClick: (SearchResult) -> Unit
) {
    var searchQuery by remember { mutableStateOf(TextFieldValue("")) }
    var searchResults by remember { mutableStateOf<List<SearchResult>>(emptyList()) }
    var isSearching by remember { mutableStateOf(false) }
    var resultLimit by remember { mutableStateOf(20f) }
    var selectedBooks by remember { mutableStateOf(setOf<Long>()) }
    val coroutineScope = rememberCoroutineScope()

    // Extract unique books from search results
    val uniqueBooks = remember(searchResults) {
        searchResults.map { it.bookId to it.bookTitle }.toSet().toList()
    }

    // Function to chunk the list into groups for grid layout
    fun <T> List<T>.chunked(size: Int): List<List<T>> {
        return this.withIndex()
            .groupBy { it.index / size }
            .map { it.value.map { indexedValue -> indexedValue.value } }
    }

    Dialog(onDismissRequest = onDismiss, properties = DialogProperties(usePlatformDefaultWidth = false)) {
        Surface(
            modifier = Modifier
                .fillMaxSize()
                .padding(16.dp),
            shape = MaterialTheme.shapes.medium
        ) {
            Column(modifier = Modifier.padding(16.dp)) {
                // Header with close button
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    Text(
                        text = "חיפוש",
                        style = MaterialTheme.typography.bodyMedium
                    )
                    IconButton(onClick = onDismiss) {
                        Icon(Icons.Default.Close, contentDescription = "Close")
                    }
                }

                HorizontalDivider(
                    modifier = Modifier.padding(vertical = 8.dp),
                    thickness = DividerDefaults.Thickness,
                    color = DividerDefaults.color
                )

                // Search input and button
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    TextField(
                        value = searchQuery,
                        onValueChange = { newValue -> searchQuery = newValue },
                        modifier = Modifier
                            .weight(1f)
                            .onKeyEvent { keyEvent ->
                                if (keyEvent.key == Key.Enter && searchQuery.text.isNotBlank()) {
                                    coroutineScope.launch {
                                        isSearching = true
                                        // Reset selected books for new search
                                        selectedBooks = emptySet()
                                        searchResults = repository.search(
                                            query = searchQuery.text,
                                            limit = resultLimit.toInt()
                                        )
                                        isSearching = false
                                    }
                                    true
                                } else {
                                    false
                                }
                            },
                        label = { Text("הזן טקסט לחיפוש") },
                        singleLine = true
                    )

                    Spacer(modifier = Modifier.width(8.dp))

                    IconButton(
                        onClick = {
                            if (searchQuery.text.isNotBlank()) {
                                coroutineScope.launch {
                                    isSearching = true
                                    // Reset selected books for new search
                                    selectedBooks = emptySet()
                                    searchResults = repository.search(
                                        query = searchQuery.text,
                                        limit = resultLimit.toInt()
                                    )
                                    isSearching = false
                                }
                            }
                        },
                        enabled = !isSearching && searchQuery.text.isNotBlank()
                    ) {
                        Icon(Icons.Default.Search, contentDescription = "Search")
                    }
                }

                Spacer(modifier = Modifier.height(8.dp))

                // Results limit slider
                Column(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalAlignment = Alignment.Start
                ) {
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.SpaceBetween
                    ) {
                        Text(
                            text = "מספר תוצאות להצגה:",
                            style = MaterialTheme.typography.bodyLarge
                        )
                        Text(
                            text = resultLimit.toInt().toString(),
                            style = MaterialTheme.typography.bodyLarge
                        )
                    }

                    Slider(
                        value = resultLimit,
                        onValueChange = { resultLimit = it },
                        valueRange = 1f..1000f,
                        steps = 999,
                        modifier = Modifier.fillMaxWidth()
                    )
                }

                Spacer(modifier = Modifier.height(8.dp))

                // Book filter chips
                if (uniqueBooks.isNotEmpty()) {
                    Column(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalAlignment = Alignment.Start
                    ) {
                        Text(
                            text = "סנן לפי ספר:",
                            style = MaterialTheme.typography.bodyLarge
                        )

                        Spacer(modifier = Modifier.height(4.dp))

                        // Grid layout for book filter chips
                        val scrollState = rememberScrollState()
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .heightIn(max = 200.dp)
                                .verticalScroll(scrollState)
                        ) {
                            // Chunk the books into rows of 4 (or adjust as needed)
                            val chunkedBooks = uniqueBooks.chunked(4)

                            chunkedBooks.forEach { rowBooks ->
                                Row(
                                    modifier = Modifier
                                        .fillMaxWidth()
                                        .padding(vertical = 2.dp),
                                    horizontalArrangement = Arrangement.spacedBy(4.dp)
                                ) {
                                    rowBooks.forEach { (bookId, bookTitle) ->
                                        val isSelected = bookId in selectedBooks
                                        OutlinedButton(
                                            onClick = {
                                                selectedBooks = if (isSelected) {
                                                    selectedBooks - bookId
                                                } else {
                                                    selectedBooks + bookId
                                                }
                                            },
                                            modifier = Modifier.weight(1f),
                                            border = BorderStroke(
                                                width = 1.dp,
                                                color = if (isSelected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurface.copy(alpha = 0.12f)
                                            )
                                        ) {
                                            Text(bookTitle, maxLines = 1)
                                        }
                                    }

                                    // Add empty spacers if the row is not full
                                    repeat(4 - rowBooks.size) {
                                        Spacer(modifier = Modifier.weight(1f))
                                    }
                                }
                            }
                        }
                    }

                    Spacer(modifier = Modifier.height(8.dp))
                }

                // Search results
                Box(modifier = Modifier.weight(1f)) {
                    if (isSearching) {
                        CircularProgressIndicator(
                            modifier = Modifier.align(Alignment.Center)
                        )
                    } else if (searchResults.isEmpty() && searchQuery.text.isNotBlank()) {
                        Text(
                            text = "לא נמצאו תוצאות",
                            modifier = Modifier.align(Alignment.Center)
                        )
                    } else {
                        // Filter results based on selected books
                        val filteredResults = if (selectedBooks.isEmpty()) {
                            searchResults
                        } else {
                            searchResults.filter { it.bookId in selectedBooks }
                        }

                        if (filteredResults.isEmpty() && searchResults.isNotEmpty()) {
                            Text(
                                text = "אין תוצאות עבור הספרים שנבחרו",
                                modifier = Modifier.align(Alignment.Center)
                            )
                        } else {
                            LazyColumn {
                                items(filteredResults) { result ->
                                    SearchResultItem(
                                        result = result,
                                        onClick = { onResultClick(result) }
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


/**
 * A composable that displays a single search result.
 *
 * @param result The search result to display
 * @param onClick Callback when the result is clicked
 */
@Composable
fun SearchResultItem(
    result: SearchResult,
    onClick: () -> Unit
) {
    ElevatedCard(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 4.dp)
            .clickable(onClick = onClick),
    ) {
        Column(
            modifier = Modifier.padding(16.dp)
        ) {
            Text(
                text = result.bookTitle,
                style = MaterialTheme.typography.titleMedium
            )

            Spacer(modifier = Modifier.height(4.dp))

            // Use RichText to render HTML snippet
            val richTextState = rememberRichTextState()

            // Set HTML content
            LaunchedEffect(result.snippet) {
                richTextState.setHtml(result.snippet)
            }

            // Render rich text
            RichText(
                state = richTextState,
                modifier = Modifier.fillMaxWidth()
            )

            Spacer(modifier = Modifier.height(4.dp))

            Text(
                text = "שורה: ${result.lineIndex + 1}",
                style = MaterialTheme.typography.bodySmall
            )
        }
    }
}
