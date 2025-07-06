package sample.app

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Search
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.key.Key
import androidx.compose.ui.input.key.key
import androidx.compose.ui.input.key.onKeyEvent
import androidx.compose.ui.text.font.FontWeight
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
 * Modern search dialog with filtering capabilities
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
    var resultLimit by remember { mutableStateOf(50f) }
    var unlimitedResults by remember { mutableStateOf(false) }
    var selectedBooks by remember { mutableStateOf(setOf<Long>()) }
    val scope = rememberCoroutineScope()

    // Extract unique books from results
    val uniqueBooks = remember(searchResults) {
        searchResults.map { it.bookId to it.bookTitle }.distinct()
    }

    // Filter results based on selected books
    val filteredResults = remember(searchResults, selectedBooks) {
        if (selectedBooks.isEmpty()) searchResults
        else searchResults.filter { it.bookId in selectedBooks }
    }

    fun performSearch() {
        if (searchQuery.text.isBlank()) return
        scope.launch {
            isSearching = true
            selectedBooks = emptySet()
            val limit = if (unlimitedResults) -1 else resultLimit.toInt()
            searchResults = repository.search(searchQuery.text, limit)
            isSearching = false
        }
    }

    Dialog(
        onDismissRequest = onDismiss,
        properties = DialogProperties(usePlatformDefaultWidth = false)
    ) {
        Surface(
            modifier = Modifier
                .fillMaxWidth(0.85f)
                .fillMaxHeight(0.9f),
            shape = RoundedCornerShape(24.dp),
            color = MaterialTheme.colorScheme.surface,
            shadowElevation = 8.dp
        ) {
            Column {
                // Modern header
                Surface(
                    modifier = Modifier.fillMaxWidth(),
                    color = MaterialTheme.colorScheme.surfaceVariant
                ) {
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 24.dp, vertical = 16.dp),
                        horizontalArrangement = Arrangement.SpaceBetween,
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = "חיפוש בספרייה", // Search Library
                            style = MaterialTheme.typography.headlineSmall,
                            fontWeight = FontWeight.Medium
                        )

                        IconButton(
                            onClick = onDismiss,
                            modifier = Modifier
                                .clip(RoundedCornerShape(12.dp))
                                .background(MaterialTheme.colorScheme.surface.copy(alpha = 0.5f))
                                .size(40.dp)
                        ) {
                            Icon(
                                Icons.Default.Close,
                                contentDescription = "Close",
                                modifier = Modifier.size(20.dp)
                            )
                        }
                    }
                }

                Column(
                    modifier = Modifier
                        .fillMaxSize()
                        .padding(24.dp)
                ) {
                    // Search bar with modern styling
                    OutlinedTextField(
                        value = searchQuery,
                        onValueChange = { searchQuery = it },
                        modifier = Modifier
                            .fillMaxWidth()
                            .onKeyEvent {
                                if (it.key == Key.Enter) {
                                    performSearch()
                                    true
                                } else false
                            },
                        placeholder = { Text("הזן טקסט לחיפוש...") }, // Enter search text...
                        leadingIcon = {
                            Icon(
                                Icons.Default.Search,
                                contentDescription = null,
                                tint = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        },
                        trailingIcon = {
                            if (searchQuery.text.isNotEmpty()) {
                                IconButton(
                                    onClick = { performSearch() },
                                    enabled = !isSearching
                                ) {
                                    if (isSearching) {
                                        CircularProgressIndicator(
                                            modifier = Modifier.size(20.dp),
                                            strokeWidth = 2.dp
                                        )
                                    } else {
                                        Icon(
                                            Icons.Default.Search,
                                            contentDescription = "Search",
                                            tint = MaterialTheme.colorScheme.primary
                                        )
                                    }
                                }
                            }
                        },
                        shape = RoundedCornerShape(16.dp),
                        singleLine = true,
                        colors = OutlinedTextFieldDefaults.colors(
                            focusedBorderColor = MaterialTheme.colorScheme.primary,
                            unfocusedBorderColor = MaterialTheme.colorScheme.outline.copy(alpha = 0.5f)
                        )
                    )

                    // Results limit control
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(vertical = 16.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = "תוצאות מקסימליות:", // Maximum results:
                            style = MaterialTheme.typography.bodyLarge,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )

                        Spacer(modifier = Modifier.weight(1f))

                        // Unlimited checkbox
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            modifier = Modifier.padding(end = 16.dp)
                        ) {
                            Checkbox(
                                checked = unlimitedResults,
                                onCheckedChange = { unlimitedResults = it }
                            )
                            Spacer(modifier = Modifier.width(4.dp))
                            Text(
                                text = "ללא הגבלה", // Unlimited
                                style = MaterialTheme.typography.bodyMedium
                            )
                        }

                        // Limit value display
                        Surface(
                            shape = RoundedCornerShape(8.dp),
                            color = if (unlimitedResults)
                                MaterialTheme.colorScheme.surfaceVariant
                            else
                                MaterialTheme.colorScheme.primaryContainer,
                            modifier = Modifier.padding(horizontal = 8.dp)
                        ) {
                            Text(
                                text = if (unlimitedResults) "∞" else resultLimit.toInt().toString(),
                                modifier = Modifier.padding(horizontal = 16.dp, vertical = 4.dp),
                                color = if (unlimitedResults)
                                    MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.5f)
                                else
                                    MaterialTheme.colorScheme.onPrimaryContainer,
                                fontWeight = FontWeight.Medium
                            )
                        }

                        // Slider (disabled when unlimited)
                        Slider(
                            value = resultLimit,
                            onValueChange = { resultLimit = it },
                            valueRange = 10f..1000f,
                            steps = 98,
                            modifier = Modifier.width(200.dp),
                            enabled = !unlimitedResults,
                            colors = SliderDefaults.colors(
                                thumbColor = MaterialTheme.colorScheme.primary,
                                activeTrackColor = MaterialTheme.colorScheme.primary,
                                disabledThumbColor = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.3f),
                                disabledActiveTrackColor = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.3f)
                            )
                        )
                    }

                    // Book filters as chips
                    if (uniqueBooks.isNotEmpty()) {
                        Column {
                            Text(
                                text = "סנן לפי ספר:", // Filter by book:
                                style = MaterialTheme.typography.titleMedium,
                                modifier = Modifier.padding(bottom = 8.dp)
                            )

                            LazyVerticalGrid(
                                columns = GridCells.Adaptive(minSize = 150.dp),
                                verticalArrangement = Arrangement.spacedBy(8.dp),
                                horizontalArrangement = Arrangement.spacedBy(8.dp),
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .height(120.dp)
                            ) {
                                items(uniqueBooks.size) { index ->
                                    val (bookId, bookTitle) = uniqueBooks[index]
                                    FilterChip(
                                        selected = bookId in selectedBooks,
                                        onClick = {
                                            selectedBooks = if (bookId in selectedBooks) {
                                                selectedBooks - bookId
                                            } else {
                                                selectedBooks + bookId
                                            }
                                        },
                                        label = {
                                            Text(
                                                bookTitle,
                                                maxLines = 1,
                                                style = MaterialTheme.typography.bodyMedium
                                            )
                                        },
                                        colors = FilterChipDefaults.filterChipColors(
                                            selectedContainerColor = MaterialTheme.colorScheme.primaryContainer
                                        )
                                    )
                                }
                            }
                        }

                        Spacer(modifier = Modifier.height(16.dp))
                    }

                    // Results area
                    Surface(
                        modifier = Modifier.fillMaxSize(),
                        shape = RoundedCornerShape(16.dp),
                        color = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.3f)
                    ) {
                        when {
                            isSearching -> {
                                Box(
                                    modifier = Modifier.fillMaxSize(),
                                    contentAlignment = Alignment.Center
                                ) {
                                    Column(
                                        horizontalAlignment = Alignment.CenterHorizontally,
                                        verticalArrangement = Arrangement.spacedBy(16.dp)
                                    ) {
                                        CircularProgressIndicator()
                                        Text(
                                            "מחפש...", // Searching...
                                            style = MaterialTheme.typography.bodyLarge
                                        )
                                        if (unlimitedResults) {
                                            Text(
                                                "חיפוש ללא הגבלה עשוי לקחת זמן רב", // Unlimited search may take a long time
                                                style = MaterialTheme.typography.bodySmall,
                                                color = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.7f)
                                            )
                                        }
                                    }
                                }
                            }

                            searchQuery.text.isBlank() -> {
                                Box(
                                    modifier = Modifier.fillMaxSize(),
                                    contentAlignment = Alignment.Center
                                ) {
                                    Text(
                                        "הזן טקסט כדי להתחיל חיפוש", // Enter text to start searching
                                        style = MaterialTheme.typography.bodyLarge,
                                        color = MaterialTheme.colorScheme.onSurfaceVariant
                                    )
                                }
                            }

                            filteredResults.isEmpty() -> {
                                Box(
                                    modifier = Modifier.fillMaxSize(),
                                    contentAlignment = Alignment.Center
                                ) {
                                    Column(
                                        horizontalAlignment = Alignment.CenterHorizontally,
                                        verticalArrangement = Arrangement.spacedBy(8.dp)
                                    ) {
                                        Text(
                                            "לא נמצאו תוצאות", // No results found
                                            style = MaterialTheme.typography.headlineSmall,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant
                                        )
                                        if (selectedBooks.isNotEmpty() && searchResults.isNotEmpty()) {
                                            Text(
                                                "נסה לשנות את הסינון", // Try changing the filter
                                                style = MaterialTheme.typography.bodyMedium,
                                                color = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.7f)
                                            )
                                        }
                                    }
                                }
                            }

                            else -> {
                                LazyColumn(
                                    contentPadding = PaddingValues(16.dp),
                                    verticalArrangement = Arrangement.spacedBy(12.dp)
                                ) {
                                    // Results count header
                                    item {
                                        Row(
                                            modifier = Modifier
                                                .fillMaxWidth()
                                                .padding(bottom = 8.dp),
                                            horizontalArrangement = Arrangement.SpaceBetween,
                                            verticalAlignment = Alignment.CenterVertically
                                        ) {
                                            Text(
                                                text = "${filteredResults.size} תוצאות נמצאו", // X results found
                                                style = MaterialTheme.typography.titleSmall,
                                                color = MaterialTheme.colorScheme.onSurfaceVariant
                                            )
                                            if (selectedBooks.isNotEmpty() && filteredResults.size != searchResults.size) {
                                                Text(
                                                    text = "(${searchResults.size} סה״כ)", // (X total)
                                                    style = MaterialTheme.typography.bodySmall,
                                                    color = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.7f)
                                                )
                                            }
                                        }
                                    }

                                    items(filteredResults) { result ->
                                        SearchResultCard(
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
}

/**
 * Modern search result card
 */
@Composable
private fun SearchResultCard(
    result: SearchResult,
    onClick: () -> Unit
) {
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick),
        shape = RoundedCornerShape(12.dp),
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surface
        ),
        elevation = CardDefaults.cardElevation(defaultElevation = 2.dp)
    ) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(16.dp)
        ) {
            // Book info and content
            Column(modifier = Modifier.weight(1f)) {
                Text(
                    text = result.bookTitle,
                    style = MaterialTheme.typography.titleMedium,
                    fontWeight = FontWeight.Medium,
                    color = MaterialTheme.colorScheme.primary
                )

                Spacer(modifier = Modifier.height(8.dp))

                // Rich text snippet
                val richTextState = rememberRichTextState()
                LaunchedEffect(result.snippet) {
                    richTextState.setHtml(result.snippet)
                }

                RichText(
                    state = richTextState,
                    modifier = Modifier.fillMaxWidth(),
                    style = MaterialTheme.typography.bodyMedium.copy(
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                )
            }

            // Line number badge
            Surface(
                modifier = Modifier
                    .align(Alignment.Top)
                    .padding(start = 16.dp),
                shape = RoundedCornerShape(8.dp),
                color = MaterialTheme.colorScheme.secondaryContainer
            ) {
                Text(
                    text = "שורה ${result.lineIndex + 1}", // Line X
                    modifier = Modifier.padding(horizontal = 12.dp, vertical = 6.dp),
                    style = MaterialTheme.typography.labelMedium,
                    color = MaterialTheme.colorScheme.onSecondaryContainer
                )
            }
        }
    }
}