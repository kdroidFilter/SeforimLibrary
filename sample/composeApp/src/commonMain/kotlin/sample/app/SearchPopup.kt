package sample.app

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.key.Key
import androidx.compose.ui.input.key.key
import androidx.compose.ui.input.key.onKeyEvent
import androidx.compose.ui.text.input.TextFieldValue
import com.mohamedrejeb.richeditor.model.rememberRichTextState
import com.mohamedrejeb.richeditor.ui.material.RichText
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
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
    val coroutineScope = rememberCoroutineScope()

    Dialog(onDismissRequest = onDismiss) {
        Surface(
            modifier = Modifier
                .fillMaxWidth(0.9f)
                .fillMaxHeight(0.9f),
            elevation = 8.dp,
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
                        style = MaterialTheme.typography.h6
                    )
                    IconButton(onClick = onDismiss) {
                        Text("X")
                    }
                }

                Divider(modifier = Modifier.padding(vertical = 8.dp))

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

                    Button(
                        onClick = {
                            if (searchQuery.text.isNotBlank()) {
                                coroutineScope.launch {
                                    isSearching = true
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
                        Text("חפש")
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
                            style = MaterialTheme.typography.body2
                        )
                        Text(
                            text = resultLimit.toInt().toString(),
                            style = MaterialTheme.typography.body2
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
                        LazyColumn {
                            items(searchResults) { result ->
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
    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 4.dp)
            .clickable(onClick = onClick),
        elevation = 2.dp
    ) {
        Column(
            modifier = Modifier.padding(16.dp)
        ) {
            Text(
                text = result.bookTitle,
                style = MaterialTheme.typography.subtitle1
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
                style = MaterialTheme.typography.caption
            )
        }
    }
}
