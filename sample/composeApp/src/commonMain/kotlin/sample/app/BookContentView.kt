package sample.app

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.KeyboardArrowRight
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.KeyboardArrowRight
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.mohamedrejeb.richeditor.model.rememberRichTextState
import com.mohamedrejeb.richeditor.ui.material.RichText
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentaryWithText
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentatorInfo
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import sample.app.getRepository
import sample.app.LineWithUniqueKey
import sample.app.withUniqueKeys

@Composable
fun BookContentView(
    book: Book,
    lines: List<Line>,
    selectedLine: Line? = null,
    onLineSelected: (Line) -> Unit = {}
) {
    Column(modifier = Modifier.fillMaxSize()) {
        // Book title
        Text(
            text = book.title,
            fontWeight = FontWeight.Bold,
            fontSize = 20.sp,
            modifier = Modifier.padding(16.dp)
        )

        // Book content
        Box(modifier = Modifier.weight(1f)) {
            BookContent(
                lines = lines,
                selectedLine = selectedLine,
                onLineSelected = onLineSelected
            )
        }
    }
}

@Composable
fun BookContent(
    lines: List<Line>,
    selectedLine: Line? = null,
    onLineSelected: (Line) -> Unit = {}
) {
    if (lines.isEmpty()) {
        Box(
            modifier = Modifier.fillMaxSize(),
            contentAlignment = Alignment.Center
        ) {
            Text("אין תוכן זמין")
        }
    } else {
        // Create a LazyListState to control scrolling
        val listState = rememberLazyListState()

        // Keep track of the currently loaded lines
        val book = remember { lines.firstOrNull() }
        val bookId = remember { book?.bookId ?: 0L }

        // Define the window size and offset for pagination
        val windowSize = 50 // Number of lines to display at once
        val loadOffset = 15 // Load more lines when this close to the edge

        // State to track the current window of lines
        var currentLines by remember { mutableStateOf(lines) }
        var startIndex by remember { mutableStateOf(lines.firstOrNull()?.lineIndex ?: 0) }
        var endIndex by remember { mutableStateOf((lines.lastOrNull()?.lineIndex ?: 0) + 1) }

        // Convert lines to LineWithUniqueKey objects
        var currentLinesWithKeys by remember { mutableStateOf(currentLines.withUniqueKeys()) }

        // Repository to load more lines
        val repository = getRepository()

        // Find the index of the selected line in the list
        val selectedIndex = selectedLine?.let { selected ->
            currentLines.indexOfFirst { it.id == selected.id }
        } ?: 0

        // Scroll to the selected line when it changes
        LaunchedEffect(selectedLine) {
            if (selectedLine != null) {
                // Check if the selected line is in the current window
                val selectedIndex = currentLines.indexOfFirst { it.id == selectedLine.id }

                if (selectedIndex >= 0) {
                    // If the line is already in our current window, just scroll to it
                    listState.animateScrollToItem(selectedIndex)
                } else {
                    // If the line is not in our current window, we need to load it
                    // Load a window of lines centered around the selected line
                    val lineIndex = selectedLine.lineIndex
                    val newStartIndex = maxOf(0, lineIndex - windowSize / 2)
                    val newEndIndex = lineIndex + windowSize / 2

                    // Load the lines from the repository
                    val newLines = repository.getLines(bookId, newStartIndex, newEndIndex)
                    if (newLines.isNotEmpty()) {
                        // Update our current window
                        currentLines = newLines
                        startIndex = newStartIndex
                        endIndex = newEndIndex
                        // Update lines with unique keys
                        currentLinesWithKeys = currentLines.withUniqueKeys()

                        // Now find the index of the selected line in the new window and scroll to it
                        val newSelectedIndex = currentLines.indexOfFirst { it.id == selectedLine.id }
                        if (newSelectedIndex >= 0) {
                            listState.scrollToItem(newSelectedIndex)
                        }
                    }
                }
            }
        }

        // Load more lines when approaching the edges
        LaunchedEffect(listState.firstVisibleItemIndex) {
            // If we're close to the top, load more lines above
            if (listState.firstVisibleItemIndex < loadOffset && startIndex > 0) {
                val newStartIndex = maxOf(0, startIndex - windowSize / 2)
                val newLines = repository.getLines(bookId, newStartIndex, startIndex)
                if (newLines.isNotEmpty()) {
                    currentLines = newLines + currentLines
                    startIndex = newStartIndex
                    // Update lines with unique keys
                    currentLinesWithKeys = currentLines.withUniqueKeys()
                }
            }
        }

        LaunchedEffect(listState.layoutInfo.visibleItemsInfo.lastOrNull()?.index) {
            // If we're close to the bottom, load more lines below
            val lastVisibleIndex = listState.layoutInfo.visibleItemsInfo.lastOrNull()?.index ?: 0
            if (lastVisibleIndex >= currentLines.size - loadOffset) {
                val newEndIndex = endIndex + windowSize / 2
                val newLines = repository.getLines(bookId, endIndex, newEndIndex)
                if (newLines.isNotEmpty()) {
                    currentLines = currentLines + newLines
                    endIndex = newEndIndex
                    // Update lines with unique keys
                    currentLinesWithKeys = currentLines.withUniqueKeys()
                }
            }
        }

        // Limit the number of lines in memory to maintain efficiency
        LaunchedEffect(currentLines.size) {
            if (currentLines.size > windowSize * 3) {
                // If we have too many lines, trim from the opposite end of where we're viewing
                if (listState.firstVisibleItemIndex < currentLines.size / 2) {
                    // We're closer to the top, trim from the bottom
                    currentLines = currentLines.take(windowSize * 2)
                    endIndex = (currentLines.lastOrNull()?.lineIndex ?: 0) + 1
                } else {
                    // We're closer to the bottom, trim from the top
                    currentLines = currentLines.takeLast(windowSize * 2)
                    startIndex = currentLines.firstOrNull()?.lineIndex ?: 0
                }
                // Update lines with unique keys
                currentLinesWithKeys = currentLines.withUniqueKeys()
            }
        }

        LazyColumn(
            state = listState,
            modifier = Modifier.fillMaxSize().padding(16.dp)
        ) {
            items(
                items = currentLinesWithKeys,
                key = { it.uniqueKey } // Use the unique key for stable identity
            ) { lineWithKey ->
                val line = lineWithKey.line
                val isSelected = selectedLine?.id == line.id
                val state = rememberRichTextState()

                // Use LaunchedEffect to set HTML content only when line changes
                LaunchedEffect(line.id) {
                    state.setHtml(line.content)
                }

                RichText(
                    state = state,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable { onLineSelected(line) }
                        .background(if (isSelected) MaterialTheme.colorScheme.primary.copy(alpha = 0.1f) else Color.Transparent)
                        .padding(vertical = 4.dp),
                )
            }
        }
    }
}

@Composable
fun TocView(
    tocEntries: List<TocEntry>,
    expandedEntries: Set<Long>,
    childrenMap: Map<Long, List<TocEntry>>,
    onEntryClick: (TocEntry) -> Unit,
    onEntryExpand: (TocEntry) -> Unit
) {
    if (tocEntries.isEmpty()) {
        Box(
            modifier = Modifier.fillMaxSize(),
            contentAlignment = Alignment.Center
        ) {
            Text("אין תוכן עניינים זמין")
        }
    } else {
        LazyColumn(
            modifier = Modifier.fillMaxSize().padding(16.dp)
        ) {
            items(tocEntries) { entry ->
                TocEntryItem(
                    entry = entry,
                    level = 0,
                    isExpanded = expandedEntries.contains(entry.id),
                    childEntries = childrenMap[entry.id] ?: emptyList(),
                    expandedEntries = expandedEntries,
                    childrenMap = childrenMap,
                    onEntryClick = onEntryClick,
                    onEntryExpand = onEntryExpand
                )
            }
        }
    }
}

@Composable
fun TocEntryItem(
    entry: TocEntry,
    level: Int,
    isExpanded: Boolean,
    childEntries: List<TocEntry>,
    expandedEntries: Set<Long>,
    childrenMap: Map<Long, List<TocEntry>>,
    onEntryClick: (TocEntry) -> Unit,
    onEntryExpand: (TocEntry) -> Unit
) {
    Column {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .clickable { onEntryClick(entry) }
                .padding(start = (level * 16).dp, top = 4.dp, bottom = 4.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            // Expand/collapse icon if entry has children
            if (childEntries.isNotEmpty()) {
                Icon(
                    imageVector = if (isExpanded) Icons.Default.KeyboardArrowDown else Icons.AutoMirrored.Filled.KeyboardArrowRight,
                    contentDescription = if (isExpanded) "Collapse" else "Expand",
                    tint = MaterialTheme.colorScheme.onSurface.copy(alpha = 0.6f),
                    modifier = Modifier
                        .size(24.dp)
                        .clickable { onEntryExpand(entry) }
                )
            } else {
                Spacer(modifier = Modifier.width(24.dp))
            }

            // Entry text
            Text(
                text = entry.text,
                fontWeight = FontWeight.Normal,
                fontSize = 14.sp
            )
        }

        // Show children if expanded
        if (isExpanded && childEntries.isNotEmpty()) {
            childEntries.forEach { childEntry ->
                TocEntryItem(
                    entry = childEntry,
                    level = level + 1,
                    isExpanded = expandedEntries.contains(childEntry.id),
                    childEntries = childrenMap[childEntry.id] ?: emptyList(),
                    expandedEntries = expandedEntries,
                    childrenMap = childrenMap,
                    onEntryClick = onEntryClick,
                    onEntryExpand = onEntryExpand
                )
            }
        }
    }
}
