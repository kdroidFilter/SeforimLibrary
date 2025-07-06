package sample.app

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material.Icon
import androidx.compose.material.MaterialTheme
import androidx.compose.material.Text
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.KeyboardArrowRight
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.KeyboardArrowRight
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

        // Find the index of the selected line in the list
        val selectedIndex = selectedLine?.let { selected ->
            lines.indexOfFirst { it.id == selected.id }
        } ?: 0

        // Scroll to the selected line when it changes
        LaunchedEffect(selectedLine) {
            if (selectedLine != null && selectedIndex >= 0) {
                listState.animateScrollToItem(selectedIndex)
            }
        }

        LazyColumn(
            state = listState,
            modifier = Modifier.fillMaxSize().padding(16.dp)
        ) {
            items(
                items = lines,
                key = { it.id } // Use line.id as key for stable identity
            ) { line ->
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
                        .background(if (isSelected) MaterialTheme.colors.primary.copy(alpha = 0.1f) else Color.Transparent)
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
                    tint = MaterialTheme.colors.onSurface.copy(alpha = 0.6f),
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
