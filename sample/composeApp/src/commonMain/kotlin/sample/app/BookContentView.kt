package sample.app

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material.Divider
import androidx.compose.material.Tab
import androidx.compose.material.TabRow
import androidx.compose.material.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.mohamedrejeb.richeditor.model.rememberRichTextState
import com.mohamedrejeb.richeditor.ui.material.RichText
import com.mohamedrejeb.richeditor.ui.material3.RichTextEditor
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.TocEntry
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentaryWithText
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentatorInfo

@Composable
fun BookContentView(
    book: Book,
    lines: List<Line>,
    commentaries: List<CommentaryWithText>,
    commentators: List<CommentatorInfo>,
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
            items(lines) { line ->
                val isSelected = selectedLine?.id == line.id
                val state = rememberRichTextState()
                RichText(
                    state = state,
                    modifier = Modifier.fillMaxWidth() .clickable { onLineSelected(line) }
                        .background(if (isSelected) Color.LightGray.copy(alpha = 0.3f) else Color.Transparent)
                        .padding(vertical = 4.dp),
                )
                state.setHtml(line.content)
            }
        }
    }
}

@Composable
fun CommentariesView(
    commentaries: List<CommentaryWithText>,
    commentators: List<CommentatorInfo>
) {
    if (commentaries.isEmpty() && commentators.isEmpty()) {
        Box(
            modifier = Modifier.fillMaxSize(),
            contentAlignment = Alignment.Center
        ) {
            Text("אין פירושים זמינים")
        }
    } else {
        Column(modifier = Modifier.fillMaxSize().padding(16.dp)) {
            // Commentators info
            if (commentators.isNotEmpty()) {
                Text(
                    text = "פרשנים זמינים:",
                    fontWeight = FontWeight.Bold,
                    modifier = Modifier.padding(bottom = 8.dp)
                )

                commentators.forEach { commentator ->
                    Row(
                        modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(
                            text = "📚",
                            modifier = Modifier.width(24.dp)
                        )
                        Column {
                            Text(
                                text = commentator.title,
                                fontWeight = FontWeight.Bold
                            )
                            if (commentator.author != null) {
                                Text(
                                    text = "מאת ${commentator.author}",
                                    fontSize = 12.sp,
                                    color = Color.Gray
                                )
                            }
                            Text(
                                text = "${commentator.linkCount} פירושים",
                                fontSize = 12.sp,
                                color = Color.Gray
                            )
                        }
                    }
                    Divider(color = Color.LightGray, thickness = 0.5.dp)
                }
            }

            // Commentaries
            if (commentaries.isNotEmpty()) {
                Text(
                    text = "פירושים:",
                    fontWeight = FontWeight.Bold,
                    modifier = Modifier.padding(top = 16.dp, bottom = 8.dp)
                )

                LazyColumn {
                    items(commentaries) { commentary ->
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(vertical = 8.dp)
                        ) {
                            Text(
                                text = commentary.targetBookTitle,
                                fontWeight = FontWeight.Bold
                            )
                            Text(
                                text = commentary.targetText,
                                modifier = Modifier.padding(top = 4.dp)
                            )
                        }
                        Divider(color = Color.LightGray, thickness = 0.5.dp)
                    }
                }
            }
        }
    }
}

@Composable
fun BookInfoView(book: Book) {
    Column(
        modifier = Modifier.fillMaxSize().padding(16.dp)
    ) {
        // Authors
        if (book.authors.isNotEmpty()) {
            Text(
                text = "מחברים:",
                fontWeight = FontWeight.Bold,
                modifier = Modifier.padding(bottom = 4.dp)
            )
            book.authors.forEach { author ->
                Text(
                    text = "• ${author.name}",
                    modifier = Modifier.padding(start = 16.dp, bottom = 2.dp)
                )
            }
            Spacer(modifier = Modifier.height(16.dp))
        }

        // Topics
        if (book.topics.isNotEmpty()) {
            Text(
                text = "נושאים:",
                fontWeight = FontWeight.Bold,
                modifier = Modifier.padding(bottom = 4.dp)
            )
            book.topics.forEach { topic ->
                Text(
                    text = "• ${topic.name}",
                    modifier = Modifier.padding(start = 16.dp, bottom = 2.dp)
                )
            }
            Spacer(modifier = Modifier.height(16.dp))
        }

        // Publication info
        if (book.pubPlaces.isNotEmpty() || book.pubDates.isNotEmpty()) {
            Text(
                text = "מידע על הפרסום:",
                fontWeight = FontWeight.Bold,
                modifier = Modifier.padding(bottom = 4.dp)
            )

            book.pubPlaces.forEach { place ->
                Text(
                    text = "• מקום: ${place.name}",
                    modifier = Modifier.padding(start = 16.dp, bottom = 2.dp)
                )
            }

            book.pubDates.forEach { date ->
                Text(
                    text = "• תאריך: ${date.date}",
                    modifier = Modifier.padding(start = 16.dp, bottom = 2.dp)
                )
            }

            Spacer(modifier = Modifier.height(16.dp))
        }

        // Description
        book.heShortDesc?.let { desc ->
            Text(
                text = "תיאור:",
                fontWeight = FontWeight.Bold,
                modifier = Modifier.padding(bottom = 4.dp)
            )
            Text(
                text = desc,
                modifier = Modifier.padding(start = 16.dp)
            )
            Spacer(modifier = Modifier.height(16.dp))
        }

        // Stats
        Text(
            text = "סטטיסטיקה:",
            fontWeight = FontWeight.Bold,
            modifier = Modifier.padding(bottom = 4.dp)
        )
        Text(
            text = "• סך הכל שורות: ${book.totalLines}",
            modifier = Modifier.padding(start = 16.dp, bottom = 2.dp)
        )
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
                Text(
                    text = if (isExpanded) "▼" else "▶",
                    color = Color.Gray,
                    modifier = Modifier
                        .width(24.dp)
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
