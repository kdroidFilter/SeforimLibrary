package sample.app

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.*
import androidx.compose.material3.Divider
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Tab
import androidx.compose.material3.TabRow
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentaryWithText

@Composable
fun LineCommentsView(
    selectedLine: Line?,
    commentaries: List<CommentaryWithText>,
    onCommentClick: (CommentaryWithText) -> Unit = {}
) {
    Column(modifier = Modifier.fillMaxSize().padding(16.dp)) {
        // Header
        Text(
            text = "פירושי שורה",
            fontWeight = FontWeight.Bold,
            fontSize = 16.sp,
            modifier = Modifier.padding(bottom = 8.dp)
        )

        if (selectedLine == null) {
            // No line selected
            Box(
                modifier = Modifier.fillMaxSize(),
                contentAlignment = Alignment.Center
            ) {
                Text("בחר שורה כדי לצפות בפירושים שלה")
            }
        } else {
            // Filter commentaries for the selected line
            val lineCommentaries = commentaries.filter { it.link.sourceLineId == selectedLine.id }

            if (lineCommentaries.isEmpty()) {
                Box(
                    modifier = Modifier.fillMaxWidth().weight(1f),
                    contentAlignment = Alignment.Center
                ) {
                    Text("אין פירושים זמינים לשורה זו")
                }
            } else {
                Text(
                    text = "פירושים (${lineCommentaries.size}):",
                    fontWeight = FontWeight.Bold,
                    fontSize = 14.sp,
                    modifier = Modifier.padding(bottom = 8.dp)
                )

                // State for selected tab
                var selectedTabIndex by remember { mutableStateOf(0) }

                // Get all connection types present in the commentaries
                val connectionTypes = lineCommentaries
                    .map { it.link.connectionType }
                    .distinct()
                    .ifEmpty { listOf(ConnectionType.OTHER) } // Fallback if no types found

                // Add "ALL" as the first tab with Hebrew titles
                val connectionTypeNames = connectionTypes.map { 
                    when (it) {
                        ConnectionType.COMMENTARY -> "פירוש"
                        ConnectionType.TARGUM -> "תרגום"
                        ConnectionType.REFERENCE -> "הפניה"
                        ConnectionType.OTHER -> "אחר"
                    }
                }
                val tabTitles = listOf("הכל") + connectionTypeNames

                // Tab row
                TabRow(
                    selectedTabIndex = selectedTabIndex,
                    modifier = Modifier.fillMaxWidth()
                ) {
                    tabTitles.forEachIndexed { index, title ->
                        Tab(
                            selected = selectedTabIndex == index,
                            onClick = { selectedTabIndex = index },
                            text = { Text(title) }
                        )
                    }
                }

                Spacer(modifier = Modifier.height(8.dp))

                // Filter commentaries by selected connection type
                val filteredCommentaries = if (selectedTabIndex == 0) {
                    // "ALL" tab selected - show all commentaries
                    lineCommentaries
                } else {
                    // Filter by selected connection type
                    // Check if the index is within bounds
                    if (selectedTabIndex - 1 < connectionTypes.size) {
                        val selectedType = connectionTypes[selectedTabIndex - 1]
                        lineCommentaries.filter { it.link.connectionType == selectedType }
                    } else {
                        // Fallback to showing all commentaries if the index is out of bounds
                        lineCommentaries
                    }
                }

                // Group commentaries by book
                val commentariesByBook = filteredCommentaries.groupBy { it.targetBookTitle }

                LazyColumn(
                    modifier = Modifier.fillMaxWidth().weight(1f)
                ) {
                    commentariesByBook.forEach { (bookTitle, bookCommentaries) ->
                        item {
                            // Book header
                            Text(
                                text = bookTitle,
                                fontWeight = FontWeight.Bold,
                                fontSize = 16.sp,
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .background(MaterialTheme.colorScheme.secondary.copy(alpha = 0.2f))
                                    .padding(8.dp)
                            )
                        }

                        items(bookCommentaries) { commentary ->
                            Column(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clickable { onCommentClick(commentary) }
                                    .padding(vertical = 8.dp, horizontal = 16.dp)
                            ) {
                                Text(
                                    text = commentary.targetText,
                                    fontSize = 14.sp
                                )
                            }
                            HorizontalDivider(Modifier, 0.5.dp, MaterialTheme.colorScheme.onSurface.copy(alpha = 0.2f))
                        }
                    }
                }
            }
        }
    }
}
