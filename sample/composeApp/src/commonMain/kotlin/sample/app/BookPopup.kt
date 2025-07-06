package sample.app

import androidx.compose.foundation.layout.*
import androidx.compose.material.*
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentaryWithText
import io.github.kdroidfilter.seforimlibrary.dao.repository.CommentatorInfo

/**
 * A popup dialog that displays a book's content.
 *
 * @param book The book to display
 * @param lines The lines of the book to display
 * @param commentaries Commentaries for the book
 * @param commentators Available commentators for the book
 * @param onDismiss Callback when the popup is dismissed
 */
@Composable
fun BookPopup(
    book: Book,
    lines: List<Line>,
    commentaries: List<CommentaryWithText>,
    commentators: List<CommentatorInfo>,
    onDismiss: () -> Unit
) {
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
                        text = book.title,
                        style = MaterialTheme.typography.h6
                    )
                    IconButton(onClick = onDismiss) {
                        Icon(
                            imageVector = Icons.Default.Close,
                            contentDescription = "Close"
                        )
                    }
                }

                Divider(modifier = Modifier.padding(vertical = 8.dp))

                // Book content
                Box(modifier = Modifier.weight(1f)) {
                    BookContent(
                        lines = lines,
                        selectedLine = null
                    )
                }
            }
        }
    }
}
