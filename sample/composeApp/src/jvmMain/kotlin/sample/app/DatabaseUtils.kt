package sample.app

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.material.AlertDialog
import androidx.compose.material.Button
import androidx.compose.material.Icon
import androidx.compose.material.IconButton
import androidx.compose.material.Text
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.FolderOpen
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.vinceglb.filekit.FileKit
import io.github.vinceglb.filekit.dialogs.FileKitType
import io.github.vinceglb.filekit.dialogs.openFilePicker
import io.github.vinceglb.filekit.path
import kotlinx.coroutines.launch
import java.io.File
import java.util.prefs.Preferences

// Preferences key for storing the database path
private const val PREF_DB_PATH = "database_path"

// Get preferences for storing the database path
private val prefs = Preferences.userNodeForPackage(SeforimRepository::class.java)

// Composable to show database selection dialog
@Composable
fun DatabaseSelectionDialog(
    currentPath: String,
    onPathSelected: (String) -> Unit,
    onDismiss: () -> Unit
) {
    val coroutineScope = rememberCoroutineScope()

    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text("Select Database File") },
        text = { 
            Column {
                Text("Please select the database file for the application.")
                Spacer(modifier = Modifier.height(8.dp))
                Text("Current path: ${if (currentPath.isEmpty()) "Not set" else currentPath}")
            }
        },
        buttons = {
            Row(
                modifier = Modifier.fillMaxWidth().padding(8.dp),
                horizontalArrangement = Arrangement.End
            ) {
                Button(
                    onClick = {
                        // Use FileKit to select a database file
                        coroutineScope.launch {
                            val file = FileKit.openFilePicker() // Use default type to allow all files
                            if (file != null) {
                                onPathSelected(file.path)
                            }
                            onDismiss()
                        }
                    }
                ) {
                    Text("Select File")
                }

                Button(
                    onClick = onDismiss,
                    modifier = Modifier.padding(start = 8.dp)
                ) {
                    Text("Cancel")
                }
            }
        }
    )
}

// Composable to show database selection button
@Composable
fun DatabaseSelectionButton(onClick: () -> Unit) {
    IconButton(onClick = onClick) {
        Icon(Icons.Default.FolderOpen, contentDescription = "Select Database File")
    }
}

// Global state for dialog visibility
private val showDatabaseDialogState = mutableStateOf(false)

// Function to show the database selection dialog
fun showDatabaseSelectionDialog() {
    showDatabaseDialogState.value = true
}

@Composable
actual fun getDatabasePath(): String {
    // State to hold the database path
    var dbPath by remember { mutableStateOf(prefs.get(PREF_DB_PATH, "")) }

    // State to control the dialog visibility
    var showDialog by remember { showDatabaseDialogState }

    // Show dialog if path is not set
    LaunchedEffect(Unit) {
        if (dbPath.isEmpty() || !File(dbPath).exists()) {
            showDialog = true
        }
    }

    // Dialog to select database file
    if (showDialog) {
        DatabaseSelectionDialog(
            currentPath = dbPath,
            onPathSelected = { newPath ->
                dbPath = newPath
                // Save the path to preferences
                prefs.put(PREF_DB_PATH, dbPath)
            },
            onDismiss = { showDialog = false }
        )
    }

    return dbPath
}

@Composable
actual fun getRepository(): SeforimRepository {
    val dbPath = getDatabasePath()
    val driver: SqlDriver = remember(dbPath) {
        // Use the SQLite driver for desktop
        JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    }

    return remember(driver) {
        SeforimRepository(dbPath, driver)
    }
}

@Composable
actual fun DatabaseSelectionButtonIfAvailable() {
    DatabaseSelectionButton(onClick = { showDatabaseSelectionDialog() })
}
