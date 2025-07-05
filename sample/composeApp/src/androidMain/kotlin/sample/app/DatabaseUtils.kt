package sample.app

import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.driver.android.AndroidSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import android.content.Context
import androidx.compose.ui.platform.LocalContext
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
// Import the androidx.sqlite classes
import androidx.sqlite.*

// Android implementation of database path
@Composable
actual fun getDatabasePath(): String {
    // For Android, we'll use the app's internal storage
    return "otzaria.db"
}

// Android implementation of repository
@Composable
actual fun getRepository(): SeforimRepository {
    val dbPath = getDatabasePath()
    val context = LocalContext.current

    // Create a custom SqlDriver that uses BundledSQLiteDriver
    val driver: SqlDriver = remember {

        AndroidSqliteDriver(SeforimDb.Schema, context, dbPath)


    }

    return remember(driver) {
        SeforimRepository(dbPath, driver)
    }
}

// Empty implementation for Android - we don't show the database selection button on mobile
@Composable
actual fun DatabaseSelectionButtonIfAvailable() {
    // No-op on Android
}

