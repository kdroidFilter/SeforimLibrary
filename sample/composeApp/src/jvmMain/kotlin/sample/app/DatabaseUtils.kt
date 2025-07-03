package sample.app

import androidx.compose.runtime.Composable
import androidx.compose.runtime.remember
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.io.File

@Composable
actual fun getDatabasePath(): String {
    // Use the database from the generator directory
    val dbFile = File("/Users/elie/IdeaProjects/SeforimLibrary/generator/otzaria.db")
    return dbFile.absolutePath
}

@Composable
actual fun getRepository(): SeforimRepository {
    val dbPath = getDatabasePath()
    val driver: SqlDriver = remember {
        // Use the SQLite driver for desktop
        JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    }

    return remember(driver) {
        SeforimRepository(dbPath, driver)
    }
}
