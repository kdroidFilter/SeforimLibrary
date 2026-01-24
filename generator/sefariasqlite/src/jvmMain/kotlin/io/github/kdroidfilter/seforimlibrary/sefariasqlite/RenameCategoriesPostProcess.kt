package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Paths
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Post-processing step to rename categories in the database.
 * This runs after all books and links are generated, so it doesn't affect
 * any path-based matching logic.
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :sefariasqlite:renameCategories -PseforimDb=/path/to/seforim.db
 *
 * Env alternatives:
 *   SEFORIM_DB
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("RenameCategories")

    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val dbPath = Paths.get(dbPathStr)

    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }

    logger.i { "Renaming categories in $dbPath" }

    // Category renames: old name -> new name
    val categoryRenames = mapOf(
        "פירושים מודרניים על התנ״ך" to "מחברי זמננו",
        "פירושים מודרניים על התלמוד" to "מחברי זמננו",
        "פירושים מודרניים על המשנה" to "מחברי זמננו"
    )

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            val updateStmt = conn.prepareStatement(
                "UPDATE category SET title = ? WHERE title = ?"
            )

            var totalUpdated = 0
            for ((oldName, newName) in categoryRenames) {
                updateStmt.setString(1, newName)
                updateStmt.setString(2, oldName)
                val updated = updateStmt.executeUpdate()
                if (updated > 0) {
                    logger.i { "Renamed '$oldName' -> '$newName' ($updated rows)" }
                    totalUpdated += updated
                }
            }

            conn.commit()
            logger.i { "Category rename complete. Total categories updated: $totalUpdated" }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to rename categories" }
        exitProcess(1)
    }
}
