package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Post-processing step to rename and merge categories in the database.
 * This runs after Sefaria import but before Otzaria, so categories are unified
 * before additional books are added.
 *
 * Handles two cases:
 * 1. Simple rename: When no target category exists under the same parent
 * 2. Merge: When a target category already exists, books are moved and source is deleted
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

    logger.i { "Renaming/merging categories in $dbPath" }

    // Category renames: old name -> new name
    // If a category with the new name already exists under the same parent,
    // books will be moved and the old category deleted (merge).
    val categoryRenames = mapOf(
        // Modern commentaries -> מחברי זמננו
        "פירושים מודרניים על התנ״ך" to "מחברי זמננו",
        "פירושים מודרניים על התלמוד" to "מחברי זמננו",
        "פירושים מודרניים על המשנה" to "מחברי זמננו",
        // Sefaria-specific categories to Otzaria-style
        "ראשונים על התנ״ך" to "ראשונים",
        "אחרונים על התנ״ך" to "אחרונים",
        "ראשונים על התלמוד" to "ראשונים",
        "אחרונים על התלמוד" to "אחרונים",
        "ראשונים על המשנה" to "ראשונים",
        "אחרונים על המשנה" to "אחרונים"
    )

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            var totalRenamed = 0
            var totalMerged = 0

            for ((oldName, newName) in categoryRenames) {
                val result = renameOrMergeCategory(conn, oldName, newName, logger)
                when (result) {
                    is RenameResult.Renamed -> totalRenamed += result.count
                    is RenameResult.Merged -> totalMerged += result.booksMoved
                    is RenameResult.NotFound -> { /* skip */ }
                }
            }

            conn.commit()
            logger.i { "Category processing complete. Renamed: $totalRenamed, Merged: $totalMerged books" }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to process categories" }
        exitProcess(1)
    }
}

private sealed class RenameResult {
    data class Renamed(val count: Int) : RenameResult()
    data class Merged(val booksMoved: Int) : RenameResult()
    data object NotFound : RenameResult()
}

/**
 * Renames a category or merges it into an existing category with the target name.
 *
 * @return The result of the operation
 */
private fun renameOrMergeCategory(
    conn: Connection,
    oldName: String,
    newName: String,
    logger: Logger
): RenameResult {
    // Find all source categories with oldName
    val sourceCats = mutableListOf<Pair<Long, Long?>>() // (id, parentId)
    conn.prepareStatement("SELECT id, parentId FROM category WHERE title = ?").use { stmt ->
        stmt.setString(1, oldName)
        stmt.executeQuery().use { rs ->
            while (rs.next()) {
                val id = rs.getLong(1)
                val parentId = rs.getObject(2) as? Long
                sourceCats.add(id to parentId)
            }
        }
    }

    if (sourceCats.isEmpty()) {
        return RenameResult.NotFound
    }

    var totalRenamed = 0
    var totalBooksMoved = 0

    for ((sourceId, parentId) in sourceCats) {
        // Check if a target category with newName exists under the same parent
        val targetId = findCategoryByNameAndParent(conn, newName, parentId)

        if (targetId != null && targetId != sourceId) {
            // Merge: move books from source to target, then delete source
            val booksMoved = moveBooksToCategory(conn, sourceId, targetId)
            val subCatsMoved = moveSubcategoriesToParent(conn, sourceId, targetId)
            deleteCategory(conn, sourceId)
            logger.i { "Merged '$oldName' (id=$sourceId) into '$newName' (id=$targetId): $booksMoved books, $subCatsMoved subcategories" }
            totalBooksMoved += booksMoved
        } else {
            // Simple rename
            conn.prepareStatement("UPDATE category SET title = ? WHERE id = ?").use { stmt ->
                stmt.setString(1, newName)
                stmt.setLong(2, sourceId)
                stmt.executeUpdate()
            }
            logger.i { "Renamed '$oldName' (id=$sourceId) -> '$newName'" }
            totalRenamed++
        }
    }

    return if (totalBooksMoved > 0) {
        RenameResult.Merged(totalBooksMoved)
    } else {
        RenameResult.Renamed(totalRenamed)
    }
}

private fun findCategoryByNameAndParent(conn: Connection, name: String, parentId: Long?): Long? {
    val sql = if (parentId != null) {
        "SELECT id FROM category WHERE title = ? AND parentId = ?"
    } else {
        "SELECT id FROM category WHERE title = ? AND parentId IS NULL"
    }
    conn.prepareStatement(sql).use { stmt ->
        stmt.setString(1, name)
        if (parentId != null) {
            stmt.setLong(2, parentId)
        }
        stmt.executeQuery().use { rs ->
            return if (rs.next()) rs.getLong(1) else null
        }
    }
}

private fun moveBooksToCategory(conn: Connection, fromCategoryId: Long, toCategoryId: Long): Int {
    conn.prepareStatement("UPDATE book SET categoryId = ? WHERE categoryId = ?").use { stmt ->
        stmt.setLong(1, toCategoryId)
        stmt.setLong(2, fromCategoryId)
        return stmt.executeUpdate()
    }
}

private fun moveSubcategoriesToParent(conn: Connection, fromCategoryId: Long, toParentId: Long): Int {
    conn.prepareStatement("UPDATE category SET parentId = ? WHERE parentId = ?").use { stmt ->
        stmt.setLong(1, toParentId)
        stmt.setLong(2, fromCategoryId)
        return stmt.executeUpdate()
    }
}

private fun deleteCategory(conn: Connection, categoryId: Long) {
    conn.prepareStatement("DELETE FROM category WHERE id = ?").use { stmt ->
        stmt.setLong(1, categoryId)
        stmt.executeUpdate()
    }
}
