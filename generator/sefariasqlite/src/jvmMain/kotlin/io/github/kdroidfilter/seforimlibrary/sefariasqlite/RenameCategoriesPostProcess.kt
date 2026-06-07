package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.util.zip.ZipInputStream
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Post-processing step to rename and merge categories, rename books, and
 * relocate books between categories. Runs after Sefaria import but before
 * Otzaria, so naming is unified before additional books are added.
 *
 * Rules are downloaded from otzaria-library/ForDB/ on GitHub (UTF-8):
 *  - תיקיות.csv      `old,new` — category renames (exact match, prefix fallback)
 *  - ספרים.csv       `old,new` — book title renames (exact match)
 *  - Moving files.csv `name,sourcePath,destPath` (simple CSV; embedded newlines in quoted fields are not supported)
 *
 * Rules are fetched from the fixed `fordb-latest` GitHub release asset.
 * Download failures are fatal because silently skipping these rules can produce
 * an invalid DB delta.
 *
 * Category renames handle two cases:
 * 1. Simple rename: When no target category exists under the same parent
 * 2. Merge: When a target category already exists, books are moved and source is deleted
 *
 * Book moves require the destination category path to already exist; missing
 * destinations are skipped with a warning (no auto-creation).
 *
 * Order of operations: category renames → book renames → book moves. Paths in
 * Moving files.csv must therefore reference the POST-rename category names.
 * If a move references a pre-rename destPath, resolveCategoryPath returns null
 * and the move is silently skipped with a "destination not found" warning.
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :sefariasqlite:renameCategories -PseforimDb=/path/to/seforim.db
 *
 * Env alternatives:
 *   SEFORIM_DB
 */
private const val FOR_DB_RELEASE_API =
    "https://api.github.com/repos/Otzaria/otzaria-library/releases/tags/fordb-latest"
private const val FOR_DB_ARCHIVE_NAME = "fordb_latest.zip"
private const val FOR_DB_USER_AGENT = "SeforimLibrary-ForDBFetcher/1.0"
internal val FOR_DB_CSV_FILES = mapOf(
    "categoryRenames" to "תיקיות.csv",
    "bookRenames" to "ספרים.csv",
    "bookMoves" to "Moving files.csv",
    "generations" to "סדר הדורות.csv",
)

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

    // Rules downloaded from otzaria-library/ForDB/ at startup.
    // תיקיות.csv and ספרים.csv have no header; Moving files.csv has one.
    val categoryRenames: List<Pair<String, String>> =
        parsePairs(downloadRequiredForDbCsv(FOR_DB_CSV_FILES.getValue("categoryRenames"), logger))
    val bookRenames: List<Pair<String, String>> =
        parsePairs(downloadRequiredForDbCsv(FOR_DB_CSV_FILES.getValue("bookRenames"), logger))
    val bookMoves: List<BookMove> =
        parseBookMoves(downloadRequiredForDbCsv(FOR_DB_CSV_FILES.getValue("bookMoves"), logger), logger)

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            var totalRenamed = 0
            var totalMerged = 0
            val categoryResult = runSection("Category renames", categoryRenames, logger) { (oldName, newName) ->
                val result = renameOrMergeCategory(conn, oldName, newName, logger)
                when (result) {
                    is RenameResult.Renamed -> totalRenamed += result.count
                    is RenameResult.Merged -> totalMerged += result.booksMoved
                    is RenameResult.NotFound -> logger.w { "Category rename: '$oldName' not found; skipping" }
                }
                result.rows()
            }

            val bookRenameResult = runSection("Book renames", bookRenames, logger) { (oldTitle, newTitle) ->
                renameBookTitle(conn, oldTitle, newTitle, logger)
            }

            val moveResult = runSection("Book moves", bookMoves, logger) { move ->
                if (applyBookMove(conn, move, logger)) 1 else 0
            }

            conn.commit()
            logger.i {
                "Post-process done: categories renamed=$totalRenamed merged=$totalMerged " +
                    "(failures=${categoryResult.failures}); books renamed=${bookRenameResult.applied} " +
                    "(failures=${bookRenameResult.failures}); books moved=${moveResult.applied} " +
                    "(failures=${moveResult.failures})"
            }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to open or commit DB; aborting" }
        exitProcess(1)
    }
}

/** `old,new` rows — skips blanks and malformed lines. */
internal fun parsePairs(lines: List<String>): List<Pair<String, String>> = lines.mapNotNull { line ->
    val f = parseCsvLine(line).map { it.trim() }
    if (f.size >= 2 && f[0].isNotEmpty() && f[1].isNotEmpty()) f[0] to f[1] else null
}

/**
 * `name,Source path,Destination path` rows. Drops the header row if present;
 * detection requires both `source path` and `destination path` tokens so a
 * stray book title containing the word "path" can't be mistaken for a header.
 * If the first line doesn't look like a header, treats it as data and logs a
 * warning so a header-less upload doesn't silently lose row 0.
 */
private fun parseBookMoves(lines: List<String>, logger: Logger): List<BookMove> {
    val firstLower = lines.firstOrNull()?.lowercase()
    val isHeader = firstLower != null && "source path" in firstLower && "destination path" in firstLower
    val body = if (isHeader) {
        lines.drop(1)
    } else {
        if (lines.isNotEmpty()) logger.w { "Moving files.csv: no header detected; treating first row as data" }
        lines
    }
    return body.mapNotNull { line ->
        val f = parseCsvLine(line).map { it.trim() }
        if (f.size >= 3 && f[0].isNotEmpty() && f[2].isNotEmpty()) BookMove(f[0], f[1], f[2]) else null
    }
}

private data class SectionResult(val applied: Int, val failures: Int)

private fun <T> runSection(name: String, items: List<T>, logger: Logger, apply: (T) -> Int): SectionResult {
    var applied = 0
    var failures = 0
    for (item in items) {
        try {
            applied += apply(item)
        } catch (e: Exception) {
            failures++
            logger.w(e) { "$name failed for '$item'; skipping" }
        }
    }
    logger.i { "$name: applied=$applied failures=$failures" }
    return SectionResult(applied, failures)
}

private sealed class RenameResult {
    data class Renamed(val count: Int) : RenameResult()
    data class Merged(val booksMoved: Int) : RenameResult()
    data object NotFound : RenameResult()

    fun rows(): Int = when (this) {
        is Renamed -> count
        is Merged -> booksMoved
        is NotFound -> 0
    }
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
    // Find all source categories with oldName (exact match, falling back to prefix)
    val sourceCats = findSourceCategories(conn, oldName)

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

/**
 * Exact match first; only if nothing matches exactly, fall back to prefix match.
 * This preserves the safer "literal" interpretation for the common case while
 * still allowing rules like "ראשונים על" to sweep "ראשונים על התלמוד",
 * "ראשונים על המשנה", etc.
 */
private fun findSourceCategories(conn: Connection, pattern: String): List<Pair<Long, Long?>> {
    fun query(sql: String, param: String): List<Pair<Long, Long?>> {
        val rows = mutableListOf<Pair<Long, Long?>>()
        conn.prepareStatement(sql).use { stmt ->
            stmt.setString(1, param)
            stmt.executeQuery().use { rs ->
                while (rs.next()) {
                    val parent = rs.getLong(2).let { if (rs.wasNull()) null else it }
                    rows.add(rs.getLong(1) to parent)
                }
            }
        }
        return rows
    }

    val exact = query("SELECT id, parentId FROM category WHERE title = ?", pattern)
    if (exact.isNotEmpty()) return exact

    val likePattern = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    return query("SELECT id, parentId FROM category WHERE title LIKE ? ESCAPE '\\'", likePattern)
}

// Upstream rename CSV uses bare-acronym keys (e.g. רדק) while Sefaria stores
// the punctuated form (רד״ק / רד"ק). Compare with quotes/geresh stripped on
// both sides; the new title is still written exactly as the CSV provides it.
internal fun stripTitlePunct(s: String): String =
    s.replace("\"", "").replace("״", "").replace("'", "").replace("׳", "")

internal const val STRIP_TITLE_PUNCT_SQL =
    "REPLACE(REPLACE(REPLACE(REPLACE(title, '\"', ''), '״', ''), '''', ''), '׳', '')"

private fun renameBookTitle(conn: Connection, oldTitle: String, newTitle: String, logger: Logger): Int {
    val sql = "UPDATE book SET title = ? WHERE $STRIP_TITLE_PUNCT_SQL = ?"
    val n = conn.prepareStatement(sql).use { stmt ->
        stmt.setString(1, newTitle)
        stmt.setString(2, stripTitlePunct(oldTitle))
        stmt.executeUpdate()
    }
    if (n > 0) logger.i { "Renamed book '$oldTitle' -> '$newTitle' ($n rows)" }
    else logger.w { "Book rename: '$oldTitle' not found; skipping" }
    return n
}

private data class BookMove(val name: String, val sourcePath: String, val destPath: String)

/**
 * Resolves destPath against the existing category tree and updates the matching
 * book's categoryId. Missing destinations are skipped (no auto-creation).
 * Disambiguates by source path when multiple books share a title; an empty
 * sourcePath is only safe when the title is globally unique.
 */
private fun applyBookMove(conn: Connection, move: BookMove, logger: Logger): Boolean {
    val candidates = mutableListOf<Pair<Long, Long>>() // (bookId, categoryId)
    conn.prepareStatement("SELECT id, categoryId FROM book WHERE title = ?").use { stmt ->
        stmt.setString(1, move.name)
        stmt.executeQuery().use { rs ->
            while (rs.next()) candidates.add(rs.getLong(1) to rs.getLong(2))
        }
    }
    if (candidates.isEmpty()) {
        logger.w { "Book move: '${move.name}' not found; skipping" }
        return false
    }

    val sourceCatId = resolveCategoryPath(conn, move.sourcePath)
    val bookId = when {
        candidates.size == 1 -> candidates.single().first
        sourceCatId != null -> candidates.firstOrNull { it.second == sourceCatId }?.first
        else -> null
    }
    if (bookId == null) {
        logger.w { "Book move: '${move.name}' has ${candidates.size} candidates; source '${move.sourcePath}' did not disambiguate; skipping" }
        return false
    }

    val destCatId = resolveCategoryPath(conn, move.destPath)
    if (destCatId == null) {
        logger.w { "Book move: destination '${move.destPath}' not found; skipping" }
        return false
    }

    conn.prepareStatement("UPDATE book SET categoryId = ? WHERE id = ?").use { stmt ->
        stmt.setLong(1, destCatId)
        stmt.setLong(2, bookId)
        stmt.executeUpdate()
    }
    logger.i { "Moved book '${move.name}' (id=$bookId) -> '${move.destPath}' (catId=$destCatId)" }
    return true
}

/** Walks `a/b/c` from category roots; returns null on the first missing segment. */
private fun resolveCategoryPath(conn: Connection, path: String): Long? {
    val segments = path.split('/').map { it.trim() }.filter { it.isNotEmpty() }
    if (segments.isEmpty()) return null
    var parentId: Long? = null
    for (segment in segments) {
        parentId = findCategoryByNameAndParent(conn, segment, parentId) ?: return null
    }
    return parentId
}

internal fun downloadRequiredForDbCsv(fileName: String, logger: Logger): List<String> {
    val lines = forDbReleaseCsvs(logger).getValue(fileName)
    return if (lines.isEmpty()) lines else listOf(lines.first().removePrefix("\uFEFF")) + lines.drop(1)
}

private var cachedForDbCsvs: Map<String, List<String>>? = null

private fun forDbReleaseCsvs(logger: Logger): Map<String, List<String>> {
    cachedForDbCsvs?.let { return it }

    val archiveUrl = forDbReleaseArchiveUrl(logger)
    logger.i { "Downloading ForDB release archive from $archiveUrl" }
    val csvs = try {
        OptimizedHttpClient.downloadStream(
            url = archiveUrl,
            userAgent = FOR_DB_USER_AGENT,
            logger = logger
        ).stream.use { stream ->
            readForDbZip(stream)
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to download or read required ForDB release archive; aborting" }
        throw IllegalStateException("Failed to load required ForDB release archive", e)
    }

    val missing = FOR_DB_CSV_FILES.values.filterNot { it in csvs }
    check(missing.isEmpty()) {
        "ForDB release archive is missing required file(s): ${missing.joinToString()}"
    }

    cachedForDbCsvs = csvs
    return csvs
}

private fun forDbReleaseArchiveUrl(logger: Logger): String {
    val body = OptimizedHttpClient.fetchJson(FOR_DB_RELEASE_API, FOR_DB_USER_AGENT, logger)
    val archiveNamePattern = Regex.escape(FOR_DB_ARCHIVE_NAME)
    return Regex(""""browser_download_url"\s*:\s*"([^"]*/$archiveNamePattern)"""")
        .find(body)
        ?.groupValues
        ?.get(1)
        ?: throw IllegalStateException("No $FOR_DB_ARCHIVE_NAME asset found in fordb-latest release")
}

private fun readForDbZip(stream: InputStream): Map<String, List<String>> {
    val csvs = mutableMapOf<String, List<String>>()
    ZipInputStream(stream).use { zip ->
        var entry = zip.nextEntry
        while (entry != null) {
            if (!entry.isDirectory) {
                val normalizedName = entry.name.replace('\\', '/')
                if (normalizedName.startsWith("ForDB/")) {
                    val fileName = normalizedName.removePrefix("ForDB/")
                    if ('/' !in fileName && fileName.endsWith(".csv")) {
                        val bytes = ByteArrayOutputStream()
                        zip.copyTo(bytes)
                        csvs[fileName] = ByteArrayInputStream(bytes.toByteArray())
                            .bufferedReader(StandardCharsets.UTF_8)
                            .readLines()
                    }
                }
            }
            zip.closeEntry()
            entry = zip.nextEntry
        }
    }
    return csvs
}
