package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
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
 *  - סדר הדורות.csv  `שם ספר,קבוצת דור` (with header) — seeds generation table and links books
 *
 * The release zip does not include ForDB/, so the CSVs are fetched directly
 * from raw.githubusercontent.com at task start. Download failures are
 * non-fatal: the corresponding section becomes a no-op (logged as a warning)
 * and the rest of the run proceeds. This is intentional — a GitHub outage
 * should not break the CI build; an un-post-processed DB is preferable to a
 * red pipeline. The warning lines are the signal to investigate.
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
private const val FOR_DB_BASE = "https://raw.githubusercontent.com/Otzaria/otzaria-library/main/ForDB"
private const val CATEGORY_RENAMES_URL = "$FOR_DB_BASE/%D7%AA%D7%99%D7%A7%D7%99%D7%95%D7%AA.csv"
private const val BOOK_RENAMES_URL = "$FOR_DB_BASE/%D7%A1%D7%A4%D7%A8%D7%99%D7%9D.csv"
private const val BOOK_MOVES_URL = "$FOR_DB_BASE/Moving%20files.csv"
private const val GENERATIONS_URL = "$FOR_DB_BASE/%D7%A1%D7%93%D7%A8%20%D7%94%D7%93%D7%95%D7%A8%D7%95%D7%AA.csv"

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
    val categoryRenames: List<Pair<String, String>> = parsePairs(downloadCsv(CATEGORY_RENAMES_URL, logger))
    val bookRenames: List<Pair<String, String>> = parsePairs(downloadCsv(BOOK_RENAMES_URL, logger))
    val bookMoves: List<BookMove> = parseBookMoves(downloadCsv(BOOK_MOVES_URL, logger), logger)
    val generations: List<Pair<String, String>> = parseGenerations(downloadCsv(GENERATIONS_URL, logger), logger)

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            var totalRenamed = 0
            var totalMerged = 0
            var categoryFailures = 0
            for ((oldName, newName) in categoryRenames) {
                try {
                    when (val result = renameOrMergeCategory(conn, oldName, newName, logger)) {
                        is RenameResult.Renamed -> totalRenamed += result.count
                        is RenameResult.Merged -> totalMerged += result.booksMoved
                        is RenameResult.NotFound -> logger.w { "Category rename: '$oldName' not found; skipping" }
                    }
                } catch (e: Exception) {
                    categoryFailures++
                    logger.w(e) { "Category rename '$oldName' -> '$newName' failed; skipping" }
                }
            }
            logger.i { "Category processing complete. Renamed: $totalRenamed, Merged: $totalMerged books, Failures: $categoryFailures" }

            var totalBookRenamed = 0
            var bookRenameFailures = 0
            for ((oldTitle, newTitle) in bookRenames) {
                try {
                    totalBookRenamed += renameBookTitle(conn, oldTitle, newTitle, logger)
                } catch (e: Exception) {
                    bookRenameFailures++
                    logger.w(e) { "Book rename '$oldTitle' -> '$newTitle' failed; skipping" }
                }
            }
            logger.i { "Book renames complete. Renamed: $totalBookRenamed books, Failures: $bookRenameFailures" }

            var totalMoved = 0
            var moveFailures = 0
            for (move in bookMoves) {
                try {
                    if (applyBookMove(conn, move, logger)) totalMoved++
                } catch (e: Exception) {
                    moveFailures++
                    logger.w(e) { "Book move '${move.name}' -> '${move.destPath}' failed; skipping" }
                }
            }
            logger.i { "Book moves complete. Moved: $totalMoved books, Failures: $moveFailures" }

            val genResult = try {
                applyGenerations(conn, generations, logger)
            } catch (e: Exception) {
                logger.w(e) { "Generation seeding failed; skipping section" }
                GenerationApplyResult(0, 0, generations.size)
            }
            logger.i {
                "Generations complete. Seeded: ${genResult.generationsCreated}, " +
                    "book links: ${genResult.linksCreated}, unmatched titles: ${genResult.unmatched}"
            }

            conn.commit()
            logger.i {
                "Post-process done: categories renamed=$totalRenamed merged=$totalMerged " +
                    "(failures=$categoryFailures); books renamed=$totalBookRenamed " +
                    "(failures=$bookRenameFailures); books moved=$totalMoved (failures=$moveFailures); " +
                    "generations seeded=${genResult.generationsCreated} " +
                    "(book links=${genResult.linksCreated}, unmatched=${genResult.unmatched})"
            }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to open or commit DB; aborting" }
        exitProcess(1)
    }
}

/** `old,new` rows — skips blanks and malformed lines. */
private fun parsePairs(lines: List<String>): List<Pair<String, String>> = lines.mapNotNull { line ->
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

/**
 * `שם ספר,קבוצת דור` rows. Drops the header row if its first field contains
 * "שם ספר"; otherwise treats every row as data (with a warning) so a
 * header-less upload doesn't silently lose row 0.
 */
private fun parseGenerations(lines: List<String>, logger: Logger): List<Pair<String, String>> {
    if (lines.isEmpty()) return emptyList()
    val isHeader = "שם ספר" in lines.first()
    val body = if (isHeader) lines.drop(1) else {
        logger.w { "סדר הדורות.csv: no header detected; treating first row as data" }
        lines
    }
    return parsePairs(body)
}

private data class GenerationApplyResult(
    val generationsCreated: Int,
    val linksCreated: Int,
    val unmatched: Int,
)

/**
 * Seeds the `generation` table with distinct names and links each book to its
 * generation via `book_generation`. Book titles are unique in this DB, so we
 * take the single match; punctuation-strip fallback mirrors `renameBookTitle`
 * for CSVs that use the bare form. INSERT OR IGNORE keeps re-runs idempotent.
 *
 * Caveat: only books present in the DB at this point are linked. The
 * appendOtzaria stage runs AFTER this one — books it adds stay unlinked.
 */
private fun applyGenerations(
    conn: Connection,
    rows: List<Pair<String, String>>,
    logger: Logger,
): GenerationApplyResult {
    if (rows.isEmpty()) return GenerationApplyResult(0, 0, 0)

    var generationsCreated = 0
    conn.prepareStatement("INSERT OR IGNORE INTO generation(name) VALUES (?)").use { stmt ->
        for (name in rows.map { it.second }.distinct()) {
            stmt.setString(1, name)
            generationsCreated += stmt.executeUpdate()
        }
    }

    val nameToId = HashMap<String, Long>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, name FROM generation").use { rs ->
            while (rs.next()) nameToId[rs.getString(2)] = rs.getLong(1)
        }
    }

    var linksCreated = 0
    var unmatched = 0
    conn.prepareStatement("SELECT id FROM book WHERE title = ?").use { findExact ->
        conn.prepareStatement("SELECT id FROM book WHERE $STRIP_TITLE_PUNCT_SQL = ?").use { findStripped ->
            conn.prepareStatement("SELECT id FROM book WHERE TRIM(title) = ?").use { findTrimmed ->
                conn.prepareStatement("INSERT OR IGNORE INTO book_generation(bookId, generationId) VALUES (?, ?)").use { linkStmt ->
                    for ((bookTitle, genName) in rows) {
                        val genId = nameToId[genName] ?: continue
                        val bookId = findBookIdForGeneration(findExact, findStripped, findTrimmed, bookTitle, logger)
                        if (bookId == null) {
                            unmatched++
                            logger.d { "Generation link: book '$bookTitle' not found; skipping" }
                            continue
                        }
                        linkStmt.setLong(1, bookId)
                        linkStmt.setLong(2, genId)
                        linksCreated += linkStmt.executeUpdate()
                    }
                }
            }
        }
    }
    return GenerationApplyResult(generationsCreated, linksCreated, unmatched)
}

// Titles are unique. Three-tier fallback: exact, then punctuation-strip (for
// CSVs that use the bare form like רדק vs רד״ק), then TRIM (for DB titles
// imported with stray leading/trailing whitespace from upstream sources).
// >1 result from a fallback means colliding normalized forms — log and skip
// rather than silently over-link.
private fun findBookIdForGeneration(
    findExact: PreparedStatement,
    findStripped: PreparedStatement,
    findTrimmed: PreparedStatement,
    title: String,
    logger: Logger,
): Long? {
    findExact.setString(1, title)
    findExact.executeQuery().use { rs ->
        if (rs.next()) return rs.getLong(1)
    }
    findStripped.setString(1, stripTitlePunct(title))
    val stripMatches = collectIds(findStripped)
    when (stripMatches.size) {
        1 -> return stripMatches.single()
        in 2..Int.MAX_VALUE -> {
            logger.w { "Generation link: '$title' has multiple punct-strip matches; skipping" }
            return null
        }
    }
    findTrimmed.setString(1, title.trim())
    val trimMatches = collectIds(findTrimmed)
    return when (trimMatches.size) {
        0 -> null
        1 -> trimMatches.single()
        else -> {
            logger.w { "Generation link: '$title' has multiple TRIM matches; skipping" }
            null
        }
    }
}

private fun collectIds(stmt: PreparedStatement): List<Long> {
    val out = ArrayList<Long>(2)
    stmt.executeQuery().use { rs ->
        while (rs.next() && out.size < 2) out += rs.getLong(1)
    }
    return out
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
private fun stripTitlePunct(s: String): String =
    s.replace("\"", "").replace("״", "").replace("'", "").replace("׳", "")

private const val STRIP_TITLE_PUNCT_SQL =
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

/**
 * Downloads a CSV and returns its lines (UTF-8, BOM stripped from line 0 if present).
 * Returns an empty list on failure (logs a warning); the corresponding section
 * then becomes a no-op and the rest of the run proceeds.
 */
private fun downloadCsv(url: String, logger: Logger): List<String> = try {
    val conn = URI(url).toURL().openConnection().apply {
        connectTimeout = 10_000
        readTimeout = 30_000
    }
    val lines = conn.getInputStream().use { it.reader(StandardCharsets.UTF_8).readLines() }
    if (lines.isEmpty()) lines else listOf(lines.first().removePrefix("\uFEFF")) + lines.drop(1)
} catch (e: Exception) {
    logger.w(e) { "Failed to download $url; skipping section" }
    emptyList()
}

