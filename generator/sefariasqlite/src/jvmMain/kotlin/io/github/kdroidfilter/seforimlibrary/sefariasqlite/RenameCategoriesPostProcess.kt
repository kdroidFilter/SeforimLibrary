package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.OptimizedHttpClient
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.ids.BOOK_MOVE_LEAF_KEY_PREFIX
import io.github.kdroidfilter.seforimlibrary.common.ids.CategoryLabels
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.intOrNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.zip.ZipInputStream
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Post-processing step to rename and merge categories, rename books, and
 * relocate books between categories. Runs after Sefaria import but before
 * Otzaria, so naming is unified before additional books are added.
 *
 * Rules are downloaded from otzaria-library/ForDB/ on GitHub (UTF-8):
 *  - category_renames.csv `old,new` — category renames (exact match, explicit prefix rules)
 *  - category_moves.csv   `sourcePath,destParentPath` — reparent a category (with its subtree) under a new parent
 *  - book_renames.csv     `old,new` — book title renames (exact match)
 *  - book_moves.csv       `name,sourcePath,destPath` (simple CSV; embedded newlines in quoted fields are not supported)
 *
 * Rules are fetched through the repository's atomic `fordb_latest_pointer.json`;
 * the pointer names an immutable content-addressed release and archive digest.
 * Download failures are fatal because silently skipping these rules can produce
 * an invalid DB delta.
 *
 * Category renames handle two cases:
 * 1. Simple rename: When no target category exists under the same parent
 * 2. Merge: When a target category already exists, books are moved and source is deleted
 *
 * Book moves require the source path and the destination's parent path to exist;
 * only the final (leaf) destination segment is auto-created if missing (idempotent).
 * A created leaf takes its id from the build state (`<db>.buildstate`, or
 * -PbuildStatePath), which must exist: see [BookMoveLeafIds].
 *
 * Category moves reparent an existing category (books and subcategories follow).
 * The destination parent must already exist (nothing is auto-created); moving a
 * category under itself or its own descendant fails. Idempotent: once applied,
 * the source path no longer resolves and the row is skipped.
 *
 * Order of operations: category renames → category moves → book renames → book
 * moves. Paths in category_moves.csv and book_moves.csv must therefore reference
 * the POST-rename category names, and book_moves paths the POST-move tree.
 * If a move references a pre-rename destPath, the task fails with a clear error.
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :sefariasqlite:renameCategories -PseforimDb=/path/to/seforim.db
 *       [-PbuildStatePath=/path/to/seforim.db.buildstate]
 *
 * Env alternatives:
 *   SEFORIM_DB, BUILD_STATE_PATH
 */
private const val FOR_DB_POINTER_URL =
    "https://raw.githubusercontent.com/Otzaria/otzaria-library/main/fordb_latest_pointer.json"
private const val FOR_DB_RELEASE_API_PREFIX =
    "https://api.github.com/repos/Otzaria/otzaria-library/releases/tags/"
private const val FOR_DB_ARCHIVE_NAME = "fordb_latest.zip"
private const val FOR_DB_USER_AGENT = "SeforimLibrary-ForDBFetcher/1.0"
// When set, every ForDB post-process reads this exact local archive instead of
// resolving the repository pointer, so the three separate JVM processes
// (renameCategories/seedGenerations/seedAllMetadata) consume byte-identical
// inputs. The release build pins it; -PforDbSha256 must accompany it.
private const val FOR_DB_ARCHIVE_PROPERTY = "forDbArchive"
private const val FOR_DB_SHA256_PROPERTY = "forDbSha256"
internal val FOR_DB_CSV_FILES = mapOf(
    "categoryRenames" to "category_renames.csv",
    "categoryMoves" to "category_moves.csv",
    "bookRenames" to "book_renames.csv",
    "bookMoves" to "book_moves.csv",
    // Generations are read from book_info.csv, or generations.csv in an older
    // archive (SeedGenerationsPostProcess.loadGenerationRows) — neither is required here.
    "categoryDescriptions" to "sefaria_category_changes.csv",
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
    // category_renames.csv and book_renames.csv have no header; book_moves.csv has one.
    val categoryRenames: List<CategoryRename> =
        parseCategoryRenames(downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("categoryRenames"), logger))
    val bookRenames: List<Pair<String, String>> =
        parsePairs(
            downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("bookRenames"), logger),
            FOR_DB_CSV_FILES.getValue("bookRenames")
        )
    val bookMoves: List<BookMove> =
        parseBookMoves(downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("bookMoves"), logger), logger)
    val categoryMoves: List<CategoryMove> =
        parseCategoryMoves(downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("categoryMoves"), logger), logger)
    // Parse every downstream ForDB asset before the first ForDB transaction.
    // Semantic checks that require post-append books still run in their real
    // appliers, but malformed JSON/CSV/header/duplicate-path input can never be
    // discovered only after rename/move mutations were committed.
    loadGenerationRows(logger)
    parseBulkMetadata(downloadRequiredForDbFile("all_metadata.json", logger))
    parseDescriptionOverrides(downloadRequiredForDbFile("sefaria_metadata_changes.csv", logger))
    parseCategoryDescriptionOverrides(
        downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("categoryDescriptions"), logger),
    )
    loadBookNoticeRows(logger)

    // Book-move destination leaves get their ids from the build state (see
    // [BookMoveLeafIds]); without it they would fall back to implicit rowids and
    // land on ids the build state reserves for Otzaria folders.
    val buildStatePath = resolveSeifimBuildStatePath(dbPath)
    if (!Files.exists(buildStatePath)) {
        logger.e { "build_state not found at $buildStatePath; run the DB generation pipeline first" }
        exitProcess(1)
    }

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            // ATTACH is not allowed inside a transaction, so it precedes autoCommit=false.
            // From here on the build state's id rows commit or roll back with the DB.
            conn.prepareStatement("ATTACH DATABASE ? AS $BOOK_MOVE_STATE_SCHEMA").use { st ->
                st.setString(1, buildStatePath.toAbsolutePath().toString())
                st.execute()
            }
            val leafIds = BookMoveLeafIds(conn)
            conn.autoCommit = false

            // PREFLIGHT (defence in depth beside the update-fordb publish gate): dry-run
            // every rule in the exact order below, collecting ALL failures, and fail
            // with the complete report BEFORE any mutation is committed — one broken
            // path (the "מהרש"ם" class) must never crash a build hours in on the first
            // bad row. The transaction is rolled back; the real pass then starts clean.
            val preflight = collectForDbRuleFailures(conn, categoryRenames, categoryMoves, bookRenames, bookMoves, logger)
            conn.rollback()
            if (preflight.isNotEmpty()) {
                reportForDbRuleFailures(preflight, logger)
                logger.e { "ForDB preflight FAILED: ${preflight.size} rule(s) cannot apply; DB left untouched" }
                exitProcess(1)
            }
            logger.i { "ForDB preflight passed — applying for real" }

            // Before any leaf is created, so a restored id is never handed to a leaf.
            leafIds.restore(CATEGORY_ID_RESTORES, logger)

            var totalRenamed = 0
            var totalMerged = 0
            runSection("Category renames", categoryRenames, logger) { rule ->
                val result = renameOrMergeCategory(conn, rule, logger)
                when (result) {
                    is RenameResult.Renamed -> totalRenamed += result.count
                    is RenameResult.Merged -> totalMerged += result.booksMoved
                    is RenameResult.NotFound -> logger.i {
                        "Category rename: '${rule.oldName}' not found; no rows changed"
                    }
                }
                result.rows()
            }

            val categoriesMoved = runSection("Category moves", categoryMoves, logger) { move ->
                applyCategoryMove(conn, move, logger)
            }

            val booksRenamed = runSection("Book renames", bookRenames, logger) { (oldTitle, newTitle) ->
                renameBookTitle(conn, oldTitle, newTitle, logger)
            }

            val booksMoved = runSection("Book moves", bookMoves, logger) { move ->
                applyBookMove(conn, move, logger, leafIds)
            }

            conn.commit()
            logger.i {
                "Post-process done: categories renamed=$totalRenamed merged=$totalMerged moved=$categoriesMoved; " +
                    "books renamed=$booksRenamed; books moved=$booksMoved"
            }
        }
        // The ATTACHed build state was mutated in place; meta stays with the stage
        // that wrote it, the counters must still be ahead of the DB.
        BuildStateVerifier.verifyFreshSnapshot(buildStatePath, dbPath, emptyMap())
    } catch (e: Exception) {
        logger.e(e) { "Failed to open or commit DB; aborting" }
        exitProcess(1)
    }
}

/** `old,new` rows — ignores blank rows and fails on malformed non-blank rows. */
internal fun parsePairs(lines: List<String>, sourceName: String = "pairs CSV"): List<Pair<String, String>> =
    parseRequiredCsvRows(lines, sourceName, minFields = 2).map { f -> f[0] to f[1] }

internal enum class CategoryMatchMode { Exact, Prefix }

internal data class CategoryRename(
    val oldName: String,
    val newName: String,
    val matchMode: CategoryMatchMode,
)

private val EXPLICIT_CATEGORY_PREFIX_RULES = setOf(
    "ראשונים על",
    "אחרונים על",
    "פירושים מודרניים",
)

internal fun parseCategoryRenames(lines: List<String>): List<CategoryRename> =
    parseRequiredCsvRows(lines, FOR_DB_CSV_FILES.getValue("categoryRenames"), minFields = 2).map { f ->
        val matchMode = if (f[0] in EXPLICIT_CATEGORY_PREFIX_RULES) {
            CategoryMatchMode.Prefix
        } else {
            CategoryMatchMode.Exact
        }
        CategoryRename(oldName = f[0], newName = f[1], matchMode = matchMode)
    }.distinct()

// Fields are kept verbatim (no trim) so rules can exact-match DB values that
// carry edge spaces.
internal fun parseRequiredCsvRows(lines: List<String>, sourceName: String, minFields: Int): List<List<String>> =
    lines.mapIndexedNotNull { index, line ->
        val fields = parseForDbCsvLine(line)
        if (fields.all { it.isBlank() }) return@mapIndexedNotNull null
        require(fields.size >= minFields && fields.take(minFields).none { it.isBlank() }) {
            "$sourceName row ${index + 1} is malformed: $line"
        }
        fields
    }

internal fun parseForDbCsvLine(line: String): List<String> {
    val result = mutableListOf<String>()
    val sb = StringBuilder()
    var inQuotes = false
    var i = 0
    while (i < line.length) {
        val c = line[i]
        if (inQuotes) {
            if (c == '"') {
                if (i + 1 < line.length && line[i + 1] == '"') {
                    sb.append('"')
                    i++
                } else {
                    inQuotes = false
                }
            } else {
                sb.append(c)
            }
        } else {
            when (c) {
                '"' -> if (sb.isEmpty()) inQuotes = true else sb.append(c)
                ',' -> {
                    result += sb.toString()
                    sb.setLength(0)
                }
                else -> sb.append(c)
            }
        }
        i++
    }
    result += sb.toString()
    return result
}

/**
 * Splits CSV text into logical records, honoring RFC-4180 quoted fields that may
 * span multiple physical lines. Needed for files whose fields can contain
 * newlines (e.g. multi-line descriptions); single-line files use parseForDbCsvLine.
 * Blank records (no fields, all empty) are dropped.
 */
internal fun parseForDbCsvRecords(text: String): List<List<String>> {
    val records = mutableListOf<List<String>>()
    var fields = mutableListOf<String>()
    val sb = StringBuilder()
    var inQuotes = false
    var recordHasCsvSyntax = false
    var i = 0

    fun endField() { fields.add(sb.toString()); sb.setLength(0) }
    fun endRecord() {
        endField()
        if (fields.any { it.isNotEmpty() } || recordHasCsvSyntax) records.add(fields)
        fields = mutableListOf()
        recordHasCsvSyntax = false
    }

    while (i < text.length) {
        val c = text[i]
        if (inQuotes) {
            if (c == '"') {
                if (i + 1 < text.length && text[i + 1] == '"') { sb.append('"'); i++ }
                else inQuotes = false
            } else sb.append(c)
        } else {
            when (c) {
                '"' -> if (sb.isEmpty()) {
                    inQuotes = true
                    recordHasCsvSyntax = true
                } else sb.append(c)
                ',' -> {
                    recordHasCsvSyntax = true
                    endField()
                }
                '\n' -> endRecord()
                '\r' -> if (i + 1 < text.length && text[i + 1] == '\n') { endRecord(); i++ } else endRecord()
                else -> sb.append(c)
            }
        }
        i++
    }
    require(!inQuotes) { "Unterminated quoted field in ForDB CSV" }
    if (sb.isNotEmpty() || fields.isNotEmpty()) endRecord()
    return records
}

/**
 * `name,Source path,Destination path` rows. Drops the header row if present;
 * detection requires both `source path` and `destination path` tokens so a
 * stray book title containing the word "path" can't be mistaken for a header.
 * Missing or malformed headers fail the task so release data issues are not
 * hidden.
 */
internal fun parseBookMoves(lines: List<String>, logger: Logger): List<BookMove> {
    val firstLower = lines.firstOrNull()?.lowercase()
    val isHeader = firstLower != null && "source path" in firstLower && "destination path" in firstLower
    require(isHeader) {
        "${FOR_DB_CSV_FILES.getValue("bookMoves")} must start with a header containing Source path and Destination path"
    }
    return parseRequiredCsvRows(lines.drop(1), FOR_DB_CSV_FILES.getValue("bookMoves"), minFields = 3)
        .map { f -> BookMove(f[0], f[1], f[2]) }
        .also { logger.i { "Loaded ${it.size} book move rule(s)" } }
}

/**
 * `Source path,Destination parent path` rows. Same header discipline as
 * [parseBookMoves]: a missing or malformed header fails the task.
 */
internal fun parseCategoryMoves(lines: List<String>, logger: Logger): List<CategoryMove> {
    val firstLower = lines.firstOrNull()?.lowercase()
    val isHeader = firstLower != null && "source path" in firstLower && "destination parent path" in firstLower
    require(isHeader) {
        "${FOR_DB_CSV_FILES.getValue("categoryMoves")} must start with a header containing Source path and Destination parent path"
    }
    return parseRequiredCsvRows(lines.drop(1), FOR_DB_CSV_FILES.getValue("categoryMoves"), minFields = 2)
        .map { f -> CategoryMove(f[0], f[1]) }
        .also { logger.i { "Loaded ${it.size} category move rule(s)" } }
}

private fun <T> runSection(name: String, items: List<T>, logger: Logger, apply: (T) -> Int): Int {
    var applied = 0
    for (item in items) {
        applied += apply(item)
    }
    logger.i { "$name: applied=$applied" }
    return applied
}

internal sealed class RenameResult {
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
internal fun renameOrMergeCategory(
    conn: Connection,
    rule: CategoryRename,
    logger: Logger
): RenameResult {
    val oldName = rule.oldName
    val newName = rule.newName
    val sourceCats = findSourceCategories(conn, rule)

    if (sourceCats.isEmpty()) {
        return RenameResult.NotFound
    }

    var totalRenamed = 0
    var totalBooksMoved = 0

    for ((sourceId, parentId) in sourceCats) {
        // Check if a target category with newName exists under the same parent
        val targetId = findCategoryByNameAndParent(conn, newName, parentId)

        if (targetId != null && targetId != sourceId) {
            // Resolve descriptions before any destructive part of the merge.
            mergeCategoryDescriptions(conn, sourceId, targetId)
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

private data class StoredCategoryDescriptions(
    val heShortDesc: String?,
    val heDesc: String?,
)

/** Preserves non-conflicting descriptions when one category is merged into another. */
internal fun mergeCategoryDescriptions(conn: Connection, sourceId: Long, targetId: Long) {
    fun read(categoryId: Long): StoredCategoryDescriptions =
        conn.prepareStatement(
            "SELECT heShortDesc, heDesc FROM category WHERE id = ?",
        ).use { stmt ->
            stmt.setLong(1, categoryId)
            stmt.executeQuery().use { result ->
                check(result.next()) { "Category id=$categoryId disappeared during merge" }
                StoredCategoryDescriptions(
                    heShortDesc = result.getString(1),
                    heDesc = result.getString(2),
                )
            }
        }

    fun resolve(field: String, source: String?, target: String?): String? = when {
        source == null -> target
        target == null -> source
        source == target -> target
        else -> error(
            "Cannot merge category id=$sourceId into id=$targetId: " +
                "conflicting $field values",
        )
    }

    val source = read(sourceId)
    val target = read(targetId)
    val mergedShort = resolve("heShortDesc", source.heShortDesc, target.heShortDesc)
    val mergedLong = resolve("heDesc", source.heDesc, target.heDesc)
    if (mergedShort == target.heShortDesc && mergedLong == target.heDesc) return

    conn.prepareStatement(
        "UPDATE category SET heShortDesc = ?, heDesc = ? WHERE id = ?",
    ).use { stmt ->
        stmt.setString(1, mergedShort)
        stmt.setString(2, mergedLong)
        stmt.setLong(3, targetId)
        check(stmt.executeUpdate() == 1) { "Failed to update merge target category id=$targetId" }
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
 * Category prefix rules are intentionally limited to [EXPLICIT_CATEGORY_PREFIX_RULES].
 * All other rows are exact-match only.
 */
private fun findSourceCategories(conn: Connection, rule: CategoryRename): List<Pair<Long, Long?>> {
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

    return when (rule.matchMode) {
        CategoryMatchMode.Exact ->
            query("SELECT id, parentId FROM category WHERE title = ?", rule.oldName)
        CategoryMatchMode.Prefix -> {
            val likePattern = rule.oldName.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            query("SELECT id, parentId FROM category WHERE title LIKE ? ESCAPE '\\'", likePattern)
        }
    }
}

internal fun renameBookTitle(conn: Connection, oldTitle: String, newTitle: String, logger: Logger): Int {
    val ids = findBookIdsByTitle(conn, oldTitle)
    if (ids.isEmpty()) {
        val alreadyRenamed = findBookIdsByTitle(conn, newTitle)
        if (alreadyRenamed.isNotEmpty()) {
            logger.i { "Book rename '$oldTitle' -> '$newTitle' already applied (${alreadyRenamed.size} row(s))" }
            return 0
        }
        logger.w { "Book rename: '$oldTitle' not found; no rows changed" }
        return 0
    }
    require(ids.size == 1) {
        "Book rename '$oldTitle' -> '$newTitle' matched ${ids.size} books"
    }
    val n = conn.prepareStatement("UPDATE book SET title = ? WHERE id = ?").use { stmt ->
        stmt.setString(1, newTitle)
        stmt.setLong(2, ids.single())
        stmt.executeUpdate()
    }
    logger.i { "Renamed book '$oldTitle' -> '$newTitle' ($n rows)" }
    return n
}

private fun findBookIdsByTitle(conn: Connection, title: String): List<Long> =
    conn.prepareStatement("SELECT id FROM book WHERE title = ?").use { stmt ->
        stmt.setString(1, title)
        stmt.executeQuery().use { rs ->
            buildList {
                while (rs.next()) add(rs.getLong(1))
            }
        }
    }

internal data class BookMove(val name: String, val sourcePath: String, val destPath: String)

internal data class CategoryMove(val sourcePath: String, val destParentPath: String)

/**
 * Reparents the category at [CategoryMove.sourcePath] (books and subtree follow via
 * parentId) under [CategoryMove.destParentPath], which must already exist. Subtree
 * levels are shifted to match the new depth. Idempotent: a source path that no
 * longer resolves but whose leaf already sits under the destination is skipped.
 */
internal fun applyCategoryMove(conn: Connection, move: CategoryMove, logger: Logger): Int {
    val title = move.sourcePath.split('/').map { it.trim() }.filter { it.isNotEmpty() }.lastOrNull()
        ?: error("Category move has an empty source path")
    val destParentId = resolveCategoryPath(conn, move.destParentPath)
        ?: error("Category move destination parent '${move.destParentPath}' not found for '${move.sourcePath}' (nothing is auto-created)")

    val sourceId = resolveCategoryPath(conn, move.sourcePath)
    if (sourceId == null) {
        val existing = findCategoryByNameAndParent(conn, title, destParentId)
        require(existing != null) { "Category move source '${move.sourcePath}' not found" }
        logger.i { "Category move '${move.sourcePath}' already applied (id=$existing, dest=${move.destParentPath})" }
        return 0
    }
    if (categoryParent(conn, sourceId) == destParentId) {
        logger.i { "Category move '${move.sourcePath}' already applied (id=$sourceId, dest=${move.destParentPath})" }
        return 0
    }
    require(findCategoryByNameAndParent(conn, title, destParentId) == null) {
        "Category move '${move.sourcePath}': a category titled '$title' already exists under '${move.destParentPath}' (merge is not supported)"
    }
    // Walk up from the destination to reject a move under the category itself or its descendant.
    var cursor: Long? = destParentId
    while (cursor != null) {
        require(cursor != sourceId) {
            "Category move '${move.sourcePath}' would create a cycle: '${move.destParentPath}' is inside the moved category"
        }
        cursor = categoryParent(conn, cursor)
    }

    val delta = categoryLevel(conn, destParentId) + 1 - categoryLevel(conn, sourceId)
    conn.prepareStatement("UPDATE category SET parentId = ? WHERE id = ?").use { stmt ->
        stmt.setLong(1, destParentId)
        stmt.setLong(2, sourceId)
        stmt.executeUpdate()
    }
    if (delta != 0) {
        conn.prepareStatement(
            """
            WITH RECURSIVE sub(id) AS (
                SELECT ? UNION ALL SELECT c.id FROM category c JOIN sub ON c.parentId = sub.id
            )
            UPDATE category SET level = level + ? WHERE id IN (SELECT id FROM sub)
            """.trimIndent(),
        ).use { stmt ->
            stmt.setLong(1, sourceId)
            stmt.setInt(2, delta)
            stmt.executeUpdate()
        }
    }
    logger.i { "Moved category '${move.sourcePath}' (id=$sourceId) under '${move.destParentPath}' (parentId=$destParentId, level delta=$delta)" }
    return 1
}

private fun categoryParent(conn: Connection, categoryId: Long): Long? =
    conn.prepareStatement("SELECT parentId FROM category WHERE id = ?").use { stmt ->
        stmt.setLong(1, categoryId)
        stmt.executeQuery().use { rs ->
            check(rs.next()) { "Category id=$categoryId not found" }
            rs.getLong(1).let { if (rs.wasNull()) null else it }
        }
    }

/**
 * Updates the matching book's categoryId. Source path must fully exist; the
 * destination's leaf is created if missing (parents must exist).
 *
 * [leafIds] gives a created leaf its id from the build state. Null (the ForDB
 * dry-run validator, which always rolls back) keeps SQLite's implicit rowid.
 */
internal fun applyBookMove(
    conn: Connection,
    move: BookMove,
    logger: Logger,
    leafIds: BookMoveLeafIds? = null,
): Int {
    val sourceCatId = resolveCategoryPath(conn, move.sourcePath)
        ?: error("Book move source '${move.sourcePath}' not found for '${move.name}'")
    val destCatId = resolveOrCreateDestCategory(conn, move.destPath, logger, leafIds)
        ?: error("Book move destination parent path '${move.destPath}' not found for '${move.name}' (only the final segment is auto-created)")

    val candidates = mutableListOf<Pair<Long, Long>>() // (bookId, categoryId)
    conn.prepareStatement("SELECT id, categoryId FROM book WHERE title = ?").use { stmt ->
        stmt.setString(1, move.name)
        stmt.executeQuery().use { rs ->
            while (rs.next()) candidates.add(rs.getLong(1) to rs.getLong(2))
        }
    }
    require(candidates.isNotEmpty()) { "Book move: '${move.name}' not found" }

    candidates.singleOrNull { it.second == destCatId }?.let { (bookId, _) ->
        logger.i { "Book move '${move.name}' already applied (id=$bookId, dest=${move.destPath})" }
        return 0
    }
    val sourceMatches = candidates.filter { it.second == sourceCatId }
    require(sourceMatches.size == 1) {
        "Book move '${move.name}' has ${candidates.size} candidate(s); source '${move.sourcePath}' matched ${sourceMatches.size}"
    }
    val bookId = sourceMatches.single().first

    conn.prepareStatement("UPDATE book SET categoryId = ? WHERE id = ?").use { stmt ->
        stmt.setLong(1, destCatId)
        stmt.setLong(2, bookId)
        stmt.executeUpdate()
    }
    logger.i { "Moved book '${move.name}' (id=$bookId) -> '${move.destPath}' (catId=$destCatId)" }
    return 1
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

/** Resolves the dest path, creating only a missing leaf; parents must exist (else null). */
private fun resolveOrCreateDestCategory(
    conn: Connection,
    path: String,
    logger: Logger,
    leafIds: BookMoveLeafIds?,
): Long? {
    val segments = path.split('/').map { it.trim() }.filter { it.isNotEmpty() }
    if (segments.isEmpty()) return null
    var parentId: Long? = null
    for ((idx, segment) in segments.withIndex()) {
        val existing = findCategoryByNameAndParent(conn, segment, parentId)
        if (existing != null) {
            parentId = existing
        } else if (idx == segments.lastIndex) {
            val stableId = leafIds?.leafId(segments)
            parentId = createCategory(conn, segment, parentId, stableId, logger)
        } else {
            return null
        }
    }
    return parentId
}

/**
 * Inserts a category under [parentId] (level = parent.level + 1) and returns its id:
 * [id] when given, otherwise SQLite's implicit rowid.
 */
private fun createCategory(conn: Connection, title: String, parentId: Long?, id: Long?, logger: Logger): Long {
    val level = if (parentId == null) 0 else categoryLevel(conn, parentId) + 1
    if (id != null) {
        // The build state does not describe this DB if the id is already taken; a
        // plain INSERT fails on the primary key rather than merging two folders.
        conn.prepareStatement("INSERT INTO category (id, parentId, title, level) VALUES (?, ?, ?, ?)").use { stmt ->
            stmt.setLong(1, id)
            if (parentId == null) stmt.setNull(2, java.sql.Types.INTEGER) else stmt.setLong(2, parentId)
            stmt.setString(3, title)
            stmt.setInt(4, level)
            stmt.executeUpdate()
        }
        logger.i { "Created category '$title' (id=$id from build state, parentId=$parentId, level=$level)" }
        return id
    }
    conn.prepareStatement(
        "INSERT INTO category (parentId, title, level) VALUES (?, ?, ?)",
        Statement.RETURN_GENERATED_KEYS,
    ).use { stmt ->
        if (parentId == null) stmt.setNull(1, java.sql.Types.INTEGER) else stmt.setLong(1, parentId)
        stmt.setString(2, title)
        stmt.setInt(3, level)
        stmt.executeUpdate()
        stmt.generatedKeys.use { rs ->
            require(rs.next()) { "Failed to create category '$title'" }
            val implicitId = rs.getLong(1)
            logger.i { "Created category '$title' (id=$implicitId, parentId=$parentId, level=$level)" }
            return implicitId
        }
    }
}

/** Schema name the build state is ATTACHed under while renameCategories runs. */
internal const val BOOK_MOVE_STATE_SCHEMA = "rename_state"

/**
 * Ids the leaves below shipped with (v20 to v30), before they were keyed in the build
 * state. A leaf's first keyed build takes its published id from here, so clients keep
 * their per-book settings and saved tabs; the next build finds it under its key.
 * Applied only while no key holds the id and the DB does not use it, so it can never
 * take an id from anything else. Leaves added later are not listed: they get fresh ids.
 *
 * `תלמוד בבלי/כללי הש״ס` is deliberately absent: it first shipped in v30 (pre-release
 * only) on 1478, the id it took from `אור הישר/סדר נשים`. That id goes back to סדר
 * נשים (see [CATEGORY_ID_RESTORES]) and כללי הש״ס gets a fresh one.
 */
internal val PUBLISHED_BOOK_MOVE_LEAF_IDS: Map<String, Long> = mapOf(
    "שו״ת/אחרונים/שות מהרשם" to 1472L,
    "קבלה/כתבי רבי אברהם אבולעפיא" to 1473L,
    "מדרש/אגדה/ילקוט שמעוני" to 1474L,
    "מדרש/אגדה/תנא דבי אליהו" to 1475L,
    "מחשבת ישראל/אחרונים/רמחל" to 1476L,
    "מחשבת ישראל/ראשונים/מורה נבוכים" to 1477L,
)

/**
 * An Otzaria folder that a book_moves leaf pushed off its id, and the id to give back.
 * [key] is the folder's build-state key (its Otzaria canonical path).
 */
internal data class CategoryIdRestore(val key: String, val reallocatedId: Long, val publishedId: Long)

/**
 * v30 renumbered `אור הישר/סדר נשים` 1478 -> 1526 (see [BookMoveLeafIds]). v28, the
 * stable release, and v29 have it on 1478, and clients key per-book settings, hidden
 * books, note drafts, user links and saved tabs on (title, categoryId). Moving it back
 * costs only the v30 pre-release one more change.
 */
internal val CATEGORY_ID_RESTORES: List<CategoryIdRestore> = listOf(
    CategoryIdRestore("תלמוד בבלי/אחרונים/אור הישר/סדר נשים", reallocatedId = 1526L, publishedId = 1478L),
)

/**
 * Stable ids for the leaves renameCategories creates for book_moves.csv destinations.
 *
 * These leaves used to get SQLite's implicit rowid, MAX(category.id) + 1: the id right
 * after the Sefaria stage's categories, which the build state may already reserve for
 * an Otzaria folder. The Otzaria stage then found that id taken and had to renumber its
 * folder (v19: 26 books misplaced; v30: `אור הישר/סדר נשים` 1478 -> 1526). The leaf ids
 * also shifted whenever the Sefaria tree grew or book_moves.csv rows were added,
 * removed or reordered.
 *
 * Now each leaf is keyed by its destination path in the build state's `id_lookup`
 * (kind `category`, prefix [BOOK_MOVE_LEAF_KEY_PREFIX]), so it keeps its id across
 * builds. A new key takes, in this order: its published id from [publishedIds] when
 * that id is free; the id of the Otzaria folder already at that path (see
 * [otzariaFolderId]); otherwise a fresh id above both the counter and the DB, never an
 * id another key holds.
 *
 * The Otzaria step matters for a folder that holds Otzaria books too. The Otzaria stage
 * runs after this one and puts its books into the leaf it finds, so a leaf on a new id
 * would renumber that folder the first time a book_moves row targets it. When the last
 * row into the folder is removed, the Otzaria stage recreates it on the leaf id
 * (IdAllocatorBindings.upsertOtzariaCategory), so it does not move back either.
 *
 * The build state must be ATTACHed to [conn] as [schema], so the id rows commit or
 * roll back together with the category rows.
 */
internal class BookMoveLeafIds(
    private val conn: Connection,
    private val schema: String = BOOK_MOVE_STATE_SCHEMA,
    private val publishedIds: Map<String, Long> = PUBLISHED_BOOK_MOVE_LEAF_IDS,
) {
    private val kind = IdTable.CATEGORY.lookupKind!!

    /**
     * Points each restore's key back at its published id, before any leaf is created.
     * Runs only while the key still holds the reallocated id and nothing (no key, no DB
     * row) uses the published one, so it applies once and is a no-op afterwards. The
     * reallocated id is left unheld; fresh ids only grow, so nothing is handed it again.
     */
    fun restore(restores: List<CategoryIdRestore>, logger: Logger): Int {
        var restored = 0
        for (restore in restores) {
            if (lookupId(restore.key) != restore.reallocatedId) continue
            if (lookupHoldsId(restore.publishedId) || dbHoldsId(restore.publishedId)) {
                logger.w {
                    "Category id ${restore.publishedId} for '${restore.key}' is taken; " +
                        "keeping ${restore.reallocatedId}"
                }
                continue
            }
            conn.prepareStatement(
                "UPDATE $schema.id_lookup SET id = ? WHERE kind = ? AND natural_key = ?",
            ).use { st ->
                st.setLong(1, restore.publishedId)
                st.setString(2, kind)
                st.setString(3, restore.key)
                check(st.executeUpdate() == 1) { "Failed to restore category id for '${restore.key}'" }
            }
            logger.i { "Restored category id for '${restore.key}': ${restore.reallocatedId} -> ${restore.publishedId}" }
            restored++
        }
        return restored
    }

    fun leafId(destSegments: List<String>): Long {
        val path = destSegments.joinToString("/")
        val key = BOOK_MOVE_LEAF_KEY_PREFIX + path
        lookupId(key)?.let { return it }

        val nextId = categoryNextId()
        val dbMax = queryMaxId(conn, "category")
        val published = publishedIds[path]?.takeIf { !lookupHoldsId(it) && !dbHoldsId(it) }
        val id = published ?: otzariaFolderId(path) ?: maxOf(nextId, dbMax + 1)

        conn.prepareStatement(
            "INSERT INTO $schema.id_lookup(kind, natural_key, id) VALUES (?, ?, ?)",
        ).use { st ->
            st.setString(1, kind)
            st.setString(2, key)
            st.setLong(3, id)
            st.executeUpdate()
        }
        if (id >= nextId) setCategoryNextId(id + 1)
        return id
    }

    /**
     * The id the Otzaria stage keeps for the folder at [path], when the leaf may take it:
     * a plain key (not [BOOK_MOVE_LEAF_KEY_PREFIX]) for the same folder holds it, no DB
     * row does, and every key that holds it is for that folder. "The same folder" is the
     * Otzaria stage's own test (Generator.findExistingCategory), segment by segment
     * [CategoryLabels.comparablePath]; a key spelled exactly as [path] wins a tie.
     */
    private fun otzariaFolderId(path: String): Long? {
        val comparable = CategoryLabels.comparablePath(path)
        val matches = plainKeys().filterKeys { CategoryLabels.comparablePath(it) == comparable }
        if (matches.isEmpty()) return null
        val id = matches[path] ?: matches.values.toSet().singleOrNull() ?: return null
        if (dbHoldsId(id)) return null
        val holders = keysHolding(id)
        val sameFolder = holders.all { key ->
            !key.startsWith(BOOK_MOVE_LEAF_KEY_PREFIX) && CategoryLabels.comparablePath(key) == comparable
        }
        return id.takeIf { sameFolder }
    }

    private fun plainKeys(): Map<String, Long> =
        conn.prepareStatement(
            "SELECT natural_key, id FROM $schema.id_lookup WHERE kind = ?",
        ).use { st ->
            st.setString(1, kind)
            st.executeQuery().use { rs ->
                buildMap {
                    while (rs.next()) {
                        val key = rs.getString(1)
                        if (!key.startsWith(BOOK_MOVE_LEAF_KEY_PREFIX)) put(key, rs.getLong(2))
                    }
                }
            }
        }

    private fun keysHolding(id: Long): List<String> =
        conn.prepareStatement(
            "SELECT natural_key FROM $schema.id_lookup WHERE kind = ? AND id = ?",
        ).use { st ->
            st.setString(1, kind)
            st.setLong(2, id)
            st.executeQuery().use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
        }

    private fun lookupId(key: String): Long? =
        conn.prepareStatement(
            "SELECT id FROM $schema.id_lookup WHERE kind = ? AND natural_key = ?",
        ).use { st ->
            st.setString(1, kind)
            st.setString(2, key)
            st.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else null }
        }

    private fun lookupHoldsId(id: Long): Boolean =
        conn.prepareStatement(
            "SELECT 1 FROM $schema.id_lookup WHERE kind = ? AND id = ? LIMIT 1",
        ).use { st ->
            st.setString(1, kind)
            st.setLong(2, id)
            st.executeQuery().use { it.next() }
        }

    private fun dbHoldsId(id: Long): Boolean =
        conn.prepareStatement("SELECT 1 FROM category WHERE id = ?").use { st ->
            st.setLong(1, id)
            st.executeQuery().use { it.next() }
        }

    private fun categoryNextId(): Long =
        conn.prepareStatement(
            "SELECT next_id FROM $schema.id_counters WHERE table_name = ?",
        ).use { st ->
            st.setString(1, IdTable.CATEGORY.tableName)
            st.executeQuery().use { rs -> if (rs.next()) rs.getLong(1) else 1L }
        }

    private fun setCategoryNextId(nextId: Long) {
        conn.prepareStatement(
            """
            INSERT INTO $schema.id_counters(table_name, next_id) VALUES (?, ?)
            ON CONFLICT(table_name) DO UPDATE SET next_id = excluded.next_id
            """.trimIndent(),
        ).use { st ->
            st.setString(1, IdTable.CATEGORY.tableName)
            st.setLong(2, nextId)
            st.executeUpdate()
        }
    }
}

private fun categoryLevel(conn: Connection, categoryId: Long): Int =
    conn.prepareStatement("SELECT level FROM category WHERE id = ?").use { stmt ->
        stmt.setLong(1, categoryId)
        stmt.executeQuery().use { rs -> if (rs.next()) rs.getInt(1) else 0 }
    }

/** Resolves the seforim.db path: positional arg, -PseforimDb, SEFORIM_DB, then build default. */
internal fun resolveSeforimDbPath(args: Array<String>): Path =
    Paths.get(
        args.getOrNull(0)
            ?: System.getProperty("seforimDb")
            ?: System.getenv("SEFORIM_DB")
            ?: Paths.get("build", "seforim.db").toString(),
    )

/**
 * Returns the lines of a required file (CSV or JSON) from the ForDB archive —
 * the pinned local archive when -P[FOR_DB_ARCHIVE_PROPERTY] is set, otherwise the
 * downloaded immutable release selected by `fordb_latest_pointer.json`. Cached per
 * process. Throws if the file is absent or any pointer/archive digest check fails.
 */
internal fun downloadRequiredForDbFile(fileName: String, logger: Logger): List<String> =
    forDbReleaseFiles(logger).getValue(fileName)

/**
 * Same, for a file that an older pinned archive may predate: absent returns null
 * rather than throwing.
 *
 * Only for inputs whose absence is a defined state. `sefaria_author_changes.csv`
 * qualifies — no file means no author is renamed, which is what every archive
 * published before it existed meant. Anything the build cannot do without stays
 * on [downloadRequiredForDbFile], where a missing file fails loudly.
 */
internal fun downloadOptionalForDbFile(fileName: String, logger: Logger): List<String>? =
    forDbReleaseFiles(logger)[fileName]

private var cachedForDbFiles: Map<String, List<String>>? = null

private fun forDbReleaseFiles(logger: Logger): Map<String, List<String>> {
    cachedForDbFiles?.let { return it }

    val pinnedArchive = System.getProperty(FOR_DB_ARCHIVE_PROPERTY)?.takeIf { it.isNotBlank() }
    val files = if (pinnedArchive != null) {
        readPinnedForDbArchive(Paths.get(pinnedArchive), logger)
    } else {
        downloadForDbArchive(logger)
    }

    val missing = FOR_DB_CSV_FILES.values.filterNot { it in files }
    check(missing.isEmpty()) {
        "ForDB release archive is missing required file(s): ${missing.joinToString()}"
    }

    cachedForDbFiles = files
    return files
}

/**
 * Reads ForDB files from a pinned local archive whose SHA-256 must equal the
 * digest supplied via -P[FOR_DB_SHA256_PROPERTY]. The release build takes this
 * path: the archive is downloaded once and its digest pinned by the dispatcher,
 * so every post-process JVM consumes byte-identical inputs and the build stays a
 * pure function of its inputs. Fails loudly on a missing digest or any mismatch —
 * it never silently falls back to the network.
 */
private fun readPinnedForDbArchive(archive: Path, logger: Logger): Map<String, List<String>> {
    val expectedSha = System.getProperty(FOR_DB_SHA256_PROPERTY)?.takeIf { it.isNotBlank() }?.lowercase()
        ?: throw IllegalStateException(
            "-P$FOR_DB_ARCHIVE_PROPERTY was given without -P$FOR_DB_SHA256_PROPERTY; " +
                "a pinned archive must carry its expected SHA-256"
        )
    check(archive.exists()) { "Pinned ForDB archive not found at $archive" }
    val bytes = archive.toFile().readBytes()
    val actualSha = sha256Hex(bytes)
    check(actualSha == expectedSha) {
        "Pinned ForDB archive SHA-256 mismatch at $archive: expected $expectedSha, got $actualSha"
    }
    logger.i { "Reading pinned ForDB archive $archive (sha256=$actualSha)" }
    return ByteArrayInputStream(bytes).use { readForDbZip(it) }
}

private fun downloadForDbArchive(logger: Logger): Map<String, List<String>> {
    val download = forDbReleaseArchive(logger)
    logger.i { "Downloading immutable ForDB archive ${download.tag} from ${download.url}" }
    return try {
        val bytes = OptimizedHttpClient.downloadStream(
            url = download.url,
            userAgent = FOR_DB_USER_AGENT,
            logger = logger
        ).stream.use { it.readBytes() }
        val actualSha = sha256Hex(bytes)
        check(actualSha == download.sha256) {
            "Immutable ForDB archive SHA-256 mismatch: expected ${download.sha256}, got $actualSha"
        }
        ByteArrayInputStream(bytes).use(::readForDbZip)
    } catch (e: Exception) {
        logger.e(e) { "Failed to download or read required ForDB release archive; aborting" }
        throw IllegalStateException("Failed to load required ForDB release archive", e)
    }
}

private fun sha256Hex(bytes: ByteArray): String {
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
    val sb = StringBuilder(digest.size * 2)
    for (b in digest) {
        val v = b.toInt() and 0xff
        sb.append("0123456789abcdef"[v ushr 4])
        sb.append("0123456789abcdef"[v and 0xf])
    }
    return sb.toString()
}

private data class ForDbDownload(val tag: String, val sha256: String, val url: String)

private fun forDbReleaseArchive(logger: Logger): ForDbDownload {
    val pointerBody = OptimizedHttpClient.fetchJson(FOR_DB_POINTER_URL, FOR_DB_USER_AGENT, logger)
    val pointerElement = Json.parseToJsonElement(pointerBody)
    check(
        pointerBody == Json.encodeToString(
            kotlinx.serialization.json.JsonElement.serializer(),
            pointerElement,
        ) + "\n"
    ) { "ForDB pointer must be canonical JSON with one trailing LF (duplicates are forbidden)" }
    val pointer = pointerElement.jsonObject
    check(
        pointer.keys == setOf(
            "schema_version", "tag", "asset", "sha256", "provenance_asset", "provenance_sha256"
        )
    ) { "ForDB pointer has an unexpected key set" }
    check(pointer.getValue("schema_version").jsonPrimitive.intOrNull == 1) {
        "Unsupported ForDB pointer schema"
    }
    fun requiredString(key: String): String {
        val primitive = pointer.getValue(key).jsonPrimitive
        check(primitive.isString) { "ForDB pointer $key must be a JSON string" }
        return primitive.content
    }
    val tag = requiredString("tag")
    val assetName = requiredString("asset")
    val expectedSha = requiredString("sha256")
    val provenanceAsset = requiredString("provenance_asset")
    val provenanceSha = requiredString("provenance_sha256")
    check(assetName == FOR_DB_ARCHIVE_NAME) { "ForDB pointer names an unexpected archive" }
    check(provenanceAsset == "fordb_provenance.json") { "ForDB pointer names an unexpected provenance asset" }
    check(expectedSha.matches(Regex("[0-9a-f]{64}"))) { "ForDB pointer archive digest is invalid" }
    check(provenanceSha.matches(Regex("[0-9a-f]{64}"))) { "ForDB pointer provenance digest is invalid" }
    check(tag == "fordb-sha256-$expectedSha") { "ForDB pointer tag does not match its archive digest" }

    val releaseBody = OptimizedHttpClient.fetchJson(
        FOR_DB_RELEASE_API_PREFIX + tag,
        FOR_DB_USER_AGENT,
        logger,
    )
    val release = Json.parseToJsonElement(releaseBody).jsonObject
    check(release.getValue("tag_name").jsonPrimitive.content == tag) {
        "GitHub returned a different ForDB release tag"
    }
    val assets = release.getValue("assets").jsonArray
    val matches = assets.filter {
        it.jsonObject["name"]?.jsonPrimitive?.content == FOR_DB_ARCHIVE_NAME
    }
    check(matches.size == 1) { "Immutable ForDB release must contain exactly one $FOR_DB_ARCHIVE_NAME asset" }
    val url = matches.single().jsonObject.getValue("browser_download_url").jsonPrimitive
    check(url.isString && url.content.startsWith("https://github.com/Otzaria/otzaria-library/releases/download/")) {
        "Immutable ForDB release returned an unexpected asset URL"
    }
    return ForDbDownload(tag, expectedSha, url.content)
}

private fun readForDbZip(stream: InputStream): Map<String, List<String>> {
    val files = mutableMapOf<String, List<String>>()
    ZipInputStream(stream).use { zip ->
        var entry = zip.nextEntry
        while (entry != null) {
            if (!entry.isDirectory) {
                val normalizedName = entry.name.replace('\\', '/')
                if (normalizedName.startsWith("ForDB/")) {
                    val fileName = normalizedName.removePrefix("ForDB/")
                    // Capture text assets directly under ForDB/ — CSV rule files
                    // plus all_metadata.json (consumed by seedAllMetadata).
                    if ('/' !in fileName && (fileName.endsWith(".csv") || fileName.endsWith(".json"))) {
                        val bytes = ByteArrayOutputStream()
                        zip.copyTo(bytes)
                        files[fileName] = ByteArrayInputStream(bytes.toByteArray())
                            .bufferedReader(StandardCharsets.UTF_8)
                            .readLines()
                    }
                }
            }
            zip.closeEntry()
            entry = zip.nextEntry
        }
    }
    return files
}
