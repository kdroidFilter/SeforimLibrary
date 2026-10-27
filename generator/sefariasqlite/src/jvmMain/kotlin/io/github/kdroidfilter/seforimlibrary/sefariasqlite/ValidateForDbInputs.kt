package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.sql.Connection
import java.sql.DriverManager
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlin.io.path.exists
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

/**
 * Dry-run validation of EVERY ForDB rename/move rule against a real seforim.db —
 * the exact appliers, in the exact build order (category renames → category moves →
 * book renames → book moves), inside a transaction that is ALWAYS rolled back.
 *
 * Unlike the build's apply pass, a failing rule does not abort: every failure is
 * collected and reported together, so one broken path (the "מהרש"ם" class: a
 * source/destination category path that no longer resolves) yields a complete,
 * actionable report instead of a deep single-row crash hours into a build.
 *
 * Consumers:
 *  - `./gradlew :sefariasqlite:validateForDbInputs -PseforimDb=… [-PforDbArchive=… -PforDbSha256=…]`
 *    (update-fordb runs this against the candidate archive + the last released DB
 *    BEFORE advancing the immutable pointer, so a broken rule set never gets published);
 *  - the build's renameCategories pass runs the same collector as a PREFLIGHT and
 *    fails with the full report before any mutation is committed.
 *
 * Rules apply-as-they-go inside the rolled-back transaction, so later rules see
 * earlier successful rules' effects — identical semantics to the real pass. Each
 * failing rule is isolated by a savepoint, preventing its partial writes from
 * contaminating later diagnostics.
 */

internal data class ForDbRuleFailure(val section: String, val rule: String, val message: String)

internal fun collectForDbRuleFailures(
    conn: Connection,
    categoryRenames: List<CategoryRename>,
    categoryMoves: List<CategoryMove>,
    bookRenames: List<Pair<String, String>>,
    bookMoves: List<BookMove>,
    logger: Logger,
    categoryDescriptionOverrides: List<CategoryDescriptionOverride> = emptyList(),
): List<ForDbRuleFailure> {
    val failures = mutableListOf<ForDbRuleFailure>()
    val descriptionOverridesByPath = categoryDescriptionOverrides.associateBy { it.canonicalPath }

    fun <T> section(name: String, items: List<T>, describe: (T) -> String, apply: (T) -> Unit) {
        for ((index, item) in items.withIndex()) {
            // Keep the effects of earlier successful rules (real build order), but
            // never let a partially-applied failing rule contaminate the checks that
            // follow it.  applyBookMove can create a destination leaf before a later
            // assertion fails, so catch-without-savepoint produces false cascades.
            val savepoint = conn.setSavepoint("fordb_${name.replace('.', '_')}_$index")
            try {
                apply(item)
                conn.releaseSavepoint(savepoint)
            } catch (e: Exception) {
                conn.rollback(savepoint)
                conn.releaseSavepoint(savepoint)
                failures += ForDbRuleFailure(name, describe(item), e.message ?: e.toString())
            }
        }
    }

    // EXACT build order — RenameCategoriesPostProcess.main applies in this sequence.
    section("category_renames.csv", categoryRenames, { "'${it.oldName}' -> '${it.newName}'" }) {
        renameOrMergeCategory(conn, it, logger, descriptionOverridesByPath)
    }
    section("category_moves.csv", categoryMoves, { "'${it.sourcePath}' -> '${it.destParentPath}'" }) {
        applyCategoryMove(conn, it, logger)
    }
    section("book_renames.csv", bookRenames, { "'${it.first}' -> '${it.second}'" }) {
        renameBookTitle(conn, it.first, it.second, logger)
    }
    section("book_moves.csv", bookMoves, { "'${it.name}': '${it.sourcePath}' -> '${it.destPath}'" }) {
        applyBookMove(conn, it, logger)
    }
    return failures
}

internal fun reportForDbRuleFailures(failures: List<ForDbRuleFailure>, logger: Logger) {
    for (failure in failures) {
        logger.e { "[${failure.section}] ${failure.rule}: ${failure.message}" }
    }
}

fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("ValidateForDbInputs")

    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }

    val categoryRenames =
        parseCategoryRenames(downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("categoryRenames"), logger))
    val bookRenames =
        parsePairs(
            downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("bookRenames"), logger),
            FOR_DB_CSV_FILES.getValue("bookRenames"),
        )
    val bookMoves =
        parseBookMoves(downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("bookMoves"), logger), logger)
    val categoryMoves =
        parseCategoryMoves(downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("categoryMoves"), logger), logger)
    // Parse every remaining ForDB consumer before opening a writable DB. A malformed
    // generations/metadata file can therefore never pass this gate merely because
    // the rename/move subset was valid.
    val generations = loadGenerationRows(logger)
    val bulkMetadata = parseBulkMetadata(downloadRequiredForDbFile("all_metadata.json", logger))
    val descriptionOverrides = parseDescriptionOverrides(
        downloadRequiredForDbFile("sefaria_metadata_changes.csv", logger),
    )
    val categoryDescriptionOverrides = parseCategoryDescriptionOverrides(
        downloadRequiredForDbFile(FOR_DB_CSV_FILES.getValue("categoryDescriptions"), logger),
    )
    // Optional: an archive published before the file existed simply has none.
    // Parsed here all the same, so a malformed row fails this gate instead of
    // the Sefaria import, hours into a release build.
    val authorCanonicalNames = SefariaAuthorCanonicalNames.load(logger)
    // Library file lists at the release's library commit and at the candidate commit.
    val baseTreePath = System.getProperty("libraryTreeBase")?.takeIf { it.isNotBlank() }
    val candidateTreePath = System.getProperty("libraryTreeCandidate")?.takeIf { it.isNotBlank() }
    require((baseTreePath == null) == (candidateTreePath == null)) {
        "libraryTreeBase and libraryTreeCandidate must be given together"
    }
    val libraryTrees = baseTreePath?.let { readLibraryTree(Path.of(it)) to readLibraryTree(Path.of(candidateTreePath!!)) }
    if (libraryTrees == null) logger.w { "No library trees given: categories of removed library folders are not checked" }
    val bookNotices = loadBookNoticeRows(logger)

    DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
        conn.autoCommit = false
        val failures = try {
            collectForDbRuleFailures(
                conn, categoryRenames, categoryMoves, bookRenames, bookMoves, logger, categoryDescriptionOverrides,
            )
        } finally {
            conn.rollback() // dry-run only — the DB is NEVER mutated by this task
        }
        if (failures.isNotEmpty()) {
            reportForDbRuleFailures(failures, logger)
            logger.e {
                "ForDB validation FAILED: ${failures.size} rule(s) cannot apply against $dbPath. " +
                    "Fix the rows above (nothing was modified, nothing may be auto-deleted)."
            }
            exitProcess(1)
        }
    }

    // Exercise the actual downstream appliers on a physical copy. This catches
    // schema, duplicate-title, allocator/buildstate and category-description errors
    // without mutating even one byte of the supplied DB/buildstate.
    val tempDir = Files.createTempDirectory("validate-all-fordb-")
    try {
        val tempDb = tempDir.resolve("seforim.db")
        Files.copy(dbPath, tempDb, StandardCopyOption.REPLACE_EXISTING)
        val sourceBuildState = Path.of("$dbPath.buildstate")
        val tempBuildState = Path.of("$tempDb.buildstate")
        if (Files.exists(sourceBuildState)) {
            Files.copy(sourceBuildState, tempBuildState, StandardCopyOption.REPLACE_EXISTING)
        }
        var lostCategories: Map<Long, String> = emptyMap()
        DriverManager.getConnection("jdbc:sqlite:$tempDb").use { conn ->
            conn.autoCommit = false
            val replayFailures = collectForDbRuleFailures(
                conn, categoryRenames, categoryMoves, bookRenames, bookMoves, logger, categoryDescriptionOverrides,
            )
            check(replayFailures.isEmpty()) { "rename/move replay unexpectedly diverged on the validation copy" }
            lostCategories = libraryTrees?.let { (base, candidate) ->
                findCategoriesLostWithLibraryFolders(
                    conn, base, candidate, categoryRenames, bookRenames, categoryMoves, bookMoves,
                )
            }.orEmpty()
            applyGenerations(conn, generations, logger)
            conn.commit()
        }
        val retitledBooks = DriverManager.getConnection("jdbc:sqlite:$tempDb").use(::loadRetitledBooks)
        runBlocking {
            val driver = JdbcSqliteDriver("jdbc:sqlite:$tempDb")
            val repository = SeforimRepository(tempDb.toString(), driver)
            try {
                val allocator = InMemoryIdAllocator.load(
                    tempBuildState.takeIf { Files.exists(it) },
                    Logger.withTag("ValidateForDbAllocator"),
                )
                val bindings = IdAllocatorBindings(allocator, repository)
                failOnLostCategoryDescriptions(repository, categoryDescriptionOverrides, lostCategories)
                applyMetadata(repository, bindings, bulkMetadata, descriptionOverrides, retitledBooks, logger)
                applyCategoryDescriptionOverrides(repository, categoryDescriptionOverrides, logger)
            } finally {
                repository.close()
            }
        }
        // Warnings only: this DB is the last published one, without new or private books.
        // The build itself (seedBookNotices) fails on an unmatched protection row.
        DriverManager.getConnection("jdbc:sqlite:$tempDb").use { conn ->
            conn.autoCommit = false
            applyBookNotices(conn, bookNotices, logger, strict = false)
            conn.commit()
        }
        // Fails like the build on a missing list or an unreadable row; unmatched refs only warn.
        val divineNameOverrides = DriverManager.getConnection("jdbc:sqlite:$tempDb").use {
            validateDivineNameOverrides(it, logger)
        }
        logger.i {
            "ForDB validation passed all real consumers: ${categoryRenames.size} category renames, " +
                "${categoryMoves.size} category moves, ${bookRenames.size} book renames, " +
                "${bookMoves.size} book moves, ${generations.size} generation rows, " +
                "${bulkMetadata.size} bulk metadata rows, ${descriptionOverrides.size} description overrides, " +
                "${categoryDescriptionOverrides.size} category-description overrides, " +
                "${authorCanonicalNames.size} author renames, ${bookNotices.banners.size} banner rows, " +
                "${bookNotices.protections.size} protection rows, $divineNameOverrides divine-name override rows. " +
                "Original DB/buildstate untouched."
        }
    } finally {
        tempDir.toFile().deleteRecursively()
    }
}
