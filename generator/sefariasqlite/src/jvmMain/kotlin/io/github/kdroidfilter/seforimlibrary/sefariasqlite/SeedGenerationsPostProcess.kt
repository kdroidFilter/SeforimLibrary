package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/**
 * Seeds the `generation` table and links books to their generation, driven by
 * otzaria-library/ForDB/generations.csv (`שם ספר,קבוצת דור` with header).
 *
 * Runs AFTER appendOtzaria so both Sefaria- and Otzaria-stage books are
 * considered. Linking is purely by book title — no transitive author-level
 * propagation — so per-book generations in the CSV are preserved (e.g. the
 * empty-author bucket where books legitimately span eras, אברבנאל's
 * ראשונים/אחרונים split).
 *
 * Download failures are fatal because silently skipping generation seeding can
 * produce an invalid DB delta.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedGenerations -PseforimDb=/path/to/seforim.db
 *
 * Env alternatives:
 *   SEFORIM_DB
 */
private val GENERATIONS_FILE = FOR_DB_CSV_FILES.getValue("generations")

fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SeedGenerations")

    val dbPathStr = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()
    val dbPath = Paths.get(dbPathStr)

    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }

    logger.i { "Seeding generations in $dbPath" }

    val rows = parseGenerations(downloadRequiredForDbCsv(GENERATIONS_FILE, logger), logger)

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            val result = try {
                applyGenerations(conn, rows, logger).also {
                    conn.commit()
                }
            } catch (e: Exception) {
                runCatching { conn.rollback() }.onFailure { logger.w(it) { "Rollback failed" } }
                throw e
            }

            logger.i {
                "Generations done: seeded=${result.generationsCreated} " +
                    "book links=${result.linksCreated} unmatched=${result.unmatched}"
            }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to seed generations; aborting" }
        exitProcess(1)
    }
}

/**
 * `שם ספר,קבוצת דור` rows. Requires the header row, ignores blank rows with a
 * visible warning, and fails on malformed non-blank data rows.
 */
private fun parseGenerations(lines: List<String>, logger: Logger): List<Pair<String, String>> {
    val sourceName = GENERATIONS_FILE
    val blankRows = lines.count { parseForDbCsvLine(it).all { field -> field.trim().isEmpty() } }
    if (blankRows > 0) {
        logger.w { "$sourceName: ignoring $blankRows blank row(s)" }
    }
    val nonBlank = lines.filter { it.isNotBlank() }
    require(nonBlank.isNotEmpty()) { "$sourceName is empty" }
    require("שם ספר" in nonBlank.first()) { "$sourceName must start with a שם ספר header" }
    val duplicateHeader = nonBlank.drop(1).indexOfFirst { "שם ספר" in parseForDbCsvLine(it).firstOrNull().orEmpty() }
    require(duplicateHeader < 0) {
        "$sourceName contains a duplicate header at non-blank row ${duplicateHeader + 2}"
    }
    return parseRequiredCsvRows(nonBlank.drop(1), sourceName, minFields = 2)
        .map { f -> f[0] to f[1] }
}

private data class GenerationApplyResult(
    val generationsCreated: Int,
    val linksCreated: Int,
    val unmatched: Int,
)

/**
 * Seeds the `generation` table with distinct names and links each book to its
 * generation via `book_generation`. Matching is exact by book title; data
 * mismatches fail the task so the CSV can be corrected instead of guessed.
 * INSERT OR IGNORE keeps re-runs idempotent.
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

    val books = ArrayList<Pair<Long, String>>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, title FROM book").use { rs ->
            while (rs.next()) books += rs.getLong(1) to rs.getString(2)
        }
    }
    val exactMap = books.groupBy({ it.second }, { it.first })

    var linksCreated = 0
    val unmatchedTitles = mutableListOf<String>()
    conn.prepareStatement("INSERT OR IGNORE INTO book_generation(bookId, generationId) VALUES (?, ?)").use { linkStmt ->
        for ((bookTitle, genName) in rows) {
            val genId = nameToId[genName] ?: continue
            val bookId = findBookIdForGeneration(exactMap, bookTitle, logger)
            if (bookId == null) {
                unmatchedTitles += bookTitle
                continue
            }
            linkStmt.setLong(1, bookId)
            linkStmt.setLong(2, genId)
            linksCreated += linkStmt.executeUpdate()
        }
    }
    // Don't abort the whole build when generations.csv references books that are
    // not in the current library (upstream otzaria-library vs ForDB drift): keep
    // every link that DID match and just warn about the rest.
    if (unmatchedTitles.isNotEmpty()) {
        logger.w {
            "Generation CSV has ${unmatchedTitles.size} unmatched book title(s) (skipped): " +
                unmatchedTitles.take(20).joinToString()
        }
    }
    return GenerationApplyResult(generationsCreated, linksCreated, 0)
}

// `book.title` is not UNIQUE in the schema, so even an exact match can return
// more than one row. Fail rather than arbitrarily picking one.
private fun findBookIdForGeneration(
    exactMap: Map<String, List<Long>>,
    title: String,
    logger: Logger,
): Long? {
    exactMap[title]?.let { return pickOne(it, title, "exact", logger) }
    return null
}

private fun pickOne(matches: List<Long>, title: String, tier: String, logger: Logger): Long? =
    if (matches.size == 1) matches.single()
    else {
        logger.e { "Generation link: '$title' has multiple $tier matches" }
        error("Generation link '$title' has ${matches.size} $tier matches")
    }
