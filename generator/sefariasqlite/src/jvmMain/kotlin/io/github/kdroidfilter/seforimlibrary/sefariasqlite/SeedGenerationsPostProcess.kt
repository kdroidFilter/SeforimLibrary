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
 * otzaria-library/ForDB/סדר הדורות.csv (`שם ספר,קבוצת דור` with header).
 *
 * Runs AFTER appendOtzaria so both Sefaria- and Otzaria-stage books are
 * considered. Linking is purely by book title — no transitive author-level
 * propagation — so per-book generations in the CSV are preserved (e.g. the
 * empty-author bucket where books legitimately span eras, אברבנאל's
 * ראשונים/אחרונים split).
 *
 * Download failures are non-fatal: a GitHub outage logs a warning and the
 * pipeline continues with no seeding (preferable to a red build).
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedGenerations -PseforimDb=/path/to/seforim.db
 *
 * Env alternatives:
 *   SEFORIM_DB
 */
private const val GENERATIONS_URL = "$FOR_DB_BASE/%D7%A1%D7%93%D7%A8%20%D7%94%D7%93%D7%95%D7%A8%D7%95%D7%AA.csv"

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

    val rows = parseGenerations(downloadCsv(GENERATIONS_URL, logger), logger)

    try {
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false

            val result = try {
                val res = applyGenerations(conn, rows, logger)
                conn.commit()
                res
            } catch (e: Exception) {
                runCatching { conn.rollback() }.onFailure { logger.w(it) { "Rollback failed" } }
                logger.w(e) { "Generation seeding failed; skipping section" }
                GenerationApplyResult(0, 0, rows.size)
            }

            logger.i {
                "Generations done: seeded=${result.generationsCreated} " +
                    "book links=${result.linksCreated} unmatched=${result.unmatched}"
            }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to open or commit DB; aborting" }
        exitProcess(1)
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
 * generation via `book_generation`. Loads books once into in-memory maps so
 * the three-tier matching (exact / punct-strip / TRIM) is O(1) per CSV row
 * instead of full table scans on REPLACE/TRIM expressions. INSERT OR IGNORE
 * keeps re-runs idempotent.
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
    val strippedMap = books.groupBy({ stripTitlePunct(it.second) }, { it.first })
    val trimmedMap = books.groupBy({ it.second.trim() }, { it.first })

    var linksCreated = 0
    var unmatched = 0
    conn.prepareStatement("INSERT OR IGNORE INTO book_generation(bookId, generationId) VALUES (?, ?)").use { linkStmt ->
        for ((bookTitle, genName) in rows) {
            val genId = nameToId[genName] ?: continue
            val bookId = findBookIdForGeneration(exactMap, strippedMap, trimmedMap, bookTitle, logger)
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
    return GenerationApplyResult(generationsCreated, linksCreated, unmatched)
}

// Three-tier fallback: exact, then TRIM (for DB titles imported with stray
// leading/trailing whitespace from upstream sources), then punctuation-strip
// (for CSVs that use the bare form like רדק vs רד״ק). TRIM before punct-strip
// so a title like "רדק " resolves to the unique "רדק" entry rather than
// hitting the broader punct-strip tier which matches both "רדק" and "רד״ק".
// `book.title` is not UNIQUE in the schema, so any tier can return >1 — log
// and skip rather than arbitrarily picking one.
private fun findBookIdForGeneration(
    exactMap: Map<String, List<Long>>,
    strippedMap: Map<String, List<Long>>,
    trimmedMap: Map<String, List<Long>>,
    title: String,
    logger: Logger,
): Long? {
    exactMap[title]?.let { return pickOne(it, title, "exact", logger) }
    trimmedMap[title.trim()]?.let { return pickOne(it, title, "TRIM", logger) }
    strippedMap[stripTitlePunct(title)]?.let { return pickOne(it, title, "punct-strip", logger) }
    return null
}

private fun pickOne(matches: List<Long>, title: String, tier: String, logger: Logger): Long? =
    if (matches.size == 1) matches.single()
    else {
        logger.w { "Generation link: '$title' has multiple $tier matches; skipping" }
        null
    }
