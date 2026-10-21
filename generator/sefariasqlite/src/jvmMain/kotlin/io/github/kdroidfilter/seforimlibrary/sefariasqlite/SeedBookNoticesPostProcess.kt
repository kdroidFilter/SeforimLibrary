package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.patch.SCHEMA_7_TABLE_DDL
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/** Per-book publisher banner text, by source with per-book overrides. Required. */
internal const val BOOK_BANNERS_FILE = "book_banners.csv"

/** Per-book protection level, by source with per-book overrides. Required. */
internal const val BOOK_PROTECTION_FILE = "book_protection.csv"

private val BANNER_HEADER = listOf("id", "sourceName", "bookName", "text")
private val PROTECTION_HEADER = listOf("sourceName", "bookName", "level")

/**
 * One CSV row: [bookName] empty = the default for every book of [sourceName];
 * a named row overrides that default for the book whose final title it is.
 */
internal data class BookNoticeRow<T>(val sourceName: String, val bookName: String, val value: T)

/** A banner text and its stable `banner.id`, set by hand in the CSV and never reused. */
internal data class BannerText(val id: Long, val text: String)

internal data class BookNoticeRows(
    val banners: List<BookNoticeRow<BannerText>>,
    val protections: List<BookNoticeRow<Int>>,
)

internal data class BookNoticesResult(
    val banners: Int,
    val protections: Int,
    /** Rows (`source / title`, `source / *` for a default) that matched no book. */
    val unmatchedBanners: List<String>,
    val unmatchedProtection: List<String>,
)

/**
 * Seeds the `banner`, `book_banner` and `book_protection` tables from
 * otzaria-library/ForDB/[BOOK_BANNERS_FILE] and [BOOK_PROTECTION_FILE].
 *
 * Both files are required: a missing one fails the build. Runs after
 * every stage that writes or renames books, so `bookName` is the final DB title.
 * A protection row (per book or source default) that matches no book fails the build:
 * dropping protection silently is worse than a failed release.
 *
 * Usage:
 *   ./gradlew :sefariasqlite:seedBookNotices -PseforimDb=/path/to/seforim.db
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("SeedBookNotices")
    val dbPath = resolveSeforimDbPath(args)
    if (!dbPath.exists()) {
        logger.e { "DB not found at $dbPath" }
        exitProcess(1)
    }
    try {
        val rows = loadBookNoticeRows(logger)
        DriverManager.getConnection("jdbc:sqlite:$dbPath").use { conn ->
            conn.autoCommit = false
            val result = try {
                applyBookNotices(conn, rows, logger).also { conn.commit() }
            } catch (e: Exception) {
                runCatching { conn.rollback() }
                throw e
            }
            logger.i { "Book notices done: banners=${result.banners} protections=${result.protections}" }
        }
    } catch (e: Exception) {
        logger.e(e) { "Failed to seed book banners/protection; aborting" }
        exitProcess(1)
    }
}

/** Reads and parses both required files from the ForDB archive ([read] is for tests). */
internal fun loadBookNoticeRows(
    logger: Logger,
    read: (String) -> List<String> = { downloadRequiredForDbFile(it, logger) },
): BookNoticeRows = BookNoticeRows(
    banners = parseBookBanners(forDbText(read(BOOK_BANNERS_FILE))),
    protections = parseBookProtection(forDbText(read(BOOK_PROTECTION_FILE))),
)

/** The archive reader splits files into lines; quoted fields may span them, so re-join first. */
internal fun forDbText(lines: List<String>): String = lines.joinToString("\n").removePrefix("\uFEFF")

internal fun parseBookBanners(text: String): List<BookNoticeRow<BannerText>> {
    val rows = parseNoticeRows(text, BOOK_BANNERS_FILE, BANNER_HEADER) { fields, where ->
        require(BANNER_ID.matches(fields[0])) { "$where has id '${fields[0]}'; expected an integer >= 1" }
        require(fields[3].isNotBlank()) { "$where has an empty text" }
        BannerText(fields[0].toLong(), fields[3])
    }
    val repeated = rows.groupBy { it.value.id }.filterValues { it.size > 1 }.keys
    require(repeated.isEmpty()) { "$BOOK_BANNERS_FILE repeats id(s) ${repeated.sorted().joinToString()}" }
    return rows
}

internal fun parseBookProtection(text: String): List<BookNoticeRow<Int>> =
    parseNoticeRows(text, BOOK_PROTECTION_FILE, PROTECTION_HEADER) { fields, where ->
        val level = fields[2].trim().toIntOrNull()
        require(level != null && level >= 1) { "$where has level '${fields[2]}'; expected an integer >= 1" }
        level
    }

private val BANNER_ID = Regex("[1-9][0-9]{0,17}")

private fun <T> parseNoticeRows(
    text: String,
    fileName: String,
    header: List<String>,
    parseValue: (List<String>, String) -> T,
): List<BookNoticeRow<T>> {
    val records = parseForDbCsvRecords(text)
    if (records.isEmpty()) return emptyList()
    require(records.first() == header) { "$fileName must start with the header ${header.joinToString(",")}" }
    val source = header.indexOf("sourceName")
    val book = header.indexOf("bookName")
    val seen = HashSet<Pair<String, String>>()
    return records.drop(1).mapIndexed { index, fields ->
        val where = "$fileName record ${index + 2}"
        require(fields.size == header.size && fields[source].isNotBlank()) { "$where is malformed: $fields" }
        require(seen.add(fields[source] to fields[book])) {
            "$where repeats source '${fields[source]}' book '${fields[book]}'"
        }
        BookNoticeRow(fields[source], fields[book], parseValue(fields, where))
    }
}

/**
 * Replaces the tables (DELETE + INSERT in key order). Source defaults expand to
 * every book of the source; a per-book row wins over its source's default.
 * `banner` keeps only the texts some book uses, verbatim: the reader expands `{title}`.
 * [strict] = false (the ForDB gate, run on the last published DB, which lacks new and
 * private books) turns unmatched protection rows into warnings.
 */
internal fun applyBookNotices(
    conn: Connection,
    rows: BookNoticeRows,
    logger: Logger,
    strict: Boolean = true,
): BookNoticesResult {
    conn.createStatement().use { st ->
        listOf("banner", "book_banner", "book_protection").forEach { st.execute(SCHEMA_7_TABLE_DDL.getValue(it)) }
    }

    data class Book(val id: Long, val title: String, val source: String)
    val books = conn.createStatement().use { st ->
        st.executeQuery(
            "SELECT b.id, b.title, s.name FROM book b JOIN source s ON s.id = b.sourceId ORDER BY b.id",
        ).use { rs -> buildList { while (rs.next()) add(Book(rs.getLong(1), rs.getString(2), rs.getString(3))) } }
    }
    val bySource = books.groupBy { it.source }
    val bySourceAndTitle = books.groupBy { it.source to it.title }

    fun <T> resolve(entries: List<BookNoticeRow<T>>, unmatched: MutableList<String>): Map<Long, Pair<Book, T>> {
        val out = HashMap<Long, Pair<Book, T>>()
        for (row in entries.filter { it.bookName.isEmpty() }) {
            val sourceBooks = bySource[row.sourceName]
            if (sourceBooks == null) unmatched += "${row.sourceName} / *"
            sourceBooks?.forEach { out[it.id] = it to row.value }
        }
        for (row in entries.filter { it.bookName.isNotEmpty() }) {
            val matches = bySourceAndTitle[row.sourceName to row.bookName]
            if (matches == null) unmatched += "${row.sourceName} / ${row.bookName}"
            matches?.forEach { out[it.id] = it to row.value }
        }
        return out
    }

    val unmatchedProtection = mutableListOf<String>()
    val protections = resolve(rows.protections, unmatchedProtection)
    check(!strict || unmatchedProtection.isEmpty()) {
        "$BOOK_PROTECTION_FILE rows match no book in the DB (source / title after renames): " +
            unmatchedProtection.joinToString()
    }
    val unmatchedBanners = mutableListOf<String>()
    val banners = resolve(rows.banners, unmatchedBanners)

    conn.createStatement().use { st ->
        st.execute("DELETE FROM book_banner")
        st.execute("DELETE FROM banner")
        st.execute("DELETE FROM book_protection")
    }
    conn.prepareStatement("INSERT INTO banner (id, text) VALUES (?, ?)").use { ps ->
        for (banner in banners.values.map { it.second }.distinct().sortedBy { it.id }) {
            ps.setLong(1, banner.id)
            ps.setString(2, banner.text)
            ps.executeUpdate()
        }
    }
    conn.prepareStatement("INSERT INTO book_banner (bookId, bannerId) VALUES (?, ?)").use { ps ->
        for ((id, entry) in banners.toSortedMap()) {
            ps.setLong(1, id)
            ps.setLong(2, entry.second.id)
            ps.executeUpdate()
        }
    }
    conn.prepareStatement("INSERT INTO book_protection (bookId, level) VALUES (?, ?)").use { ps ->
        for ((id, entry) in protections.toSortedMap()) {
            ps.setLong(1, id)
            ps.setInt(2, entry.second)
            ps.executeUpdate()
        }
    }

    if (unmatchedBanners.isNotEmpty() || unmatchedProtection.isNotEmpty()) {
        logger.w {
            "Book notices: ${unmatchedBanners.size} banner row(s) and ${unmatchedProtection.size} protection " +
                "row(s) matched no book: ${(unmatchedBanners + unmatchedProtection).take(20).joinToString()}"
        }
        GeneratorReport.write("book-notices-unmatched", logger) {
            putStrings("banners", unmatchedBanners)
            putStrings("protection", unmatchedProtection)
        }
    }
    return BookNoticesResult(banners.size, protections.size, unmatchedBanners, unmatchedProtection)
}
