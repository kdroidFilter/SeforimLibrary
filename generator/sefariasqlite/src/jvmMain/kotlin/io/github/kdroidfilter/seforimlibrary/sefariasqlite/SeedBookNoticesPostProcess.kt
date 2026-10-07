package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.patch.OPTIONAL_PATCH_TABLES
import io.github.kdroidfilter.seforimlibrary.common.reports.GeneratorReport
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.DriverManager
import kotlin.io.path.exists
import kotlin.system.exitProcess

/** Per-book publisher banner text, by source with per-book overrides. */
internal const val BOOK_BANNERS_FILE = "book_banners.csv"

/** Per-book protection level, by source with per-book overrides. */
internal const val BOOK_PROTECTION_FILE = "book_protection.csv"

private val BANNER_HEADER = listOf("sourceName", "bookName", "text")
private val PROTECTION_HEADER = listOf("sourceName", "bookName", "level")

/**
 * One CSV row: [bookName] empty = the default for every book of [sourceName];
 * a named row overrides that default for the book whose final title it is.
 */
internal data class BookNoticeRow<T>(val sourceName: String, val bookName: String, val value: T)

internal data class BookNoticeRows(
    val banners: List<BookNoticeRow<String>>,
    val protections: List<BookNoticeRow<Int>>,
)

internal data class BookNoticesResult(
    val banners: Int,
    val protections: Int,
    /** Rows (`source / title`) that matched no book; for banners only a warning. */
    val unmatchedBanners: List<String>,
    val unmatchedProtectionSources: List<String>,
)

/**
 * Seeds the optional `book_banner` and `book_protection` tables from
 * otzaria-library/ForDB/[BOOK_BANNERS_FILE] and [BOOK_PROTECTION_FILE].
 *
 * Both files are optional: an archive without one leaves its table empty. Runs after
 * every stage that writes or renames books, so `bookName` is the final DB title.
 * A protection row naming a book that is not in the DB fails the build — dropping
 * protection silently is worse than a failed release.
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

/** Reads and parses both optional files from the ForDB archive. */
internal fun loadBookNoticeRows(logger: Logger): BookNoticeRows = BookNoticeRows(
    banners = downloadOptionalForDbFile(BOOK_BANNERS_FILE, logger)
        ?.let { parseBookBanners(forDbText(it)) }.orEmpty(),
    protections = downloadOptionalForDbFile(BOOK_PROTECTION_FILE, logger)
        ?.let { parseBookProtection(forDbText(it)) }.orEmpty(),
)

/** The archive reader splits files into lines; quoted fields may span them, so re-join first. */
internal fun forDbText(lines: List<String>): String = lines.joinToString("\n").removePrefix("﻿")

internal fun parseBookBanners(text: String): List<BookNoticeRow<String>> =
    parseNoticeRows(text, BOOK_BANNERS_FILE, BANNER_HEADER) { value, where ->
        require(value.isNotBlank()) { "$where has an empty text" }
        value
    }

internal fun parseBookProtection(text: String): List<BookNoticeRow<Int>> =
    parseNoticeRows(text, BOOK_PROTECTION_FILE, PROTECTION_HEADER) { value, where ->
        val level = value.trim().toIntOrNull()
        require(level != null && level >= 1) { "$where has level '$value'; expected an integer >= 1" }
        level
    }

private fun <T> parseNoticeRows(
    text: String,
    fileName: String,
    header: List<String>,
    parseValue: (String, String) -> T,
): List<BookNoticeRow<T>> {
    val records = parseForDbCsvRecords(text)
    if (records.isEmpty()) return emptyList()
    require(records.first() == header) { "$fileName must start with the header ${header.joinToString(",")}" }
    val seen = HashSet<Pair<String, String>>()
    return records.drop(1).mapIndexed { index, fields ->
        val where = "$fileName record ${index + 2}"
        require(fields.size == header.size && fields[0].isNotBlank()) { "$where is malformed: $fields" }
        require(seen.add(fields[0] to fields[1])) { "$where repeats source '${fields[0]}' book '${fields[1]}'" }
        BookNoticeRow(fields[0], fields[1], parseValue(fields[2], where))
    }
}

/**
 * Expands `{title}`: percent-encoded (space → %20) inside a `[label](target)` link
 * target, raw everywhere else.
 */
internal fun expandBannerTitle(template: String, title: String): String {
    val encoded = URLEncoder.encode(title, StandardCharsets.UTF_8).replace("+", "%20")
    val linked = LINK_PATTERN.replace(template) { m ->
        "[${m.groupValues[1]}](${m.groupValues[2].replace(TITLE_PLACEHOLDER, encoded)})"
    }
    return linked.replace(TITLE_PLACEHOLDER, title)
}

private const val TITLE_PLACEHOLDER = "{title}"
private val LINK_PATTERN = Regex("""\[([^\]\n]*)]\(([^)\n]*)\)""")

/**
 * Replaces both tables (DELETE + INSERT in bookId order). Source defaults expand to
 * every book of the source; a per-book row wins over its source's default.
 */
internal fun applyBookNotices(conn: Connection, rows: BookNoticeRows, logger: Logger): BookNoticesResult {
    conn.createStatement().use { st -> OPTIONAL_PATCH_TABLES.forEach { st.execute(it.ddl) } }

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
    val missingBooks = unmatchedProtection.filterNot { it.endsWith(" / *") }
    check(missingBooks.isEmpty()) {
        "$BOOK_PROTECTION_FILE names book(s) that are not in the DB (title after renames): " +
            missingBooks.joinToString()
    }
    val unmatchedBanners = mutableListOf<String>()
    val banners = resolve(rows.banners, unmatchedBanners)

    conn.createStatement().use { st ->
        st.execute("DELETE FROM book_banner")
        st.execute("DELETE FROM book_protection")
    }
    conn.prepareStatement("INSERT INTO book_banner (bookId, text) VALUES (?, ?)").use { ps ->
        for ((id, entry) in banners.toSortedMap()) {
            ps.setLong(1, id)
            ps.setString(2, expandBannerTitle(entry.second, entry.first.title))
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

    val unmatchedSources = unmatchedProtection.filter { it.endsWith(" / *") }
    if (unmatchedBanners.isNotEmpty() || unmatchedSources.isNotEmpty()) {
        logger.w {
            "Book notices: ${unmatchedBanners.size} banner row(s) and ${unmatchedSources.size} protection " +
                "source default(s) matched no book: ${(unmatchedBanners + unmatchedSources).take(20).joinToString()}"
        }
        GeneratorReport.write("book-notices-unmatched", logger) {
            putStrings("banners", unmatchedBanners)
            putStrings("protectionSources", unmatchedSources)
        }
    }
    return BookNoticesResult(banners.size, protections.size, unmatchedBanners, unmatchedSources)
}
