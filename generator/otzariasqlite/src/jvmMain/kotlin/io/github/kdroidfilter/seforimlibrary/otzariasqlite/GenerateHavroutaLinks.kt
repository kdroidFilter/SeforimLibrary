package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateVerifier
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.DefaultCommentatorPosition
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Generates links between Havrouta commentaries and their corresponding Talmud tractates.
 *
 * Havrouta books contain the original Talmud text in bold (<b>...</b> tags).
 * This script extracts the bold text and matches it to the corresponding Talmud lines.
 *
 * Usage:
 *   ./gradlew -p SeforimLibrary :otzariasqlite:generateHavroutaLinks -PseforimDb=/path/to.db
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("GenerateHavroutaLinks")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()

    val jdbcUrl = "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repository = SeforimRepository(dbPath, driver)
    // The repository init downgrades the GLOBAL kermit severity to Assert;
    // restore Info so this CLI's logs stay visible.
    Logger.setMinSeverity(Severity.Info)

    val sourceDir = System.getProperty("sourceDir")
        ?: System.getenv("OTZARIA_SOURCE_DIR")
        ?: OtzariaFetcher.ensureLocalSource(logger).toString()

    // ─── IdAllocator (delta-update support) ────────────────────────────────────
    val buildStatePath: Path = run {
        val explicit = System.getProperty("buildStatePath") ?: System.getenv("BUILD_STATE_PATH")
        if (explicit != null) Paths.get(explicit) else Paths.get("$dbPath.buildstate")
    }
    val prev = buildStatePath.takeIf { java.nio.file.Files.exists(it) }
    val allocator = InMemoryIdAllocator.load(prev, Logger.withTag("IdAllocator"))
    val bindings = IdAllocatorBindings(allocator, repository)
    // Pre-register every connection type so link ids resolve deterministically.
    ConnectionType.values().forEach { bindings.upsertConnectionType(it.name) }

    try {
        // Performance optimizations
        logger.i { "Setting PRAGMA for bulk operations..." }
        repository.executeRawQuery("PRAGMA foreign_keys = OFF")
        repository.executeRawQuery("PRAGMA synchronous = OFF")
        repository.executeRawQuery("PRAGMA journal_mode = OFF")

        logger.i { "Starting Havrouta-Talmud link generation..." }
        val talmudLinksCreated = generateHavroutaLinks(repository, bindings, logger)
        logger.i { "Havrouta-Talmud link generation completed. Created $talmudLinksCreated links." }

        logger.i { "Starting Havrouta-Hearot link generation from Otzaria files..." }
        val hearotLinksCreated = generateHavroutaHearotLinks(repository, bindings, logger, sourceDir)
        logger.i { "Havrouta-Hearot link generation completed. Created $hearotLinksCreated links." }

        // After both steps, so flags include Hearot FOOTNOTES links.
        updateBookHasLinks(repository, logger)

        logger.i { "Setting Hearot as default commentators for their base books..." }
        setHearotAsDefaultCommentators(repository, driver, logger)

        // No transitive Talmud→Hearot links: notes on Havrouta are a commentary on a
        // commentary, not on the Talmud (Otzaria/otzaria#1001).
        logger.i { "Total links created: ${talmudLinksCreated + hearotLinksCreated}" }

        // Restore PRAGMAs
        logger.i { "Restoring PRAGMA settings..." }
        repository.executeRawQuery("PRAGMA foreign_keys = ON")
        repository.executeRawQuery("PRAGMA synchronous = NORMAL")
        repository.executeRawQuery("PRAGMA journal_mode = WAL")

        // Persist build_state so subsequent runs preserve link ids. This stage
        // writes straight to the on-disk DB (no VACUUM INTO), so there is no
        // persist step for the snapshot to run ahead of.
        val buildStateMeta = mapOf(
            "generator" to "havroutalinks",
            "generated_at" to java.time.Instant.now().toString(),
        )
        runCatching {
            allocator.snapshotTo(target = buildStatePath, extraMeta = buildStateMeta)
        }.onFailure { e ->
            // Fail closed: a build that cannot write its allocator state would
            // publish last week's — and the build after it would re-issue ids
            // this one already handed out.
            logger.e(e) { "Failed to write build_state to $buildStatePath" }
            throw e
        }
        BuildStateVerifier.verifyFreshSnapshot(
            buildStatePath = buildStatePath,
            dbPath = Paths.get(dbPath),
            expectedMeta = buildStateMeta,
            logger = logger,
        )
    } catch (e: Exception) {
        logger.e(e) { "Error generating Havrouta links" }
        throw e
    } finally {
        repository.close()
    }
}

/**
 * Data class representing a daf section with its line range.
 */
internal data class DafSection(
    val dafRef: String,
    val startLineIndex: Int,
    val endLineIndex: Int  // exclusive
)

/**
 * Mapping of Havrouta tractate names to Talmud tractate names (for special cases).
 */
private val tractateNameMapping = mapOf(
    "נידה" to "נדה"
)

/**
 * Regex to extract bold text from Havrouta lines.
 */
private val boldPattern = Regex("""<b>([^<]+)</b>""")

/**
 * Regex to extract daf headers.
 */
private val havroutaDafPattern = Regex("""<h3>\s*דף\s+([^<]+)</h3>""")
private val anyTagPattern = Regex("<[^>]*>")
private val talmudDafPattern = Regex("""<h2>דף\s*([^<]+)</h2>""")

/**
 * Normalizes text for comparison by removing nikud, punctuation, and extra spaces.
 */
private fun normalizeText(text: String): String {
    return text
        // Remove Hebrew nikud (vowel marks)
        .replace(Regex("[\u0591-\u05C7]"), "")
        // Remove punctuation and special characters
        .replace(Regex("[()\\[\\]{}\"',.;:!?׳״]"), "")
        // Normalize spaces
        .replace(Regex("\\s+"), " ")
        .trim()
        .lowercase()
}

/** A parenthesised marker in small print: a note number `(12)`, `(תחילת העמוד)`, an editor's `(משמע)`. */
private val smallMarkerPattern = Regex("""(?:<small>)+\s*\([^()<>]*\)\s*(?:</small>)+""")

/** Any tag other than `<b>`/`</b>`. */
private val nonBoldTagPattern = Regex("""</?(?!b>)(?!/b>)[a-zA-Z][^>]*>""")

/**
 * Extracts bold text from a Havrouta line.
 *
 * [boldPattern] reads only a bold span with no tag inside, so `<b><small>quote</small></b>`
 * and `<b>quote <small>(12)</small> quote</b>` used to give nothing (חברותא על ערכין 1057,
 * פסחים 1837, and the 6000 lines of זבחים in v30). Small parenthesised markers are dropped
 * first, being no Talmud text, and then every other tag is unwrapped. `<b>` inside `<b>` is
 * left alone: there the inner spans are the quotes and the outer one wraps explanation too.
 */
internal fun extractBoldText(content: String): String {
    val cleaned = content.replace(smallMarkerPattern, " ").replace(nonBoldTagPattern, "")
    return boldPattern.findAll(cleaned).map { it.groupValues[1] }.joinToString(" ")
}

private val brPattern = Regex("""<br\s*/?>""", RegexOption.IGNORE_CASE)
private val dashPattern = Regex("[\u2013\u2014\u2026\u200e\u200f-]")

/**
 * [normalizeText] for the containment checks of [realignQuotes]: tags out (`<br>` as a
 * space) and dashes out. A Sefaria line opens with `<big><strong>word</strong></big>` and
 * puts '–'/'—' between clauses, so the plain normalization misses a quote that is there.
 */
internal fun enhancedNormalize(text: String): String =
    normalizeText(text.replace(brPattern, " ").replace(anyTagPattern, "").replace(dashPattern, " "))

/**
 * Checks if a line is a section header (מתניתין, גמרא, etc.)
 */
private fun isSectionHeader(content: String): Boolean {
    val headerPatterns = listOf(
        "<big><b>מתניתין",
        "<big><b>גמרא",
        "<big><b>הדרן",
        "<h2>", "<h3>", "<h4>"
    )
    return headerPatterns.any { content.contains(it) }
}

/**
 * Tractates the Shas prints with no Bavli Gemara. The Vilna Shas carries the Yerushalmi of
 * שקלים in its place, and חברותא על שקלים explains that text by the Yerushalmi's own
 * division: the same eight chapters and 32 halachot as Sefaria's "תלמוד ירושלמי שקלים", in
 * the same order. Its daf headings follow the Shas pagination (ב.–כב:), which neither
 * Sefaria structure has (the "Vilna" alt structure is the Yerushalmi edition's א.–לג:), so
 * the book is linked by perek and halacha instead, see [processYerushalmiPair].
 */
private val yerushalmiOnlyTractates = setOf("שקלים")

private const val YERUSHALMI_PREFIX = "תלמוד ירושלמי "

/**
 * Generates links between Havrouta books and their corresponding Talmud tractates.
 *
 * `internal` rather than private so the found-vs-processed accounting can be
 * tested: the audited build found 38 Havrouta books and processed 37 without
 * saying which one it dropped or why.
 *
 * Every warning goes to [logger] and to [annotate], which in GitHub Actions prints it as a
 * `::warning::` annotation: a `Warn:` line is one of dozens in the Gradle log of a release
 * build, and v30 lost 6000 links of one book behind an INFO line nobody read.
 */
internal suspend fun generateHavroutaLinks(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    logger: Logger,
    annotate: (String) -> Unit = ::printGithubWarning,
): Int {
    val ctCommentary = bindings.upsertConnectionType(ConnectionType.COMMENTARY.name)
    val allBooks = repository.getAllBooks()
    // Find all Havrouta books
    val havroutaBooks = allBooks.filter { it.title.startsWith("חברותא על ") }
    logger.i { "Found ${havroutaBooks.size} Havrouta books" }

    // Find all Talmud Bavli tractates
    val talmudBooks = allBooks.filter { book ->
        book.sourceId == 1L &&
            !book.title.startsWith("משנה") &&
            !book.title.startsWith("תלמוד ירושלמי") &&
            !book.title.startsWith("תוספתא")
    }.associateBy { it.title }
    val yerushalmiBooks = allBooks.filter { it.sourceId == 1L && it.title.startsWith(YERUSHALMI_PREFIX) }
        .associateBy { it.title }

    fun warn(message: String) {
        logger.w { message }
        annotate(message)
    }

    var totalLinksCreated = 0

    // Delete any existing Havrouta-Talmud and Havrouta-Hearot links before creating
    // new ones. Scoped to the two types this task owns — COMMENTARY (Talmud→Havrouta)
    // and FOOTNOTES (Havrouta→Hearot); a blanket delete would also kill the book's
    // LINKER citations when re-run on an already-built DB.
    for (havroutaBook in havroutaBooks) {
        repository.executeRawQuery(
            "DELETE FROM link WHERE (sourceBookId = ${havroutaBook.id} OR targetBookId = ${havroutaBook.id}) " +
                "AND connectionTypeId IN (SELECT id FROM connection_type WHERE name IN ('COMMENTARY', 'FOOTNOTES'))"
        )
    }
    logger.i { "Deleted existing Havrouta links" }

    // "Found N Havrouta books" followed by fewer "Processing:" lines used to be
    // the only trace that a book was dropped; the reason is named per book below
    // and the gap is closed by an explicit N/M line after the loop.
    var processedBooks = 0
    val unmatched = mutableListOf<String>()

    for (havroutaBook in havroutaBooks) {
        val tractateName = havroutaBook.title.removePrefix("חברותא על ")
        val talmudTractateName = tractateNameMapping[tractateName] ?: tractateName

        val talmudBook = talmudBooks[talmudTractateName]
        val yerushalmiBook = if (talmudBook == null && talmudTractateName in yerushalmiOnlyTractates) {
            yerushalmiBooks[YERUSHALMI_PREFIX + talmudTractateName]
        } else {
            null
        }
        if (talmudBook == null && yerushalmiBook == null) {
            unmatched += havroutaBook.title
            warn(
                if (talmudTractateName in yerushalmiOnlyTractates) {
                    "No Talmud match for: ${havroutaBook.title} — '$talmudTractateName' has no Bavli Gemara, " +
                        "and no Sefaria book is titled '$YERUSHALMI_PREFIX$talmudTractateName'; no links created for it"
                } else {
                    "No Talmud match for: ${havroutaBook.title} — no book titled '$talmudTractateName' " +
                        "among the Bavli tractates (sourceId=1, excluding משנה / תלמוד ירושלמי / תוספתא); " +
                        "no links created for it"
                }
            )
            continue
        }

        processedBooks++
        val stats = if (talmudBook != null) {
            logger.i { "Processing: ${havroutaBook.title} -> ${talmudBook.title}" }
            processBookPair(
                repository = repository,
                bindings = bindings,
                ctCommentary = ctCommentary,
                havroutaBookId = havroutaBook.id,
                talmudBookId = talmudBook.id,
                havroutaTotalLines = havroutaBook.totalLines,
                talmudTotalLines = talmudBook.totalLines,
                logger = logger
            )
        } else {
            val yerushalmi = yerushalmiBook!!
            logger.i {
                "Processing: ${havroutaBook.title} -> ${yerushalmi.title} " +
                    "(by perek and halacha: '$talmudTractateName' has no Bavli Gemara)"
            }
            processYerushalmiPair(
                repository = repository,
                bindings = bindings,
                ctCommentary = ctCommentary,
                havroutaBookId = havroutaBook.id,
                yerushalmiBookId = yerushalmi.id,
                havroutaTotalLines = havroutaBook.totalLines,
                yerushalmiTotalLines = yerushalmi.totalLines,
            )
        }

        logger.i { "  Created ${stats.links} links" }
        havroutaFormattingWarnings(havroutaBook.title, (talmudBook ?: yerushalmiBook)!!.title, stats)
            .forEach { warn(it) }
        totalLinksCreated += stats.links
    }

    if (unmatched.isEmpty()) {
        logger.i { "Havrouta-Talmud: $processedBooks/${havroutaBooks.size} books processed" }
    } else {
        warn(
            "Havrouta-Talmud: $processedBooks/${havroutaBooks.size} books processed, " +
                "${unmatched.size} skipped with no matching Talmud tractate: ${unmatched.joinToString()}"
        )
    }

    return totalLinksCreated
}

/**
 * The GitHub Actions workflow command for a warning annotation. The runner reads it from a
 * line of its own, which kermit's `Warn: (tag) …` prefix would break, so it is printed apart.
 */
internal fun githubWarningCommand(title: String, message: String): String {
    fun data(s: String) = s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    fun property(s: String) = data(s).replace(":", "%3A").replace(",", "%2C")
    return "::warning title=${property(title)}::${data(message)}"
}

private fun printGithubWarning(message: String) {
    if (System.getenv("GITHUB_ACTIONS") == "true") println(githubWarningCommand("Havrouta links", message))
}

/** How a Havrouta book is divided, and the headings each side marks the division with. */
internal enum class HavroutaSectionKind(
    val singular: String,
    val plural: String,
    val bookHeading: String,
    val talmudHeading: String,
) {
    DAF("daf", "dafs", "<h3>דף …</h3>", "<h2>דף …</h2>"),
    HALACHA("halacha", "halachot", "<big><b>הלכה …</b></big> under <h2>פרק …</h2>", "<h3>הלכה …</h3> under <h2>פרק …</h2>"),
}

/**
 * The sections (dafs, or halachot) a book pair was matched by.
 *
 * @property inBook distinct sections the Havrouta book has a heading for
 * @property inTalmud distinct sections of the Talmud book
 * @property shared sections both have, the only ones whose lines can be linked
 */
internal data class HavroutaSections(val kind: HavroutaSectionKind, val inBook: Int, val inTalmud: Int, val shared: Int)

/**
 * What one book pair produced, for the sanity checks of [havroutaFormattingWarnings].
 *
 * @property links links created
 * @property bookLines lines of the Havrouta book
 * @property boldLines text lines (after the first section heading, not section headers) with a
 *   bold opener: `<b>`, `<b …>` or `<strong>` — the last two are not read as quotes, so they
 *   count as bold lines that were not linked instead of vanishing from the count
 * @property wholeLineBoldLines of those, long lines that are bold from start to end
 */
internal data class HavroutaPairStats(
    val links: Int,
    val bookLines: Int,
    val boldLines: Int,
    val wholeLineBoldLines: Int,
    val sections: HavroutaSections,
)

/** Below this many bold lines a book is too small for the shares below to mean anything. */
private const val MIN_BOLD_LINES_FOR_CHECK = 100

/** Every healthy Chavruta tractate links over 94% of its bold lines (v29: 0.945-0.97). */
private const val MIN_LINKED_SHARE = 0.8

/** Healthy tractates have up to 8% whole-line bold lines; a leaked `<b>` gave 29% (חולין, v30). */
private const val MAX_WHOLE_LINE_BOLD_SHARE = 0.2

/** A book whose headings stopped matching loses whole sections; healthy books share 99.6%+ of them. */
private const val MIN_SHARED_SECTION_SHARE = 0.9

/** The fewest links per daf of any healthy book is 21 (חברותא על מעילה); שקלים has 68 per halacha. */
private const val MIN_LINKS_PER_SECTION = 5

/** Below this many lines a book is a fixture, not a tractate: the smallest Chavruta book (תמיד) has 708. */
private const val MIN_LINES_FOR_LINK_DENSITY = 100

/**
 * Warnings for a Havrouta book whose formatting stopped the matcher from working.
 *
 * The Talmud text is found through `<b>` spans, so a formatting tag that leaked over
 * thousands of lines does not fail anything: `<b><small>…</small></b>` is not read
 * at all (חברותא על זבחים, v30: 391 links instead of 6400), and a bold that covers
 * whole paragraphs mixes the explanation into the quote (חברותא על חולין: -494).
 * Both stayed an INFO "Created N links" line, inside the global drift gate.
 *
 * Lines are only read inside a section the two books share, so a book whose headings
 * changed level or wording (`<h4>דף ב.</h4>`) collapses to 0 links with 0 bold lines
 * counted. That is checked first, from the headings themselves; then a link count far
 * below the book's sections catches bold markup the matcher does not know at all.
 */
internal fun havroutaFormattingWarnings(title: String, talmudTitle: String, stats: HavroutaPairStats): List<String> {
    val sections = stats.sections
    val kind = sections.kind
    if (stats.bookLines == 0) return emptyList()
    if (sections.inBook == 0) {
        return listOf(
            "$title: none of its ${stats.bookLines} lines is a ${kind.bookHeading} heading, so nothing in it " +
                "was matched to $talmudTitle and ${stats.links} links were created: look for a heading level " +
                "or wording that changed in the book file"
        )
    }
    if (sections.inTalmud == 0) {
        return listOf(
            "$title: $talmudTitle has no ${kind.talmudHeading} heading, so nothing in the book was matched " +
                "to it and ${stats.links} links were created"
        )
    }
    val warnings = mutableListOf<String>()
    val sectionsLost = sections.shared < sections.inTalmud * MIN_SHARED_SECTION_SHARE
    if (sectionsLost) {
        warnings += "$title: only ${sections.shared} of the ${sections.inTalmud} ${kind.plural} of $talmudTitle " +
            "have a heading of the same name in the book (${sections.inBook} headed there), and only those are " +
            "linked: compare the ${kind.bookHeading} headings of the book with the ${kind.talmudHeading} of the Talmud"
    }
    fun pct(n: Int) = "${n * 100 / stats.boldLines}%"
    var shareWarned = false
    if (!sectionsLost && stats.boldLines >= MIN_BOLD_LINES_FOR_CHECK && stats.links < stats.boldLines * MIN_LINKED_SHARE) {
        shareWarned = true
        warnings += "$title: only ${stats.links} of ${stats.boldLines} lines with bold Talmud text were " +
            "linked (${pct(stats.links)}; a sound Chavruta book links over 90%): look for formatting " +
            "left open over many lines in the book file, bold that is not Talmud text, or bold written " +
            "other than <b>…</b> (<b class=…>, <strong>), which is not read"
    }
    if (!sectionsLost && !shareWarned && stats.bookLines >= MIN_LINES_FOR_LINK_DENSITY &&
        stats.links < sections.shared * MIN_LINKS_PER_SECTION
    ) {
        warnings += "$title: only ${stats.links} links over the ${sections.shared} ${kind.plural} it shares with " +
            "$talmudTitle (a sound Chavruta book has over 20 per ${kind.singular}), with ${stats.boldLines} bold lines " +
            "counted: look for Talmud quotes no longer marked <b>…</b> in the book file"
    }
    if (stats.boldLines >= MIN_BOLD_LINES_FOR_CHECK && stats.wholeLineBoldLines > stats.boldLines * MAX_WHOLE_LINE_BOLD_SHARE) {
        warnings += "$title: ${stats.wholeLineBoldLines} of ${stats.boldLines} bold lines " +
            "(${pct(stats.wholeLineBoldLines)}; usually under 10%) are bold from start to end, so the " +
            "explanation is matched as Talmud text: look for an unclosed <b> in the book file"
    }
    return warnings
}

/** A bold opener: `<b>`, `<b class=…>` or `<strong>`. Only the first is read as a quote. */
private val boldOpenerPattern = Regex("""<b>|<b\s[^>]*>|<strong\b[^>]*>""")

/** A long line whose bold text is (nearly) all of its text. */
private fun isWholeLineBold(content: String, boldText: String): Boolean {
    val plain = content.replace(anyTagPattern, "").trim()
    if (plain.length < 40) return false
    return normalizeText(boldText).length >= 0.9 * normalizeText(plain).length
}

/** Distinct section names of each side and how many they share. */
private fun sectionsOf(kind: HavroutaSectionKind, inBook: Set<String>, inTalmud: Set<String>) =
    HavroutaSections(kind, inBook.size, inTalmud.size, inBook.count { it in inTalmud })

/**
 * Processes a single Havrouta-Talmud book pair and creates links.
 */
private suspend fun processBookPair(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    ctCommentary: Long,
    havroutaBookId: Long,
    talmudBookId: Long,
    havroutaTotalLines: Int,
    talmudTotalLines: Int,
    logger: Logger
): HavroutaPairStats {
    // Load all lines for both books
    val havroutaLines = repository.getLines(havroutaBookId, 0, havroutaTotalLines - 1)
    val talmudLines = repository.getLines(talmudBookId, 0, talmudTotalLines - 1)

    // Extract daf sections from both books
    val havroutaDafs = extractDafSections(havroutaLines.map { it.content }, havroutaDafPattern)
    val talmudDafs = extractDafSections(talmudLines.map { it.content }, talmudDafPattern)

    // Create map of dafRef -> Talmud lines in that daf
    val talmudLinesByDaf = mutableMapOf<String, List<IndexedValue<String>>>()
    for (daf in talmudDafs) {
        val linesInDaf = talmudLines
            .filter { it.lineIndex >= daf.startLineIndex && it.lineIndex < daf.endLineIndex }
            .map { IndexedValue(it.lineIndex, normalizeText(it.content)) }
        talmudLinesByDaf[daf.dafRef] = linesInDaf
    }

    var boldLines = 0
    var wholeLineBoldLines = 0
    val rows = mutableListOf<QuoteRow>()

    // Process each Havrouta line
    var currentDafRef: String? = null
    var lastMatchedIndex = 0  // Track last matched line for sequential reading
    for (havroutaLine in havroutaLines) {
        // Check if this is a daf header
        val dafMatch = havroutaDafPattern.find(havroutaLine.content)
        if (dafMatch != null) {
            currentDafRef = dafMatch.groupValues[1].trim()
            lastMatchedIndex = 0  // Reset for new daf
            continue
        }

        // Skip if we haven't encountered a daf yet
        if (currentDafRef == null) continue

        // Skip section headers
        if (isSectionHeader(havroutaLine.content)) continue

        // Extract bold text
        val boldText = extractBoldText(havroutaLine.content)
        if (boldOpenerPattern.containsMatchIn(havroutaLine.content)) {
            boldLines++
            if (isWholeLineBold(havroutaLine.content, boldText)) wholeLineBoldLines++
        }
        if (boldText.isBlank()) continue

        val normalizedBold = normalizeText(boldText)
        if (normalizedBold.length < 5) continue  // Skip very short matches

        // Find matching Talmud line in the same daf
        val talmudLinesInDaf = talmudLinesByDaf[currentDafRef] ?: continue

        // Find the best matching line
        val matchingLineIndex = findBestMatch(normalizedBold, talmudLinesInDaf, lastMatchedIndex)
        if (matchingLineIndex != null) lastMatchedIndex = matchingLineIndex  // Update for sequential reading
        rows += QuoteRow(havroutaLine, currentDafRef, boldText, matchingLineIndex)
    }

    // Second pass: a link whose Talmud line does not hold its quote is moved to the
    // line that does, when that line fits between its neighbours.
    val realigned = realignQuotes(rows, talmudLines, talmudDafs, logger)
    if (realigned > 0) logger.i { "  Realigned $realigned links to the Talmud line that holds their quote" }

    val links = insertQuoteLinks(repository, bindings, ctCommentary, rows, talmudLines, talmudBookId, havroutaBookId)
    val sections = sectionsOf(
        HavroutaSectionKind.DAF,
        havroutaDafs.mapTo(HashSet()) { it.dafRef },
        talmudDafs.mapTo(HashSet()) { it.dafRef },
    )
    return HavroutaPairStats(links, havroutaLines.size, boldLines, wholeLineBoldLines, sections)
}

/** Writes one Talmud → Havrouta link per [rows] entry that found a Talmud line; returns how many. */
private suspend fun insertQuoteLinks(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    ctCommentary: Long,
    rows: List<QuoteRow>,
    talmudLines: List<Line>,
    talmudBookId: Long,
    havroutaBookId: Long,
): Int {
    // Create map of lineIndex -> lineId for Talmud
    val talmudLineIdByIndex = talmudLines.associate { it.lineIndex to it.id }
    var linksCreated = 0
    val linkBatch = mutableListOf<Link>()
    for (row in rows) {
        val havroutaLine = row.havroutaLine
        val matchingLineIndex = row.talmudLineIndex ?: continue
        val talmudLineId = talmudLineIdByIndex[matchingLineIndex] ?: continue

        // Single canonical direction Talmud → Havrouta (base → commentary).
        // The reverse SOURCE view is synthesized at read time by the repository.
        linkBatch.add(Link(
            id = bindings.allocator.linkId(talmudLineId, havroutaLine.id, ctCommentary),
            sourceBookId = talmudBookId,
            targetBookId = havroutaBookId,
            sourceLineId = talmudLineId,
            targetLineId = havroutaLine.id,
            targetLineIndex = havroutaLine.lineIndex,
            connectionType = ConnectionType.COMMENTARY
        ))

        linksCreated += 1

        // Batch insert
        if (linkBatch.size >= 1000) {
            repository.insertLinksBatch(linkBatch)
            linkBatch.clear()
        }
    }

    // Insert remaining links
    if (linkBatch.isNotEmpty()) {
        repository.insertLinksBatch(linkBatch)
    }
    return linksCreated
}

private val hebrewLetterValues: Map<Char, Int> = "אבגדהוזחטיכלמנסעפצקרשת".toList()
    .zip(listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400))
    .toMap()

private val hebrewOrdinals = mapOf(
    "ראשון" to 1, "שני" to 2, "שלישי" to 3, "רביעי" to 4, "חמישי" to 5, "שישי" to 6, "ששי" to 6,
    "שביעי" to 7, "שמיני" to 8, "תשיעי" to 9, "עשירי" to 10,
)

/**
 * `א` → 1, `טו` → 15, `כ"ג` → 23; an ordinal word (`ראשון`, `שמיני`) as its number; anything
 * else null. A numeral's letters fall in value (save טו and טז), so a word is not one.
 */
internal fun hebrewNumber(text: String): Int? {
    hebrewOrdinals[text]?.let { return it }
    val letters = text.filterNot { it in "'\"׳״" }
    if (letters.isEmpty() || letters.any { it !in hebrewLetterValues }) return null
    val values = letters.map { hebrewLetterValues.getValue(it) }
    val ordered = values.zipWithNext().withIndex().all { (i, pair) ->
        pair.first >= pair.second || (i == values.size - 2 && pair.first == 9 && pair.second in setOf(6, 7))
    }
    return if (ordered) values.sum() else null
}

private val yerushalmiPerekPattern = Regex("""<h2>\s*פרק\s+([^<]+?)\s*</h2>""")
private val yerushalmiHalachaPattern = Regex("""<h3>\s*הלכה\s+([^<]+?)\s*</h3>""")
private val anyHeadingPattern = Regex("""<h[1-6][\s>]""")
private val havroutaPerekPattern = Regex("""<h2>\s*פרק\s+([^\s<]+)""")
private val havroutaHalachaPattern = Regex("""^\s*<big><b>\s*הלכה\s+([^\s<:\-–]+)""")
private val finalLetters = mapOf('ך' to 'כ', 'ם' to 'מ', 'ן' to 'נ', 'ף' to 'פ', 'ץ' to 'צ')

/**
 * The words of a Yerushalmi quote or line, spelling aside: [enhancedNormalize], final
 * letters as plain ones, and ו and י dropped. The Shas text of שקלים and Sefaria's Yerushalmi
 * are the same text in two spellings (משמיעין / משמעין, בחדש / בחודש, תתרם / תיתרם), so
 * only 30% of the quotes are found verbatim. One-letter words (a stray ו, ה) are dropped.
 */
internal fun yerushalmiWords(text: String): List<String> =
    enhancedNormalize(text).replace("‌", "").replace("‍", "")
        .map { finalLetters[it] ?: it }.joinToString("")
        .split(' ')
        .map { it.replace("ו", "").replace("י", "") }
        .filter { it.length > 1 }

private fun wordPairs(words: List<String>): Set<Pair<String, String>> = words.zipWithNext().toHashSet()

/** Fixed-point weight of a full match in [alignYerushalmiQuotes]. */
private const val FULL_MATCH_WEIGHT = 10_000

/**
 * Links the quotes of one halacha to the lines of the same halacha in the Yerushalmi.
 *
 * Returns, per quote, the index into [lineWords] it is linked to, or null.
 *
 * 1. A quote *matches* a line when at least half of its word pairs (in [yerushalmiWords]
 *    form) are word pairs of that line. The Chavruta reads the halacha in order, so the
 *    quotes are linked to non-decreasing lines: of all such assignments the one with the
 *    largest sum of matched shares is taken, and on a tie the earlier line.
 * 2. A quote left unlinked whose linked neighbours before and after it are on one line, and
 *    half of whose words are words of that line, is linked to it too: a variant reading
 *    the word pairs miss, between two quotes the line holds.
 */
internal fun alignYerushalmiQuotes(quotes: List<List<String>>, lineWords: List<List<String>>): List<Int?> {
    val m = lineWords.size
    val result = arrayOfNulls<Int>(quotes.size)
    if (m == 0) return result.toList()
    val linePairs = lineWords.map { wordPairs(it) }
    // best[t]: the largest total over the quotes so far with the last link on a line <= t.
    var best = LongArray(m)
    val how = Array(quotes.size) { ByteArray(m) } // 0 = quote unlinked, 1 = linked to t, 2 = see t-1
    for ((k, quote) in quotes.withIndex()) {
        val pairs = wordPairs(quote)
        val cur = best.copyOf()
        if (pairs.isNotEmpty()) {
            for (t in 0 until m) {
                val hit = pairs.count { it in linePairs[t] }
                if (hit == 0 || hit * 2 < pairs.size) continue
                val total = best[t] + hit.toLong() * FULL_MATCH_WEIGHT / pairs.size
                if (total > cur[t]) {
                    cur[t] = total
                    how[k][t] = 1
                }
            }
        }
        for (t in 1 until m) {
            if (cur[t - 1] >= cur[t]) {
                cur[t] = cur[t - 1]
                how[k][t] = 2
            }
        }
        best = cur
    }
    var t = m - 1
    for (k in quotes.indices.reversed()) {
        while (how[k][t] == 2.toByte()) t--
        if (how[k][t] == 1.toByte()) result[k] = t
    }
    val aligned = result.copyOf()
    for (k in quotes.indices) {
        if (aligned[k] != null || quotes[k].isEmpty()) continue
        val before = (k - 1 downTo 0).firstNotNullOfOrNull { aligned[it] } ?: continue
        val after = (k + 1 until quotes.size).firstNotNullOfOrNull { aligned[it] } ?: continue
        if (before != after) continue
        val words = lineWords[before].toHashSet()
        if (quotes[k].count { it in words } * 2 >= quotes[k].size) result[k] = before
    }
    return result.toList()
}

/**
 * Links a Havrouta book to the Yerushalmi tractate it explains, for a tractate with no Bavli
 * Gemara ([yerushalmiOnlyTractates]).
 *
 * The Yerushalmi is divided by `<h2>פרק א</h2>` and `<h3>הלכה א</h3>`; the Havrouta book by
 * `<h2>פרק ראשון - …</h2>` and a `<big><b>הלכה א - מתניתין:</b></big>` line. Each halacha's
 * bold quotes are linked to the lines of the same halacha by [alignYerushalmiQuotes]. The
 * daf headings of the Havrouta book are skipped like any other header.
 */
private suspend fun processYerushalmiPair(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    ctCommentary: Long,
    havroutaBookId: Long,
    yerushalmiBookId: Long,
    havroutaTotalLines: Int,
    yerushalmiTotalLines: Int,
): HavroutaPairStats {
    val havroutaLines = repository.getLines(havroutaBookId, 0, havroutaTotalLines - 1)
    val yerushalmiLines = repository.getLines(yerushalmiBookId, 0, yerushalmiTotalLines - 1)

    // Halacha "perek:halacha" -> the Yerushalmi's text lines under it.
    val halachaLines = LinkedHashMap<String, MutableList<Line>>()
    var perek: Int? = null
    var current: MutableList<Line>? = null
    for (line in yerushalmiLines) {
        if (anyHeadingPattern.containsMatchIn(line.content)) {
            current = null
            yerushalmiPerekPattern.find(line.content)?.let { perek = hebrewNumber(it.groupValues[1]) }
            val halacha = yerushalmiHalachaPattern.find(line.content)?.let { hebrewNumber(it.groupValues[1]) }
            val p = perek
            if (halacha != null && p != null) current = halachaLines.getOrPut("$p:$halacha") { mutableListOf() }
            continue
        }
        current?.add(line)
    }

    var boldLines = 0
    var wholeLineBoldLines = 0
    val rows = mutableListOf<QuoteRow>()
    val rowWords = mutableListOf<List<String>>()
    val bookHalachot = mutableSetOf<String>()
    var havroutaPerek: Int? = null
    var key: String? = null
    for (havroutaLine in havroutaLines) {
        val content = havroutaLine.content
        val perekMatch = havroutaPerekPattern.find(content)
        if (perekMatch != null) {
            havroutaPerek = hebrewNumber(perekMatch.groupValues[1])
            key = null
            continue
        }
        val halachaMatch = havroutaHalachaPattern.find(content)
        if (halachaMatch != null) {
            val halacha = hebrewNumber(halachaMatch.groupValues[1])
            val p = havroutaPerek
            key = if (p != null && halacha != null) "$p:$halacha" else null
            key?.let { bookHalachot += it }
            continue
        }
        val section = key ?: continue
        if (isSectionHeader(content)) continue

        val boldText = extractBoldText(content)
        if (boldOpenerPattern.containsMatchIn(content)) {
            boldLines++
            if (isWholeLineBold(content, boldText)) wholeLineBoldLines++
        }
        if (boldText.isBlank()) continue
        if (normalizeText(boldText).length < 5) continue
        rows += QuoteRow(havroutaLine, section, boldText, null)
        rowWords += yerushalmiWords(boldText)
    }

    val rowsByHalacha = LinkedHashMap<String, MutableList<Int>>()
    rows.forEachIndexed { k, row -> rowsByHalacha.getOrPut(row.dafRef) { mutableListOf() }.add(k) }
    for ((halacha, ks) in rowsByHalacha) {
        val lines = halachaLines[halacha] ?: continue
        val aligned = alignYerushalmiQuotes(ks.map { rowWords[it] }, lines.map { yerushalmiWords(it.content) })
        ks.forEachIndexed { i, k -> rows[k].talmudLineIndex = aligned[i]?.let { lines[it].lineIndex } }
    }

    val links = insertQuoteLinks(repository, bindings, ctCommentary, rows, yerushalmiLines, yerushalmiBookId, havroutaBookId)
    val sections = sectionsOf(HavroutaSectionKind.HALACHA, bookHalachot, halachaLines.keys)
    return HavroutaPairStats(links, havroutaLines.size, boldLines, wholeLineBoldLines, sections)
}

/** A Havrouta line with a bold quote, and the Talmud line (index) it is linked to, if any. */
internal class QuoteRow(
    val havroutaLine: Line,
    val dafRef: String,
    val boldText: String,
    var talmudLineIndex: Int?,
)

/** Shorter quotes are too common to move a link on the strength of a containing line. */
private const val MIN_REALIGN_QUOTE_LENGTH = 15

/** Rows on each side whose links bound the window when the two nearest anchors disagree. */
private const val ANCHOR_SPREAD = 5

/**
 * Moves links whose Talmud line does not hold the full bold quote to a line of the same
 * daf that does. Returns how many links it moved or added.
 *
 * The first pass reads forward from the last match and falls back to the first three
 * words or a 12-character overlap, so one weak match sends the following lines to the
 * wrong line too, even when the exact quote sits a line before (about 430 links, e.g.
 * חברותא על כתובות 9379 → 2500 instead of 2499). That pass is left as it was; this one only
 * corrects its result, so a link that already holds its quote never moves:
 *
 * - An *anchor* is a row whose linked line holds its quote (after [enhancedNormalize]),
 *   alone or together with the text line before or after it — a quote may run over two
 *   Talmud lines, also across a daf heading (כתובות 4217 → 996).
 * - Another row with a quote of [MIN_REALIGN_QUOTE_LENGTH]+ characters moves to a line
 *   that holds the quote (or, if no line does, one it runs over), but only between the
 *   lines of the nearest anchor before it and after it. Outside that window the quote is a
 *   repetition — the Mishnah quoted again in the Gemara, an "איכא דאמרי" version, "אמר מר"
 *   — and the first pass, which follows the reading order, is kept. When the two anchors
 *   are out of order, the window spans them and the anchors [ANCHOR_SPREAD] rows around.
 * - Among several lines in the window, the one nearest the first-pass line wins (the
 *   lower on a tie); a row the first pass left unlinked starts from the window's start.
 */
internal fun realignQuotes(
    rows: List<QuoteRow>,
    talmudLines: List<Line>,
    talmudDafs: List<DafSection>,
    logger: Logger? = null,
): Int {
    if (talmudLines.withIndex().any { (i, l) -> l.lineIndex != i }) {
        logger?.w { "Talmud lines are not indexed 0..n-1; links are not realigned" }
        return 0
    }
    val text = talmudLines.map { enhancedNormalize(it.content) }
    val heading = talmudLines.map { talmudDafPattern.containsMatchIn(it.content) }
    val sections = talmudDafs.associateBy { it.dafRef }

    fun prevText(i: Int): Int {
        var j = i - 1
        while (j >= 0 && heading[j]) j--
        return j
    }
    fun nextText(i: Int): Int {
        var j = i + 1
        while (j < text.size && heading[j]) j++
        return if (j < text.size) j else -1
    }
    fun runsOver(q: String, a: Int, b: Int): Boolean =
        a >= 0 && b >= 0 && !text[a].contains(q) && !text[b].contains(q) && "${text[a]} ${text[b]}".contains(q)
    fun covers(q: String, i: Int): Boolean =
        text[i].contains(q) || runsOver(q, prevText(i), i) || runsOver(q, i, nextText(i))

    var moved = 0
    val byDaf = LinkedHashMap<String, MutableList<QuoteRow>>()
    for (row in rows) byDaf.getOrPut(row.dafRef) { mutableListOf() }.add(row)
    for ((dafRef, rs) in byDaf) {
        val daf = sections[dafRef] ?: continue
        val start = daf.startLineIndex
        val end = daf.endLineIndex
        val quotes = rs.map { enhancedNormalize(it.boldText) }
        // Anchors and their lines are fixed before any row moves: the result does not
        // depend on the order the rows are repaired in.
        val anchorLine = rs.indices.map { k -> rs[k].talmudLineIndex?.takeIf { covers(quotes[k], it) } }
        for (k in rs.indices) {
            val q = quotes[k]
            if (anchorLine[k] != null || q.length < MIN_REALIGN_QUOTE_LENGTH) continue
            var candidates = (start until end).filter { !heading[it] && text[it].contains(q) }
            if (candidates.isEmpty()) candidates = (start until end).filter { !heading[it] && covers(q, it) }
            if (candidates.isEmpty()) continue
            var lo = (k - 1 downTo 0).firstNotNullOfOrNull { anchorLine[it] } ?: start
            var hi = (k + 1 until rs.size).firstNotNullOfOrNull { anchorLine[it] } ?: (end - 1)
            if (lo > hi) {
                val near = listOf(lo, hi) + (maxOf(0, k - ANCHOR_SPREAD) until minOf(rs.size, k + ANCHOR_SPREAD + 1))
                    .filter { it != k }.mapNotNull { anchorLine[it] }
                lo = near.min()
                hi = near.max()
            }
            val inWindow = candidates.filter { it in lo..hi }
            if (inWindow.isEmpty()) continue
            val ref = rs[k].talmudLineIndex ?: lo
            rs[k].talmudLineIndex = inWindow.minWith(compareBy<Int>({ kotlin.math.abs(it - ref) }, { it }))
            moved++
        }
    }
    return moved
}

/**
 * Extracts daf sections from a list of line contents.
 */
private fun extractDafSections(contents: List<String>, pattern: Regex): List<DafSection> {
    val sections = mutableListOf<DafSection>()
    var lastDafRef: String? = null
    var lastStartIndex = 0

    for ((index, content) in contents.withIndex()) {
        val match = pattern.find(content)
        if (match != null) {
            // Close previous section
            if (lastDafRef != null) {
                sections.add(DafSection(lastDafRef, lastStartIndex, index))
            }
            lastDafRef = match.groupValues[1].trim()
            lastStartIndex = index
        }
    }

    // Close last section
    if (lastDafRef != null) {
        sections.add(DafSection(lastDafRef, lastStartIndex, contents.size))
    }

    return sections
}

/**
 * Finds the best matching Talmud line for a given bold text.
 * Uses a sequential approach where we start searching from lastMatchedIndex.
 * Returns the lineIndex of the best match, or null if no good match found.
 */
private fun findBestMatch(
    normalizedBoldText: String,
    talmudLines: List<IndexedValue<String>>,
    lastMatchedIndex: Int
): Int? {
    if (normalizedBoldText.length < 3) return null

    // Search all lines but prefer lines >= lastMatchedIndex
    val linesAfter = talmudLines.filter { it.index >= lastMatchedIndex }
    val linesBefore = talmudLines.filter { it.index < lastMatchedIndex }

    // First try exact substring match on lines after lastMatchedIndex
    for (line in linesAfter) {
        if (line.value.contains(normalizedBoldText)) {
            return line.index
        }
    }

    // Try matching first significant words on lines after
    val words = normalizedBoldText.split(" ").filter { it.length > 1 }
    if (words.size >= 2) {
        val firstWords = words.take(3).joinToString(" ")
        for (line in linesAfter) {
            if (line.value.contains(firstWords)) {
                return line.index
            }
        }
    }

    // Try significant overlap (but only forward) - this catches minor text variations
    if (normalizedBoldText.length >= 8) {
        for (line in linesAfter) {
            if (hasSignificantOverlap(normalizedBoldText, line.value, 8)) {
                return line.index
            }
        }
    }

    // If nothing found forward, search backwards (but be stricter)
    // Only use exact substring match when going backwards
    for (line in linesBefore) {
        if (line.value.contains(normalizedBoldText)) {
            return line.index
        }
    }

    // Last resort: try first 2-3 words on all lines
    if (words.size >= 2) {
        val twoWords = words.take(2).joinToString(" ")
        for (line in linesAfter + linesBefore) {
            if (line.value.contains(twoWords)) {
                return line.index
            }
        }
    }

    return null
}

/**
 * Checks if two strings have significant overlap (shared substring of minLen+ chars).
 */
private fun hasSignificantOverlap(text1: String, text2: String, minLen: Int = 6): Boolean {
    if (text1.length < minLen || text2.length < minLen) return false

    // Check overlapping substrings from text1
    for (i in 0..text1.length - minLen) {
        val sub = text1.substring(i, minOf(i + minLen + 4, text1.length))
        if (text2.contains(sub)) return true
    }
    return false
}

/**
 * Updates the book_has_links table after generating links.
 */
internal suspend fun updateBookHasLinks(repository: SeforimRepository, logger: Logger) {
    logger.i { "Updating book_has_links table..." }

    repository.executeRawQuery(
        "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
            "SELECT id, 0, 0 FROM book"
    )

    repository.executeRawQuery(
        "UPDATE book_has_links SET hasSourceLinks=1 " +
            "WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
    )

    repository.executeRawQuery(
        "UPDATE book_has_links SET hasTargetLinks=1 " +
            "WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
    )

    repository.executeRawQuery(
        "UPDATE book SET hasCommentaryConnection=1 WHERE id IN (" +
            "SELECT DISTINCT sourceBookId FROM link l " +
            "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
            "WHERE ct.name='COMMENTARY'" +
            ")"
    )

    repository.recomputeHasSourceConnection()

    logger.i { "book_has_links table updated" }
}

/**
 * Data class for parsing Otzaria link JSON files.
 */
@Serializable
private data class OtzariaLinkData(
    val line_index_1: Long,
    val line_index_2: Long,
    val heRef_2: String,
    val path_2: String,
    @SerialName("Conection Type")
    val connectionType: String
)

private val json = Json {
    ignoreUnknownKeys = true
    coerceInputValues = true
}

/**
 * Generates links between Havrouta and Hearot al Havrouta using Otzaria link files.
 * Only notes books that stayed standalone get links; a merged one has no book and is skipped.
 */
private suspend fun generateHavroutaHearotLinks(
    repository: SeforimRepository,
    bindings: IdAllocatorBindings,
    logger: Logger,
    sourceDir: String
): Int {
    val ctFootnotes = bindings.upsertConnectionType(ConnectionType.FOOTNOTES.name)
    val linksDir = File(sourceDir, "links")
    if (!linksDir.exists()) {
        logger.w { "Links directory not found: ${linksDir.absolutePath}" }
        return 0
    }

    // Get all books and build lookup maps
    logger.i { "Loading all books into RAM..." }
    val allBooks = repository.getAllBooks()
    val havroutaBooks = allBooks.filter { it.title.startsWith("חברותא על ") }
    val hearotBooks = allBooks.filter { it.title.startsWith("הערות על חברותא") }
    val booksByTitle = allBooks.associateBy { it.title }

    logger.i { "Found ${havroutaBooks.size} Havrouta books and ${hearotBooks.size} Hearot books" }

    // Preload all line IDs for Havrouta and Hearot books into RAM
    // Map: bookId -> (lineIndex -> lineId)
    logger.i { "Loading all line IDs into RAM..." }
    val lineIdCache = mutableMapOf<Long, Map<Int, Long>>()

    for (book in havroutaBooks + hearotBooks) {
        val lines = repository.getLines(book.id, 0, book.totalLines - 1)
        lineIdCache[book.id] = lines.associate { it.lineIndex to it.id }
    }
    logger.i { "Loaded line IDs for ${lineIdCache.size} books" }

    // Preload all link files into RAM
    logger.i { "Loading all Havrouta link files into RAM..." }
    val allLinkData = mutableMapOf<String, List<OtzariaLinkData>>()
    for (havroutaBook in havroutaBooks) {
        val linkFileName = "${havroutaBook.title}_links.json"
        val linkFile = File(linksDir, linkFileName)
        if (linkFile.exists()) {
            try {
                val content = linkFile.readText()
                val links: List<OtzariaLinkData> = json.decodeFromString(content)
                val hearotLinks = links.filter { it.path_2.contains("הערות על חברותא") }
                if (hearotLinks.isNotEmpty()) {
                    allLinkData[havroutaBook.title] = hearotLinks
                }
            } catch (e: Exception) {
                logger.w { "Failed to parse ${linkFileName}: ${e.message}" }
            }
        }
    }
    logger.i { "Loaded ${allLinkData.size} link files with Hearot links" }

    // Process all links in RAM and batch insert
    logger.i { "Processing links..." }
    val allLinks = mutableListOf<Link>()
    var skippedSource = 0
    var skippedTarget = 0
    var skippedBook = 0

    for ((bookTitle, links) in allLinkData) {
        val havroutaBook = booksByTitle[bookTitle] ?: continue
        val sourceLineIds = lineIdCache[havroutaBook.id] ?: continue

        for (linkData in links) {
            // Extract target book title from path
            val targetTitle = linkData.path_2.split('\\').last().substringBeforeLast('.')
            val targetBook = booksByTitle[targetTitle]

            if (targetBook == null) {
                skippedBook++
                continue
            }

            val targetLineIds = lineIdCache[targetBook.id]
            if (targetLineIds == null) {
                skippedBook++
                continue
            }

            // Get line indices (Otzaria links use 1-based, our DB uses 0-based)
            val sourceLineIndex = linkData.line_index_1.toInt() - 1
            val targetLineIndex = linkData.line_index_2.toInt() - 1

            val sourceLineId = sourceLineIds[sourceLineIndex]
            if (sourceLineId == null) {
                skippedSource++
                continue
            }

            val targetLineId = targetLineIds[targetLineIndex]
            if (targetLineId == null) {
                skippedTarget++
                continue
            }

            // Single canonical direction Havrouta → Hearot (base → notes).
            // SOURCE is synthesized at read time.
            allLinks.add(Link(
                id = bindings.allocator.linkId(sourceLineId, targetLineId, ctFootnotes),
                sourceBookId = havroutaBook.id,
                targetBookId = targetBook.id,
                sourceLineId = sourceLineId,
                targetLineId = targetLineId,
                targetLineIndex = targetLineIndex,
                connectionType = ConnectionType.FOOTNOTES
            ))
        }
    }

    logger.i { "Prepared ${allLinks.size} links (skipped: $skippedBook book not found, $skippedSource source line, $skippedTarget target line)" }

    // Batch insert all links using optimized method
    logger.i { "Inserting ${allLinks.size} links into database..." }
    repository.insertLinksBatch(allLinks)

    logger.i { "Inserted ${allLinks.size} Havrouta-Hearot links" }
    return allLinks.size
}

/**
 * Sets each notes companion as a default commentator of the book it annotates, so
 * its notes are visible on open instead of waiting for the reader to know that a
 * "הערות על X" book exists and to tick it by hand. Without this the notes reach no
 * display path at all: a dependent-text link only enters `linksByLine` once its
 * target book is an active commentator, which is what the printed-marker popup
 * reads.
 *
 * A pair qualifies on either of two grounds:
 *  - the link is typed [ConnectionType.FOOTNOTES] — the authoritative signal, and
 *    the only one for a companion whose title does not follow the "הערות על" shape;
 *  - the target is titled exactly "הערות על <source title>" — legacy links still
 *    stored as COMMENTARY, kept so a library that has not been retyped yet does not
 *    regress. The equality (rather than a LIKE prefix) is what keeps the legacy transitive
 *    Talmud→Hearot layer (older DBs) out: those are COMMENTARY and their source is the tractate,
 *    not the annotated Havrouta volume, so on a re-run over an already-built DB they
 *    can no longer turn a tractate into a notes reader.
 *
 * Appends to existing defaults so seeded ones survive.
 */
private suspend fun setHearotAsDefaultCommentators(
    repository: SeforimRepository,
    driver: JdbcSqliteDriver,
    logger: Logger
) {
    val pairs = driver.executeQuery(
        null,
        "SELECT DISTINCT l.sourceBookId, l.targetBookId FROM link l " +
            "JOIN book tb ON tb.id = l.targetBookId " +
            "JOIN book sb ON sb.id = l.sourceBookId " +
            "JOIN connection_type ct ON ct.id = l.connectionTypeId " +
            "WHERE ct.name = 'FOOTNOTES' OR tb.title = 'הערות על ' || sb.title " +
            "ORDER BY l.sourceBookId, l.targetBookId",
        { cursor ->
            val list = mutableListOf<Pair<Long, Long>>()
            while (cursor.next().value) {
                list.add(cursor.getLong(0)!! to cursor.getLong(1)!!)
            }
            QueryResult.Value(list)
        },
        0
    ).value

    var count = 0
    var alreadySet = 0
    for ((baseBookId, hearotBookId) in pairs) {
        val existing = repository.getDefaultCommentatorsForBook(baseBookId)
        if (existing.any { it.commentatorBookId == hearotBookId }) { alreadySet++; continue }
        val nextPosition = (existing.maxOfOrNull { it.position } ?: -1) + 1
        repository.setDefaultCommentatorsForBook(
            baseBookId,
            existing + DefaultCommentatorPosition(hearotBookId, nextPosition)
        )
        count++
    }

    // Fail closed. Silence here is indistinguishable from success in the log, and the
    // only symptom downstream is "the notes do not open" — a DB shipped that way is
    // not reportable as a generator failure by anyone who sees it.
    if (pairs.isEmpty()) {
        val companions = driver.executeQuery(
            null,
            "SELECT COUNT(*) FROM book WHERE title LIKE 'הערות על %'",
            { cursor -> cursor.next(); QueryResult.Value(cursor.getLong(0) ?: 0L) },
            0
        ).value
        check(companions == 0L) {
            "Found $companions 'הערות על' companion books but not one base→notes pair: " +
                "no book will open its notes. Expect FOOTNOTES-typed links from the base " +
                "book, or a companion titled exactly 'הערות על <base title>'."
        }
    }

    logger.i { "Set default commentators for $count base→hearot pairs ($alreadySet already set)" }
}
