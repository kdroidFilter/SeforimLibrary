package io.github.kdroidfilter.seforimlibrary.common.divine

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.patch.SCHEMA_7_TABLE_DDL
import org.sqlite.SQLiteConfig
import org.sqlite.SQLiteOpenMode
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.random.Random

/*
 * Builds `line_divine_name` — the verified sacred divine names of every line, in
 * visible-char coordinates — for the Otzaria app's display substitution. The
 * entry point is `:sefariasqlite:buildLineDivineNameIndex`, which reads the
 * curated verse list from ForDB; detection lives in [DivineNameDetector].
 */

/** Opens [path] read-only and immutable: the DB file is never written, not even its journal. */
fun openReadOnly(path: Path): Connection {
    val config = SQLiteConfig().apply {
        setReadOnly(true)
        setOpenMode(SQLiteOpenMode.OPEN_URI)
    }
    val uri = "file:" + path.toAbsolutePath().toString().replace('\\', '/') + "?mode=ro&immutable=1"
    val conn = DriverManager.getConnection("jdbc:sqlite:$uri", config.toProperties())
    conn.createStatement().use { it.execute("PRAGMA query_only = 1") }
    return conn
}

data class DivineNameIndexReport(
    val lines: Long,
    val total: Long,
    val countsByKindSource: Map<String, Long>,
    val appHvyh: Long,
    val hvyhNotEmitted: Long,
    val dropped: Map<String, Long>,
    /** Note bodies scanned and spans written to line_note_divine_name; 0 for the dry-run report. */
    val noteBodies: Long = 0,
    val noteSpans: Long = 0,
)

/**
 * Atomically replaces the existing table, and `line_note_divine_name` from the note
 * bodies of `line_note` (run after buildLineNoteIndex). [overrides] null (no usable list) leaves
 * every Tanach base line HVYH-only. Fails (and keeps the previous contents) when the
 * Tanach books cannot all be resolved — the overrides depend on them — or when an
 * app HVYH match outside markup is not emitted.
 */
fun rebuildLineDivineNameIndex(
    conn: Connection,
    logger: Logger,
    overrides: DivineNameOverrides?,
    requireTanach: Boolean = true,
): DivineNameIndexReport {
    check(conn.autoCommit) { "rebuildLineDivineNameIndex requires an unowned JDBC connection" }
    conn.autoCommit = false
    return try {
        conn.createStatement().use { st ->
            st.execute(CREATE_LINE_DIVINE_NAME)
            st.execute("DELETE FROM line_divine_name")
            st.execute(SCHEMA_7_TABLE_DDL.getValue("line_note"))
            st.execute(SCHEMA_7_TABLE_DDL.getValue("line_note_divine_name"))
            st.execute("DELETE FROM line_note_divine_name")
        }
        val lineReport = conn.prepareStatement(
            "INSERT INTO line_divine_name (lineId, charStart, charEnd, kind, source) VALUES (?, ?, ?, ?, ?)",
        ).use { insert ->
            var pending = 0
            val report = scanCorpus(conn, overrides, logger, requireTanach) { line, result ->
                for (span in result.spans) {
                    insert.setLong(1, line.id)
                    insert.setInt(2, span.charStart)
                    insert.setInt(3, span.charEnd)
                    insert.setInt(4, span.kind.code)
                    insert.setInt(5, span.source.code)
                    insert.addBatch()
                    if (++pending >= 10_000) { insert.executeBatch(); pending = 0 }
                }
            }
            if (pending > 0) insert.executeBatch()
            report
        }
        val noteReport = conn.prepareStatement(
            "INSERT INTO line_note_divine_name (lineId, noteIndex, charStart, charEnd, kind, source) VALUES (?, ?, ?, ?, ?, ?)",
        ).use { insert ->
            var pending = 0
            val report = scanNoteBodies(conn, logger, requireTanach) { lineId, noteIndex, result ->
                for (span in result.spans) {
                    insert.setLong(1, lineId)
                    insert.setInt(2, noteIndex)
                    insert.setInt(3, span.charStart)
                    insert.setInt(4, span.charEnd)
                    insert.setInt(5, span.kind.code)
                    insert.setInt(6, span.source.code)
                    insert.addBatch()
                    if (++pending >= 10_000) { insert.executeBatch(); pending = 0 }
                }
            }
            if (pending > 0) insert.executeBatch()
            report
        }
        val report = lineReport.copy(
            appHvyh = lineReport.appHvyh + noteReport.appHvyh,
            hvyhNotEmitted = lineReport.hvyhNotEmitted + noteReport.hvyhNotEmitted,
            dropped = (lineReport.dropped.keys + noteReport.dropped.keys).associateWith {
                (lineReport.dropped[it] ?: 0L) + (noteReport.dropped[it] ?: 0L)
            },
            noteBodies = noteReport.lines,
            noteSpans = noteReport.total,
        )
        // An app match may stay unwritten only inside markup or where the app's own word-boundary
        // check would reject the span (its runtime regex keeps covering those).
        val explainedReasons = setOf("not-visible-text", "app-boundary")
        val hvyhDrops = report.dropped.filterKeys { it.startsWith("HVYH/") }
        val unexplained = hvyhDrops.filterKeys { it.substringAfter(':') !in explainedReasons }.values.sum()
        check(unexplained == 0L && report.hvyhNotEmitted == hvyhDrops.values.sum()) {
            "HVYH superset gate failed: ${report.hvyhNotEmitted} app matches not emitted, $unexplained unexplained"
        }
        conn.commit()
        report
    } catch (failure: Throwable) {
        runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        conn.autoCommit = true
    }
}

internal val CREATE_LINE_DIVINE_NAME: String = SCHEMA_7_TABLE_DDL.getValue("line_divine_name")

internal data class CorpusLine(val id: Long, val bookId: Long, val heRef: String?, val content: String, val verse: VerseRef?)

private data class BookRow(val id: Long, val title: String, val source: String, val isBase: Boolean, val categoryId: Long)

/** Category id → the title of its root category. */
private fun categoryRootTitles(conn: Connection): (Long) -> String? {
    val parents = HashMap<Long, Long?>()
    val titles = HashMap<Long, String>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, parentId, title FROM category").use { rs ->
            while (rs.next()) {
                val id = rs.getLong(1)
                parents[id] = rs.getLong(2).takeUnless { rs.wasNull() }
                titles[id] = rs.getString(3)
            }
        }
    }
    return { categoryId ->
        var id: Long? = categoryId
        var guard = 0
        var root: String? = null
        while (id != null && guard++ < 64) {
            val parent = parents[id]
            if (parent == null) { root = titles[id]; break }
            id = parent
        }
        root
    }
}

/** Root category of the prayer book: the only non-Tanach scope of the אֵל / אֱלוֹהַּ NIKUD rules. */
internal const val PRAYER_BOOK_ROOT = "סדר התפילה"

/** Ids of every book under the [PRAYER_BOOK_ROOT] root category. */
internal fun resolvePrayerBooks(conn: Connection): Set<Long> {
    val rootTitle = categoryRootTitles(conn)
    val out = HashSet<Long>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, categoryId FROM book").use { rs ->
            while (rs.next()) if (rootTitle(rs.getLong(2)) == PRAYER_BOOK_ROOT) out += rs.getLong(1)
        }
    }
    return out
}

/** Book id → Tanach title for the 39 Sefaria base books under the תנ"ך root category. */
internal fun resolveTanachBooks(conn: Connection, requireComplete: Boolean): Map<Long, String> {
    val rootTitle = categoryRootTitles(conn)
    val books = ArrayList<BookRow>()
    conn.prepareStatement(
        "SELECT b.id, b.title, s.name, b.isBaseBook, b.categoryId FROM book b JOIN source s ON s.id = b.sourceId " +
            "WHERE b.title IN (${TANACH_TITLES.joinToString { "?" }})",
    ).use { ps ->
        TANACH_TITLES.forEachIndexed { i, t -> ps.setString(i + 1, t) }
        ps.executeQuery().use { rs ->
            while (rs.next()) books += BookRow(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getInt(4) != 0, rs.getLong(5))
        }
    }
    val tanach = books.filter { b ->
        b.isBase && b.source == "Sefaria" &&
            rootTitle(b.categoryId)?.replace('״', '"')?.replace("\"", "") == "תנך"
    }
    val byTitle = tanach.groupBy { it.title }
    if (requireComplete) {
        val missing = TANACH_TITLES.filter { byTitle[it]?.size != 1 }
        check(missing.isEmpty()) {
            "line_divine_name needs exactly one Tanach base book per title; unresolved: ${missing.joinToString()}"
        }
    }
    return tanach.associate { it.id to it.title }
}

/**
 * Override range ends (`book c:v`) that match no verse line of the resolved Tanach books
 * in [conn] — a typo'd ref would silently cover nothing. Empty when the books are not all there.
 */
fun unmatchedOverrideRefs(conn: Connection, overrides: DivineNameOverrides): List<String> {
    val tanach = resolveTanachBooks(conn, requireComplete = false)
    if (tanach.size != TANACH_TITLES.size) return emptyList()
    val verses = HashSet<VerseRef>()
    conn.prepareStatement("SELECT heRef FROM line WHERE bookId = ? AND heRef IS NOT NULL").use { ps ->
        for ((id, title) in tanach) {
            ps.setLong(1, id)
            ps.executeQuery().use { rs -> while (rs.next()) parseTanachHeRef(rs.getString(1), title)?.let(verses::add) }
        }
    }
    return overrides.entries.flatMap { listOf(it.from, it.to) }.distinct().filter { it !in verses }.map { it.toString() }
}

/**
 * Every `line_note` body through the detector, offsets within the body. A body is no verse:
 * in a Tanach book it gets the HVYH rule only, elsewhere the rules its line gets.
 */
internal fun scanNoteBodies(
    conn: Connection,
    logger: Logger,
    requireTanach: Boolean,
    onNote: (lineId: Long, noteIndex: Int, DivineNameDetector.Result) -> Unit,
): DivineNameIndexReport {
    val tanach = resolveTanachBooks(conn, requireTanach)
    val prayerBooks = resolvePrayerBooks(conn)
    val detector = DivineNameDetector(null)
    var bodies = 0L
    var total = 0L
    var appHvyh = 0L
    var hvyhNotEmitted = 0L
    val counts = sortedMapOf<String, Long>()
    val dropped = sortedMapOf<String, Long>()
    conn.prepareStatement(
        "SELECT n.lineId, n.noteIndex, n.body, l.bookId FROM line_note n JOIN line l ON l.id = n.lineId " +
            "ORDER BY n.lineId, n.noteIndex",
    ).use { ps ->
        ps.fetchSize = 2_000
        ps.executeQuery().use { rs ->
            while (rs.next()) {
                val bookId = rs.getLong(4)
                val prayer = bookId in prayerBooks
                val result = detector.detect(
                    rs.getString(3),
                    verse = null,
                    nonHvyh = tanach[bookId] == null,
                    elScope = DivineNameDetector.EL_NIKUD_IN_PRAYER_BOOK && prayer,
                    prayerBook = prayer,
                )
                bodies++
                total += result.spans.size
                appHvyh += result.appHvyh.size
                val emitted = result.spans.filter { it.kind == DivineKind.HVYH }.mapTo(HashSet()) { it.rawStart until it.rawEnd }
                hvyhNotEmitted += result.appHvyh.count { it !in emitted }
                for (span in result.spans) counts.merge("${span.kind.name}/${span.source.name}", 1L, Long::plus)
                for ((k, v) in result.dropped) dropped.merge(k, v.toLong(), Long::plus)
                onNote(rs.getLong(1), rs.getInt(2), result)
            }
        }
    }
    logger.i { "line_note_divine_name: $total spans in $bodies note bodies" }
    return DivineNameIndexReport(bodies, total, counts, appHvyh, hvyhNotEmitted, dropped)
}

/** Streams every line through the detector; shared by the build and the report. */
internal fun scanCorpus(
    conn: Connection,
    overrides: DivineNameOverrides?,
    logger: Logger,
    requireTanach: Boolean,
    onLine: (CorpusLine, DivineNameDetector.Result) -> Unit,
): DivineNameIndexReport {
    val tanach = resolveTanachBooks(conn, requireTanach)
    val prayerBooks = resolvePrayerBooks(conn)
    val detector = DivineNameDetector(overrides)
    val hasInlineContent = conn.prepareStatement("PRAGMA table_info(line)").use { ps ->
        ps.executeQuery().use { rs -> generateSequence { if (rs.next()) rs.getString("name") else null }.any { it == "content" } }
    }
    val sql = if (hasInlineContent) {
        "SELECT id, bookId, heRef, content FROM line ORDER BY id"
    } else {
        val compressed = conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name='zstd_dict'")
            .use { ps -> ps.executeQuery().use { it.next() } }
        check(!compressed) { "line_divine_name must run before compressLineContent (line text is zstd frames)" }
        "SELECT l.id, l.bookId, l.heRef, c.content FROM line l JOIN line_content c ON c.id = l.id ORDER BY l.id"
    }

    var lines = 0L
    var total = 0L
    var appHvyh = 0L
    var hvyhNotEmitted = 0L
    val counts = sortedMapOf<String, Long>()
    val dropped = sortedMapOf<String, Long>()
    conn.prepareStatement(sql).use { ps ->
        ps.fetchSize = 2_000
        ps.executeQuery().use { rs ->
            while (rs.next()) {
                val content = rs.getString(4) ?: continue
                val bookId = rs.getLong(2)
                val heRef = rs.getString(3)
                val tanachTitle = tanach[bookId]
                val verse = if (tanachTitle != null && heRef != null) parseTanachHeRef(heRef, tanachTitle) else null
                val line = CorpusLine(rs.getLong(1), bookId, heRef, content, verse)
                // A Tanach line whose verse or override list is missing cannot honour the profane list: HVYH only.
                val result = detector.detect(
                    content,
                    verse,
                    nonHvyh = tanachTitle == null || (verse != null && overrides != null),
                    elScope = (verse != null && overrides != null) ||
                        (DivineNameDetector.EL_NIKUD_IN_PRAYER_BOOK && bookId in prayerBooks),
                    prayerBook = bookId in prayerBooks,
                )
                lines++
                total += result.spans.size
                appHvyh += result.appHvyh.size
                val emitted = result.spans.filter { it.kind == DivineKind.HVYH }.mapTo(HashSet()) { it.rawStart until it.rawEnd }
                hvyhNotEmitted += result.appHvyh.count { it !in emitted }
                for (span in result.spans) counts.merge("${span.kind.name}/${span.source.name}", 1L, Long::plus)
                for ((k, v) in result.dropped) dropped.merge(k, v.toLong(), Long::plus)
                onLine(line, result)
                if (lines % 500_000 == 0L) logger.i { "line_divine_name: $lines lines, $total spans" }
            }
        }
    }
    return DivineNameIndexReport(lines, total, counts, appHvyh, hvyhNotEmitted, dropped)
}

// ── Report (dry run) ─────────────────────────────────────────────────────────

private const val SAMPLE_PER_STRATUM = 200
private const val GATE_SAMPLE = 500

/**
 * Runs the full detection over a read-only DB and writes, under [outDir]:
 * `counts.csv` (kind × source), `summary.txt`, `sample.csv` (≈200 random spans per
 * kind × source, the span bracketed in its line) and `hvyh_gate_mismatches.csv`.
 */
fun writeDivineNameReport(
    conn: Connection,
    outDir: Path,
    logger: Logger,
    overrides: DivineNameOverrides?,
): DivineNameIndexReport {
    Files.createDirectories(outDir)
    val started = System.nanoTime()
    val titles = HashMap<Long, String>()
    conn.createStatement().use { st ->
        st.executeQuery("SELECT id, title FROM book").use { rs -> while (rs.next()) titles[rs.getLong(1)] = rs.getString(2) }
    }
    val random = Random(20261012)
    val seen = HashMap<String, Long>()
    val samples = HashMap<String, Array<List<String>?>>()
    val gateRows = ArrayList<List<String>>()
    val sacredForced = HashMap<VerseRef, Int>()
    val booksByStratum = HashMap<String, HashMap<String, Long>>()

    val report = scanCorpus(conn, overrides, logger, requireTanach = true) { line, result ->
        val emitted = result.spans.filter { it.kind == DivineKind.HVYH }.mapTo(HashSet()) { it.rawStart until it.rawEnd }
        for (miss in result.appHvyh) {
            if (miss in emitted || gateRows.size >= GATE_SAMPLE) continue
            gateRows += listOf(titles[line.bookId].orEmpty(), line.heRef.orEmpty(), line.id.toString(), excerptRaw(line.content, miss))
        }
        for (span in result.spans) {
            val stratum = "${span.kind.name}/${span.source.name}"
            if (span.source == DivineSource.EXCEPTION_LIST && line.verse != null) sacredForced.merge(line.verse, 1, Int::plus)
            booksByStratum.getOrPut(stratum) { HashMap() }.merge(titles[line.bookId].orEmpty(), 1L, Long::plus)
            val n = seen.merge(stratum, 1L, Long::plus)!!
            val slot = if (n <= SAMPLE_PER_STRATUM) (n - 1).toInt() else random.nextLong(n).toInt().takeIf { it < SAMPLE_PER_STRATUM }
            if (slot != null) {
                samples.getOrPut(stratum) { arrayOfNulls(SAMPLE_PER_STRATUM) }[slot] = listOf(
                    span.kind.name, span.source.name, line.bookId.toString(), titles[line.bookId].orEmpty(),
                    line.heRef.orEmpty(), line.id.toString(), span.charStart.toString(), span.charEnd.toString(),
                    line.content.substring(span.rawStart, span.rawEnd), excerpt(line.content, span),
                )
            }
        }
    }
    val seconds = (System.nanoTime() - started) / 1e9

    writeCsv(outDir.resolve("counts.csv"), listOf("kind", "source", "count", "books")) {
        report.countsByKindSource.map { (k, v) ->
            val (kind, source) = k.split('/')
            listOf(kind, source, v.toString(), booksByStratum[k]?.size?.toString() ?: "0")
        }
    }
    writeCsv(
        outDir.resolve("sample.csv"),
        listOf("kind", "source", "book_id", "book_title", "heRef", "line_id", "char_start", "char_end", "span", "excerpt"),
    ) { samples.toSortedMap().values.flatMap { it.filterNotNull() } }
    writeCsv(outDir.resolve("hvyh_gate_mismatches.csv"), listOf("book_title", "heRef", "line_id", "excerpt")) { gateRows }
    writeCsv(outDir.resolve("top_books.csv"), listOf("kind", "source", "book_title", "count")) {
        booksByStratum.toSortedMap().flatMap { (stratum, books) ->
            val (kind, source) = stratum.split('/')
            books.entries.sortedByDescending { it.value }.take(25).map { listOf(kind, source, it.key, it.value.toString()) }
        }
    }
    val summary = buildString {
        appendLine("line_divine_name dry run (DB opened read-only)")
        appendLine("runtime_seconds=${"%.1f".format(seconds)}")
        appendLine("lines=${report.lines} spans=${report.total}")
        appendLine("hvyh_gate: app_matches=${report.appHvyh} not_emitted=${report.hvyhNotEmitted}")
        appendLine()
        appendLine("[counts kind/source]")
        report.countsByKindSource.forEach { (k, v) -> appendLine("$k=$v") }
        appendLine()
        appendLine("[dropped candidates kind/source:reason]")
        report.dropped.forEach { (k, v) -> appendLine("$k=$v") }
        appendLine()
        appendLine("[sacred exception entries -> spans forced]")
        appendLine("override_list=${if (overrides == null) "ABSENT (Tanach base lines HVYH-only)" else "${overrides.entries.size} entries"}")
        overrides?.entries.orEmpty().filter { it.decision == OverrideDecision.SACRED }.forEach {
            appendLine("${it.from} ${it.kind} -> ${sacredForced[it.from] ?: 0}")
        }
    }
    Files.writeString(outDir.resolve("summary.txt"), summary)
    return report
}

private fun visibleText(raw: String): Pair<String, IntArray> {
    // Same walk as ScannedLine: one output char per visible char, entity -> '·'.
    val out = StringBuilder()
    val rawToVisible = IntArray(raw.length + 1)
    var inTag = false
    var i = 0
    while (i < raw.length) {
        val c = raw[i]
        rawToVisible[i] = out.length
        when {
            inTag -> { if (c == '>') inTag = false; i++ }
            c == '<' -> { inTag = true; i++ }
            c == '&' -> {
                val end = minOf(raw.length, i + 10)
                var j = i + 1
                var terminated = false
                while (j < end) { if (raw[j] == ';') { terminated = true; break }; j++ }
                out.append(if (terminated) '·' else '&')
                val last = if (terminated) j else i
                for (k in i..last) rawToVisible[k] = out.length - 1
                i = last + 1
            }
            else -> { out.append(c); i++ }
        }
    }
    rawToVisible[raw.length] = out.length
    return out.toString() to rawToVisible
}

private fun excerpt(raw: String, span: DivineSpan): String {
    val (text, _) = visibleText(raw)
    val from = maxOf(0, span.charStart - 60)
    val to = minOf(text.length, span.charEnd + 60)
    return text.substring(from, span.charStart) + "⟦" + text.substring(span.charStart, span.charEnd) + "⟧" +
        text.substring(span.charEnd, to)
}

private fun excerptRaw(raw: String, range: IntRange): String {
    val from = maxOf(0, range.first - 80)
    val to = minOf(raw.length, range.last + 81)
    return raw.substring(from, range.first) + "⟦" + raw.substring(range.first, range.last + 1) + "⟧" + raw.substring(range.last + 1, to)
}

private fun writeCsv(path: Path, header: List<String>, rows: () -> List<List<String>>) {
    fun esc(s: String) = "\"" + s.replace("\"", "\"\"").replace('\n', ' ').replace('\r', ' ') + "\""
    Files.newBufferedWriter(path, Charsets.UTF_8).use { w ->
        w.write("﻿")
        w.write(header.joinToString(",", transform = ::esc)); w.write("\n")
        for (row in rows()) { w.write(row.joinToString(",", transform = ::esc)); w.write("\n") }
    }
}
