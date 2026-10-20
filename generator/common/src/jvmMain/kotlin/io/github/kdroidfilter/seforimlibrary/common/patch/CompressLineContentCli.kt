package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdDictCompress
import com.github.luben.zstd.ZstdDictDecompress
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentCompression
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentShape
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CharsetDecoder
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * Compresses the line text of a finished schema-6 `seforim.db`: every
 * `line_content.content`, every non-NULL `version_line.content` and (schema 7) every
 * `line_note.body` becomes one zstd frame (BLOB) made with
 * [LineContentCompression.bundledDictionary], which is stored in `zstd_dict`.
 *
 * Runs after the last reader of plain text (Phase-2 `generateLinkerLinks`) and
 * before `compactSeforimDb`, which reclaims the space the shorter rows free.
 *
 * Every frame is decompressed and compared before it is written. Rows already
 * stored as BLOB are skipped, so an interrupted run resumes; [validateCompressed]
 * then decodes every frame, the skipped ones included. NULL in `version_line`
 * (inherit the base text) stays NULL. Compression keeps the schema version: readers decode when `zstd_dict` exists.
 *
 * System properties: `dbPath` (required), `chunkRows` (default [COMPRESS_CHUNK_ROWS]),
 * `threads` (default: available processors).
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("CompressLineContentCli")

    val dbPath = System.getProperty("dbPath") ?: error("-PdbPath= missing")
    val chunkRows = System.getProperty("chunkRows")?.toIntOrNull() ?: COMPRESS_CHUNK_ROWS
    val threads = System.getProperty("threads")?.toIntOrNull() ?: Runtime.getRuntime().availableProcessors()
    val path = Paths.get(dbPath)
    require(Files.isRegularFile(path)) { "Database file not found: $dbPath" }

    val started = System.nanoTime()
    DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
        val report = compressLineContent(conn, chunkRows = chunkRows, threads = threads, logger = logger)
        logger.i { report.describe() }
    }
    logger.i { "Compressed $dbPath in ${(System.nanoTime() - started) / 1_000_000} ms" }
}

internal const val COMPRESS_CHUNK_ROWS: Int = 20_000

internal data class CompressLineContentReport(
    val lineRowsCompressed: Long,
    val versionRowsCompressed: Long,
    val noteRowsCompressed: Long,
    val textBytes: Long,
    val frameBytes: Long,
    val framesValidated: Long,
) {
    val alreadyCompressed: Boolean
        get() = lineRowsCompressed == 0L && versionRowsCompressed == 0L && noteRowsCompressed == 0L

    fun describe(): String =
        "line text compression ${if (alreadyCompressed) "already in place (validated)" else "done"}: " +
            "line_content=$lineRowsCompressed version_line=$versionRowsCompressed line_note=$noteRowsCompressed rows, " +
            "$textBytes → $frameBytes bytes, $framesValidated frames decoded"
}

/** See [main]. [dictionary] is the bundled one in production; tests pass their own. */
internal fun compressLineContent(
    conn: Connection,
    dictionary: ByteArray = LineContentCompression.bundledDictionary,
    chunkRows: Int = COMPRESS_CHUNK_ROWS,
    threads: Int = Runtime.getRuntime().availableProcessors(),
    logger: Logger = Logger.withTag("CompressLineContent"),
): CompressLineContentReport {
    require(chunkRows > 0) { "chunkRows=$chunkRows must be positive" }
    require(threads > 0) { "threads=$threads must be positive" }
    check(conn.autoCommit) { "compressLineContent requires an unowned JDBC connection" }
    check(LineContentShape.isSplit(conn)) { "compressLineContent needs the schema-6 line_content shape" }

    storeDictionary(conn, dictionary)

    val journalMode = conn.createStatement().use { st ->
        st.executeQuery("PRAGMA journal_mode").use { rs -> rs.next(); rs.getString(1) }
    }
    val switchJournal = journalMode.equals("wal", ignoreCase = true)
    if (switchJournal) conn.createStatement().use { it.execute("PRAGMA journal_mode=DELETE") }

    val dictId = Zstd.getDictIdFromDict(dictionary)
    val cdict = ZstdDictCompress(dictionary, LineContentCompression.LEVEL)
    val ddict = ZstdDictDecompress(dictionary)
    val workers = ConcurrentLinkedQueue<Pair<LineContentCompression.Compressor, LineContentCompression.Decompressor>>()
    val local = ThreadLocal.withInitial {
        (LineContentCompression.Compressor(cdict) to LineContentCompression.Decompressor(ddict, dictId))
            .also(workers::add)
    }
    val pool = Executors.newFixedThreadPool(threads)
    try {
        val encode = { rows: List<Pair<LongArray, ByteArray>> ->
            val slice = (rows.size + threads - 1) / threads
            rows.chunked(maxOf(slice, 1)).map { part ->
                pool.submit<List<ByteArray>> {
                    val (compressor, decompressor) = local.get()
                    part.map { (key, text) ->
                        check(text.size <= LineContentCompression.MAX_LINE_BYTES) {
                            "row ${keyText(key)} is ${text.size} bytes, over the reader cap ${LineContentCompression.MAX_LINE_BYTES}"
                        }
                        val frame = compressor.compress(text)
                        check(decompressor.decompress(frame).contentEquals(text)) {
                            "zstd round trip failed for row ${keyText(key)}"
                        }
                        frame
                    }
                }
            }.flatMap { it.get() }
        }
        val line = compressColumn(
            conn,
            selectSql = "SELECT id, CAST(content AS BLOB) FROM line_content " +
                "WHERE id >= ? AND typeof(content) = 'text' ORDER BY id LIMIT $chunkRows",
            updateSql = "UPDATE line_content SET content = ? WHERE id = ?",
            encode = encode,
        )
        logger.i { "line_content: ${line.rows} rows compressed" }
        val version = compressColumn(
            conn,
            selectSql = "SELECT rowid, CAST(content AS BLOB) FROM version_line " +
                "WHERE rowid >= ? AND typeof(content) = 'text' ORDER BY rowid LIMIT $chunkRows",
            updateSql = "UPDATE version_line SET content = ? WHERE rowid = ?",
            encode = encode,
        )
        logger.i { "version_line: ${version.rows} rows compressed" }
        val note = if (hasLineNote(conn)) {
            compressColumn(
                conn,
                selectSql = "SELECT lineId, noteIndex, CAST(body AS BLOB) FROM line_note " +
                    "WHERE (lineId, noteIndex) >= (?, ?) AND typeof(body) = 'text' " +
                    "ORDER BY lineId, noteIndex LIMIT $chunkRows",
                updateSql = "UPDATE line_note SET body = ? WHERE lineId = ? AND noteIndex = ?",
                keyColumns = 2,
                encode = encode,
            ).also { logger.i { "line_note: ${it.rows} rows compressed" } }
        } else {
            ColumnResult(0, 0, 0)
        }
        val validated = validateCompressed(conn, dictionary, threads)
        return CompressLineContentReport(
            lineRowsCompressed = line.rows,
            versionRowsCompressed = version.rows,
            noteRowsCompressed = note.rows,
            textBytes = line.textBytes + version.textBytes + note.textBytes,
            frameBytes = line.frameBytes + version.frameBytes + note.frameBytes,
            framesValidated = validated,
        )
    } finally {
        // A failed chunk leaves other tasks running; their contexts close only after they stop.
        pool.shutdownNow()
        pool.awaitTermination(1, TimeUnit.MINUTES)
        workers.forEach { (c, d) -> c.close(); d.close() }
        cdict.close()
        ddict.close()
        if (switchJournal) conn.createStatement().use { it.execute("PRAGMA journal_mode=$journalMode") }
    }
}

/** A DB holds exactly one dictionary; a different one already stored is a hard error. */
private fun storeDictionary(conn: Connection, dictionary: ByteArray) {
    val id = com.github.luben.zstd.Zstd.getDictIdFromDict(dictionary)
    check(id != 0L) { "the dictionary has no zstd dictionary id" }
    conn.createStatement().use {
        it.execute(
            "CREATE TABLE IF NOT EXISTS ${LineContentCompression.DICT_TABLE} " +
                "(id INTEGER PRIMARY KEY NOT NULL, dict BLOB NOT NULL)",
        )
    }
    val stored = LineContentCompression.storedDictionaries(conn)
    if (stored.isEmpty()) {
        conn.prepareStatement("INSERT INTO ${LineContentCompression.DICT_TABLE}(id, dict) VALUES (?, ?)").use { ps ->
            ps.setLong(1, id)
            ps.setBytes(2, dictionary)
            ps.executeUpdate()
        }
        return
    }
    check(stored.keys == setOf(id) && stored.getValue(id).contentEquals(dictionary)) {
        "${LineContentCompression.DICT_TABLE} already holds dictionaries ${stored.keys}, not $id"
    }
}

/** `line_note` exists (schema 7); its bodies are compressed and validated like line text. */
private fun hasLineNote(conn: Connection): Boolean =
    conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name='line_note'").use { ps ->
        ps.executeQuery().use { it.next() }
    }

private class ColumnResult(val rows: Long, val textBytes: Long, val frameBytes: Long)

/** A row key as messages name it: `id`, or `(lineId, noteIndex)`. */
private fun keyText(key: LongArray): String = key.singleOrNull()?.toString() ?: key.joinToString(", ", "(", ")")

/** The key right after [key] in key order; null after the last one, (MAX_VALUE, …). */
private fun keyAfter(key: LongArray): LongArray? {
    val next = key.copyOf()
    for (i in next.indices.reversed()) {
        if (next[i] != Long.MAX_VALUE) return next.also { it[i]++ }
        next[i] = Long.MIN_VALUE
    }
    return null
}

/** Rows of [selectSql]: its key columns, then the value; [from] binds the inclusive lower key bound. */
private fun readChunk(conn: Connection, selectSql: String, from: LongArray): List<Pair<LongArray, ByteArray?>> =
    conn.prepareStatement(selectSql).use { ps ->
        from.forEachIndexed { i, k -> ps.setLong(i + 1, k) }
        ps.executeQuery().use { rs ->
            buildList {
                while (rs.next()) add(LongArray(from.size) { rs.getLong(it + 1) } to rs.getBytes(from.size + 1))
            }
        }
    }

private fun compressColumn(
    conn: Connection,
    selectSql: String,
    updateSql: String,
    keyColumns: Int = 1,
    encode: (List<Pair<LongArray, ByteArray>>) -> List<ByteArray>,
): ColumnResult {
    // An inclusive bound covers the full signed range; stop before incrementing past the last key.
    var nextKey: LongArray? = LongArray(keyColumns) { Long.MIN_VALUE }
    var rows = 0L
    var textBytes = 0L
    var frameBytes = 0L
    while (nextKey != null) {
        val chunk = readChunk(conn, selectSql, nextKey).map { (key, text) -> key to checkNotNull(text) }
        if (chunk.isEmpty()) break
        val frames = encode(chunk)
        conn.autoCommit = false
        try {
            conn.prepareStatement(updateSql).use { ps ->
                chunk.forEachIndexed { i, (key, _) ->
                    ps.setBytes(1, frames[i])
                    key.forEachIndexed { k, v -> ps.setLong(k + 2, v) }
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            conn.commit()
        } catch (failure: Throwable) {
            runCatching { conn.rollback() }.exceptionOrNull()?.let(failure::addSuppressed)
            throw failure
        } finally {
            conn.autoCommit = true
        }
        rows += chunk.size
        textBytes += chunk.sumOf { it.second.size.toLong() }
        frameBytes += frames.sumOf { it.size.toLong() }
        nextKey = keyAfter(chunk.last().first)
    }
    return ColumnResult(rows, textBytes, frameBytes)
}

internal const val VALIDATE_CHUNK_ROWS: Int = 20_000

/**
 * The DB holds only [approvedDictionary], and every line text is a frame the app
 * decodes: that dictionary's id, a known size within the reader cap, strict UTF-8.
 * Decodes every row and throws on the first bad one, naming it. Returns the frames decoded.
 */
internal fun validateCompressed(
    conn: Connection,
    approvedDictionary: ByteArray = LineContentCompression.bundledDictionary,
    threads: Int = Runtime.getRuntime().availableProcessors(),
    chunkRows: Int = VALIDATE_CHUNK_ROWS,
): Long {
    require(threads > 0) { "threads=$threads must be positive" }
    require(chunkRows > 0) { "chunkRows=$chunkRows must be positive" }
    val dictionaries = LineContentCompression.storedDictionaries(conn)
    check(dictionaries.size == 1) { "expected one dictionary in ${LineContentCompression.DICT_TABLE}, found ${dictionaries.keys}" }
    val (storedId, stored) = dictionaries.entries.single()
    val approvedId = Zstd.getDictIdFromDict(approvedDictionary)
    check(storedId == approvedId && stored.contentEquals(approvedDictionary)) {
        "${LineContentCompression.DICT_TABLE} holds dictionary $storedId, not the approved $approvedId"
    }
    fun count(sql: String): Long =
        conn.createStatement().use { st -> st.executeQuery(sql).use { rs -> rs.next(); rs.getLong(1) } }
    // A BLOB that is not a zstd frame (wrong magic) would pass a typeof check alone.
    fun notFrame(column: String) = "(typeof($column) <> 'blob' OR substr($column, 1, 4) <> X'28B52FFD')"
    val plainLines = count("SELECT COUNT(*) FROM line_content WHERE ${notFrame("content")}")
    check(plainLines == 0L) { "$plainLines line_content rows are not zstd frames" }
    val plainVersions = count("SELECT COUNT(*) FROM version_line WHERE content IS NOT NULL AND ${notFrame("content")}")
    check(plainVersions == 0L) { "$plainVersions version_line rows are not zstd frames" }
    val notes = hasLineNote(conn)
    if (notes) {
        val plainNotes = count("SELECT COUNT(*) FROM line_note WHERE ${notFrame("body")}")
        check(plainNotes == 0L) { "$plainNotes line_note rows are not zstd frames" }
    }

    val ddict = ZstdDictDecompress(approvedDictionary)
    val checkers = ConcurrentLinkedQueue<FrameChecker>()
    val local = ThreadLocal.withInitial {
        FrameChecker(LineContentCompression.Decompressor(ddict, approvedId)).also(checkers::add)
    }
    val pool = Executors.newFixedThreadPool(threads)
    try {
        val lineFrames = decodeColumn(
            conn, "line_content",
            "SELECT id, content FROM line_content WHERE id >= ? ORDER BY id LIMIT $chunkRows",
            pool, threads, local,
        ) + decodeColumn(
            conn, "version_line",
            "SELECT rowid, content FROM version_line WHERE rowid >= ? AND content IS NOT NULL ORDER BY rowid LIMIT $chunkRows",
            pool, threads, local,
        )
        if (!notes) return lineFrames
        return lineFrames + decodeColumn(
            conn, "line_note",
            "SELECT lineId, noteIndex, body FROM line_note WHERE (lineId, noteIndex) >= (?, ?) " +
                "ORDER BY lineId, noteIndex LIMIT $chunkRows",
            pool, threads, local, keyColumns = 2,
        )
    } finally {
        // Contexts close only after every task stopped using them.
        pool.shutdownNow()
        pool.awaitTermination(1, TimeUnit.MINUTES)
        checkers.forEach { it.decompressor.close() }
        ddict.close()
    }
}

private class FrameChecker(val decompressor: LineContentCompression.Decompressor) {
    // newDecoder() reports malformed input; String(bytes, UTF_8) would replace it silently.
    private val utf8: CharsetDecoder = Charsets.UTF_8.newDecoder()

    fun check(table: String, key: LongArray, frame: ByteArray) {
        val text = try {
            decompressor.decompress(frame)
        } catch (failure: RuntimeException) {
            throw IllegalStateException("$table row ${keyText(key)}: ${failure.message}", failure)
        }
        try {
            utf8.decode(ByteBuffer.wrap(text))
        } catch (failure: CharacterCodingException) {
            throw IllegalStateException("$table row ${keyText(key)} does not decode to valid UTF-8", failure)
        }
    }
}

/** Reads the next chunk while the previous one decodes, so at most two chunks are in memory. */
private fun decodeColumn(
    conn: Connection,
    table: String,
    selectSql: String,
    pool: ExecutorService,
    threads: Int,
    local: ThreadLocal<FrameChecker>,
    keyColumns: Int = 1,
): Long {
    // An inclusive bound covers the full signed range; stop before incrementing past the last key.
    var nextKey: LongArray? = LongArray(keyColumns) { Long.MIN_VALUE }
    var rows = 0L
    var pending: List<Future<*>> = emptyList()
    while (nextKey != null) {
        val chunk = readChunk(conn, selectSql, nextKey)
        awaitAll(pending)
        pending = emptyList()
        if (chunk.isEmpty()) return rows
        val slice = maxOf((chunk.size + threads - 1) / threads, 1)
        pending = chunk.chunked(slice).map { part ->
            pool.submit {
                val checker = local.get()
                for ((key, frame) in part) checker.check(table, key, checkNotNull(frame) { "$table row ${keyText(key)} is NULL" })
            }
        }
        rows += chunk.size
        nextKey = keyAfter(chunk.last().first)
    }
    awaitAll(pending)
    return rows
}

private fun awaitAll(futures: List<Future<*>>) {
    var first: Throwable? = null
    for (future in futures) {
        try {
            future.get()
        } catch (failure: ExecutionException) {
            if (first == null) first = failure.cause ?: failure
        }
    }
    first?.let { throw it }
}
