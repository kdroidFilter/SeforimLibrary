package io.github.kdroidfilter.seforimlibrary.packaging

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.dao.text.LineTextCodec
import io.github.kdroidfilter.seforimlibrary.dao.text.LineTextDictionaries
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.io.path.deleteIfExists
import kotlin.io.path.fileSize
import kotlin.random.Random

private val logger = Logger.withTag("CompactDb")

// Deflate's window: a dictionary beyond 32 KiB is never read
private const val DICTIONARY_SIZE = 32 * 1024
private const val DICTIONARY_SAMPLES = 30_000
private const val DICTIONARY_SEED = 20_261_103L

private const val DEFAULT_PAGE_SIZE = 4096
private const val CHUNK_LINES = 20_000

/**
 * Writes the shipped database from the generator's: the line texts deflated with a dictionary of the corpus' own text
 * ([LineTextCodec]), the tables and indexes of the current schema only (the generator's dev-only indexes go), every
 * page written once and full, small pages (a line read at random costs one 4 KiB page, not 16).
 *
 * Usage: ./gradlew :packaging:compactDb [-PseforimDb=…] [-PcompactDb=…] [-PtrainDictionaries=<dir>]
 *
 * The dictionaries are the packaging's resources (`line_codec_dict/<id>.bin`), the same for every release: a delta
 * between two releases carries texts compressed alike. `trainDictionaries` builds new ones from the corpus and writes
 * them to <dir>; a release with new dictionaries can't be reached by a delta.
 */
fun main() {
    val source = Paths.get(requireNotNull(System.getProperty("seforimDb")) { "seforimDb is required" })
    val target = System.getProperty("compactDb")?.let(Paths::get) ?: source.resolveSibling("seforim.compact.db")
    val pageSize = System.getProperty("pageSize")?.toInt() ?: DEFAULT_PAGE_SIZE
    val trainTo = System.getProperty("trainDictionaries")?.takeIf { it.isNotBlank() }?.let(Paths::get)
    require(source != target) { "The compact database must be another file" }
    compactDb(source, target, pageSize, trainTo)
}

fun compactDb(source: Path, target: Path, pageSize: Int = DEFAULT_PAGE_SIZE, trainDictionariesTo: Path? = null) {
    val started = System.nanoTime()
    listOf("", "-wal", "-shm", "-journal").forEach { Paths.get("$target$it").deleteIfExists() }

    // The current schema, its indexes put aside until the rows are in
    createSchema(target, pageSize)
    DriverManager.getConnection("jdbc:sqlite:$target").use { conn ->
        conn.exec("PRAGMA journal_mode=OFF")
        conn.exec("PRAGMA synchronous=OFF")
        conn.exec("PRAGMA cache_size=-262144")
        conn.exec("PRAGMA foreign_keys=OFF")
        val indexes = conn.strings("SELECT sql FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")
        conn.strings("SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")
            .forEach { conn.exec("DROP INDEX \"$it\"") }
        conn.exec("ATTACH DATABASE 'file:$source?mode=ro' AS src")

        val dictionaries =
            if (trainDictionariesTo == null) {
                shippedDictionaries()
            } else {
                buildDictionaries(conn).onEach { (id, data) ->
                    Files.createDirectories(trainDictionariesTo)
                    Files.write(trainDictionariesTo.resolve("$id.bin"), data)
                }
            }
        conn.prepareStatement("INSERT INTO line_codec_dict(id, data) VALUES (?, ?)").use { ps ->
            dictionaries.forEach { (id, data) ->
                ps.setInt(1, id)
                ps.setBytes(2, data)
                ps.executeUpdate()
            }
        }

        conn.autoCommit = false
        copyTables(conn)
        copyLines(conn, LineTextCodec { dictionaries })
        conn.commit()
        conn.autoCommit = true

        logger.i { "Creating ${indexes.size} indexes" }
        indexes.forEach(conn::exec)
        // No ANALYZE: the queries are tuned on plans without statistics, and loading sqlite_stat4 on each connection
        // costs (SQLDelight's JDBC driver opens one per query)
        conn.exec("DETACH DATABASE src")
        conn.exec("PRAGMA journal_mode=DELETE")
    }
    val seconds = (System.nanoTime() - started) / 1e9
    logger.i { "Compact database: ${source.fileSize() / 1_048_576} MB → ${target.fileSize() / 1_048_576} MB in ${"%.0f".format(seconds)} s" }
}

private fun createSchema(target: Path, pageSize: Int) {
    DriverManager.getConnection("jdbc:sqlite:$target").use { it.exec("PRAGMA page_size=$pageSize") }
    JdbcSqliteDriver("jdbc:sqlite:$target").use { SeforimDb.Schema.create(it) }
}

/** Every table but the lines (and the dictionaries), copied row for row in rowid order, by their common columns. */
private fun copyTables(conn: Connection) {
    val tables = conn.strings("SELECT name FROM main.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
    val sourceTables = conn.strings("SELECT name FROM src.sqlite_master WHERE type = 'table'").toSet()
    for (table in tables) {
        if (table == "line" || table == "line_codec_dict" || table !in sourceTables) continue
        val sourceColumns = conn.strings("SELECT name FROM pragma_table_info('$table', 'src')").toSet()
        val columns = conn.strings("SELECT name FROM pragma_table_info('$table', 'main')").filter { it in sourceColumns }
        val withoutRowid = conn.strings("SELECT sql FROM src.sqlite_master WHERE name = '$table'")
            .single().contains("WITHOUT ROWID", ignoreCase = true)
        val list = columns.joinToString(",") { "\"$it\"" }
        conn.exec(
            "INSERT INTO main.\"$table\"($list) SELECT $list FROM src.\"$table\"" +
                if (withoutRowid) "" else " ORDER BY rowid",
        )
    }
}

/** The lines in id order, their texts compressed in parallel by chunks and written in order. */
private fun copyLines(conn: Connection, codec: LineTextCodec) {
    val total = conn.strings("SELECT COUNT(*) FROM src.line").single().toLong()
    val pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors().coerceAtMost(8))
    var written = 0L
    var plainBytes = 0L
    var storedBytes = 0L
    try {
        conn.prepareStatement(
            "INSERT INTO main.line(id, bookId, lineIndex, content, heRef, tocEntryId, charCount) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ).use { insert ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT id, bookId, lineIndex, content, heRef, tocEntryId, charCount FROM src.line ORDER BY id").use { rs ->
                    // One chunk compressing while the previous one is written
                    var pending: List<Future<List<CompactLine>>>? = null
                    while (true) {
                        val chunk = ArrayList<SourceLine>(CHUNK_LINES)
                        while (chunk.size < CHUNK_LINES && rs.next()) {
                            chunk +=
                                SourceLine(
                                    id = rs.getLong(1),
                                    bookId = rs.getLong(2),
                                    lineIndex = rs.getLong(3),
                                    content = codec.decode(rs.getBytes(4) ?: ByteArray(0)),
                                    heRef = rs.getString(5),
                                    tocEntryId = rs.getLong(6).takeUnless { rs.wasNull() },
                                    charCount = rs.getLong(7),
                                )
                        }
                        val next = if (chunk.isEmpty()) null else compressChunk(pool, codec, chunk)
                        pending?.flatMap { it.get() }?.forEach { line ->
                            insert.setLong(1, line.id)
                            insert.setLong(2, line.bookId)
                            insert.setLong(3, line.lineIndex)
                            insert.setBytes(4, line.content)
                            insert.setString(5, line.heRef)
                            if (line.tocEntryId == null) insert.setNull(6, java.sql.Types.INTEGER) else insert.setLong(6, line.tocEntryId)
                            insert.setLong(7, line.charCount)
                            insert.addBatch()
                            plainBytes += line.plainBytes
                            storedBytes += line.content.size
                            written++
                        }
                        insert.executeBatch()
                        if (written > 0 && written % 500_000 < CHUNK_LINES) {
                            logger.i { "Lines: $written / $total, texts ${plainBytes / 1_048_576} MB → ${storedBytes / 1_048_576} MB" }
                        }
                        pending = next ?: break
                    }
                }
            }
        }
    } finally {
        pool.shutdown()
    }
    logger.i { "Lines: $written, texts ${plainBytes / 1_048_576} MB → ${storedBytes / 1_048_576} MB" }
}

private class SourceLine(
    val id: Long,
    val bookId: Long,
    val lineIndex: Long,
    val content: String,
    val heRef: String?,
    val tocEntryId: Long?,
    val charCount: Long,
)

private class CompactLine(
    val id: Long,
    val bookId: Long,
    val lineIndex: Long,
    val content: ByteArray,
    val heRef: String?,
    val tocEntryId: Long?,
    val charCount: Long,
    val plainBytes: Int,
)

private fun compressChunk(
    pool: ExecutorService,
    codec: LineTextCodec,
    chunk: List<SourceLine>,
): List<Future<List<CompactLine>>> =
    chunk.chunked(1_000).map { part -> pool.submit<List<CompactLine>> { part.map { it.compact(codec) } } }

private fun SourceLine.compact(codec: LineTextCodec): CompactLine {
    val plainContent = codec.encode(content)
    return CompactLine(
        id = id,
        bookId = bookId,
        lineIndex = lineIndex,
        content = smaller(plainContent, codec.compress(content, LineTextDictionaries.CONTENT)),
        heRef = heRef,
        tocEntryId = tocEntryId,
        charCount = charCount,
        plainBytes = plainContent.size,
    )
}

// A text too short to gain from deflate stays plain
private fun smaller(plain: ByteArray, compressed: ByteArray): ByteArray = if (compressed.size < plain.size) compressed else plain

/**
 * Random lines of the corpus, end to end, the last 32 KiB kept: on a sample of the corpus it gets within 3% of zstd's
 * trained dictionary (3.13x against 3.22x).
 */
private fun buildDictionaries(conn: Connection): Map<Int, ByteArray> {
    val maxId = conn.strings("SELECT MAX(id) FROM src.line").single().toLong()
    val random = Random(DICTIONARY_SEED)
    val content = java.io.ByteArrayOutputStream()
    conn.prepareStatement("SELECT content FROM src.line WHERE id >= ? ORDER BY id LIMIT 1").use { ps ->
        repeat(DICTIONARY_SAMPLES) {
            ps.setLong(1, random.nextLong(1, maxId + 1))
            ps.executeQuery().use { rs ->
                if (rs.next()) rs.getBytes(1)?.let(content::write)
            }
        }
    }
    return mapOf(LineTextDictionaries.CONTENT to content.toByteArray().takeLast32k())
}

private fun ByteArray.takeLast32k(): ByteArray = copyOfRange((size - DICTIONARY_SIZE).coerceAtLeast(0), size)

private fun shippedDictionaries(): Map<Int, ByteArray> =
    listOf(LineTextDictionaries.CONTENT).associateWith { id ->
        val resource = "/line_codec_dict/$id.bin"
        requireNotNull(object {}.javaClass.getResourceAsStream(resource)) { "Missing dictionary $resource" }.use { it.readBytes() }
    }

private fun Connection.exec(sql: String) {
    createStatement().use { it.execute(sql) }
}

private fun Connection.strings(sql: String): List<String> =
    createStatement().use { st ->
        st.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
    }
