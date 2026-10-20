package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentCompression
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.addJsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.Base64
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CompressLineContentCliTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val lines = mapOf(
        1L to "בראשית ברא אלהים את השמים ואת הארץ",
        2L to "<b>אמר רבי יוחנן</b> משום רבי שמעון בן יוחי",
        3L to "",
        4L to "plain ascii",
        5L to "ד\"ה ".repeat(500),
    )
    private val versions = listOf(
        Triple(1L, 1L, "בראשית ברא אלהים את השמים ואת הארץ"),
        Triple(1L, 2L, "גרסה אחרת"),
        Triple(1L, 3L, "שורה ריקה בבסיס"),
    )
    // (lineId, noteIndex) -> body; the first is line 1's text, so its frame must be line 1's.
    private val notes = mapOf(
        (1L to 0L) to "בראשית ברא אלהים את השמים ואת הארץ",
        (1L to 1L) to "גוף <i>הערה</i>\nשורה שנייה",
        (4L to 0L) to "",
    )

    @Test
    fun `line_note bodies become frames of the same codec and decode to the same bytes`() {
        val db = schemaSixDb("notes.db", withNotes = true)

        val report = connect(db).use { compressLineContent(it, chunkRows = 2, threads = 3) }

        assertEquals(notes.size.toLong(), report.noteRowsCompressed)
        assertEquals(lines.size + 2L + notes.size, report.framesValidated)
        connect(db).use { conn ->
            val bodies = noteBlobs(conn)
            LineContentCompression.Decompressor().use { d -> assertEquals(notes, bodies.mapValues { d.text(it.value) }) }
            assertContentEquals(
                blobs(conn, "SELECT id, content FROM line_content WHERE id = 1").getValue(1L),
                bodies.getValue(1L to 0L),
                "a body is compressed exactly like line text",
            )
            assertTrue(compressLineContent(conn, threads = 2).alreadyCompressed)
            stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 6)
            conn.createStatement().use { it.execute("UPDATE line_note SET body = 'plain' WHERE lineId = 4") }
            val error = assertFailsWith<IllegalStateException> { validateCompressed(conn) }
            assertTrue("1 line_note rows are not zstd frames" in error.message.orEmpty(), error.message)
            assertFailsWith<IllegalStateException> { stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 6) }
        }
    }

    @Test
    fun `line_note keys at the int64 edges are compressed and validated chunk by chunk`() {
        val keys = listOf(
            Long.MIN_VALUE to Long.MIN_VALUE,
            5L to Long.MAX_VALUE,
            6L to Long.MIN_VALUE,
            Long.MAX_VALUE to Long.MAX_VALUE,
        )
        val db = schemaSixDb("note-keys.db")
        connect(db).use { conn ->
            conn.prepareStatement("INSERT INTO line_note (lineId, noteIndex, charPos, marker, body) VALUES (?, ?, 0, NULL, ?)").use { ps ->
                for ((lineId, noteIndex) in keys) {
                    ps.setLong(1, lineId); ps.setLong(2, noteIndex); ps.setString(3, "גוף $lineId $noteIndex"); ps.executeUpdate()
                }
            }
            assertEquals(keys.size.toLong(), compressLineContent(conn, chunkRows = 1, threads = 2).noteRowsCompressed)
            assertEquals(lines.size + 2L + keys.size, validateCompressed(conn, threads = 2, chunkRows = 1))
            LineContentCompression.Decompressor().use { d ->
                assertEquals(keys.associateWith { (l, n) -> "גוף $l $n" }, noteBlobs(conn).mapValues { d.text(it.value) })
            }
            conn.createStatement().use {
                it.execute("UPDATE line_note SET body = X'28B52FFD' WHERE lineId = ${Long.MAX_VALUE}")
            }
            val error = assertFailsWith<IllegalStateException> { validateCompressed(conn, threads = 2, chunkRows = 1) }
            assertTrue("line_note row (${Long.MAX_VALUE}, ${Long.MAX_VALUE})" in error.message.orEmpty(), error.message)
        }
    }

    @Test
    fun `a 7 to 7 delta between compressed DBs carries only the changed note frame and reproduces the hash`() {
        val prev = schemaSixDb("prev7.db", withNotes = true).also { db -> connect(db).use { compressLineContent(it) } }
        val next = schemaSixDb("next7.db", withNotes = true, changeNote = true)
            .also { db -> connect(db).use { compressLineContent(it) } }
        val patch = tmp.root.toPath().resolve("p7.db")

        val produced = PatchDbProducer().produce(
            prev, next, patch, fromVersion = 31, toVersion = 32, fromSchemaVersion = 7, toSchemaVersion = 7,
        )

        assertEquals(1, produced.upsertCounts.getValue("line_note"), "only the changed note")
        assertEquals(0, produced.upsertCounts["line_content"] ?: 0, "unchanged lines keep their frames")
        val target = tmp.root.toPath().resolve("t7.db").also { Files.copy(prev, it) }
        val expected = connect(next).use { LogicalContentHasher.forSchemaVersion(7).compute(it) }
        connect(target).use { PatchApplier().apply(it, patch, expectedToContentHash = expected, expectedToSchemaVersion = 7) }
        connect(target).use { conn ->
            LineContentCompression.Decompressor().use { d -> assertEquals("שונה", d.text(noteBlobs(conn).getValue(1L to 1L))) }
        }
    }

    @Test
    fun `every text becomes a frame that decodes to the same bytes and NULL stays NULL`() {
        val db = schemaSixDb("c.db")

        val report = connect(db).use { compressLineContent(it, chunkRows = 2, threads = 3) }

        assertEquals(lines.size.toLong(), report.lineRowsCompressed)
        assertEquals(2L, report.versionRowsCompressed, "one inherited (NULL) row is left alone")
        connect(db).use { conn ->
            assertTrue(LineContentCompression.isCompressed(conn))
            assertEquals(
                setOf(LineContentCompression.bundledDictionaryId),
                LineContentCompression.storedDictionaries(conn).keys,
            )
            LineContentCompression.Decompressor().use { d ->
                assertEquals(lines, blobs(conn, "SELECT id, content FROM line_content").mapValues { d.text(it.value) })
                val v = rows(conn, "SELECT lineId, content FROM version_line ORDER BY lineId")
                assertNull(v.getValue(1L), "identical edition text is inherited")
                assertEquals("גרסה אחרת", d.text(v.getValue(2L)!!))
                assertEquals("שורה ריקה בבסיס", d.text(v.getValue(3L)!!))
            }
            stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 6)
            conn.createStatement().use { it.execute("UPDATE line_content SET content = 'plain' WHERE id = 1") }
            assertFailsWith<IllegalStateException> { stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 6) }
        }
    }

    @Test
    fun `a second run only validates and leaves the hash unchanged`() {
        val db = schemaSixDb("again.db")
        connect(db).use { compressLineContent(it, threads = 2) }
        val first = connect(db).use { LogicalContentHasher.forSchemaVersion(6).computeReport(it) }

        val again = connect(db).use { compressLineContent(it, threads = 2) }

        assertTrue(again.alreadyCompressed)
        assertEquals(first, connect(db).use { LogicalContentHasher.forSchemaVersion(6).computeReport(it) })
    }

    @Test
    fun `an interrupted run resumes and leaves already compressed rows untouched`() {
        val db = schemaSixDb("resume.db")
        val oneRow = connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            blobs(conn, "SELECT id, content FROM line_content WHERE id = 2").getValue(2L)
        }
        connect(db).use { conn ->
            conn.prepareStatement("UPDATE line_content SET content = ? WHERE id <> 2").use { ps ->
                ps.setString(1, "placeholder"); ps.executeUpdate()
            }
        }

        val report = connect(db).use { compressLineContent(it, threads = 2) }

        assertEquals(lines.size - 1L, report.lineRowsCompressed)
        connect(db).use { conn ->
            assertContentEquals(oneRow, blobs(conn, "SELECT id, content FROM line_content WHERE id = 2").getValue(2L))
            validateCompressed(conn)
        }
    }

    @Test
    fun `a BLOB that is not a zstd frame fails validation`() {
        val db = schemaSixDb("notframe.db")
        connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            conn.createStatement().use { it.execute("UPDATE line_content SET content = x'00010203' WHERE id = 1") }
            val error = assertFailsWith<IllegalStateException> { validateCompressed(conn) }
            assertTrue("1 line_content rows" in error.message.orEmpty(), error.message)
        }
    }

    @Test
    fun `a frame that is only the zstd magic fails validation with its row`() {
        val error = corruptAfterCompress(line = 3, frame = byteArrayOf(0x28, 0xB5.toByte(), 0x2F, 0xFD.toByte()))
        assertTrue("line_content row 3" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a version frame made with another dictionary id fails validation`() {
        val other = LineContentCompression.bundledDictionary.copyOf().also { it[4] = (it[4] + 1).toByte() }
        val frame = LineContentCompression.Compressor(other).use { it.compress("גרסה אחרת".toByteArray()) }
        val db = schemaSixDb("dictid.db")
        val error = connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            conn.prepareStatement("UPDATE version_line SET content = ? WHERE lineId = 2").use { ps ->
                ps.setBytes(1, frame); ps.executeUpdate()
            }
            assertFailsWith<IllegalStateException> { validateCompressed(conn) }
        }
        assertTrue("version_line row" in error.message.orEmpty() && "dictionary" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a frame that decodes past the reader cap fails validation`() {
        val huge = LineContentCompression.Compressor().use {
            it.compress(ByteArray(LineContentCompression.MAX_LINE_BYTES + 1) { 'a'.code.toByte() })
        }
        val error = corruptAfterCompress(line = 4, frame = huge)
        assertTrue("line_content row 4" in error.message.orEmpty() && "cap" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a frame that decodes to invalid UTF-8 fails validation`() {
        val frame = LineContentCompression.Compressor().use { it.compress(byteArrayOf(0x61, 0xFF.toByte(), 0x62)) }
        val error = corruptAfterCompress(line = 2, frame = frame)
        assertTrue("line_content row 2" in error.message.orEmpty() && "UTF-8" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `a resumed run decodes the BLOBs it skips`() {
        val db = schemaSixDb("resume-corrupt.db")
        connect(db).use { conn ->
            conn.createStatement().use { it.execute("UPDATE line_content SET content = x'28B52FFD' WHERE id = 5") }
        }
        val error = assertFailsWith<IllegalStateException> { connect(db).use { compressLineContent(it, threads = 2) } }
        assertTrue("line_content row 5" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `compression and validation include both ends of the signed key range`() {
        val db = schemaSixDb("key-extremes.db")
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("UPDATE line SET id = ${Long.MIN_VALUE} WHERE id = 1")
                st.execute("UPDATE line SET id = ${Long.MAX_VALUE} WHERE id = 5")
                st.execute("UPDATE line_content SET id = ${Long.MIN_VALUE} WHERE id = 1")
                st.execute("UPDATE line_content SET id = ${Long.MAX_VALUE} WHERE id = 5")
                st.execute("UPDATE version_line SET lineId = ${Long.MIN_VALUE} WHERE lineId = 1")
                st.execute("UPDATE version_line SET rowid = ${Long.MIN_VALUE} WHERE lineId = 2")
                st.execute("UPDATE version_line SET rowid = ${Long.MAX_VALUE} WHERE lineId = 3")
            }
            val report = compressLineContent(conn, chunkRows = 1, threads = 2)
            assertEquals(lines.size.toLong(), report.lineRowsCompressed)
            assertEquals(2L, report.versionRowsCompressed)
            assertEquals(7L, report.framesValidated)
            assertEquals(7L, validateCompressed(conn, threads = 2, chunkRows = 1))
            LineContentCompression.Decompressor().use { d ->
                val base = blobs(conn, "SELECT id, content FROM line_content")
                assertEquals(lines.getValue(1L), d.text(base.getValue(Long.MIN_VALUE)))
                assertEquals(lines.getValue(5L), d.text(base.getValue(Long.MAX_VALUE)))
                val editions = blobs(conn, "SELECT rowid, content FROM version_line WHERE content IS NOT NULL")
                assertEquals("גרסה אחרת", d.text(editions.getValue(Long.MIN_VALUE)))
                assertEquals("שורה ריקה בבסיס", d.text(editions.getValue(Long.MAX_VALUE)))
            }
        }
    }

    @Test
    fun `corrupt boundary keys fail validation resume and stamping`() {
        for ((table, key) in listOf(
            "line_content" to Long.MIN_VALUE,
            "line_content" to Long.MAX_VALUE,
            "version_line" to Long.MIN_VALUE,
            "version_line" to Long.MAX_VALUE,
        )) {
            val db = schemaSixDb("boundary-corrupt-$table-$key.db")
            connect(db).use { conn ->
                compressLineContent(conn, threads = 1)
                conn.createStatement().use { st ->
                    if (table == "line_content") {
                        st.execute("UPDATE line SET id = $key WHERE id = 1")
                        st.execute("UPDATE line_content SET id = $key, content = X'28B52FFD' WHERE id = 1")
                        st.execute("UPDATE version_line SET lineId = $key WHERE lineId = 1")
                    } else {
                        st.execute("UPDATE version_line SET rowid = $key, content = X'28B52FFD' WHERE lineId = 2")
                    }
                }
                for (attempt in listOf<() -> Unit>(
                    { validateCompressed(conn, threads = 2, chunkRows = 1) },
                    { compressLineContent(conn, threads = 2, chunkRows = 1) },
                    { stampSchemaVersion(conn, dbVersion = 31, dbSchemaVersion = 6) },
                )) {
                    val error = assertFailsWith<IllegalStateException> { attempt() }
                    assertTrue("$table row $key" in error.message.orEmpty(), error.message)
                }
            }
        }
    }

    @Test
    fun `validator rejects invalid parallelism and chunk sizes before reading the DB`() {
        val conn = DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.close()
        for ((threads, chunkRows) in listOf(0 to 1, -1 to 1, 1 to 0, 1 to -1)) {
            val error = assertFailsWith<IllegalArgumentException> {
                validateCompressed(conn, threads = threads, chunkRows = chunkRows)
            }
            assertTrue("must be positive" in error.message.orEmpty(), error.message)
        }
    }

    /** Shared dictionaries must not change a byte: the stored frames match the frozen contract. */
    @Test
    fun `frames written by the parallel run are byte-identical to the frozen contract`() {
        val db = schemaSixDb("frozen.db")
        connect(db).use { compressLineContent(it, chunkRows = 2, threads = 3) }
        val stored = connect(db).use { blobs(it, "SELECT id, content FROM line_content ORDER BY id") }
        val digest = LineContentCompression.sha256(stored.toSortedMap().values.fold(ByteArray(0)) { acc, f -> acc + f })
        assertEquals(FROZEN_FRAMES_SHA256, digest)
    }

    @Test
    fun `a DB that already holds another dictionary is refused`() {
        val db = schemaSixDb("other.db")
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE zstd_dict (id INTEGER PRIMARY KEY NOT NULL, dict BLOB NOT NULL)")
                st.execute("INSERT INTO zstd_dict VALUES (7, x'00')")
            }
        }
        val error = assertFailsWith<IllegalStateException> { connect(db).use { compressLineContent(it) } }
        assertTrue("already holds dictionaries" in error.message.orEmpty(), error.message)
    }

    /**
     * Frozen bytes: a different zstd-jni, level or dictionary changes every row and
     * turns each patch into the whole library. Update only with a full-rebase bump.
     */
    @Test
    fun `frames are byte-identical to the frozen contract`() {
        val frames = LineContentCompression.Compressor().use { c ->
            lines.toSortedMap().values.map { c.compress(it.toByteArray()) }
        }
        val digest = LineContentCompression.sha256(frames.fold(ByteArray(0)) { acc, f -> acc + f })
        assertEquals(FROZEN_FRAMES_SHA256, digest)
        assertEquals(LineContentCompression.DICT_SHA256, LineContentCompression.sha256(LineContentCompression.bundledDictionary))

        val expected = goldenVector(lines.toSortedMap().values.zip(frames))
        val stored = requireNotNull(javaClass.getResourceAsStream(GOLDEN_VECTOR)) { "missing $GOLDEN_VECTOR" }
            .bufferedReader(Charsets.UTF_8).readText().replace("\r\n", "\n")
        assertEquals(expected, stored, "regenerate $GOLDEN_VECTOR (the app decodes it) with:\n$expected")
    }

    /** The frames above as a test vector the app decodes with the same dictionary. */
    private fun goldenVector(frames: List<Pair<String, ByteArray>>): String {
        val vector = buildJsonObject {
            put("dictId", LineContentCompression.bundledDictionaryId)
            put("dictSha256", LineContentCompression.DICT_SHA256)
            put("level", LineContentCompression.LEVEL)
            put("framesSha256", FROZEN_FRAMES_SHA256)
            putJsonArray("frames") {
                for ((text, frame) in frames) {
                    addJsonObject {
                        put("text", text)
                        put("frame", Base64.getEncoder().encodeToString(frame))
                    }
                }
            }
        }
        return Json { prettyPrint = true }.encodeToString(JsonObject.serializer(), vector) + "\n"
    }

    /** Ids 32768..65535 take a 2-byte Dictionary_ID field in every frame; ≤32767 is reserved by zdict.h. */
    @Test
    fun `the dictionary id fits the 2-byte frame header field`() {
        assertTrue(LineContentCompression.bundledDictionaryId in 32768L..65535L, "${LineContentCompression.bundledDictionaryId}")
        val frame = LineContentCompression.Compressor().use { it.compress("בראשית".toByteArray()) }
        assertEquals(2, frame[4].toInt() and 0x03, "Frame_Header_Descriptor Dictionary_ID_flag")
    }

    @Test
    fun `a delta between DBs with the same dictionary carries frames and reproduces the hash`() {
        val prev = schemaSixDb("prev.db").also { db -> connect(db).use { compressLineContent(it) } }
        val next = schemaSixDb("next.db", changeLine2 = true).also { db -> connect(db).use { compressLineContent(it) } }
        val patch = tmp.root.toPath().resolve("p.db")

        val produced = PatchDbProducer().produce(
            prev, next, patch, fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 6,
        )

        assertEquals(1, produced.upsertCounts.getValue("line_content"), "only the changed row")
        val target = tmp.root.toPath().resolve("t.db").also { Files.copy(prev, it) }
        val expected = connect(next).use { LogicalContentHasher.forSchemaVersion(6).compute(it) }
        connect(target).use { PatchApplier().apply(it, patch, expectedToContentHash = expected, expectedToSchemaVersion = 6) }
        connect(target).use { conn ->
            LineContentCompression.Decompressor().use { d ->
                assertEquals("שונה", d.text(blobs(conn, "SELECT id, content FROM line_content WHERE id = 2").getValue(2L)))
            }
        }
    }

    @Test
    fun `a delta across a dictionary change is unpatchable`() {
        val plain = schemaSixDb("plain.db")
        val compressed = schemaSixDb("compressed.db").also { db -> connect(db).use { compressLineContent(it) } }

        val error = assertFailsWith<UnpatchableAnchorException> {
            PatchDbProducer().produce(
                plain, compressed, tmp.root.toPath().resolve("x.db"),
                fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 6,
            )
        }
        assertEquals("zstd_dict", error.table)
    }

    // ── fixtures ────────────────────────────────────────────────────────────

    private fun LineContentCompression.Decompressor.text(frame: ByteArray): String =
        String(decompress(frame), Charsets.UTF_8)

    private fun schemaSixDb(
        name: String,
        changeLine2: Boolean = false,
        withNotes: Boolean = false,
        changeNote: Boolean = false,
    ): Path {
        val db = tmp.root.toPath().resolve(name)
        JdbcSqliteDriver("jdbc:sqlite:${db.toAbsolutePath()}").use { SeforimDb.Schema.create(it) }
        connect(db).use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO source(id, name) VALUES (1, 'src')")
                st.execute("INSERT INTO category(id, title) VALUES (1, 'cat')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'book')")
                st.execute("INSERT INTO book_version(id, bookId, versionTitle, hasContent) VALUES (1, 1, 'A', 1)")
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content) VALUES (?, 1, ?, ?)").use { ps ->
                for ((id, text) in lines) {
                    ps.setLong(1, id); ps.setLong(2, id)
                    ps.setString(3, if (changeLine2 && id == 2L) "שונה" else text)
                    ps.executeUpdate()
                }
            }
            conn.prepareStatement("INSERT INTO version_line(versionId, lineId, content) VALUES (?, ?, ?)").use { ps ->
                for ((versionId, lineId, text) in versions) {
                    ps.setLong(1, versionId); ps.setLong(2, lineId); ps.setString(3, text); ps.executeUpdate()
                }
            }
            splitLineContent(conn)
            if (withNotes) {
                conn.prepareStatement(
                    "INSERT INTO line_note (lineId, noteIndex, charPos, marker, body) VALUES (?, ?, 0, NULL, ?)",
                ).use { ps ->
                    for ((key, body) in notes) {
                        ps.setLong(1, key.first); ps.setLong(2, key.second)
                        ps.setString(3, if (changeNote && key == (1L to 1L)) "שונה" else body)
                        ps.executeUpdate()
                    }
                }
            }
        }
        return db
    }

    private fun noteBlobs(conn: Connection): Map<Pair<Long, Long>, ByteArray> =
        conn.createStatement().use { st ->
            st.executeQuery("SELECT lineId, noteIndex, body FROM line_note").use { rs ->
                buildMap {
                    while (rs.next()) {
                        assertFalse(rs.getObject(3) is String, "note ${rs.getLong(1)}/${rs.getLong(2)} is still TEXT")
                        put(rs.getLong(1) to rs.getLong(2), rs.getBytes(3))
                    }
                }
            }
        }

    private fun corruptAfterCompress(line: Long, frame: ByteArray): IllegalStateException {
        val db = schemaSixDb("corrupt-$line.db")
        return connect(db).use { conn ->
            compressLineContent(conn, threads = 1)
            conn.prepareStatement("UPDATE line_content SET content = ? WHERE id = ?").use { ps ->
                ps.setBytes(1, frame); ps.setLong(2, line); ps.executeUpdate()
            }
            assertFailsWith<IllegalStateException> { validateCompressed(conn) }
        }
    }

    private fun connect(db: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}")

    private fun blobs(conn: Connection, sql: String): Map<Long, ByteArray> =
        rows(conn, sql).mapValues { checkNotNull(it.value) }

    private fun rows(conn: Connection, sql: String): Map<Long, ByteArray?> =
        conn.createStatement().use { st ->
            st.executeQuery(sql).use { rs ->
                buildMap {
                    while (rs.next()) {
                        assertFalse(rs.getObject(2) is String, "row ${rs.getLong(1)} is still TEXT")
                        put(rs.getLong(1), rs.getBytes(2))
                    }
                }
            }
        }

    private companion object {
        const val FROZEN_FRAMES_SHA256 = "262dbd7fe3fc9395582e83c2d3b69dcb789c19e64d102b3b02614263716018fe"
        const val GOLDEN_VECTOR = "/zstd/line_content_golden_frames.json"
    }
}
