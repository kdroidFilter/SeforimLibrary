package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateSnapshot
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BuildStateWriter
import io.github.kdroidfilter.seforimlibrary.common.buildstate.IdTable
import io.github.kdroidfilter.seforimlibrary.common.notes.LineNoteRehydration
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerializationException
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import java.lang.reflect.InvocationTargetException
import java.nio.file.Files

/**
 * Proves the resolver-reuse mechanism the Phase-2 LINKER importer relies on: build
 * refsByCanonical/refsByBase from sidecar-style RefEntry rows, then resolve a target_ref
 * back to the RefEntry whose (path, lineIndex) → lineId. (The full ~99% corpus coverage
 * is proven at bootstrap against a real sidecar; the earlier Python proxy showed 98–99%.)
 */
class GenerateLinkerLinksTest {

    private val refs = listOf(
        RefEntry("Psalms 16:8", "תהלים ט״ז:ח׳", "Tanakh/Writings/Psalms", 100),
        RefEntry("Genesis 1:1", "בראשית א׳:א׳", "Tanakh/Torah/Genesis", 0),
        RefEntry("Exodus 29:43", "שמות כ״ט:מ״ג", "Tanakh/Torah/Exodus", 500),
        RefEntry("Exodus 29:44", "שמות כ״ט:מ״ד", "Tanakh/Torah/Exodus", 501),
        RefEntry("Exodus 29:46", "שמות כ״ט:מ״ו", "Tanakh/Torah/Exodus", 503),
        RefEntry("Berakhot 2a:1", "ברכות ב׳ א:א", "Talmud/Bavli/Berakhot", 3),
        RefEntry("Berakhot 2a:2", "ברכות ב׳ א:ב", "Talmud/Bavli/Berakhot", 4),
        RefEntry("Berakhot 2a:3", "ברכות ב׳ א:ג", "Talmud/Bavli/Berakhot", 5),
        RefEntry("Berakhot 2b:1", "ברכות ב׳ ב:א", "Talmud/Bavli/Berakhot", 6),
        RefEntry("Berakhot 2b:2", "ברכות ב׳ ב:ב", "Talmud/Bavli/Berakhot", 7),
    )
    private val byCanonical = refs.groupBy { canonicalCitation(it.ref) }
    private val byBase = HashMap<String, RefEntry>().apply {
        for (e in refs) {
            val base = canonicalBase(e.ref)
            val ex = this[base]
            if (ex == null || e.lineIndex < ex.lineIndex) this[base] = e
        }
    }
    private val lastByBase = HashMap<String, RefEntry>().apply {
        for (e in refs) {
            val base = canonicalBase(e.ref)
            val ex = this[base]
            if (ex == null || e.lineIndex > ex.lineIndex) this[base] = e
        }
    }

    private fun resolveOne(ref: String): RefEntry? =
        resolveRefs(ref, byCanonical, byBase).firstOrNull()

    @Test
    fun exactSegmentRefResolves() {
        val e = resolveOne("Psalms 16:8")
        assertNotNull(e)
        assertEquals(100, e.lineIndex)
        assertEquals("Tanakh/Writings/Psalms", e.path)
    }

    @Test
    fun rangeStartResolvesToItsSegment() {
        // A ranged citation resolves via its range-start (Exodus 29:43-44 → Exodus 29:43).
        val e = resolveOne("Exodus 29:43-44")
        assertNotNull(e)
        assertEquals(500, e.lineIndex)
    }

    @Test
    fun baseFallbackResolves() {
        // "Berakhot 2a" (no segment) falls back to the base's first entry.
        val e = resolveOne("Berakhot 2a")
        assertNotNull(e)
        assertEquals(3, e.lineIndex)
    }

    @Test
    fun unknownRefResolvesToEmpty() {
        assertTrue(resolveRefs("Nonexistent Book 9:9", byCanonical, byBase).isEmpty())
    }

    @Test
    fun linkerConnectionTypeIsWiredAndOrdinalIsStable() {
        assertEquals(ConnectionType.LINKER, ConnectionType.fromString("linker"))
        // LINKER stays ordinal 14 so ids 1–15 don't shift; named types append after it.
        assertEquals(14, ConnectionType.LINKER.ordinal)
        val afterLinker = listOf(
            ConnectionType.SIFREI_MITZVOT,
            ConnectionType.ESSAY,
            ConnectionType.ALLUSION,
            ConnectionType.LITURGY,
            ConnectionType.ELUCIDATION,
            ConnectionType.EXPLICATION,
            ConnectionType.LAW,
            ConnectionType.SUMMARY,
        )
        afterLinker.forEachIndexed { i, t ->
            assertEquals(15 + i, t.ordinal)
        }
    }

    @Test
    fun exactSegmentHasNoRangeEnd() {
        assertEquals(null, resolveRefEnd("Psalms 16:8", byCanonical, byBase, lastByBase))
    }

    @Test
    fun sectionCitationEndsAtItsLastSegment() {
        // "Berakhot 2a" (a whole amud) → scope ends at 2a:3 (lineIndex 5).
        val end = resolveRefEnd("Berakhot 2a", byCanonical, byBase, lastByBase)
        assertNotNull(end)
        assertEquals(5, end.lineIndex)
    }

    @Test
    fun dashedRangeEndsAtItsEndSegment() {
        // "Exodus 29:43-44" → end at 29:44 (lineIndex 501); start resolves per resolveRefs.
        val end = resolveRefEnd("Exodus 29:43-44", byCanonical, byBase, lastByBase)
        assertNotNull(end)
        assertEquals(501, end.lineIndex)
        assertEquals("Tanakh/Torah/Exodus", end.path)
    }

    @Test
    fun amudRangeEndsAtLastSegmentOfEndAmud() {
        // "Berakhot 2a-2b" → end = last segment of 2b (lineIndex 7).
        val end = resolveRefEnd("Berakhot 2a-2b", byCanonical, byBase, lastByBase)
        assertNotNull(end)
        assertEquals(7, end.lineIndex)
    }

    @Test
    fun rangeEndRefReconstruction() {
        assertEquals("exodus 29:44", rangeEndRef("exodus 29:43", "exodus 29:43-44"))
        assertEquals("exodus 30:2", rangeEndRef("exodus 29:43", "exodus 29:43-30:2"))
        assertEquals("berakhot 2b", rangeEndRef("berakhot 2a", "berakhot 2a-2b"))
    }

    @Test
    fun contentHashMatchesPythonContract() {
        // Locks the cross-language contract: the SAME constants are asserted in
        // LinkerToOtzaria/tests/test_linker_artifact.py for linker_artifact.content_hash.
        assertEquals("aaf4c61ddcc5e8a2", linkerContentHash("hello"))
        assertEquals("643cbc0fbf2800d7", linkerContentHash("שלום עולם"))
        assertEquals("da39a3ee5e6b4b0d", linkerContentHash(""))
    }

    @Test
    fun headingsAreRejectedWithWhitespaceAndAllHeadingLevels() {
        assertTrue(isHeadingContent("<h1>ספר</h1>"))
        assertTrue(isHeadingContent("  \uFEFF<H6 class=\"x\">פרק</H6>"))
        assertFalse(isHeadingContent("לפני <h2>כותרת בתוך הטקסט</h2>"))
        assertFalse(isHeadingContent("<header>כותרת</header>"))
    }

    @Test
    fun targetIdentityRequiresBookLineAndSemanticRef() {
        val entry = RefEntry("Genesis 1:1", "בראשית א, א", "Genesis", 7)
        assertTrue(targetIdentityMatches(entry, 11, 6, "בראשית א, א", 11))
        assertFalse(targetIdentityMatches(entry, 12, 6, "בראשית א, א", 11))
        assertFalse(targetIdentityMatches(entry, 11, 7, "בראשית א, א", 11))
        assertFalse(targetIdentityMatches(entry, 11, 6, "שמות א, א", 11))
        assertFalse(targetIdentityMatches(entry, 11, 6, "בראשית א, א", null))
    }

    @Test
    fun sidecarV2RoundTripsStableIdentityAndRejectsLegacyRows() {
        val directory = Files.createTempDirectory("linker-sidecar-test")
        val path = directory.resolve("refs.tsv")
        try {
            val entry = RefEntry("Genesis 1:1", "בראשית א, א", "Tanakh/Genesis", 1)
            writeLinkerSidecar(
                path.toString(),
                listOf(entry),
                mapOf((entry.path to 0) to 123L),
                mapOf(entry.path to "בראשית"),
                "ספריא",
            )
            assertEquals(
                listOf(
                    LinkerSidecarEntry(
                        "Genesis 1:1", "בראשית א, א", "Tanakh/Genesis", 1,
                        123L, "ספריא", "בראשית",
                    ),
                ),
                readLinkerSidecar(path.toString()),
            )
            Files.writeString(path, "Genesis 1:1\tבראשית א, א\tTanakh/Genesis\t1\t123\n")
            val error = runCatching { readLinkerSidecar(path.toString()) }.exceptionOrNull()
            assertNotNull(error)
            assertTrue(error.message.orEmpty().contains("sidecar header"))
        } finally {
            Files.deleteIfExists(path)
            Files.deleteIfExists(directory)
        }
    }

    @Test
    fun strictFlagAcceptsOnlyTrueAndFalse() {
        assertTrue(parseStrictFlag("linkerStrict", "true"))
        assertTrue(parseStrictFlag("linkerStrict", "TRUE"))
        assertTrue(parseStrictFlag("linkerStrict", "  True  "))
        assertFalse(parseStrictFlag("linkerStrict", "false"))
        assertFalse(parseStrictFlag("linkerStrict", "FALSE"))
    }

    @Test
    fun strictFlagIsOffOnlyWhenThePropertyIsAbsent() {
        assertFalse(parseStrictFlag("linkerStrict", null))
    }

    @Test
    fun strictFlagFailsLoudlyOnAnythingElse() {
        for (raw in listOf("1", "0", "yes", "no", "on", "treu", "", "   ")) {
            val error = assertFailsWith<IllegalStateException>("value should have failed: \"$raw\"") {
                parseStrictFlag("linkerStrict", raw)
            }
            assertTrue(error.message.orEmpty().contains("linkerStrict"))
            assertTrue(error.message.orEmpty().contains("\"$raw\""))
        }
    }

    // Drives the real entry point (reflectively: the package holds several `main`s) to prove the
    // strict gate is fed by the strict parser, and that a junk value stops the run before the DB.
    @Test
    fun mainRejectsAnInvalidStrictFlagBeforeOpeningTheDatabase() {
        val dir = Files.createTempDirectory("linkerStrictMain")
        val artifacts = Files.createDirectory(dir.resolve("artifacts"))
        val artifact = Files.writeString(artifacts.resolve("a.jsonl"), "")
        val db = dir.resolve("seforim.db")
        val keys = listOf("seforimDb", "linkerArtifacts", "linkerSidecar", "linkerStrict")
        val saved = keys.associateWith { System.getProperty(it) }
        val severity = Logger.config.minSeverity
        try {
            System.setProperty("seforimDb", db.toString())
            System.setProperty("linkerArtifacts", artifacts.toString())
            System.setProperty("linkerSidecar", dir.resolve("sidecar.tsv").toString())
            System.setProperty("linkerStrict", "banana")
            val facade = "io.github.kdroidfilter.seforimlibrary.sefariasqlite.GenerateLinkerLinksKt"
            val main = Class.forName(facade).getMethod("main", Array<String>::class.java)
            val failure = assertFailsWith<InvocationTargetException> {
                main.invoke(null, arrayOf<String>() as Any)
            }
            val cause = failure.cause
            assertTrue(cause is IllegalStateException, "unexpected failure: $cause")
            assertTrue(cause.message.orEmpty().contains("unrecognized value"))
            // The flag is parsed before the type-replacing import, so nothing was opened.
            assertFalse(Files.exists(db))
        } finally {
            keys.forEach { key ->
                val value = saved[key]
                if (value == null) System.clearProperty(key) else System.setProperty(key, value)
            }
            Logger.setMinSeverity(severity)
            Files.deleteIfExists(artifact)
            Files.deleteIfExists(artifacts)
            Files.deleteIfExists(dir)
        }
    }

    private val sourceContent = "see Genesis 1:1 and again Genesis 1:1"

    private fun record(
        start: Int,
        end: Int,
        base: Int?,
        target: String = "Genesis 1:1",
        hashedContent: String = sourceContent,
    ): String {
        val baseField = if (base == null) "" else "\"line_index_base\":$base,"
        return "{\"book_key\":{\"source_name\":\"Sefaria\",\"canonical_he_title\":\"SrcBook\"}," +
            "\"line_index\":0,$baseField\"start\":$start,\"end\":$end,\"target_ref\":\"$target\"," +
            "\"source_hash\":\"${linkerContentHash(hashedContent)}\"}"
    }

    private class LinkerRun(
        val failure: Throwable?,
        val links: Long,
        val anchors: Long,
        val anchorRanges: List<Pair<Int, Int>> = emptyList(),
        val targetSpans: List<Pair<Int, Int?>> = emptyList(),
    )

    // Runs the real entry point (reflective main) on a fixture DB + build_state + sidecar.
    private fun runLinkerMain(
        artifactBody: String,
        strict: String?,
        splitLineContent: Boolean = false,
        storedSource: String = sourceContent,
        targets: List<RefEntry> = listOf(RefEntry("Genesis 1:1", "TgtBook 1:1", "Tanakh/Genesis", 1)),
        notes: List<Pair<Int, String>> = emptyList(),
        anchoredNotes: Set<Int> = emptySet(),
    ): LinkerRun {
        val dir = Files.createTempDirectory("linkerMainFixture")
        val artifacts = Files.createDirectory(dir.resolve("artifacts"))
        Files.writeString(artifacts.resolve("a.jsonl"), artifactBody)
        val db = dir.resolve("seforim.db")
        val buildState = dir.resolve("seforim.db.buildstate")
        val sidecar = dir.resolve("sidecar.tsv")
        val keys = listOf("seforimDb", "linkerArtifacts", "linkerSidecar", "linkerStrict", "buildStatePath")
        val saved = keys.associateWith { System.getProperty(it) }
        val severity = Logger.config.minSeverity
        try {
            val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$db")
            val repo = SeforimRepository(db.toString(), driver)
            val linkerTypeId = runBlocking {
                val sourceId = repo.insertSource("Sefaria")
                val catId = repo.insertCategory(Category(0, null, "Cat", level = 0, order = 1))
                repo.insertBook(Book(id = 1, categoryId = catId, sourceId = sourceId, title = "TgtBook", heRef = "TgtBook"))
                repo.insertBook(Book(id = 2, categoryId = catId, sourceId = sourceId, title = "SrcBook", heRef = "SrcBook"))
                repo.insertLinesBatch(
                    targets.mapIndexed { i, t ->
                        Line(id = 10L + i, bookId = 1, lineIndex = t.lineIndex - 1, content = "target", heRef = t.heRef)
                    } + Line(id = 20, bookId = 2, lineIndex = 0, content = storedSource, heRef = "SrcBook 1"),
                )
                repo.executeRawQuery("INSERT INTO connection_type(name) VALUES ('${ConnectionType.LINKER.name}')")
                var id = 0L
                driver.executeQuery(null, "SELECT id FROM connection_type WHERE name = 'LINKER'",
                    { c -> if (c.next().value) id = c.getLong(0)!!; QueryResult.Value(Unit) }, 0)
                id
            }
            repo.close()
            DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                c.prepareStatement("INSERT INTO line_note (lineId, noteIndex, charPos, marker, body) VALUES (20, ?, ?, ?, ?)").use { ps ->
                    notes.forEachIndexed { i, (charPos, body) ->
                        ps.setInt(1, i); ps.setInt(2, charPos)
                        ps.setString(3, if (i in anchoredNotes) "" else null)
                        ps.setString(4, body); ps.executeUpdate()
                    }
                }
            }
            if (splitLineContent) {
                // The schema-6 shape SplitLineContentCli leaves behind.
                DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                    c.createStatement().use { st ->
                        st.execute("CREATE TABLE line_content (id INTEGER PRIMARY KEY NOT NULL, content TEXT NOT NULL)")
                        st.execute("INSERT INTO line_content SELECT id, content FROM line")
                        st.execute("ALTER TABLE line DROP COLUMN content")
                    }
                }
            }
            BuildStateWriter().write(
                BuildStateSnapshot.empty().copy(
                    lookups = mapOf(IdTable.CONNECTION_TYPE to mapOf(ConnectionType.LINKER.name to linkerTypeId)),
                ),
                buildState,
            )
            writeLinkerSidecar(
                sidecar.toString(), targets,
                targets.mapIndexed { i, t -> (t.path to t.lineIndex - 1) to 10L + i }.toMap(),
                targets.associate { it.path to "TgtBook" }, "Sefaria",
            )

            System.setProperty("seforimDb", db.toString())
            System.setProperty("linkerArtifacts", artifacts.toString())
            System.setProperty("linkerSidecar", sidecar.toString())
            System.setProperty("buildStatePath", buildState.toString())
            if (strict == null) System.clearProperty("linkerStrict") else System.setProperty("linkerStrict", strict)
            val facade = "io.github.kdroidfilter.seforimlibrary.sefariasqlite.GenerateLinkerLinksKt"
            val main = Class.forName(facade).getMethod("main", Array<String>::class.java)
            val failure = runCatching { main.invoke(null, arrayOf<String>() as Any) }.exceptionOrNull()
                ?.let { (it as? InvocationTargetException)?.cause ?: it }
            fun count(sql: String): Long = DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                c.createStatement().use { st -> st.executeQuery(sql).use { rs -> rs.next(); rs.getLong(1) } }
            }
            val linkerLinks = "SELECT id FROM link WHERE connectionTypeId = $linkerTypeId"
            val ranges = DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                c.createStatement().use { st ->
                    st.executeQuery(
                        "SELECT charStart, charEnd FROM link_anchor WHERE linkId IN ($linkerLinks) ORDER BY charStart",
                    ).use { rs -> buildList { while (rs.next()) add(rs.getInt(1) to rs.getInt(2)) } }
                }
            }
            val spans = DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                c.createStatement().use { st ->
                    st.executeQuery(
                        "SELECT l.targetLineIndex, r.endLineIndex FROM link l " +
                            "LEFT JOIN link_range r ON r.linkId = l.id AND r.side = 1 " +
                            "WHERE l.connectionTypeId = $linkerTypeId ORDER BY l.targetLineIndex",
                    ).use { rs -> buildList { while (rs.next()) add(rs.getInt(1) to rs.getObject(2)?.let { (it as Number).toInt() }) } }
                }
            }
            return LinkerRun(
                failure,
                count("SELECT COUNT(*) FROM ($linkerLinks)"),
                count("SELECT COUNT(*) FROM link_anchor WHERE linkId IN ($linkerLinks)"),
                ranges,
                spans,
            )
        } finally {
            keys.forEach { key ->
                val value = saved[key]
                if (value == null) System.clearProperty(key) else System.setProperty(key, value)
            }
            Logger.setMinSeverity(severity)
            dir.toFile().deleteRecursively()
        }
    }

    @Test
    fun theLinkerReadsTheLineWithItsNotesAndAnAnchorOnANoteIsNotWritten() {
        val stored = "see Genesis 1:1 and again"
        val notes = listOf(15 to "Genesis 1:1")
        val read = "see Genesis 1:1<i class=\"footnote\">Genesis 1:1</i> and again"
        assertEquals(read, rehydrateLinkerLine(stored, notes).first)
        val inNote = read.indexOf(">Genesis") + 1
        val body = record(4, 15, base = 0, hashedContent = read) + "\n" +
            record(inNote, inNote + 11, base = 0, hashedContent = read) + "\n"
        for (split in listOf(false, true)) {
            val run = runLinkerMain(body, strict = "true", splitLineContent = split, storedSource = stored, notes = notes)
            assertEquals(null, run.failure)
            assertEquals(1L, run.links, "the citation in the note still links its line")
            assertEquals(listOf(4 to 15), run.anchorRanges, "only the citation outside the note gets an anchor")
        }
        // A record hashed on the stored text alone is stale once the line has notes.
        val stale = runLinkerMain(record(4, 15, base = 0, hashedContent = stored) + "\n", strict = null, storedSource = stored, notes = notes)
        assertEquals(0L, stale.links)
    }

    @Test
    fun anAnchorAfterANoteMapsBackToTheStoredLine() {
        val stored = "x Genesis 1:1"
        val notes = listOf(1 to "הערה")
        val read = rehydrateLinkerLine(stored, notes).first
        val start = read.indexOf("Genesis")
        val run = runLinkerMain(record(start, start + 11, base = 0, hashedContent = read) + "\n", strict = "true", storedSource = stored, notes = notes)
        assertEquals(listOf(2 to 13), run.anchorRanges)
    }

    @Test
    fun theInlineRehydrationMatchesTheSharedOne() {
        data class Case(val stored: String, val notes: List<Pair<Int, String>>, val anchored: Set<Int> = emptySet())
        val cases = listOf(
            Case("א<sup class=\"footnote-marker\">1</sup> ב &amp; ג</b>", listOf(2 to "x", 2 to "<i>y</i>", 8 to "z")),
            Case("<h2>כותרת</h2>", listOf(0 to "a", 5 to "b")),
            Case("<h2>א<sup>1</sup></h2> ב<b>ג</b>", listOf(2 to "x", 4 to "y")),
            Case("", listOf(0 to "only")),
            Case("<sup class=\"footnote-marker\">1<i class=\"footnote\"></i></sup>", listOf(1 to "a", 1 to "b"), setOf(0)),
            Case("<sup class=\"footnote-marker\"><b>1</b></sup>", listOf(1 to "a", 1 to "b")),
            Case("<sup class=\"footnote-marker\"></sup>", listOf(0 to "a", 0 to "b"), setOf(0)),
            Case("<sup>1</sup> סוף", listOf(0 to "before")),
            Case("<b>א<sup class=\"footnote-marker\"></sup></b>", listOf(1 to "a"), setOf(0)),
            Case("<b>א</b><sup class=\"footnote-marker\"></sup>", listOf(1 to "a"), setOf(0)),
            Case("<p><b>אב</b></p>המשך", listOf(2 to "a")),
            Case("<p><b>אב</b></p>", listOf(2 to "a")),
            Case("<custom-block>אב</custom-block>", listOf(2 to "a")),
            Case("<B>אב</B ></p>", listOf(2 to "a")),
            Case("<b>אב</ b></p>", listOf(2 to "a")),
            Case("<b>סוף</b><br>", listOf(3 to "a")),
            Case("😀&amp; סוף", listOf(2 to "a", 3 to "b")),
        )
        for ((stored, notes, anchored) in cases) {
            val shared = LineNoteRehydration.rehydrate(stored, notes.mapIndexed { i, note ->
                LineNoteRehydration.NoteAt(note.first, note.second, i in anchored)
            })
            val (text, bodies) = rehydrateLinkerLine(stored, notes, anchored)
            assertEquals(shared.text, text)
            assertEquals(shared.bodies, bodies)
            for (start in 0..text.length) for (end in start..text.length) {
                assertEquals(shared.storedRange(start, end), linkerStoredRange(bodies, start, end))
            }
        }
    }

    @Test
    fun anEmptyMarkerIsReadFromSqlAndAdjacentBodiesKeepTheirOrder() {
        val marker = "<sup class=\"footnote-marker\"></sup>"
        val stored = "$marker Genesis 1:1"
        val notes = listOf(0 to "ראשונה", 0 to "שנייה")
        val read = "$marker<i class=\"footnote\">ראשונה</i><i class=\"footnote\">שנייה</i> Genesis 1:1"
        val start = read.indexOf("Genesis")
        for (split in listOf(false, true)) {
            val run = runLinkerMain(
                record(start, start + 11, base = 0, hashedContent = read) + "\n", strict = "true",
                splitLineContent = split, storedSource = stored, notes = notes, anchoredNotes = setOf(0),
            )
            assertEquals(null, run.failure)
            // Anchors use visible offsets in stored text: the empty sup contributes none.
            assertEquals(listOf(1 to 12), run.anchorRanges)
        }
    }

    @Test
    fun strictRunFailsOnAnEmptyArtifactPayload() {
        val run = runLinkerMain("", strict = "true")
        val cause = run.failure
        assertTrue(cause is IllegalStateException, "unexpected failure: $cause")
        assertTrue(cause.message.orEmpty().contains("0 LINKER links written from 0 records parsed in 1 artifact files"))
    }

    @Test
    fun nonStrictRunAcceptsAnEmptyArtifactPayload() {
        val run = runLinkerMain("", strict = null)
        assertEquals(null, run.failure)
        assertEquals(0L, run.links)
    }

    @Test
    fun oneBasedLineIndexRecordIsRejected() {
        val run = runLinkerMain(record(4, 15, base = 1) + "\n", strict = null)
        val cause = run.failure
        assertTrue(cause is IllegalStateException, "unexpected failure: $cause")
        assertTrue(cause.message.orEmpty().contains("line_index_base=1"))
        assertTrue(cause.message.orEmpty().contains("a.jsonl"))
    }

    @Test
    fun strictRunReportsParsedRecordsWhenNoneBecomeALink() {
        // An unresolved target is advisory, so only the zero-links gate can fail this run.
        val run = runLinkerMain(record(4, 15, base = 0, target = "Nowhere 9:9") + "\n", strict = "true")
        val cause = run.failure
        assertTrue(cause is IllegalStateException, "unexpected failure: $cause")
        assertTrue(
            cause.message.orEmpty().contains("0 LINKER links written from 1 records parsed in 1 artifact files"),
            "wrong diagnostic: ${cause.message}",
        )
    }

    @Test
    fun nonZeroLineIndexBaseIsRejectedBeforeAnySkipPath() {
        for (base in listOf(1, 2, -1)) {
            val run = runLinkerMain(record(4, 15, base = base, target = "Nowhere 9:9") + "\n", strict = null)
            val cause = run.failure
            assertTrue(cause is IllegalStateException, "base=$base: unexpected failure: $cause")
            assertTrue(cause.message.orEmpty().contains("line_index_base=$base "), "base=$base: ${cause.message}")
        }
    }

    @Test
    fun zeroBasedAndOmittedLineIndexBaseAreAccepted() {
        val body = record(4, 15, base = null) + "\n" + record(26, 37, base = 0) + "\n"
        val run = runLinkerMain(body, strict = "true")
        assertEquals(null, run.failure)
        assertEquals(1L, run.links)
        assertEquals(2L, run.anchors)
    }

    @Test
    fun sourceLinesAreReadFromLineContentOnASchema6Db() {
        val body = record(4, 15, base = null) + "\n" + record(26, 37, base = 0) + "\n"
        val run = runLinkerMain(body, strict = "true", splitLineContent = true)
        assertEquals(null, run.failure)
        assertEquals(1L, run.links)
        assertEquals(2L, run.anchors)
    }

    @Test
    fun recordsOnTheLinkerViewOfASeifPrefixedLineLandOnTheStoredLine() {
        // The snapshot handed the linker the line without "(ח) "; its offsets index that text.
        val stored = "(ח) $sourceContent"
        val body = record(4, 15, base = 0, hashedContent = sourceContent) + "\n" +
            record(26, 37, base = 0, hashedContent = sourceContent) + "\n"
        val run = runLinkerMain(body, strict = "true", storedSource = stored)
        assertEquals(null, run.failure)
        assertEquals(1L, run.links)
        assertEquals(listOf(8 to 19, 30 to 41), run.anchorRanges)
        assertEquals("Genesis 1:1", stored.substring(8, 19))
    }

    @Test
    fun aRecordMadeOnTheWholePrefixedLineIsStillPlacedExactly() {
        val stored = "(ח) $sourceContent"
        val run = runLinkerMain(record(8, 19, base = 0, hashedContent = stored) + "\n", strict = "true", storedSource = stored)
        assertEquals(null, run.failure)
        assertEquals(listOf(8 to 19), run.anchorRanges)
    }

    @Test
    fun aWholeDafCitationLinksItsFirstSegmentAndCoversBothAmudim() {
        val daf = listOf(
            RefEntry("Gittin 51a:1", "גיטין, נא., א", "Talmud/Gittin", 1),
            RefEntry("Gittin 51a:2", "גיטין, נא., ב", "Talmud/Gittin", 2),
            RefEntry("Gittin 51b:1", "גיטין, נא:, א", "Talmud/Gittin", 3),
            RefEntry("Gittin 52a:1", "גיטין, נב., א", "Talmud/Gittin", 4),
        )
        val run = runLinkerMain(record(4, 15, base = 0, target = "Gittin 51") + "\n", strict = "true", targets = daf)
        assertEquals(null, run.failure)
        assertEquals(1L, run.links)
        assertEquals(listOf(0 to 2), run.targetSpans)
    }

    @Test
    fun aRecordForChangedTextIsStillSafeDropped() {
        val stored = "(ח) $sourceContent"
        val run = runLinkerMain(record(4, 15, base = 0, hashedContent = "something else") + "\n", strict = null, storedSource = stored)
        assertEquals(null, run.failure)
        assertEquals(0L, run.links)
    }

    @Test
    fun offsetShiftFollowsTheHashedText() {
        val stored = "(יב) <b>דיבור</b> סי' ע\"ט"
        val view = "<b>דיבור</b> סי' ע\"ט"
        assertEquals(5, linkerOffsetShift("Sefaria", stored, linkerContentHash(view)))
        assertEquals(0, linkerOffsetShift("Sefaria", stored, linkerContentHash(stored)))
        assertEquals(null, linkerOffsetShift("Sefaria", stored, linkerContentHash("other")))
        assertEquals(5, linkerOffsetShift("Sefaria", stored, null))
        // Any source: the hash, not the book's view, says which text the offsets index.
        assertEquals(5, linkerOffsetShift("MoreBooks", stored, linkerContentHash(view)))
        assertEquals(0, linkerOffsetShift("MoreBooks", stored, linkerContentHash(stored)))
        assertEquals(0, linkerOffsetShift("MoreBooks", stored, null))
        val tagged = "<b>(א)</b> <b>במג\"א</b> סק\"ו"
        assertEquals(11, linkerOffsetShift("DictaToOtzaria", tagged, linkerContentHash("<b>במג\"א</b> סק\"ו")))
        assertEquals(0, linkerOffsetShift("DictaToOtzaria", tagged, linkerContentHash(tagged)))
    }

    @Test
    fun aCitationThatStopsBeforeItsClosingParenTakesIt() {
        // Sefaria's NER ends "ובאיכה רבה (א, כו" right before its ")" (Otzaria/otzaria#2366).
        val stored = "ובאיכה רבה (א, כו) שכל"
        val run = runLinkerMain(record(0, 17, base = 0, hashedContent = stored) + "\n", strict = "true", storedSource = stored)
        assertEquals(null, run.failure)
        assertEquals(listOf(0 to 18), run.anchorRanges)
    }

    @Test
    fun aClosingBracketIsTakenOnlyAcrossSpacesAndTags() {
        val stored = "ראה <b>(א, כו</b> ) וכן [ובאיכה רבה (ב, ג)] וכן (ד, ה ועוד) וכן (ו, ז) וכן ח, ט)"
        val citations = listOf("(א, כו", "[ובאיכה רבה (ב, ג", "(ד, ה", "(ו, ז)", "ח, ט")
        val body = citations.joinToString("") {
            val start = stored.indexOf(it)
            record(start, start + it.length, base = 0, hashedContent = stored) + "\n"
        }
        val run = runLinkerMain(body, strict = "true", storedSource = stored)
        assertEquals(null, run.failure)
        val visible = "ראה (א, כו ) וכן [ובאיכה רבה (ב, ג)] וכן (ד, ה ועוד) וכן (ו, ז) וכן ח, ט)"
        val painted = listOf("(א, כו )", "[ובאיכה רבה (ב, ג)]", "(ד, ה", "(ו, ז)", "ח, ט")
        assertEquals(painted.map { visible.indexOf(it) to visible.indexOf(it) + it.length }, run.anchorRanges)
    }

    @Test
    fun nestedClosersAndUnicodeSpacesAreMappedThroughTheRealImporter() {
        val stored = "(א) ראה <b>([א, ב</b>&nbsp;]\u00a0) וגם ((ג, ד))"
        val first = stored.indexOf("([")
        val second = stored.indexOf("((")
        val body = record(first - 4, stored.indexOf("</b>") - 4, base = 0, hashedContent = stored.substring(4)) + "\n" +
            record(second - 4, stored.indexOf("))") - 4, base = 0, hashedContent = stored.substring(4)) + "\n"
        for (split in listOf(false, true)) {
            val run = runLinkerMain(body, strict = "true", splitLineContent = split, storedSource = stored)
            assertEquals(null, run.failure)
            assertEquals(listOf(8 to 18, 23 to 31), run.anchorRanges)
            assertEquals(1L, run.links, "multiple occurrences still share their stable link")
        }
    }

    @Test
    fun aClosingBracketOwnedByTheNextCitationIsNotTaken() {
        val stored = "ראה (א, ב)"
        val start = stored.indexOf('(')
        val end = stored.indexOf(')')
        val records = listOf(
            record(start, end, base = 0, hashedContent = stored),
            record(end, end + 1, base = 0, hashedContent = stored),
        )
        for (ordered in listOf(records, records.reversed(), listOf(records[0], records[0], records[1]))) {
            val run = runLinkerMain(ordered.joinToString("\n"), strict = "true", storedSource = stored)
            assertEquals(null, run.failure)
            assertEquals(listOf(start to end, end to end + 1), run.anchorRanges)
        }
    }

    @Test
    fun aFootnoteNeverSuppliesOrHidesAClosingBracket() {
        for (note in listOf(") הערה", "", " ", "&nbsp;")) {
            val stored = "ראה (א, ב<i class=\"footnote\">$note</i>)"
            val start = stored.indexOf('(')
            val end = stored.indexOf("<i")
            val run = runLinkerMain(record(start, end, base = 0, hashedContent = stored), strict = "true", storedSource = stored)
            assertEquals(null, run.failure)
            assertEquals(listOf(start to end), run.anchorRanges, "note=$note")
        }
    }

    @Test
    fun corruptArtifactLineFailsTheRun() {
        val run = runLinkerMain(record(4, 15, base = 0) + "\n{\"book_key\":\n", strict = null)
        assertTrue(run.failure is SerializationException, "unexpected failure: ${run.failure}")
    }
}
