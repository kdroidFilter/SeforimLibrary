package io.github.kdroidfilter.seforimlibrary.packaging

import io.github.kdroidfilter.seforimlibrary.common.linker.LinkerInputView
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

class DumpLinesTest {

    @Test
    fun `the snapshot is identical for the schema 5 and the schema 6 line shape`() {
        val legacy = dump(split = false)
        val split = dump(split = true)
        assertEquals(listOf("src|Book|0|first|Book 1", "src|Book|1|second|Book"), legacy)
        assertEquals(legacy, split)
    }

    @Test
    fun `a line is dumped with its line_note bodies put back at their charPos`() {
        val books = listOf(TestBook(1, "src", "Book", listOf("א<sup class=\"footnote-marker\">1</sup> ב", "ג")))
        for (split in listOf(false, true)) {
            val snapshot = dumpBooks(split, books) { c ->
                c.createStatement().use { st ->
                    st.execute(
                        "CREATE TABLE line_note (lineId INTEGER NOT NULL, noteIndex INTEGER NOT NULL, " +
                            "charPos INTEGER NOT NULL, hasMarker INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY (lineId, noteIndex))",
                    )
                    st.execute(
                        "INSERT INTO line_note SELECT id, 0, 2, 1, 'ראה בראשית א א' FROM line WHERE lineIndex = 0",
                    )
                    st.execute("INSERT INTO line_note SELECT id, 1, 4, 0, 'סוף' FROM line WHERE lineIndex = 0")
                }
            }
            assertEquals(
                listOf(
                    "src|Book|0|א<sup class=\"footnote-marker\">1</sup><i class=\"footnote\">ראה בראשית א א</i> ב" +
                        "<i class=\"footnote\">סוף</i>|Book",
                    "src|Book|1|ג|Book",
                ),
                snapshot.rows,
            )
        }
    }

    @Test
    fun `an empty marker remains anchored and adjacent bodies keep note order`() {
        val marker = "<sup class=\"footnote-marker\"></sup>"
        val books = listOf(TestBook(1, "src", "Book", listOf("<p><b>$marker סוף</b></p>")))
        for (split in listOf(false, true)) {
            val snapshot = dumpBooks(split, books) { c ->
                c.createStatement().use { st ->
                    st.execute(
                        "CREATE TABLE line_note (lineId INTEGER NOT NULL, noteIndex INTEGER NOT NULL, " +
                            "charPos INTEGER NOT NULL, hasMarker INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY (lineId, noteIndex))",
                    )
                    st.execute("INSERT INTO line_note SELECT id, 0, 0, 1, 'ראשונה' FROM line")
                    st.execute("INSERT INTO line_note SELECT id, 1, 0, 0, 'שנייה' FROM line")
                }
                assertEquals(listOf(true, false), loadLineNotes(c).values.single().map { it.hasMarker })
            }
            assertEquals(
                listOf("src|Book|0|<p><b>$marker<i class=\"footnote\">ראשונה</i><i class=\"footnote\">שנייה</i> סוף</b></p>|Book"),
                snapshot.rows,
            )
        }
    }

    @Test
    fun `a Sefaria line reaches the linker without its opening seif label`() {
        val rows = dump(
            split = true,
            sourceName = "Sefaria",
            lines = listOf("(ח)  <b>כפרן. </b> כמ\"ש בסי' ע\"ט", "(שם) כמבואר", "ראה (נח) שם"),
        )
        assertEquals(
            listOf(
                "Sefaria|Book|0| <b>כפרן. </b> כמ\"ש בסי' ע\"ט|Book 1",
                "Sefaria|Book|1|(שם) כמבואר|Book",
                "Sefaria|Book|2|ראה (נח) שם|Book",
            ),
            rows,
        )
        // Other sources keep the text verbatim.
        assertEquals("(ח) כפרן", dump(split = false, lines = listOf("(ח) כפרן", "x")).first().split("|")[3])
    }

    @Test
    fun `a book of another source is stripped only with enough labelled lines`() {
        val labels = listOf("א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י", "יא", "יב", "יג", "יד", "טו", "טז", "יז", "יח", "יט")
        val few = listOf("פתיחה") + labels.map { "($it) סעיף" }
        var strippedBooks = emptyList<String>()
        val fewRows = dump(split = true, sourceName = "DictaToOtzaria", lines = few) { strippedBooks = strippedBooksOf(it) }
        assertEquals(few, fewRows.map { it.split("|")[3] })
        assertEquals(emptyList(), strippedBooks)

        val many = few + "<b>(כ)</b> <b>דיבור</b> סעיף"
        val manyRows = dump(split = true, sourceName = "DictaToOtzaria", lines = many) { strippedBooks = strippedBooksOf(it) }
        assertEquals(listOf("פתיחה") + List(19) { "סעיף" } + "<b>דיבור</b> סעיף", manyRows.map { it.split("|")[3] })
        assertEquals(listOf("DictaToOtzaria|Book|PLAIN_AND_TAGGED|20"), strippedBooks)
    }

    @Test
    fun `books are streamed one after another and each keeps its own view`() {
        val labels = listOf("א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט", "י", "יא", "יב", "יג", "יד", "טו", "טז", "יז", "יח", "יט", "כ", "כא", "כב")
        val noise = listOf("(שם) כמבואר", "ראה (נח) שם", "<h4>(א)</h4>", "<b>(א) נידון</b> השאלה", "פתיחה")
        fun labelled(n: Int, tagged: Boolean = false) =
            labels.take(n).map { if (tagged) "<b>($it)</b> סעיף" else "($it) סעיף" }
        val books = listOf(
            // The twentieth label is the book's last line, after hundreds without one.
            TestBook(1, "DictaToOtzaria", "Late", List(300) { noise[it % noise.size] } + labelled(20, tagged = true)),
            TestBook(2, "Sefaria", "Plain", labelled(22) + labelled(3, tagged = true) + noise),
            TestBook(3, "DictaToOtzaria", "Few", noise + labelled(19) + noise),
            TestBook(5, "MoreBooks", "Many", labelled(10) + noise + labelled(12, tagged = true).drop(10) + labelled(22).drop(12)),
            TestBook(9, "DictaToOtzaria", "Empty", listOf("")),
            TestBook(11, "Sefaria", "Bare", noise),
        )
        for (split in listOf(false, true)) {
            val snapshot = dumpBooks(split, books)
            assertEquals(expectedRows(books), snapshot.rows, "split=$split")
            assertEquals(
                listOf("DictaToOtzaria|Late|PLAIN_AND_TAGGED|20", "MoreBooks|Many|PLAIN_AND_TAGGED|22"),
                snapshot.strippedBooks,
                "split=$split",
            )
            assertEquals(books.size.toString(), snapshot.meta["book_count"])
            assertEquals(books.sumOf { it.lines.size }.toString(), snapshot.meta["line_count"])
        }
        // The smoke-test book limit counts only the books it dumps.
        val limited = dumpBooks(split = true, books, bookLimit = 3)
        assertEquals(expectedRows(books.take(3)), limited.rows)
        assertEquals(listOf("DictaToOtzaria|Late|PLAIN_AND_TAGGED|20"), limited.strippedBooks)
    }

    @Test
    fun `the counting pass counts labelled lines of the books without a fixed view`() {
        val books = listOf(
            TestBook(1, "Sefaria", "S", listOf("(א) x", "(ב) x")),
            TestBook(2, "DictaToOtzaria", "D", listOf("(א) x", "<b>(ב)</b> x", "(שם) x", "y", "ראה (ג)")),
            TestBook(3, "MoreBooks", "M", listOf("x", "y")),
        )
        for (split in listOf(false, true)) {
            withSourceDb(split, books) { db ->
                DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                    assertEquals(mapOf(2L to 2), labelledLineCounts(c, split, null))
                    assertEquals(emptyMap(), labelledLineCounts(c, split, "l.bookId IN (1,3)"))
                }
            }
        }
    }

    private class TestBook(val id: Long, val source: String, val title: String, val lines: List<String>)

    private class Snapshot(val rows: List<String>, val strippedBooks: List<String>, val meta: Map<String, String>)

    /** What the snapshot holds, from [LinkerInputView.modeFor] over each book's whole line list. */
    private fun expectedRows(books: List<TestBook>): List<String> = books.flatMap { book ->
        val mode = LinkerInputView.modeFor(book.source, book.lines)
        book.lines.mapIndexed { index, line ->
            "${book.source}|${book.title}|$index|${LinkerInputView.linkerContent(mode, line)}|${book.title}"
        }
    }

    private fun <T> withSourceDb(
        split: Boolean,
        books: List<TestBook>,
        prepare: (java.sql.Connection) -> Unit = {},
        block: (Path) -> T,
    ): T {
        val dir = Files.createTempDirectory("dumpLinesBooks")
        try {
            val db = dir.resolve("seforim.db")
            DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                c.createStatement().use { st ->
                    st.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                    st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, sourceId INTEGER, title TEXT, heRef TEXT)")
                    st.execute(
                        "CREATE TABLE line (id INTEGER PRIMARY KEY, bookId INTEGER, lineIndex INTEGER, " +
                            "content TEXT NOT NULL, heRef TEXT)",
                    )
                }
                val sources = books.map { it.source }.distinct()
                c.prepareStatement("INSERT INTO source VALUES (?, ?)").use { ins ->
                    sources.forEachIndexed { i, name -> ins.setInt(1, i + 1); ins.setString(2, name); ins.executeUpdate() }
                }
                c.prepareStatement("INSERT INTO book VALUES (?, ?, ?, NULL)").use { ins ->
                    for (book in books) {
                        ins.setLong(1, book.id)
                        ins.setInt(2, sources.indexOf(book.source) + 1)
                        ins.setString(3, book.title)
                        ins.executeUpdate()
                    }
                }
                // Line ids run against book and line order, so only the dump's ORDER BY sorts them.
                var id = 1_000_000L
                c.prepareStatement("INSERT INTO line VALUES (?, ?, ?, ?, NULL)").use { ins ->
                    for (book in books) {
                        book.lines.forEachIndexed { index, content ->
                            ins.setLong(1, id--)
                            ins.setLong(2, book.id)
                            ins.setInt(3, index)
                            ins.setString(4, content)
                            ins.executeUpdate()
                        }
                    }
                }
                if (split) {
                    c.createStatement().use { st ->
                        st.execute("CREATE TABLE line_content (id INTEGER PRIMARY KEY NOT NULL, content TEXT NOT NULL)")
                        st.execute("INSERT INTO line_content SELECT id, content FROM line")
                        st.execute("ALTER TABLE line DROP COLUMN content")
                    }
                }
                prepare(c)
            }
            return block(db)
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    private fun dumpBooks(
        split: Boolean,
        books: List<TestBook>,
        bookLimit: Int? = null,
        prepare: (java.sql.Connection) -> Unit = {},
    ): Snapshot =
        withSourceDb(split, books, prepare) { db ->
            val out = db.resolveSibling("lines_snapshot.db")
            val properties = listOfNotNull(
                "seforimDb" to db.toString(),
                "linesSnapshot" to out.toString(),
                bookLimit?.let { "linesSnapshotBookLimit" to it.toString() },
            )
            withProperties(*properties.toTypedArray()) {
                Class.forName("io.github.kdroidfilter.seforimlibrary.packaging.DumpLinesKt")
                    .getMethod("main", Array<String>::class.java)
                    .invoke(null, arrayOf<String>() as Any)
            }
            DriverManager.getConnection("jdbc:sqlite:$out").use { c ->
                fun query(sql: String, columns: Int): List<String> = c.createStatement().use { st ->
                    st.executeQuery(sql).use { rs ->
                        buildList { while (rs.next()) add((1..columns).joinToString("|") { rs.getString(it) }) }
                    }
                }
                Snapshot(
                    rows = query(
                        "SELECT source_name, canonical_he_title, line_index, content, context_ref FROM lines_snapshot ORDER BY rowid",
                        5,
                    ),
                    strippedBooks = query(
                        "SELECT source_name, canonical_he_title, mode, stripped_lines FROM lines_snapshot_stripped_books ORDER BY rowid",
                        4,
                    ),
                    meta = query("SELECT key, value FROM lines_snapshot_meta", 2)
                        .associate { it.substringBefore("|") to it.substringAfter("|") },
                )
            }
        }

    private fun strippedBooksOf(snapshot: Path): List<String> =
        DriverManager.getConnection("jdbc:sqlite:$snapshot").use { c ->
            c.createStatement().use { st ->
                st.executeQuery("SELECT source_name, canonical_he_title, mode, stripped_lines FROM lines_snapshot_stripped_books").use { rs ->
                    buildList { while (rs.next()) add((1..4).joinToString("|") { rs.getString(it) }) }
                }
            }
        }

    private fun dump(
        split: Boolean,
        sourceName: String = "src",
        lines: List<String> = listOf("first", "second"),
        inspect: (Path) -> Unit = {},
    ): List<String> {
        val dir = Files.createTempDirectory("dumpLines")
        try {
            val db = dir.resolve("seforim.db")
            val out = dir.resolve("lines_snapshot.db")
            DriverManager.getConnection("jdbc:sqlite:$db").use { c ->
                c.createStatement().use { st ->
                    st.execute("CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
                    st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, sourceId INTEGER, title TEXT, heRef TEXT)")
                    st.execute(
                        "CREATE TABLE line (id INTEGER PRIMARY KEY, bookId INTEGER, lineIndex INTEGER, " +
                            "content TEXT NOT NULL, heRef TEXT)",
                    )
                    st.execute("INSERT INTO source VALUES (1, '$sourceName')")
                    st.execute("INSERT INTO book VALUES (1, 1, 'Book', NULL)")
                }
                c.prepareStatement("INSERT INTO line VALUES (?, 1, ?, ?, ?)").use { ins ->
                    lines.forEachIndexed { index, content ->
                        ins.setInt(1, when (index) { 0 -> 7; 1 -> 3; else -> 100 + index })
                        ins.setInt(2, index)
                        ins.setString(3, content)
                        if (index == 0) ins.setString(4, "Book 1") else ins.setNull(4, java.sql.Types.VARCHAR)
                        ins.executeUpdate()
                    }
                }
                c.createStatement().use { st ->
                    if (split) {
                        st.execute("CREATE TABLE line_content (id INTEGER PRIMARY KEY NOT NULL, content TEXT NOT NULL)")
                        st.execute("INSERT INTO line_content SELECT id, content FROM line")
                        st.execute("ALTER TABLE line DROP COLUMN content")
                    }
                }
            }
            withProperties("seforimDb" to db.toString(), "linesSnapshot" to out.toString()) {
                // Several files of this package define a top-level main.
                Class.forName("io.github.kdroidfilter.seforimlibrary.packaging.DumpLinesKt")
                    .getMethod("main", Array<String>::class.java)
                    .invoke(null, arrayOf<String>() as Any)
            }
            DriverManager.getConnection("jdbc:sqlite:$out").use { c ->
                c.createStatement().use { st ->
                    st.executeQuery("SELECT value FROM lines_snapshot_meta WHERE key = 'linker_input_policy'").use { rs ->
                        check(rs.next() && rs.getString(1).startsWith("leading-numeral-label-stripped-v2;"))
                    }
                }
            }
            inspect(out)
            return rows(out)
        } finally {
            dir.toFile().deleteRecursively()
        }
    }

    private fun rows(snapshot: Path): List<String> =
        DriverManager.getConnection("jdbc:sqlite:$snapshot").use { c ->
            c.createStatement().use { st ->
                st.executeQuery(
                    "SELECT source_name, canonical_he_title, line_index, content, context_ref " +
                        "FROM lines_snapshot ORDER BY line_index",
                ).use { rs ->
                    buildList {
                        while (rs.next()) add((1..5).joinToString("|") { rs.getString(it) })
                    }
                }
            }
        }

    private fun withProperties(vararg values: Pair<String, String>, block: () -> Unit) {
        val saved = values.associate { (key, _) -> key to System.getProperty(key) }
        try {
            values.forEach { (key, value) -> System.setProperty(key, value) }
            block()
        } finally {
            saved.forEach { (key, value) -> if (value == null) System.clearProperty(key) else System.setProperty(key, value) }
        }
    }
}
