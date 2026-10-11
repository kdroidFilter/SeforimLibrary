package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.LogWriter
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.DriverManager
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A renamed book keeps the acronyms the Acronymizer still files under its old title
 * (v30: 'אמת ואמונה - מנחם מנדל מקוצק' → 'אמת ואמונה' lost 'קוצקי' and 'הקוצקי'),
 * and no other book gains or loses any.
 */
class AcronymsFollowRenamesTest {

    // ─── Which renames are the same book ──────────────────────────────────────

    @Test
    fun renamesThatOnlyAddDropOrRespellWordsAreTheSameWork() {
        val same = listOf(
            "אמת ואמונה - מנחם מנדל מקוצק" to "אמת ואמונה",
            "ר' סעדיה גאון על בראשית" to "רבינו סעדיה גאון על בראשית",
            "הזוהר המתורגם - תיקוני הזוהר" to "הזהר המתורגם - תקוני הזהר",
            "דרשת הרמבן" to "דרשת הרמבן (תורת ה' תמימה)",
            "אַקְדָּמוּת מִלִּין" to "אקדמות מילין",
            "הגהות הבח על מסכת ברכות" to "הגהות הב״ח על מסכת ברכות",
            "שות ברוך השם" to "ברוך השם",
        )
        for ((old, new) in same) assertTrue(RenamedBookTitles.isSameWorkTitle(old, new), "$old → $new")
    }

    @Test
    fun renamesThatReplaceWordsAreNotFollowed() {
        val different = listOf(
            // A corrected identification: another edition, another author.
            "פאר הדור תשובות הרמב\"ם" to "תשובות הרמב\"ם - מהדורת בלאו",
            "כתר תורה (ר לוי יצחק)" to "כתר תורה (ר מאיר מברדיטשוב)",
            "צלח על פסחים" to "ציון לנפש חיה על פסחים",
            "בתי מדרשות חלק ב - מכילתא" to "מכילתא לפרשת שמות וארא",
        )
        for ((old, new) in different) assertFalse(RenamedBookTitles.isSameWorkTitle(old, new), "$old → $new")
    }

    // ─── Finding the old title ────────────────────────────────────────────────

    private fun hashes(vararg lines: String): List<ByteArray> =
        lines.map { MessageDigest.getInstance("SHA-1").digest(it.toByteArray()) }

    private val body = (1..20).map { "שורה $it" }.toTypedArray()

    private fun resolve(
        books: List<LibraryBook>,
        targets: Set<Long>,
        keys: Map<BookKey, Long>,
        lines: Map<Long, List<ByteArray>>,
    ) = RenamedBookTitles().resolve(books, targets, keys) { ids -> lines.filterKeys { it in ids } }

    @Test
    fun aRenamedFileIsFoundByItsContent() {
        val books = listOf(LibraryBook(7659, "אמת ואמונה", "OnYourWay"))
        val result = resolve(
            books,
            targets = setOf(7659),
            keys = mapOf(
                BookKey("OnYourWay", "אמת ואמונה - מנחם מנדל מקוצק") to 5983L,
                BookKey("OnYourWay", "אמת ואמונה") to 7659L,
            ),
            lines = mapOf(
                5983L to hashes("<h1>אמת ואמונה - מנחם מנדל מקוצק</h1>", *body),
                7659L to hashes("<h1>אמת ואמונה</h1>", *body),
            ),
        )
        assertEquals(
            mapOf(7659L to listOf(FormerTitle("אמת ואמונה - מנחם מנדל מקוצק", "same content"))),
            result.byBookId,
        )
    }

    @Test
    fun aBookRenamedInPlaceIsFoundByItsId() {
        val books = listOf(
            LibraryBook(3702, "רבינו סעדיה גאון על בראשית", "Sefaria"),
            LibraryBook(5732, "תשובות הרמב\"ם - מהדורת בלאו", "Sefaria"),
        )
        val result = resolve(
            books,
            targets = setOf(3702, 5732),
            keys = mapOf(
                BookKey("Sefaria", "ר' סעדיה גאון על בראשית") to 3702L,
                BookKey("Sefaria", "פאר הדור תשובות הרמב\"ם") to 5732L,
            ),
            lines = emptyMap(),
        )
        assertEquals(
            mapOf(3702L to listOf(FormerTitle("ר' סעדיה גאון על בראשית", "same id"))),
            result.byBookId,
        )
        assertEquals(listOf("פאר הדור תשובות הרמב\"ם"), result.refused.map { it.oldTitle })
    }

    @Test
    fun contentShortOfTheThresholdOrInAnotherSourceIsNotARename() {
        val half = body.take(10).toTypedArray()
        val books = listOf(
            LibraryBook(2, "ספר חדש", "A"),
            LibraryBook(3, "ספר אחר", "B"),
        )
        val result = resolve(
            books,
            targets = setOf(2, 3),
            keys = mapOf(BookKey("A", "ספר ישן") to 1L, BookKey("A", "ספר חדש") to 2L, BookKey("B", "ספר אחר") to 3L),
            lines = mapOf(1L to hashes(*body), 2L to hashes("<h1>ספר חדש</h1>", *half), 3L to hashes(*body)),
        )
        assertTrue(result.byBookId.isEmpty(), "${result.byBookId}")
    }

    @Test
    fun aDeadTitleWithTwoHeirsGoesToNeither() {
        // The second copy already has acronyms of its own; it still competes.
        val books = listOf(
            LibraryBook(2, "ספר", "A"),
            LibraryBook(3, "ספר - מהדורה ב", "A"),
        )
        val result = resolve(
            books,
            targets = setOf(2),
            keys = mapOf(BookKey("A", "ספר - מחבר") to 1L, BookKey("A", "ספר") to 2L, BookKey("A", "ספר - מהדורה ב") to 3L),
            lines = mapOf(1L to hashes(*body), 2L to hashes(*body), 3L to hashes(*body)),
        )
        assertTrue(result.byBookId.isEmpty(), "${result.byBookId}")
        assertEquals("its content matches 2 books", result.refused.single().reason)
    }

    // Repeated lines: a book's line count is not its number of distinct lines.
    private val twice = (body + body)

    @Test
    fun aRenamedFileWhoseLinesRepeatIsFoundByItsContent() {
        // 20 lines, each twice: 40 lines, 20 distinct, on both sides.
        val books = listOf(LibraryBook(2, "ספר", "A"))
        val result = resolve(
            books,
            targets = setOf(2),
            keys = mapOf(BookKey("A", "ספר - מחבר") to 1L, BookKey("A", "ספר") to 2L),
            lines = mapOf(1L to hashes(*twice), 2L to hashes(*twice)),
        )
        assertEquals(mapOf(2L to listOf(FormerTitle("ספר - מחבר", "same content"))), result.byBookId)
        assertTrue(result.refused.isEmpty(), "${result.refused}")
    }

    @Test
    fun aRivalWhoseLinesRepeatStillCompetes() {
        // The rival holds the same 20 distinct lines, each twice. Left out for its line
        // count, it would make the target look like the single heir.
        val books = listOf(
            LibraryBook(2, "ספר", "A"),
            LibraryBook(3, "ספר אחר", "A"),
        )
        val result = resolve(
            books,
            targets = setOf(2),
            keys = mapOf(BookKey("A", "ספר - מחבר") to 1L, BookKey("A", "ספר") to 2L, BookKey("A", "ספר אחר") to 3L),
            lines = mapOf(1L to hashes(*body), 2L to hashes(*body), 3L to hashes(*twice)),
        )
        assertTrue(result.byBookId.isEmpty(), "${result.byBookId}")
        assertEquals(RefusedRename("ספר - מחבר", "ספר | ספר אחר", "its content matches 2 books"), result.refused.single())
    }

    @Test
    fun anUnrelatedBookIsNotAnHeirWhateverItsLineCount() {
        val other = (1..20).map { "שורה אחרת $it" }.toTypedArray()
        val books = listOf(
            // As many lines as the old book, all of them different.
            LibraryBook(2, "ספר", "A"),
            // Half the old book's lines, each four times: 40 lines, 10 distinct.
            LibraryBook(3, "ספר - חלק", "A"),
            // The old book's lines among as many others, each twice.
            LibraryBook(4, "ספר - מורחב", "A"),
        )
        val result = resolve(
            books,
            targets = setOf(2, 3, 4),
            keys = mapOf(
                BookKey("A", "ספר - מחבר") to 1L,
                BookKey("A", "ספר") to 2L,
                BookKey("A", "ספר - חלק") to 3L,
                BookKey("A", "ספר - מורחב") to 4L,
            ),
            lines = mapOf(
                1L to hashes(*twice),
                2L to hashes(*other, *other),
                3L to hashes(*(1..4).flatMap { body.take(10) }.toTypedArray()),
                4L to hashes(*twice, *other, *other),
            ),
        )
        assertTrue(result.byBookId.isEmpty(), "${result.byBookId}")
        assertTrue(result.refused.isEmpty(), "${result.refused}")
    }

    @Test
    fun aDeadTitleWhoseHeirHasItsOwnAcronymsIsLeftAlone() {
        val books = listOf(LibraryBook(2, "הזהר המתורגם - בראשית", "A"))
        val result = resolve(
            books,
            targets = emptySet(),
            keys = mapOf(BookKey("A", "הזוהר המתורגם - בראשית") to 1L, BookKey("A", "הזהר המתורגם - בראשית") to 2L),
            lines = mapOf(1L to hashes(*body), 2L to hashes(*body)),
        )
        assertTrue(result.byBookId.isEmpty())
    }

    @Test
    fun anOldTitleThatIsNowAnotherBookIsNotInherited() {
        val books = listOf(
            LibraryBook(1, "ספר", "Sefaria"),
            LibraryBook(2, "ספר - מהדורה חדשה", "Sefaria"),
        )
        val result = resolve(
            books,
            targets = setOf(2),
            keys = mapOf(BookKey("Sefaria", "ספר") to 2L, BookKey("Sefaria", "ספר ") to 1L),
            lines = emptyMap(),
        )
        assertTrue(result.byBookId.isEmpty(), "${result.byBookId}")
        assertEquals("the old title is another book's title now", result.refused.single().reason)
    }

    @Test
    fun anOldTitleTwoBooksWouldInheritGoesToNeither() {
        val books = listOf(
            LibraryBook(1, "מדרש א", "Sefaria"),
            LibraryBook(2, "מדרש א - כתב יד ב", "Sefaria"),
        )
        val result = resolve(
            books,
            targets = setOf(1, 2),
            keys = mapOf(BookKey("Sefaria", "מדרש א - כתב יד") to 1L, BookKey("Sefaria", "מדרש א - כתב יד ") to 2L),
            lines = emptyMap(),
        )
        assertTrue(result.byBookId.isEmpty(), "${result.byBookId}")
        assertEquals(setOf("two books have this old title"), result.refused.map { it.reason }.toSet())
    }

    // ─── End to end, across two builds ────────────────────────────────────────

    private fun newRepo(): SeforimRepository {
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        return SeforimRepository(":memory:", driver)
    }

    private fun acronymizer(entries: Map<String, List<String>>): Path {
        val path = Files.createTempDirectory("acronymizer").resolve("acronymizer.db")
        DriverManager.getConnection("jdbc:sqlite:$path").use { c ->
            c.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE Books (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE Acronyms (id INTEGER PRIMARY KEY AUTOINCREMENT, acronym TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE BookAcronyms (book_id INTEGER NOT NULL, acronym_id INTEGER NOT NULL)")
            }
            for ((title, terms) in entries) {
                c.prepareStatement("INSERT INTO Books(title) VALUES (?)").use { it.setString(1, title); it.executeUpdate() }
                for (term in terms) {
                    c.prepareStatement("INSERT OR IGNORE INTO Acronyms(acronym) VALUES (?)").use {
                        it.setString(1, term); it.executeUpdate()
                    }
                    c.prepareStatement(
                        "INSERT INTO BookAcronyms SELECT b.id, a.id FROM Books b, Acronyms a WHERE b.title = ? AND a.acronym = ?",
                    ).use { it.setString(1, title); it.setString(2, term); it.executeUpdate() }
                }
            }
        }
        return path
    }

    private fun library(books: Map<String, String>): Path {
        val root = Files.createTempDirectory("otzaria-renames")
        val dir = Files.createDirectories(root.resolve("אוצריא").resolve("חסידות"))
        for ((title, text) in books) Files.writeString(dir.resolve("$title.txt"), text)
        return root
    }

    private fun text(title: String) = (listOf("<h1>$title</h1>") + body).joinToString("\n")

    /** Builds [books] against [state] and returns every book's acronyms; [log] gets the build's log. */
    private fun build(
        state: Path,
        acronyms: Path,
        books: Map<String, String>,
        log: Capture? = null,
    ): Map<String, Set<String>> = runBlocking {
        val repo = newRepo()
        try {
            // The repository drops the global severity, so the capture goes in after it.
            if (log != null) {
                Logger.setLogWriters(listOf(log))
                Logger.setMinSeverity(Severity.Verbose)
            }
            val allocator = InMemoryIdAllocator.load(state.takeIf { Files.exists(it) })
            DatabaseGenerator(
                sourceDirectory = library(books),
                repository = repo,
                acronymDbPath = acronyms.toString(),
                allocator = allocator,
            ).generateLinesOnly()
            allocator.snapshotTo(state)
            repo.getAllBooks().associate { it.title to repo.getAcronymsForBook(it.id).toSet() }
        } finally {
            repo.close()
        }
    }

    @Test
    fun aRenamedBookKeepsItsAcronymsAndNoOtherBookChanges() {
        val acronyms = acronymizer(
            mapOf(
                "אמת ואמונה - מנחם מנדל מקוצק" to listOf("קוצקי", "הקוצקי", "אמת ואמונה"),
                "כתר תורה (ר לוי יצחק)" to listOf("כתר תורה לרבי לוי יצחק"),
                "נועם אלימלך" to listOf("נוע\"א"),
            ),
        )
        val state = Files.createTempDirectory("otzaria-renames-state").resolve("build_state.db")
        val before = build(
            state, acronyms,
            mapOf(
                "אמת ואמונה - מנחם מנדל מקוצק" to text("אמת ואמונה - מנחם מנדל מקוצק"),
                "כתר תורה (ר לוי יצחק)" to (listOf("<h1>כתר תורה</h1>") + body.map { "כתר $it" }).joinToString("\n"),
                "נועם אלימלך" to (listOf("<h1>נועם אלימלך</h1>") + body.map { "נועם $it" }).joinToString("\n"),
            ),
        )
        assertEquals(setOf("קוצקי", "הקוצקי", "אמת ואמונה"), before.getValue("אמת ואמונה - מנחם מנדל מקוצק"))

        val after = build(
            state, acronyms,
            mapOf(
                "אמת ואמונה" to text("אמת ואמונה"),
                // A corrected attribution: the old name is wrong for this book.
                "כתר תורה (ר מאיר מברדיטשוב)" to (listOf("<h1>כתר תורה</h1>") + body.map { "כתר $it" }).joinToString("\n"),
                "נועם אלימלך" to (listOf("<h1>נועם אלימלך</h1>") + body.map { "נועם $it" }).joinToString("\n"),
            ),
        )
        assertEquals(
            setOf("קוצקי", "הקוצקי", "אמת ואמונה - מנחם מנדל מקוצק"),
            after.getValue("אמת ואמונה"),
        )
        assertEquals(emptySet(), after.getValue("כתר תורה (ר מאיר מברדיטשוב)"))
        assertEquals(before.getValue("נועם אלימלך"), after.getValue("נועם אלימלך"))

        // The next build still finds it: the old key outlives the build that saw the rename.
        assertEquals(after, build(state, acronyms, mapOf(
            "אמת ואמונה" to text("אמת ואמונה"),
            "כתר תורה (ר מאיר מברדיטשוב)" to (listOf("<h1>כתר תורה</h1>") + body.map { "כתר $it" }).joinToString("\n"),
            "נועם אלימלך" to (listOf("<h1>נועם אלימלך</h1>") + body.map { "נועם $it" }).joinToString("\n"),
        )))
    }

    private class Capture : LogWriter() {
        val lines = mutableListOf<String>()
        override fun log(severity: Severity, message: String, tag: String, throwable: Throwable?) {
            lines += message
        }

        /** The lines about renamed books. */
        fun renames(): List<String> = lines.filter { it.startsWith("Acronyms of renamed books:") }
    }

    private val previousWriters = Logger.config.logWriterList
    private val previousSeverity = Logger.config.minSeverity

    @AfterTest
    fun restoreLogger() {
        Logger.setLogWriters(previousWriters)
        Logger.setMinSeverity(previousSeverity)
    }

    private fun textTwice(title: String) = (listOf("<h1>$title</h1>") + body + body).joinToString("\n")

    @Test
    fun aRenamedBookWhoseLinesRepeatKeepsItsAcronyms() {
        val acronyms = acronymizer(mapOf("ספר - מחבר" to listOf("סמח")))
        val state = Files.createTempDirectory("otzaria-renames-state").resolve("build_state.db")
        build(state, acronyms, mapOf("ספר - מחבר" to textTwice("ספר - מחבר")))
        val log = Capture()
        val after = build(state, acronyms, mapOf("ספר" to textTwice("ספר")), log)
        assertEquals(setOf("סמח", "ספר - מחבר"), after.getValue("ספר"))
        // Otzaria: the renamed file keeps its id (BookIdContinuity), so the old key names it.
        assertContains(log.renames().single(), "2 terms carried to 1 books: 'ספר - מחבר' → 'ספר' (2, same id)")
    }

    @Test
    fun aRenameWithTwoHeirsIsReportedAndCarriesNothing() {
        val acronyms = acronymizer(mapOf("ספר - מחבר" to listOf("סמח")))
        val state = Files.createTempDirectory("otzaria-renames-state").resolve("build_state.db")
        build(state, acronyms, mapOf("ספר - מחבר" to text("ספר - מחבר")))
        val log = Capture()
        val after = build(state, acronyms, mapOf("ספר" to text("ספר"), "ספר אחר" to textTwice("ספר אחר")), log)
        assertEquals(mapOf("ספר" to emptySet<String>(), "ספר אחר" to emptySet()), after)
        val refused = log.renames().single { "not followed" in it }
        assertContains(refused, "1 renames not followed")
        assertContains(refused, "'ספר - מחבר' → ")
        assertContains(refused, "(its content matches 2 books)")
    }

    @Test
    fun anUnreadableAcronymizerFailsTheBuild() {
        // An empty file opens as a DB without the Acronymizer's tables.
        val broken = Files.createTempFile("acronymizer", ".db")
        val state = Files.createTempDirectory("otzaria-renames-state").resolve("build_state.db")
        assertFailsWith<IllegalStateException> {
            build(state, broken, mapOf("ספר ישן" to text("ספר ישן")))
        }
    }

    @Test
    fun anAcronymizerEntryForTheNewTitleWins() {
        val acronyms = acronymizer(
            mapOf(
                "הזוהר המתורגם - בראשית" to listOf("זוהר מתורגם", "תיקונים."),
                "הזהר המתורגם - בראשית" to listOf("זוהר מתורגם", "הזוהר המתורגם - בראשית"),
            ),
        )
        val state = Files.createTempDirectory("otzaria-renames-state").resolve("build_state.db")
        build(state, acronyms, mapOf("הזוהר המתורגם - בראשית" to text("הזוהר המתורגם - בראשית")))
        val after = build(state, acronyms, mapOf("הזהר המתורגם - בראשית" to text("הזהר המתורגם - בראשית")))
        assertEquals(setOf("זוהר מתורגם", "הזוהר המתורגם - בראשית"), after.getValue("הזהר המתורגם - בראשית"))
    }
}
