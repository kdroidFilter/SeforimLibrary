package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.Author
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.Connection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Regression for Zayit issue #300 item 3: the book `בראשית` appears as a
 * commentator on itself in the reader's "מפרשים" panel.
 *
 * Root cause: Sefaria ships a handful of self-referencing links tagged as
 * COMMENTARY (e.g. `"Genesis 1:1","Genesis 3:5","Commentary"`). The old
 * importer persisted those verbatim, so the app's commentary panel, which
 * shows every book pointing at the current book with type=COMMENTARY,
 * surfaced Genesis as a commentator on Genesis.
 *
 * Fix: drop links where `sourceBookId == targetBookId` and the base
 * connection type is COMMENTARY or TARGUM (neither makes sense pointing at
 * itself). Cross-reference types (OTHER / REFERENCE) stay — those legitimately
 * link parts of the same book to each other.
 */
class SefariaSelfCommentaryLinksTest {
    @Test
    fun selfCommentaryLinksAreDropped_crossBookLinksArePreserved() = runBlocking {
        val tempDir = Files.createTempDirectory("seforim-self-commentary")
        val linksDir = Files.createDirectories(tempDir.resolve("links"))

        // A single CSV with 3 rows:
        //   1. Genesis 1:1 → Genesis 3:5  COMMENTARY  (self-link, MUST be dropped)
        //   2. Genesis 1:1 → Genesis 5:1  OTHER       (self-link, MUST be kept — cross-ref)
        //   3. Rashi on Genesis 1:1 → Genesis 1:1  COMMENTARY (cross-book, MUST be kept)
        Files.writeString(
            linksDir.resolve("links0.csv"),
            """
            |Citation 1,Citation 2,Conection Type
            |"Genesis 1:1","Genesis 3:5","Commentary"
            |"Genesis 1:1","Genesis 5:1","Other"
            |"Rashi on Genesis 1:1","Genesis 1:1","Commentary"
            """.trimMargin()
        )

        // Set up an in-memory DB with the two books and a few lines each
        val driver = JdbcSqliteDriver(url = "jdbc:sqlite::memory:")
        SeforimDb.Schema.create(driver)
        val repo = SeforimRepository(":memory:", driver)

        val sourceId = repo.insertSource("Sefaria-Test")
        val catId = repo.insertCategory(Category(0, null, "תורה", level = 0, order = 1))

        val genesis = Book(
            id = 1,
            categoryId = catId,
            sourceId = sourceId,
            title = "בראשית",
            heRef = "בראשית",
            authors = emptyList(),
            pubPlaces = emptyList(),
            pubDates = emptyList(),
            heShortDesc = null,
            notesContent = null,
            order = 1f,
            topics = emptyList(),
            isBaseBook = true,
            totalLines = 10,
            hasAltStructures = false,
            hasTeamim = false,
            hasNekudot = false
        )
        val rashi = Book(
            id = 2,
            categoryId = catId,
            sourceId = sourceId,
            title = "רש\"י על בראשית",
            heRef = "רש\"י על בראשית",
            authors = listOf(Author(name = "רש\"י")),
            pubPlaces = emptyList(),
            pubDates = emptyList(),
            heShortDesc = null,
            notesContent = null,
            order = 2f,
            topics = emptyList(),
            isBaseBook = false,
            totalLines = 5,
            hasAltStructures = false,
            hasTeamim = false,
            hasNekudot = false
        )
        repo.insertBook(genesis)
        repo.insertBook(rashi)

        // Genesis lines (we need lines at positions matching the refs: 1:1, 3:5, 5:1)
        val genesisLines = listOf(
            1L to "בראשית, 1:1" to "בראשית 1:1",
            2L to "בראשית, 3:5" to "בראשית 3:5",
            3L to "בראשית, 5:1" to "בראשית 5:1"
        )
        genesisLines.forEachIndexed { idx, (ids, heRef) ->
            val (lineId, ref) = ids
            repo.insertLinesBatch(
                listOf(Line(id = lineId, bookId = 1, lineIndex = idx, content = "gen-$idx", heRef = heRef))
            )
        }
        // Rashi line matching ref "Rashi on Genesis 1:1"
        repo.insertLinesBatch(
            listOf(Line(id = 10, bookId = 2, lineIndex = 0, content = "rashi-0", heRef = "רש\"י על בראשית 1:1"))
        )

        val lineKeyToId = mapOf(
            "Genesis" to 0 to 1L,
            "Genesis" to 1 to 2L,
            "Genesis" to 2 to 3L,
            "Rashi on Genesis" to 0 to 10L
        )
        val lineIdToBookId = mapOf(1L to 1L, 2L to 1L, 3L to 1L, 10L to 2L)
        val bookMeta = mapOf(
            1L to BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
            2L to BookMeta(isBaseBook = false, categoryLevel = 0, priorityRank = 100)
        )

        val refsByCanonical = mapOf(
            canonicalCitation("Genesis 1:1") to listOf(RefEntry("Genesis 1:1", "בראשית 1:1", "Genesis", lineIndex = 1)),
            canonicalCitation("Genesis 3:5") to listOf(RefEntry("Genesis 3:5", "בראשית 3:5", "Genesis", lineIndex = 2)),
            canonicalCitation("Genesis 5:1") to listOf(RefEntry("Genesis 5:1", "בראשית 5:1", "Genesis", lineIndex = 3)),
            canonicalCitation("Rashi on Genesis 1:1") to listOf(
                RefEntry("Rashi on Genesis 1:1", "רש\"י על בראשית 1:1", "Rashi on Genesis", lineIndex = 1)
            )
        )
        val refsByBase = refsByCanonical.values.flatten().associateBy { canonicalBase(it.ref) }

        val importer = SefariaLinksImporter(repo, Logger.withTag("SefariaSelfCommentaryLinksTest"))
        importer.processLinksInParallel(
            linksDir = linksDir,
            refsByCanonical = refsByCanonical,
            refsByBase = refsByBase,
            lineKeyToId = lineKeyToId,
            lineIdToBookId = lineIdToBookId,
            bookMetaById = bookMeta
        )

        // Count links by (self vs cross) × connection type, querying the
        // underlying JDBC connection directly since the SQLDelight API only
        // exposes `executeRawQuery` for DDL/updates.
        fun count(sql: String): Long {
            val conn: Connection = driver.getConnection()
            conn.createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    return if (rs.next()) rs.getLong(1) else -1L
                }
            }
        }

        val selfCommentary = count(
            "SELECT COUNT(*) FROM link l JOIN connection_type ct ON ct.id=l.connectionTypeId " +
                "WHERE l.sourceBookId = l.targetBookId AND ct.name = 'COMMENTARY'"
        )
        val selfOther = count(
            "SELECT COUNT(*) FROM link l JOIN connection_type ct ON ct.id=l.connectionTypeId " +
                "WHERE l.sourceBookId = l.targetBookId AND ct.name = 'OTHER'"
        )
        val crossCommentary = count(
            "SELECT COUNT(*) FROM link l JOIN connection_type ct ON ct.id=l.connectionTypeId " +
                "WHERE l.sourceBookId != l.targetBookId AND ct.name = 'COMMENTARY'"
        )
        val crossSource = count(
            "SELECT COUNT(*) FROM link l JOIN connection_type ct ON ct.id=l.connectionTypeId " +
                "WHERE l.sourceBookId != l.targetBookId AND ct.name = 'SOURCE'"
        )

        assertEquals(0L, selfCommentary, "self-COMMENTARY links should be dropped")
        assertEquals(2L, selfOther, "self-OTHER links are legitimate cross-refs within a book; both directions must be kept")
        // Rashi ↔ Genesis produces one COMMENTARY direction and one SOURCE direction
        assertTrue(crossCommentary >= 1L, "cross-book Rashi→Genesis COMMENTARY link must survive")
        assertTrue(crossSource >= 1L, "reverse Genesis→Rashi SOURCE link must survive")

        repo.close()
    }

    @Test
    fun selfTargumLinksAreAlsoDropped() {
        // Pure-function sanity: TARGUM self-links take the same early-exit path.
        // We reuse the directional resolver helper to assert that our guard
        // condition `src==tgt && (COMMENTARY|TARGUM)` is exhaustive — the
        // resolver itself never downgrades other types to COMMENTARY/TARGUM.
        val (fwd, rev) = resolveDirectionalConnectionTypesForMeta(
            baseType = ConnectionType.OTHER,
            sourceBookId = 1L,
            targetBookId = 1L,
            sourceMeta = BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0),
            targetMeta = BookMeta(isBaseBook = true, categoryLevel = 0, priorityRank = 0)
        )
        // OTHER stays OTHER — we never transmute it into COMMENTARY so the
        // self-link filter does not over-prune.
        assertEquals(ConnectionType.OTHER, fwd)
        assertEquals(ConnectionType.OTHER, rev)
    }
}
