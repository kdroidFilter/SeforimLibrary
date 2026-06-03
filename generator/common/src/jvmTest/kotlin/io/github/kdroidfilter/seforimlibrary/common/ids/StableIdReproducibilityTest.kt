package io.github.kdroidfilter.seforimlibrary.common.ids

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end proof that two builds of the same synthetic dataset, with the
 * IdAllocator persisted between runs, produce identical primary-key
 * assignments.
 *
 * This is the Phase 1 acceptance test described in DELTA_UPDATE_PLAN.md
 * Phase 1 plan (last item):
 *
 *   "2 builds successifs sans modification source → contenu sémantique
 *   identique (hash logique)"
 *
 * We don't run the full Sefaria/Otzaria pipeline here (that takes ~30 min);
 * we exercise the IdAllocator + Bindings + SeforimRepository in isolation.
 */
class StableIdReproducibilityTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun newRepo(): SeforimRepository {
        val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
        SeforimDb.Schema.create(driver)
        // Path string is only used for logging by the repo; an in-memory marker is fine.
        return SeforimRepository(":memory:", driver)
    }

    private data class BuildSnapshot(
        val source1Id: Long,
        val source2Id: Long,
        val catRootId: Long,
        val catChildId: Long,
        val authorRashiId: Long,
        val genesisId: Long,
        val exodusId: Long,
        val linePalette: List<Pair<String, Long>>, // (content, lineId)
        val tocTextIds: Map<String, Long>,
    )

    private suspend fun runBuild(repo: SeforimRepository, statePath: Path?): Pair<BuildSnapshot, Path> {
        val allocator = InMemoryIdAllocator.load(statePath)
        val bindings = IdAllocatorBindings(allocator, repo)

        // Lookup tables
        val source1 = bindings.upsertSource("Sefaria")
        val source2 = bindings.upsertSource("Otzaria")
        val authorRashi = bindings.upsertAuthor("Rashi")
        val topic = bindings.upsertTopic("Torah")
        val place = bindings.upsertPubPlace("Venice")
        val date = bindings.upsertPubDate("1525")
        val connType = bindings.upsertConnectionType("commentary")
        val tocTextGen = bindings.upsertTocText("Genesis")
        val tocTextCh1 = bindings.upsertTocText("Chapter 1")

        // Categories
        val rootCat = bindings.upsertCategory(
            canonicalPath = "Tanakh",
            parentId = null,
            title = "Tanakh",
            level = 0,
            orderIndex = 1,
        )
        val torahCat = bindings.upsertCategory(
            canonicalPath = "Tanakh/Torah",
            parentId = rootCat,
            title = "Torah",
            level = 1,
            orderIndex = 1,
        )

        // Books
        val genesisId = bindings.insertBookStable(
            sourceName = "Sefaria",
            canonicalHeTitle = "בראשית",
            book = Book(
                id = 0,
                categoryId = torahCat,
                sourceId = source1,
                title = "Genesis",
                heRef = "Genesis",
            ),
        )
        val exodusId = bindings.insertBookStable(
            sourceName = "Sefaria",
            canonicalHeTitle = "שמות",
            book = Book(
                id = 0,
                categoryId = torahCat,
                sourceId = source1,
                title = "Exodus",
                heRef = "Exodus",
            ),
        )

        // Lines (a few synthetic ones in Genesis)
        val linePalette = listOf(
            "בראשית ברא אלהים את השמים ואת הארץ",
            "ויאמר אלהים יהי אור ויהי אור",
            "וירא אלהים את האור כי טוב",
        )
        val lineIds = linePalette.mapIndexed { idx, content ->
            content to bindings.insertLineStable(
                Line(
                    id = 0,
                    bookId = genesisId,
                    lineIndex = idx,
                    content = content,
                ),
            )
        }

        // Snapshot allocator state for next run
        val target = tmp.newFolder().toPath().resolve("build_state.db")
        allocator.snapshotTo(target, mapOf("build_version" to "1"))

        return BuildSnapshot(
            source1Id = source1,
            source2Id = source2,
            catRootId = rootCat,
            catChildId = torahCat,
            authorRashiId = authorRashi,
            genesisId = genesisId,
            exodusId = exodusId,
            linePalette = lineIds,
            tocTextIds = mapOf("Genesis" to tocTextGen, "Chapter 1" to tocTextCh1),
        ) to target
    }

    @Test
    fun `two builds with identical input produce identical ids`() = runBlocking {
        // Build A — fresh, no previous state.
        val repoA = newRepo()
        val (snapA, statePathA) = runBuild(repoA, statePath = null)
        repoA.close()

        // Build B — fresh DB, but seeded by the previous build_state.
        val repoB = newRepo()
        val (snapB, _) = runBuild(repoB, statePath = statePathA)
        repoB.close()

        assertEquals(snapA, snapB, "all stable ids should match between two builds")
    }

    @Test
    fun `a new book added in build B gets a fresh non-colliding id`() = runBlocking {
        val repoA = newRepo()
        val (snapA, statePathA) = runBuild(repoA, statePath = null)
        repoA.close()

        // Build C: same data + one extra book.
        val repoC = newRepo()
        val allocator = InMemoryIdAllocator.load(statePathA)
        val bindings = IdAllocatorBindings(allocator, repoC)

        // Replay the parts that registered existing ids …
        bindings.upsertSource("Sefaria")
        val rootCat = bindings.upsertCategory("Tanakh", null, "Tanakh", 0, 1)
        val torahCat = bindings.upsertCategory("Tanakh/Torah", rootCat, "Torah", 1, 1)
        val source1 = allocator.sourceId("Sefaria")
        // … and then add a brand-new book.
        val leviticusId = bindings.insertBookStable(
            sourceName = "Sefaria",
            canonicalHeTitle = "ויקרא",
            book = Book(id = 0, categoryId = torahCat, sourceId = source1, title = "Leviticus"),
        )
        repoC.close()

        // Sanity: existing ids unchanged, new id higher.
        assertEquals(snapA.catRootId, rootCat)
        assertEquals(snapA.catChildId, torahCat)
        assertTrue(
            leviticusId > snapA.exodusId,
            "new book id ($leviticusId) must be higher than existing exodus id (${snapA.exodusId})",
        )
        // Specifically, no collision with previously used ids.
        val previouslyUsed = setOf(snapA.genesisId, snapA.exodusId)
        assertTrue(leviticusId !in previouslyUsed, "fresh id collided with a known book id")
    }

    @Test
    fun `restarting from a snapshot reuses ids even after restart`() = runBlocking {
        // Build A
        val repoA = newRepo()
        val (snapA, statePathA) = runBuild(repoA, statePath = null)
        repoA.close()

        // Build B — same data again
        val repoB = newRepo()
        val (snapB, statePathB) = runBuild(repoB, statePath = statePathA)
        repoB.close()
        assertEquals(snapA, snapB)

        // Build C — same data once more, chained off Build B's state
        val repoC = newRepo()
        val (snapC, _) = runBuild(repoC, statePath = statePathB)
        repoC.close()
        assertEquals(snapA, snapC)
    }

    @Test
    fun `logical content hash of book + line tables is stable across two builds`() = runBlocking {
        // This is a coarse-grained logical-hash check inspired by §3.7:
        // dump rows ordered by id and hash the canonical encoding.
        val repoA = newRepo()
        val (_, statePathA) = runBuild(repoA, statePath = null)
        val hashA = logicalHashOfBooksAndLines(repoA)
        repoA.close()

        val repoB = newRepo()
        runBuild(repoB, statePath = statePathA)
        val hashB = logicalHashOfBooksAndLines(repoB)
        repoB.close()

        assertEquals(hashA, hashB, "logical hash of book+line must match across builds")
    }

    private suspend fun logicalHashOfBooksAndLines(repo: SeforimRepository): String {
        val md = MessageDigest.getInstance("SHA-256")
        // Books — ordered by id ascending.
        val books = repo.getAllBooks()
        for (b in books.sortedBy { it.id }) {
            md.update("book:${b.id}:${b.title}:${b.heRef ?: ""}:${b.sourceId}:${b.categoryId}".toByteArray())
            md.update(byteArrayOf(0xFF.toByte()))
        }
        for (b in books.sortedBy { it.id }) {
            val lines = repo.getLines(b.id, 0, Int.MAX_VALUE)
            for (l in lines.sortedBy { it.id }) {
                md.update("line:${l.id}:${l.bookId}:${l.lineIndex}:${l.content}".toByteArray())
                md.update(byteArrayOf(0xFF.toByte()))
            }
        }
        return md.digest().joinToString("") { "%02x".format(it) }
    }
}
