package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import kotlin.test.assertEquals

/** 7 -> 7 deltas of banner + book_banner: removals and id changes apply cleanly under enforced FKs. */
class BannerDeltaTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `a banner no book uses any more is deleted with its last reference`() {
        val produced = roundTrip(db("prev.db"), db("next.db") {
            it.execute("UPDATE book_banner SET bannerId = 1 WHERE bookId = 3")
            it.execute("DELETE FROM banner WHERE id = 4")
        })
        assertEquals(mapOf("book_banner" to 1), bannerCounts(produced.upsertCounts))
        assertEquals(mapOf("banner" to 1), bannerCounts(produced.deleteCounts))
    }

    @Test
    fun `an override moved to a new id upserts the new text and deletes the old one`() {
        val produced = roundTrip(db("prev.db"), db("next.db") {
            it.execute("INSERT INTO banner VALUES (5, 'מיוחד [כאן](https://w.org/b)')")
            it.execute("UPDATE book_banner SET bannerId = 5 WHERE bookId = 3")
            it.execute("DELETE FROM banner WHERE id = 4")
        })
        assertEquals(mapOf("banner" to 1, "book_banner" to 1), bannerCounts(produced.upsertCounts))
        assertEquals(mapOf("banner" to 1), bannerCounts(produced.deleteCounts))
    }

    @Test
    fun `deleting a book that has a banner drops its row and its now unused text`() {
        val produced = roundTrip(db("prev.db"), db("next.db") {
            it.execute("DELETE FROM book_banner WHERE bookId = 3")
            it.execute("DELETE FROM book WHERE id = 3")
            it.execute("DELETE FROM banner WHERE id = 4")
        })
        assertEquals(1, produced.deleteCounts.getValue("book"))
        assertEquals(emptyMap(), bannerCounts(produced.upsertCounts))
        assertEquals(mapOf("banner" to 1, "book_banner" to 1), bannerCounts(produced.deleteCounts))
    }

    private fun bannerCounts(counts: Map<String, Int>): Map<String, Int> =
        counts.filterKeys { it == "banner" || it == "book_banner" }.filterValues { it > 0 }

    /** Produces prev -> next, applies it to a copy of prev with FKs on, and checks the hash and the FK state. */
    private fun roundTrip(prev: Path, next: Path): PatchDbProducer.Output {
        val patch = path("patch.db")
        val produced = PatchDbProducer().produce(
            prev, next, patch, fromVersion = 31, toVersion = 32, fromSchemaVersion = 7, toSchemaVersion = 7,
        )
        val target = path("target.db").also { Files.copy(prev, it) }
        connect(target).use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(conn, patch, expectedToContentHash = hash(next), expectedToSchemaVersion = 7)
        }
        assertEquals(hash(next), hash(target))
        connect(target).use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM pragma_foreign_key_check").use { rs ->
                    rs.next()
                    assertEquals(0, rs.getInt(1))
                }
            }
        }
        return produced
    }

    /** Books 1-3; 1 and 2 share source text 1, book 3 has its own override text 4. */
    private fun db(name: String, change: (Statement) -> Unit = {}): Path {
        val p = path(name)
        connect(p).use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book VALUES (1, 'א'), (2, 'ב'), (3, 'ג')")
                st.execute(SCHEMA_7_TABLE_DDL.getValue("banner"))
                st.execute(SCHEMA_7_TABLE_DDL.getValue("book_banner"))
                st.execute("INSERT INTO banner VALUES (1, 'באדיבות [כאן](https://w.org/{title})'), (4, 'מיוחד [כאן](https://w.org/a)')")
                st.execute("INSERT INTO book_banner VALUES (1, 1), (2, 1), (3, 4)")
                change(st)
            }
        }
        return p
    }

    private fun hash(db: Path): String = connect(db).use { LogicalContentHasher.forSchemaVersion(7).compute(it) }

    private fun path(name: String): Path = tmp.newFolder().toPath().resolve(name)

    private fun connect(p: Path): Connection = DriverManager.getConnection("jdbc:sqlite:${p.toAbsolutePath()}")
}
