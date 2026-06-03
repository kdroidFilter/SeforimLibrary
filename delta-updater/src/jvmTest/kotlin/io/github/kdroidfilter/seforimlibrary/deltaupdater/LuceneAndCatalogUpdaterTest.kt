package io.github.kdroidfilter.seforimlibrary.deltaupdater

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class LuceneAndCatalogUpdaterTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun buildTinyPatch(): java.nio.file.Path {
        val path = tmp.newFile("patch.db").toPath()
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate(
                    """
                    CREATE TABLE blobs (name TEXT PRIMARY KEY NOT NULL, content BLOB NOT NULL)
                    """.trimIndent(),
                )
                st.executeUpdate(
                    """
                    CREATE TABLE upsert_line (
                        id INTEGER PRIMARY KEY NOT NULL,
                        bookId INTEGER NOT NULL,
                        lineIndex INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        heRef TEXT
                    )
                    """.trimIndent(),
                )
                st.executeUpdate("CREATE TABLE delete_line (id INTEGER PRIMARY KEY)")
            }
            // Two upserts + three deletes.
            conn.prepareStatement("INSERT INTO upsert_line VALUES (?, ?, ?, ?, ?)").use { ps ->
                listOf(
                    listOf(101L, 1L, 0, "<h1>בראשית</h1>", null),
                    listOf(102L, 1L, 1, "(א) בראשית ברא אלהים", "Genesis 1:1"),
                ).forEach { row ->
                    ps.setLong(1, row[0] as Long)
                    ps.setLong(2, row[1] as Long)
                    ps.setLong(3, (row[2] as Int).toLong())
                    ps.setString(4, row[3] as String)
                    ps.setString(5, row[4] as String?)
                    ps.executeUpdate()
                }
            }
            conn.prepareStatement("INSERT INTO delete_line(id) VALUES (?)").use { ps ->
                listOf(201L, 202L, 203L).forEach { ps.setLong(1, it); ps.executeUpdate() }
            }
            // One catalog blob.
            conn.prepareStatement("INSERT INTO blobs VALUES (?, ?)").use { ps ->
                ps.setString(1, "catalog.pb")
                ps.setBytes(2, byteArrayOf(0x42, 0x99.toByte(), 0x77))
                ps.executeUpdate()
            }
        }
        return path
    }

    @Test
    fun `lucene updater enumerates deletes and upserts`() {
        val patch = buildTinyPatch()
        val deletes = ArrayList<Long>()
        val upserts = ArrayList<LuceneUpdater.PatchLine>()
        val stats = LuceneUpdater().applyTo(
            patchDb = patch,
            deletes = { id -> deletes += id },
            upserts = { line -> upserts += line },
        )
        assertEquals(3, stats.deletes)
        assertEquals(2, stats.upserts)
        assertContentEquals(listOf(201L, 202L, 203L), deletes.sorted())
        assertEquals(setOf(101L, 102L), upserts.map { it.id }.toSet())
        val genesisVerse = upserts.first { it.id == 102L }
        assertEquals("Genesis 1:1", genesisVerse.heRef)
        assertEquals(1L, genesisVerse.bookId)
    }

    @Test
    fun `lucene updater no-ops when delete_line or upsert_line tables are missing`() {
        val empty = tmp.newFile("empty.db").toPath()
        DriverManager.getConnection("jdbc:sqlite:${empty.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.executeUpdate("CREATE TABLE noise (x INT)") }
        }
        val stats = LuceneUpdater().applyTo(empty, deletes = {}, upserts = {})
        assertEquals(0, stats.deletes)
        assertEquals(0, stats.upserts)
    }

    @Test
    fun `catalog updater writes blob to target atomically`() {
        val patch = buildTinyPatch()
        val target = tmp.newFolder().toPath().resolve("catalog.pb")

        val written = CatalogUpdater().update(patch, target)
        assertTrue(written)
        assertContentEquals(byteArrayOf(0x42, 0x99.toByte(), 0x77), Files.readAllBytes(target))
        // The .tmp sibling must NOT linger after a successful move.
        assertFalse(Files.exists(target.resolveSibling("${target.fileName}.tmp")))
    }

    @Test
    fun `catalog updater returns false when blob is missing`() {
        val empty = tmp.newFile("empty.db").toPath()
        DriverManager.getConnection("jdbc:sqlite:${empty.toAbsolutePath()}").use { conn ->
            conn.createStatement().use {
                it.executeUpdate("CREATE TABLE blobs (name TEXT PRIMARY KEY, content BLOB)")
            }
        }
        val target = tmp.newFolder().toPath().resolve("catalog.pb")
        val written = CatalogUpdater().update(empty, target, "no.such.blob")
        assertFalse(written)
        assertFalse(Files.exists(target))
    }
}
