package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class SourceHashComputerTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    // ─── Otzaria: parses files_manifest.json and emits hashes for each .txt ───

    @Test
    fun `otzaria parses manifest and emits one entry per txt book`() {
        val root = tmp.newFolder().toPath()
        Files.writeString(
            root.resolve("files_manifest.json"),
            """
            {
              "אוצריא/Tanakh/Genesis.txt": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
              "אוצריא/Tanakh/Exodus.txt":  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
              "metadata.json":              "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            }
            """.trimIndent(),
        )
        val computer = OtzariaSourceHashComputer()
        val out = computer.compute(root, version = 7)

        // Two books — metadata.json is filtered out (not .txt).
        assertEquals(2, out.size)
        val genesis = BookKey("Otzaria", "Genesis")
        val exodus = BookKey("Otzaria", "Exodus")
        assertNotNull(out[genesis])
        assertNotNull(out[exodus])
        assertEquals(7, out.getValue(genesis).lastSeenVersion)
        // First byte of "aaaaaaaa..." sha256 hex is 0xaa.
        assertEquals(0xaa.toByte(), out.getValue(genesis).hash[0])
        assertEquals(0xbb.toByte(), out.getValue(exodus).hash[0])
        // Hashes must be 32 bytes.
        assertEquals(32, out.getValue(genesis).hash.size)
    }

    @Test
    fun `otzaria detects content change by hash diff`() {
        val rootA = tmp.newFolder().toPath()
        Files.writeString(
            rootA.resolve("files_manifest.json"),
            """{"foo/A.txt": "1111111111111111111111111111111111111111111111111111111111111111"}""",
        )
        val rootB = tmp.newFolder().toPath()
        Files.writeString(
            rootB.resolve("files_manifest.json"),
            """{"foo/A.txt": "2222222222222222222222222222222222222222222222222222222222222222"}""",
        )
        val a = OtzariaSourceHashComputer().compute(rootA, 1)
        val b = OtzariaSourceHashComputer().compute(rootB, 2)
        val key = BookKey("Otzaria", "A")
        assertNotEquals(a.getValue(key).hash.toList(), b.getValue(key).hash.toList())
    }

    @Test
    fun `otzaria honors source resolver`() {
        val root = tmp.newFolder().toPath()
        Files.writeString(
            root.resolve("files_manifest.json"),
            """{"foo/A.txt": "1111111111111111111111111111111111111111111111111111111111111111"}""",
        )
        val out = OtzariaSourceHashComputer(sourceNameResolver = { "wikisourceToOtzaria" })
            .compute(root, 1)
        assertEquals(setOf(BookKey("wikisourceToOtzaria", "A")), out.keys)
    }

    // ─── Sefaria: walks json/<...>/merged.json and incorporates the schema file ───

    @Test
    fun `sefaria hashes a single merged_json and emits canonical key`() {
        val root = tmp.newFolder().toPath()
        val bookDir = root.resolve("json/Tanakh/Torah/Genesis")
        Files.createDirectories(bookDir)
        Files.writeString(
            bookDir.resolve("merged.json"),
            """
            {
              "title": "Genesis",
              "heTitle": "בראשית",
              "text": [["a", "b"]]
            }
            """.trimIndent(),
        )
        Files.createDirectories(root.resolve("schemas"))
        val out = SefariaSourceHashComputer().compute(root, version = 3)

        assertEquals(1, out.size)
        val key = BookKey("Sefaria", "בראשית")
        assertNotNull(out[key])
        assertEquals(32, out.getValue(key).hash.size)
        assertEquals(3, out.getValue(key).lastSeenVersion)
    }

    @Test
    fun `sefaria hash changes when schema changes (not just merged_json)`() {
        val root = tmp.newFolder().toPath()
        val bookDir = root.resolve("json/Tanakh/Torah/Genesis")
        Files.createDirectories(bookDir)
        Files.writeString(
            bookDir.resolve("merged.json"),
            """{"title":"Genesis","heTitle":"בראשית","text":[["a"]]}""",
        )
        val schemaDir = root.resolve("schemas")
        Files.createDirectories(schemaDir)
        Files.writeString(schemaDir.resolve("בראשית.json"), """{"schema":"v1"}""")

        val a = SefariaSourceHashComputer().compute(root, 1)

        Files.writeString(schemaDir.resolve("בראשית.json"), """{"schema":"v2"}""")
        val b = SefariaSourceHashComputer().compute(root, 2)

        val key = BookKey("Sefaria", "בראשית")
        assertNotEquals(
            a.getValue(key).hash.toList(),
            b.getValue(key).hash.toList(),
            "schema change must alter the hash",
        )
    }

    @Test
    fun `sefaria skips books missing heTitle`() {
        val root = tmp.newFolder().toPath()
        val bookDir = root.resolve("json/foo/Bar")
        Files.createDirectories(bookDir)
        Files.writeString(bookDir.resolve("merged.json"), """{"title":"NoHeTitle","text":[]}""")
        Files.createDirectories(root.resolve("schemas"))
        val out = SefariaSourceHashComputer().compute(root, 1)
        assertTrue(out.isEmpty())
    }
}
