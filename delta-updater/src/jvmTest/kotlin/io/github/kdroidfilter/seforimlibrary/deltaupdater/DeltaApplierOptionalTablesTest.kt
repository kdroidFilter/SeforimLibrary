package io.github.kdroidfilter.seforimlibrary.deltaupdater

import io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher
import io.github.kdroidfilter.seforimlibrary.common.patch.OPTIONAL_PATCH_TABLES
import io.github.kdroidfilter.seforimlibrary.common.patch.PatchCompressor
import io.github.kdroidfilter.seforimlibrary.common.patch.PatchDbProducer
import io.github.kdroidfilter.seforimlibrary.common.patch.ReleaseManifestWriter
import kotlinx.serialization.json.Json
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** The client applies the optional-table side channel and verifies the manifest's hashes. */
class DeltaApplierOptionalTablesTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }

    @Test
    fun `manifest written by the producer side round-trips through the client apply`() {
        val prev = db("prev.db", banner = null)
        val next = db("next.db", banner = "באדיבות\nהמו\"ל")
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2, fromSchemaVersion = 6, toSchemaVersion = 6)

        val manifest = manifestFor(prev, next, patch, optionalHashes(next))
        assertEquals(listOf("book_banner"), manifest.optionalTableContentHashes.keys.toList())

        val result = DeltaApplierClient().apply(prev, patch, manifest)
        assertEquals(mapOf("book_banner" to 1), result.applied.optionalTablesReplaced)
        assertEquals(optionalHashes(next), optionalHashes(prev))
    }

    @Test
    fun `a wrong optional hash restores the pre-apply DB`() {
        val prev = db("prev.db", banner = "ישן")
        val next = db("next.db", banner = "חדש")
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2, fromSchemaVersion = 6, toSchemaVersion = 6)
        val before = Files.readAllBytes(prev)

        val manifest = manifestFor(prev, next, patch, mapOf("book_banner" to "0".repeat(64)))
        assertFailsWith<IllegalStateException> { DeltaApplierClient().apply(prev, patch, manifest) }
        assertEquals(before.toList(), Files.readAllBytes(prev).toList())
    }

    @Test
    fun `line_divine_name travels the side channel and is verified by the client`() {
        val prev = db("prev.db", banner = null, divineNames = listOf(listOf(1, 0, 4, 1, 1)))
        val next = db("next.db", banner = null, divineNames = listOf(listOf(1, 3, 7, 5, 3), listOf(2, 0, 2, 3, 2)))
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2, fromSchemaVersion = 6, toSchemaVersion = 6)

        val hashes = optionalHashes(next, "line_divine_name")
        val manifest = manifestFor(prev, next, patch, hashes)
        val result = DeltaApplierClient().apply(prev, patch, manifest)
        assertEquals(mapOf("line_divine_name" to 2), result.applied.optionalTablesReplaced)
        assertEquals(hashes, optionalHashes(prev, "line_divine_name"))
    }

    private fun manifestFor(prev: Path, next: Path, patch: Path, optional: Map<String, String>): DeltaManifest {
        val compressed = PatchCompressor.compress(patch, level = 3, workers = 1)
        val file = ReleaseManifestWriter().writeManifest(
            patchFile = patch,
            fromVersion = 1, toVersion = 2,
            fromSchemaVersion = 6, toSchemaVersion = 6,
            fromContentHash = hash(prev), toContentHash = hash(next),
            compressed = ReleaseManifestWriter.CompressedPatchSpec(
                file = compressed.compressedFile,
                sha256 = compressed.compressedSha256,
                size = compressed.compressedSize,
                compression = "zstd",
            ),
            optionalTableContentHashes = optional,
        )
        return json.decodeFromString(DeltaManifest.serializer(), Files.readString(file))
    }

    private fun db(name: String, banner: String?, divineNames: List<List<Int>>? = null): Path {
        val path = tmp.newFolder().toPath().resolve(name)
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book VALUES (1, 'א')")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL)")
                st.execute("INSERT INTO line VALUES (1, 1), (2, 1)")
                if (divineNames != null) {
                    st.execute(OPTIONAL_PATCH_TABLES.single { it.name == "line_divine_name" }.ddl)
                    for (r in divineNames) st.execute("INSERT INTO line_divine_name VALUES (${r.joinToString()})")
                }
                if (banner != null) {
                    st.execute(OPTIONAL_PATCH_TABLES.single { it.name == "book_banner" }.ddl)
                }
            }
            if (banner != null) {
                conn.prepareStatement("INSERT INTO book_banner (bookId, text) VALUES (1, ?)").use {
                    it.setString(1, banner)
                    it.executeUpdate()
                }
            }
        }
        return path
    }

    private fun hash(path: Path): String = DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
        LogicalContentHasher.forSchemaVersion(6).compute(it)
    }

    private fun optionalHashes(path: Path, table: String = "book_banner"): Map<String, String> =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
            LogicalContentHasher(listOf(table)).computeReport(it).tableHashes
        }
}
