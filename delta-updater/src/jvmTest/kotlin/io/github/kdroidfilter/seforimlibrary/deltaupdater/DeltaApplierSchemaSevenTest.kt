package io.github.kdroidfilter.seforimlibrary.deltaupdater

import io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher
import io.github.kdroidfilter.seforimlibrary.common.patch.PatchCompressor
import io.github.kdroidfilter.seforimlibrary.common.patch.PatchDbProducer
import io.github.kdroidfilter.seforimlibrary.common.patch.ReleaseManifestWriter
import io.github.kdroidfilter.seforimlibrary.common.patch.SCHEMA_7_TABLE_DDL
import kotlinx.serialization.json.Json
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** A schema-6 client reaches schema 7 through an ordinary delta, verified by the schema-7 hash. */
class DeltaApplierSchemaSevenTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private val json = Json { ignoreUnknownKeys = true; isLenient = true }

    @Test
    fun `a 6 to 7 manifest round-trips through the client apply`() {
        val prev = db("prev.db", schemaSeven = false)
        val next = db("next.db", schemaSeven = true)
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 7)

        val manifest = manifestFor(prev, next, patch)
        assertEquals(6, manifest.fromSchemaVersion)
        assertEquals(7, manifest.toSchemaVersion)
        val result = DeltaApplierClient().apply(prev, patch, manifest)

        assertEquals(8, result.applied.migrationsApplied)
        assertEquals(2, result.applied.upsertCounts.getValue("line_note"))
        assertEquals(hash(next, 7), hash(prev, 7))
    }

    @Test
    fun `a wrong schema 7 hash restores the schema 6 DB`() {
        val prev = db("prev.db", schemaSeven = false)
        val next = db("next.db", schemaSeven = true)
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        PatchDbProducer().produce(prev, next, patch, fromVersion = 30, toVersion = 31, fromSchemaVersion = 6, toSchemaVersion = 7)
        val before = Files.readAllBytes(prev)

        val manifest = manifestFor(prev, next, patch).copy(toContentHash = "0".repeat(64))
        assertFailsWith<IllegalStateException> { DeltaApplierClient().apply(prev, patch, manifest) }
        assertEquals(before.toList(), Files.readAllBytes(prev).toList())
    }

    private fun manifestFor(prev: Path, next: Path, patch: Path): DeltaManifest {
        val compressed = PatchCompressor.compress(patch, level = 3, workers = 1)
        val file = ReleaseManifestWriter().writeManifest(
            patchFile = patch,
            fromVersion = 30, toVersion = 31,
            fromSchemaVersion = 6, toSchemaVersion = 7,
            fromContentHash = hash(prev, 6), toContentHash = hash(next, 7),
            compressed = ReleaseManifestWriter.CompressedPatchSpec(
                file = compressed.compressedFile,
                sha256 = compressed.compressedSha256,
                size = compressed.compressedSize,
                compression = "zstd",
            ),
        )
        return json.decodeFromString(DeltaManifest.serializer(), Files.readString(file))
    }

    private fun db(name: String, schemaSeven: Boolean): Path {
        val path = tmp.newFolder().toPath().resolve(name)
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
                st.execute("INSERT INTO book VALUES (1, 'א')")
                st.execute("CREATE TABLE line (id INTEGER PRIMARY KEY NOT NULL, bookId INTEGER NOT NULL)")
                st.execute("INSERT INTO line VALUES (1, 1), (2, 1)")
                if (schemaSeven) {
                    SCHEMA_7_TABLE_DDL.values.forEach { st.execute(it) }
                    st.execute("INSERT INTO book_banner VALUES (1, 'באדיבות\nהמו\"ל')")
                    st.execute("INSERT INTO line_note VALUES (1, 0, '1', 'גוף'), (2, 0, NULL, 'נספח')")
                }
            }
        }
        return path
    }

    private fun hash(path: Path, schemaVersion: Int): String = DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
        LogicalContentHasher.forSchemaVersion(schemaVersion).compute(it)
    }
}
