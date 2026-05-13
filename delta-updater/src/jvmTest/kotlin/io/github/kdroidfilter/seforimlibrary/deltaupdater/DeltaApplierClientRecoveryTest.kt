package io.github.kdroidfilter.seforimlibrary.deltaupdater

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DeltaApplierClientRecoveryTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `recoverIfNeeded restores backup when marker is present`() {
        val seforim = tmp.newFile("seforim.db").toPath()
        Files.writeString(seforim, "post-apply (corrupted)")
        val backup = seforim.resolveSibling("seforim.db.backup")
        Files.writeString(backup, "pre-apply (good)")
        val marker = seforim.resolveSibling("seforim.db.applying")
        Files.writeString(marker, "from=1 to=2")

        val recovered = DeltaApplierClient().recoverIfNeeded(seforim)

        assertTrue(recovered)
        assertEquals("pre-apply (good)", Files.readString(seforim))
        assertFalse(Files.exists(backup), "backup should be removed after recovery")
        assertFalse(Files.exists(marker), "marker should be removed after recovery")
    }

    @Test
    fun `recoverIfNeeded does nothing when no marker is present`() {
        val seforim = tmp.newFile("seforim.db").toPath()
        Files.writeString(seforim, "live data")

        val recovered = DeltaApplierClient().recoverIfNeeded(seforim)

        assertFalse(recovered)
        assertEquals("live data", Files.readString(seforim))
    }

    @Test
    fun `recoverIfNeeded does nothing when marker exists but backup is missing`() {
        // Edge case: someone deleted the backup but left the marker. Don't
        // wipe the live DB out of an abundance of caution.
        val seforim = tmp.newFile("seforim.db").toPath()
        Files.writeString(seforim, "live data")
        val marker = seforim.resolveSibling("seforim.db.applying")
        Files.writeString(marker, "from=1 to=2")

        val recovered = DeltaApplierClient().recoverIfNeeded(seforim)

        assertFalse(recovered)
        assertEquals("live data", Files.readString(seforim))
        assertTrue(Files.exists(marker), "marker stays so the next launch can decide")
    }

    @Test
    fun `apply clears marker and backup when in-process restore succeeds`() {
        // Build a real (empty) SeforimDb so the from-hash check passes, then
        // hand the applier a bogus patch.db so PatchApplier.apply() throws
        // mid-flight. The catch handler must restore the backup AND clear
        // marker + backup so a later recoverIfNeeded() doesn't log a
        // misleading "recovered from interrupted apply" warning.
        val seforim = tmp.newFile("seforim.db").toPath()
        Files.delete(seforim)
        JdbcSqliteDriver("jdbc:sqlite:${seforim.toAbsolutePath()}").use { driver ->
            SeforimDb.Schema.create(driver)
        }
        val fromHash = DriverManager.getConnection("jdbc:sqlite:${seforim.toAbsolutePath()}").use {
            LogicalContentHasher().compute(it)
        }
        val originalBytes = Files.readAllBytes(seforim)

        val patch = tmp.newFile("patch.db").toPath()
        Files.writeString(patch, "not actually a sqlite file") // attach will fail

        val manifest = DeltaManifest(
            fromVersion = 1,
            toVersion = 2,
            fromSchemaVersion = 1,
            toSchemaVersion = 1,
            fromContentHash = fromHash,
            toContentHash = "anything-else",
            patchFiles = emptyList(),
        )

        assertFailsWith<Throwable> {
            DeltaApplierClient().apply(seforim, patch, manifest)
        }

        val backup: Path = seforim.resolveSibling("seforim.db.backup")
        val marker: Path = seforim.resolveSibling("seforim.db.applying")
        assertFalse(Files.exists(backup), "backup must be cleared after in-process restore")
        assertFalse(Files.exists(marker), "marker must be cleared after in-process restore")
        assertTrue(
            originalBytes.contentEquals(Files.readAllBytes(seforim)),
            "seforim.db must be byte-identical to its pre-apply state",
        )
    }

    @Test
    fun `apply refuses when local schema_meta version differs from manifest fromSchemaVersion`() {
        val seforim = tmp.newFile("seforim.db").toPath()
        Files.delete(seforim)
        JdbcSqliteDriver("jdbc:sqlite:${seforim.toAbsolutePath()}").use { driver ->
            SeforimDb.Schema.create(driver)
        }
        // Stamp the live DB at schema v2; manifest will claim v3.
        DriverManager.getConnection("jdbc:sqlite:${seforim.toAbsolutePath()}").use { conn ->
            conn.prepareStatement("INSERT INTO schema_meta(key, value) VALUES (?, ?)").use { ps ->
                ps.setString(1, "db_schema_version"); ps.setString(2, "2"); ps.executeUpdate()
            }
        }
        val fromHash = DriverManager.getConnection("jdbc:sqlite:${seforim.toAbsolutePath()}").use {
            LogicalContentHasher().compute(it)
        }
        val patch = tmp.newFile("patch.db").toPath()
        Files.writeString(patch, "irrelevant")

        val manifest = DeltaManifest(
            fromVersion = 1, toVersion = 2,
            fromSchemaVersion = 3, toSchemaVersion = 3, // mismatch with local v2
            fromContentHash = fromHash, toContentHash = "anything",
            patchFiles = emptyList(),
        )
        val ex = assertFailsWith<IllegalStateException> {
            DeltaApplierClient().apply(seforim, patch, manifest)
        }
        val msg = ex.message.orEmpty()
        assertTrue("db_schema_version=2" in msg, "error must name the local version: $msg")
        assertTrue("fromSchemaVersion=3" in msg, "error must name the manifest version: $msg")
        // Backup must NOT have been written — we refused before touching disk.
        assertFalse(Files.exists(seforim.resolveSibling("seforim.db.backup")))
        assertFalse(Files.exists(seforim.resolveSibling("seforim.db.applying")))
    }

    @Test
    fun `apply refuses to run when marker already exists`() {
        // Build a real DB so the from-hash check would pass, then pre-create
        // the marker to simulate a concurrent apply or an un-recovered crash.
        val seforim = tmp.newFile("seforim.db").toPath()
        Files.delete(seforim)
        JdbcSqliteDriver("jdbc:sqlite:${seforim.toAbsolutePath()}").use { driver ->
            SeforimDb.Schema.create(driver)
        }
        val fromHash = DriverManager.getConnection("jdbc:sqlite:${seforim.toAbsolutePath()}").use {
            LogicalContentHasher().compute(it)
        }
        val marker = seforim.resolveSibling("seforim.db.applying")
        Files.writeString(marker, "from=1 to=2")

        val patch = tmp.newFile("patch.db").toPath()
        Files.writeString(patch, "irrelevant")

        val manifest = DeltaManifest(
            fromVersion = 1, toVersion = 2,
            fromSchemaVersion = 1, toSchemaVersion = 1,
            fromContentHash = fromHash, toContentHash = "x",
            patchFiles = emptyList(),
        )

        val ex = assertFailsWith<IllegalStateException> {
            DeltaApplierClient().apply(seforim, patch, manifest)
        }
        val msg = ex.message.orEmpty()
        assertTrue("marker already present" in msg, "error must name the concurrency cause: $msg")
        // Pre-existing marker is preserved — the second caller didn't trample
        // the first caller's bookkeeping.
        assertTrue(Files.exists(marker))
    }

    @Test
    fun `assertEnoughFreeSpace passes on a healthy partition`() {
        // Sanity check: the function shouldn't throw on a normal tmpfs/disk
        // where free space comfortably exceeds the tiny test files.
        val db = tmp.newFile("seforim.db").toPath()
        Files.writeString(db, "x")
        val patch = tmp.newFile("patch.db").toPath()
        Files.writeString(patch, "y")
        DeltaApplierClient().assertEnoughFreeSpace(db, patch)
    }

    @Test
    fun `finalizeApply removes both backup and marker`() {
        val seforim = tmp.newFile("seforim.db").toPath()
        val backup = seforim.resolveSibling("seforim.db.backup")
        val marker = seforim.resolveSibling("seforim.db.applying")
        Files.writeString(backup, "x")
        Files.writeString(marker, "y")

        DeltaApplierClient().finalizeApply(seforim)

        assertFalse(Files.exists(backup))
        assertFalse(Files.exists(marker))
    }
}
