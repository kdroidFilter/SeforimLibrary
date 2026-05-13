package io.github.kdroidfilter.seforimlibrary.deltaupdater

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import kotlin.test.assertEquals
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
