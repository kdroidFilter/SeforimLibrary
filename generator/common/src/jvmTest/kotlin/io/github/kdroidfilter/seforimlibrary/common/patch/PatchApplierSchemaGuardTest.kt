package io.github.kdroidfilter.seforimlibrary.common.patch

import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Verifies that [PatchApplier] refuses to apply a `patch.db` whose
 * `patch_meta.schema_version` is greater than what this build understands,
 * and refuses to apply a `patch.db` that's missing that key entirely.
 *
 * Without these guards a future-schema patch would be silently mis-applied:
 * new tables ignored, new patch_meta keys not honoured, FK check passes
 * but the live DB ends up in a state the patch never described.
 */
class PatchApplierSchemaGuardTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun buildSeforimDb(path: Path) {
        Files.deleteIfExists(path)
        JdbcSqliteDriver("jdbc:sqlite:${path.toAbsolutePath()}").use { driver ->
            SeforimDb.Schema.create(driver)
        }
        System.gc(); Thread.sleep(50)
    }

    private fun buildPatchDb(
        path: Path,
        schemaVersion: Int?,
        fromVersion: Int = 1,
        toVersion: Int = 2,
    ) {
        Files.deleteIfExists(path)
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.autoCommit = false
            conn.createStatement().use { st ->
                PatchDbSchema.baseStatements.forEach { st.executeUpdate(it) }
            }
            conn.prepareStatement("INSERT INTO patch_meta(key, value) VALUES (?, ?)").use { ps ->
                val meta = mutableListOf(
                    "from_version" to fromVersion.toString(),
                    "to_version" to toVersion.toString(),
                )
                if (schemaVersion != null) meta += "schema_version" to schemaVersion.toString()
                for ((k, v) in meta) {
                    ps.setString(1, k); ps.setString(2, v); ps.executeUpdate()
                }
            }
            conn.commit()
        }
        System.gc(); Thread.sleep(50)
    }

    @Test
    fun `applier refuses a patch with a newer schema_version`() {
        val seforim = tmp.newFolder().toPath().resolve("seforim.db")
        buildSeforimDb(seforim)
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        buildPatchDb(patch, schemaVersion = PatchDbSchema.CURRENT_VERSION + 1)

        DriverManager.getConnection("jdbc:sqlite:${seforim.toAbsolutePath()}").use { conn ->
            val ex = assertFailsWith<IllegalStateException> {
                PatchApplier().apply(conn = conn, patchDb = patch)
            }
            val msg = ex.message.orEmpty()
            assertTrue("schema_version" in msg, "error must name the cause: $msg")
            assertTrue("Upgrade the client" in msg, "error must direct the operator: $msg")
        }
    }

    @Test
    fun `applier refuses a patch missing patch_meta schema_version`() {
        val seforim = tmp.newFolder().toPath().resolve("seforim.db")
        buildSeforimDb(seforim)
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        buildPatchDb(patch, schemaVersion = null)

        DriverManager.getConnection("jdbc:sqlite:${seforim.toAbsolutePath()}").use { conn ->
            val ex = assertFailsWith<IllegalStateException> {
                PatchApplier().apply(conn = conn, patchDb = patch)
            }
            assertTrue("missing patch_meta.schema_version" in ex.message.orEmpty())
        }
    }

    @Test
    fun `applier accepts a patch at exactly the current schema_version`() {
        val seforim = tmp.newFolder().toPath().resolve("seforim.db")
        buildSeforimDb(seforim)
        val patch = tmp.newFolder().toPath().resolve("patch.db")
        buildPatchDb(patch, schemaVersion = PatchDbSchema.CURRENT_VERSION)
        DriverManager.getConnection("jdbc:sqlite:${seforim.toAbsolutePath()}").use { conn ->
            // No upsert/delete tables → no-op apply, no exception.
            PatchApplier().apply(conn = conn, patchDb = patch)
        }
    }
}
