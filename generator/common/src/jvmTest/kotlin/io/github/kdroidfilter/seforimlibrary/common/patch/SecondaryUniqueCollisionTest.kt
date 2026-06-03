package io.github.kdroidfilter.seforimlibrary.common.patch

import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Verifies that [PatchDbProducer] fails fast — with an actionable error
 * message — when two seforim.db files were generated from different
 * `IdAllocator` lineages, causing a row to be re-emitted under a new
 * primary key while a previous row at a different PK still carries the
 * same value on a secondary UNIQUE column (e.g. `topic.name`).
 *
 * Without this guard the failure would only surface mid-transaction in
 * [PatchApplier] as an opaque `UNIQUE constraint failed: topic.name`.
 */
class SecondaryUniqueCollisionTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    private fun buildDb(path: Path, topicRows: List<Pair<Long, String>>) {
        val driver = JdbcSqliteDriver("jdbc:sqlite:${path.toAbsolutePath()}")
        SeforimDb.Schema.create(driver)
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.autoCommit = false
            conn.prepareStatement("INSERT INTO topic(id, name) VALUES (?, ?)").use { ps ->
                for ((id, name) in topicRows) {
                    ps.setLong(1, id); ps.setString(2, name); ps.addBatch()
                }
                ps.executeBatch()
            }
            conn.commit()
        }
        driver.close()
        System.gc(); Thread.sleep(50)
    }

    @Test
    fun `producer rejects secondary UNIQUE collision on topic name`() {
        val prev = tmp.newFolder().toPath().resolve("prev.db")
        val new = tmp.newFolder().toPath().resolve("new.db")
        // Same topic name "Halakha" is allocated under id=20 in prev and id=21 in new:
        // simulates two builds with mismatched build_state.db lineages.
        buildDb(prev, listOf(20L to "Halakha"))
        buildDb(new, listOf(21L to "Halakha"))

        val out = tmp.newFolder().toPath().resolve("patch.db")
        val ex = assertFailsWith<IllegalStateException> {
            PatchDbProducer().produce(prev, new, out, fromVersion = 1, toVersion = 2)
        }
        val msg = ex.message.orEmpty()
        assertTrue("topic" in msg, "error message must name the offending table: $msg")
        assertTrue("name" in msg, "error message must name the offending column: $msg")
        assertTrue("build_state.db" in msg, "error message must hint at the lineage cause: $msg")
    }

    @Test
    fun `producer accepts identical secondary UNIQUE rows under the same PK`() {
        val prev = tmp.newFolder().toPath().resolve("prev.db")
        val new = tmp.newFolder().toPath().resolve("new.db")
        // Same id AND same name in both DBs — no collision.
        buildDb(prev, listOf(20L to "Halakha"))
        buildDb(new, listOf(20L to "Halakha"))

        val out = tmp.newFolder().toPath().resolve("patch.db")
        PatchDbProducer().produce(prev, new, out, fromVersion = 1, toVersion = 2)
        // No exception thrown.
    }
}
