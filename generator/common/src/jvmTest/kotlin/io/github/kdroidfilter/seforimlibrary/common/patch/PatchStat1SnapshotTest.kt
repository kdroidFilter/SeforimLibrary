package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Delta clients get the target DB's planner statistics through `stat1_snapshot`,
 * a table released appliers never read, and the logical hash stays blind to it.
 */
class PatchStat1SnapshotTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `an analyzed target ships its sqlite_stat1 and the applier installs it`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        buildDb(prev, rows = 30, analyze = false)
        buildDb(next, rows = 60, analyze = true)

        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        val expected = stat1Of(next)
        assertTrue(expected.isNotEmpty())
        assertEquals(expected, rowsOf(patch, "SELECT tbl, idx, stat FROM ${PatchDbSchema.STAT1_SNAPSHOT_TABLE}"))

        val target = applyTo(prev, patch)
        assertEquals(expected, stat1Of(target))
        assertEquals(logicalHash(next), logicalHash(target))
    }

    @Test
    fun `a target without statistics ships no snapshot and the client keeps its own`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        buildDb(prev, rows = 30, analyze = true)
        buildDb(next, rows = 60, analyze = false)

        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        assertFalse(hasTable(patch, PatchDbSchema.STAT1_SNAPSHOT_TABLE))
        val before = stat1Of(prev)
        assertEquals(before, stat1Of(applyTo(prev, patch)))
    }

    @Test
    fun `the applying connection immediately plans with the installed statistics`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        buildDb(prev, rows = 30, analyze = true)
        buildDb(next, rows = 60, analyze = true)
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        // Give the two indexes deliberately opposite estimates. The test checks
        // the planner's behavior on the connection returned by PatchApplier,
        // rather than only checking the persisted sqlite_stat1 rows.
        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("UPDATE stat1_snapshot SET stat = '60 60' WHERE idx = 'idx_category_order'")
                st.executeUpdate("UPDATE stat1_snapshot SET stat = '60 1' WHERE idx = 'idx_category_title'")
            }
        }

        val target = tmp.newFolder().toPath().resolve("target.db")
        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate("DELETE FROM sqlite_stat4")
                st.executeUpdate("UPDATE sqlite_stat1 SET stat = '30 1' WHERE idx = 'idx_category_order'")
                st.executeUpdate("UPDATE sqlite_stat1 SET stat = '30 30' WHERE idx = 'idx_category_title'")
                st.execute("ANALYZE main.sqlite_schema")
            }
            val beforePlan = queryPlan(conn)
            assertTrue(beforePlan.contains("idx_category_order"), beforePlan)

            PatchApplier().apply(conn, patch)

            val afterPlan = queryPlan(conn)
            assertTrue(afterPlan.contains("idx_category_title"), afterPlan)
        }
    }

    @Test
    fun `analyzed clients discard stale stat4 histograms when installing a snapshot`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        buildDb(prev, rows = 30, analyze = true)
        buildDb(next, rows = 60, analyze = true)
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        val target = tmp.newFolder().toPath().resolve("target.db")
        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            assertTrue(stat4Count(conn) > 0)
            PatchApplier().apply(conn, patch)
            assertEquals(0, stat4Count(conn))
        }
        assertEquals(stat1Of(next), stat1Of(target))
    }

    private fun applyTo(base: Path, patch: Path): Path {
        val target = tmp.newFolder().toPath().resolve("target.db")
        Files.copy(base, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            PatchApplier().apply(conn, patch)
        }
        return target
    }

    private fun newDbPath(name: String): Path = tmp.newFolder().toPath().resolve(name)

    private fun queryPlan(conn: java.sql.Connection): String =
        conn.createStatement().use { st ->
            st.executeQuery(
                "EXPLAIN QUERY PLAN SELECT id FROM category WHERE orderIndex = 1 AND title = 't1'",
            ).use { rs ->
                assertTrue(rs.next())
                rs.getString("detail")
            }
        }

    private fun stat4Count(conn: java.sql.Connection): Int =
        conn.createStatement().use { st ->
            st.executeQuery("SELECT COUNT(*) FROM sqlite_stat4").use { rs ->
                assertTrue(rs.next())
                rs.getInt(1)
            }
        }

    private fun buildDb(path: Path, rows: Int, analyze: Boolean) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeUpdate(CATEGORY_DDL)
                st.executeUpdate("CREATE INDEX idx_category_order ON category(orderIndex)")
                st.executeUpdate("CREATE INDEX idx_category_title ON category(title)")
                st.executeUpdate(
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < $rows) " +
                        "INSERT INTO category (id, title, orderIndex) SELECT i, 't' || i, i % 7 FROM n",
                )
                if (analyze) st.execute("ANALYZE")
            }
        }
    }

    private fun stat1Of(db: Path): List<List<String?>> =
        if (!hasTable(db, "sqlite_stat1")) emptyList()
        else rowsOf(db, "SELECT tbl, idx, stat FROM sqlite_stat1")

    private fun rowsOf(db: Path, sql: String): List<List<String?>> =
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("$sql ORDER BY 1, 2").use { rs ->
                    buildList { while (rs.next()) add(listOf(rs.getString(1), rs.getString(2), rs.getString(3))) }
                }
            }
        }

    private fun hasTable(db: Path, name: String): Boolean =
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            conn.prepareStatement("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").use { ps ->
                ps.setString(1, name)
                ps.executeQuery().use { it.next() }
            }
        }

    private fun logicalHash(path: Path): String =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use {
            LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).compute(it)
        }

    private companion object {
        val CATEGORY_DDL =
            """
            CREATE TABLE category (
                id INTEGER PRIMARY KEY NOT NULL,
                parentId INTEGER,
                title TEXT NOT NULL,
                level INTEGER NOT NULL DEFAULT 0,
                orderIndex INTEGER NOT NULL DEFAULT 999
            )
            """.trimIndent()
    }
}
