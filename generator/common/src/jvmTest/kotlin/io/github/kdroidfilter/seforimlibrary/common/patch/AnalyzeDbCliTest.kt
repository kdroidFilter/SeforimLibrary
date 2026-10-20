package io.github.kdroidfilter.seforimlibrary.common.patch

import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AnalyzeDbCliTest {

    @Test
    fun `analyze writes planner statistics without changing the logical hash`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            conn.createStatement().use { st ->
                st.execute(
                    "CREATE TABLE link (id INTEGER PRIMARY KEY, sourceLineId INTEGER NOT NULL, " +
                        "connectionTypeId INTEGER NOT NULL)",
                )
                st.execute("CREATE INDEX idx_link_type_source_line ON link(connectionTypeId, sourceLineId)")
                st.execute(
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100) " +
                        "INSERT INTO link SELECT i, i, i % 3 FROM n",
                )
            }
            val before = LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).computeReport(conn)

            analyzeForQueryPlanner(conn)

            val stat = conn.createStatement().use { st ->
                st.executeQuery("SELECT stat FROM sqlite_stat1 WHERE idx = 'idx_link_type_source_line'").use { rs ->
                    assertTrue(rs.next())
                    rs.getString(1)
                }
            }
            assertEquals("100 34 1", stat)
            assertEquals(before, LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).computeReport(conn))
            assertTrue(conn.autoCommit)
        }
    }
}
