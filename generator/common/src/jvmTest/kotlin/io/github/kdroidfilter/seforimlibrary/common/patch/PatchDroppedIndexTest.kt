package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals

/**
 * An index removed from the schema must also disappear from patch-updated
 * clients, through a plain-SQL migration every released applier runs verbatim.
 */
class PatchDroppedIndexTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `an index prev has and new lacks ships as DROP INDEX IF EXISTS`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        buildDb(prev) { st ->
            st.executeUpdate(CATEGORY_DDL)
            st.executeUpdate("CREATE INDEX idx_category_order ON category(orderIndex)")
            st.executeUpdate("CREATE INDEX idx_category_title ON category(title)")
            st.executeUpdate(SEED)
        }
        buildDb(next) { st ->
            st.executeUpdate(CATEGORY_DDL)
            st.executeUpdate("CREATE INDEX idx_category_order ON category(orderIndex)")
            st.executeUpdate(SEED)
        }

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        assertEquals(listOf("""DROP INDEX IF EXISTS main."idx_category_title""""), migrationsOf(patch))
        assertEquals(0, produced.upsertCounts.getValue("category"))

        val target = applyTo(prev, patch)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            assertEquals(listOf("idx_category_order"), indexNames(conn, "category"))
            assertEquals(logicalHash(next), LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).compute(conn))
        }
        // A client that never had the index (full download) applies it as a no-op.
        applyTo(next, patch)
    }

    @Test
    fun `no DROP migration when prev and new agree on indexes`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        for (db in listOf(prev, next)) {
            buildDb(db) { st ->
                st.executeUpdate(CATEGORY_DDL)
                st.executeUpdate("CREATE INDEX idx_category_order ON category(orderIndex)")
                st.executeUpdate(SEED)
            }
        }

        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        assertEquals(emptyList(), migrationsOf(patch))
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

    private fun buildDb(path: Path, block: (java.sql.Statement) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { conn ->
            conn.createStatement().use(block)
        }
    }

    private fun migrationsOf(patch: Path): List<String> =
        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT sql FROM migrations ORDER BY version ASC").use { rs ->
                    buildList { while (rs.next()) add(rs.getString(1)) }
                }
            }
        }

    private fun indexNames(conn: Connection, table: String): List<String> =
        conn.createStatement().use { st ->
            st.executeQuery(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='$table' " +
                    "AND sql IS NOT NULL ORDER BY name",
            ).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
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

        const val SEED =
            "INSERT INTO category(id, parentId, title, level, orderIndex) VALUES " +
                "(1, NULL, 'Tanakh', 0, 1), (2, NULL, 'Mishnah', 0, 2)"
    }
}
