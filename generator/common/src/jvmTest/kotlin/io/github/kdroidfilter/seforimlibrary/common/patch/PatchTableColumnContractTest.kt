package io.github.kdroidfilter.seforimlibrary.common.patch

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.assertEquals

/**
 * Guard against the 2026-07-16 recurrence: `category.heShortDesc` was added to
 * the SQLDelight schema without bumping `db_schema_version`, so the released
 * v9–v13 DBs and the v25 DB claimed the same schema 1 while carrying different
 * physical columns. The patch fan then died on `no such column: prev.heShortDesc`.
 *
 * The producer now survives that (it emits an `ALTER TABLE … ADD COLUMN`
 * migration), but a silent column change is still a schema change: it must be
 * a *deliberate* one. This test pins the physical column set of every patch
 * table in a freshly created DB against a committed expectation keyed by
 * `db_schema_version`, so any add/rename/drop fails CI until someone either
 * bumps the version or consciously records the new column list.
 */
class PatchTableColumnContractTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    // The DB schema and patch-artifact format are independent version axes.
    // The generator writes the schema-5 working shape; SplitLineContentCli turns it into 6/7.

    @Test
    fun `physical columns of every patch table match the committed expectation`() {
        assertColumns(CURRENT_DB_SCHEMA_VERSION, PATCH_TABLES_IN_FK_ORDER, split = true, compressed = true)
    }

    @Test
    fun `the schema 6 tables still match the frozen schema 6 expectation`() {
        assertColumns(6, PATCH_TABLES_SCHEMA_6, split = true, compressed = true)
    }

    @Test
    fun `the generator working shape still matches the frozen schema 5 expectation`() {
        assertColumns(5, PATCH_TABLES_SCHEMA_5, split = false)
    }

    private fun assertColumns(
        dbSchemaVersion: Int,
        tables: List<PatchTable>,
        split: Boolean,
        compressed: Boolean = false,
    ) {
        val fixtureName = "/patch_table_columns_schema_$dbSchemaVersion.json"
        val fixture = javaClass.getResourceAsStream(fixtureName)
            ?.readBytes()?.toString(Charsets.UTF_8)
            ?: error("fixture $fixtureName missing from test resources")
        val root = Json.parseToJsonElement(fixture).jsonObject
        assertEquals(
            dbSchemaVersion,
            root.getValue("dbSchemaVersion").jsonPrimitive.content.toInt(),
            "fixture $fixtureName must describe db_schema_version $dbSchemaVersion",
        )
        val expected = root.getValue("tables").jsonObject.mapValues { (_, v) ->
            v.jsonArray.map { it.jsonPrimitive.content }
        }

        // A frozen fixture predates the columns a later schema added to its tables.
        val laterColumns = if (dbSchemaVersion < 7) SCHEMA_7_COLUMNS else emptyMap()
        val actual = freshDatabaseColumns(tables, split, compressed).mapValues { (table, cols) ->
            cols - laterColumns[table].orEmpty()
        }

        val hint = "bump db_schema_version or add a column migration"
        val followUp = "If you bump db_schema_version, add a NEW " +
            "patch_table_columns_schema_<N>.json for it (and keep $fixtureName frozen) rather " +
            "than editing this one — the older fixture still describes DBs already released " +
            "under the previous version."
        assertEquals(
            expected.keys.sorted(),
            actual.keys.sorted(),
            "the set of patch tables changed — $hint (fixture: $fixtureName). $followUp",
        )
        for (table in expected.keys.sorted()) {
            assertEquals(
                expected.getValue(table),
                actual.getValue(table),
                "physical columns of '$table' drifted from the committed schema " +
                    "$dbSchemaVersion expectation — $hint. " +
                    "A column added without a bump makes every older release " +
                    "carry a different physical schema under the same signed version. " +
                    followUp,
            )
        }
    }

    /** Column names (alphabetical, like LogicalContentHasher) per patch table. */
    private fun freshDatabaseColumns(
        tables: List<PatchTable>,
        split: Boolean,
        compressed: Boolean,
    ): Map<String, List<String>> {
        val db = tmp.newFolder().toPath().resolve("fresh-seforim.db")
        Files.deleteIfExists(db)
        JdbcSqliteDriver("jdbc:sqlite:${db.toAbsolutePath()}").use { driver ->
            SeforimDb.Schema.create(driver)
        }
        val out = LinkedHashMap<String, List<String>>()
        DriverManager.getConnection("jdbc:sqlite:${db.toAbsolutePath()}").use { conn ->
            if (split) splitLineContent(conn)
            if (compressed) compressLineContent(conn, threads = 1)
            for (table in tables) {
                val cols = PatchDbSchema.readTableInfo(conn, "main", table.name).map { it.name }
                if (cols.isEmpty()) error("patch table '${table.name}' is missing from a freshly created DB")
                out[table.name] = cols.sorted()
            }
        }
        return out
    }
}
