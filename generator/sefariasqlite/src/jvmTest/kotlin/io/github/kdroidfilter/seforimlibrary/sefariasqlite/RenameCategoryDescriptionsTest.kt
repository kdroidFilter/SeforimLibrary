package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class RenameCategoryDescriptionsTest {
    @Test
    fun `merge descriptions copies source keeps target and accepts equal values`() {
        withConnection { connection ->
            insert(connection, 1, "source", "source long")
            insert(connection, 2, null, null)
            mergeCategoryDescriptions(connection, 1, 2)
            assertEquals("source" to "source long", read(connection, 2))

            insert(connection, 3, null, null)
            insert(connection, 4, "target", "target long")
            mergeCategoryDescriptions(connection, 3, 4)
            assertEquals("target" to "target long", read(connection, 4))

            insert(connection, 5, "equal", "same")
            insert(connection, 6, "equal", "same")
            mergeCategoryDescriptions(connection, 5, 6)
            assertEquals("equal" to "same", read(connection, 6))
        }
    }

    @Test
    fun `merge descriptions rejects conflicts before either category changes`() {
        withConnection { connection ->
            insert(connection, 1, "source", "same")
            insert(connection, 2, "target", "same")

            assertFailsWith<IllegalStateException> {
                mergeCategoryDescriptions(connection, 1, 2)
            }
            assertEquals("source" to "same", read(connection, 1))
            assertEquals("target" to "same", read(connection, 2))
        }
    }

    @Test
    fun `rename merge path updates descriptions moves relations and deletes source`() {
        withConnection { connection ->
            insert(connection, 1, "source short", "source long", title = "old")
            insert(connection, 2, null, null, title = "new")
            insert(connection, 3, null, null, title = "child", parentId = 1)
            insertBook(connection, id = 10, categoryId = 1)

            val result = renameOrMergeCategory(
                connection,
                CategoryRename("old", "new", CategoryMatchMode.Exact),
                Logger.withTag("test"),
            )

            assertIs<RenameResult.Merged>(result)
            assertFalse(categoryExists(connection, 1))
            assertEquals("source short" to "source long", read(connection, 2))
            assertEquals(2, bookCategory(connection, 10))
            assertEquals(2, categoryParent(connection, 3))
        }
    }

    @Test
    fun `rename merge conflict leaves source book and child untouched`() {
        withConnection { connection ->
            insert(connection, 1, "source", "same", title = "old")
            insert(connection, 2, "target", "same", title = "new")
            insert(connection, 3, null, null, title = "child", parentId = 1)
            insertBook(connection, id = 10, categoryId = 1)

            assertFailsWith<IllegalStateException> {
                renameOrMergeCategory(
                    connection,
                    CategoryRename("old", "new", CategoryMatchMode.Exact),
                    Logger.withTag("test"),
                )
            }

            assertTrue(categoryExists(connection, 1))
            assertEquals("source" to "same", read(connection, 1))
            assertEquals("target" to "same", read(connection, 2))
            assertEquals(1, bookCategory(connection, 10))
            assertEquals(1, categoryParent(connection, 3))
        }
    }

    @Test
    fun `conflict settled by a ForDB replace or clear keeps the target value`() {
        withConnection { connection ->
            insert(connection, 1, "source", "source long", title = "old")
            insert(connection, 2, "target", "target long", title = "new")
            val overrides = overridesOf(
                override("new", shortEdit = DescriptionEdit.Replace("final"), longEdit = DescriptionEdit.Clear),
            )

            mergeCategoryDescriptions(connection, 1, 2, overrides)

            assertEquals("target" to "target long", read(connection, 2))
        }
    }

    @Test
    fun `conflict is not settled by a keep edit or by a row for another path`() {
        withConnection { connection ->
            insert(connection, 1, "source", "same", title = "old", parentId = 9)
            insert(connection, 2, "target", "same", title = "new", parentId = 9)
            insert(connection, 9, null, null, title = "root")

            // Only the long description is edited; the short one stays in conflict.
            assertFailsWith<IllegalStateException> {
                mergeCategoryDescriptions(
                    connection, 1, 2,
                    overridesOf(override("root/new", longEdit = DescriptionEdit.Replace("final"))),
                )
            }
            // A row for the bare title is not the target's path.
            assertFailsWith<IllegalStateException> {
                mergeCategoryDescriptions(
                    connection, 1, 2,
                    overridesOf(override("new", shortEdit = DescriptionEdit.Replace("final"))),
                )
            }
            assertEquals("target" to "same", read(connection, 2))
        }
    }

    @Test
    fun `rename merge folds same-named subcategories instead of duplicating them`() {
        withConnection { connection ->
            // old > {shared > {deep, only-source}, unique}; new > {shared > deep}
            insert(connection, 1, null, null, title = "old")
            insert(connection, 2, null, null, title = "shared", parentId = 1)
            insert(connection, 3, null, null, title = "deep", parentId = 2)
            insert(connection, 4, null, null, title = "only-source", parentId = 2)
            insert(connection, 5, null, null, title = "unique", parentId = 1)
            insert(connection, 11, null, null, title = "new")
            insert(connection, 12, "kept", null, title = "shared", parentId = 11)
            insert(connection, 13, null, null, title = "deep", parentId = 12)
            insertBook(connection, id = 100, categoryId = 1)
            insertBook(connection, id = 101, categoryId = 2)
            insertBook(connection, id = 102, categoryId = 3)

            val result = renameOrMergeCategory(
                connection,
                CategoryRename("old", "new", CategoryMatchMode.Exact),
                Logger.withTag("test"),
            )

            assertEquals(RenameResult.Merged(3), result)
            listOf(1L, 2L, 3L).forEach { assertFalse(categoryExists(connection, it), "source id=$it must be gone") }
            assertEquals(11, bookCategory(connection, 100))
            assertEquals(12, bookCategory(connection, 101))
            assertEquals(13, bookCategory(connection, 102))
            assertEquals(12, categoryParent(connection, 4))
            assertEquals(11, categoryParent(connection, 5))
            assertEquals("kept" to null, read(connection, 12))
            assertEquals(
                listOf("new", "new/shared", "new/shared/deep", "new/shared/only-source", "new/unique"),
                allPaths(connection),
            )
        }
    }

    @Test
    fun `nested description conflict fails before the merge changes anything`() {
        withConnection { connection ->
            insert(connection, 1, null, null, title = "old")
            insert(connection, 2, "source", null, title = "shared", parentId = 1)
            insert(connection, 11, null, null, title = "new")
            insert(connection, 12, "target", null, title = "shared", parentId = 11)
            insertBook(connection, id = 100, categoryId = 1)

            assertFailsWith<IllegalStateException> {
                renameOrMergeCategory(
                    connection,
                    CategoryRename("old", "new", CategoryMatchMode.Exact),
                    Logger.withTag("test"),
                )
            }

            assertTrue(categoryExists(connection, 1))
            assertEquals(1, bookCategory(connection, 100))
            assertEquals(1, categoryParent(connection, 2))
        }
    }

    /** The released-DB shape behind the ForDB gate failure: a Sefaria tree renamed onto an Otzaria one. */
    @Test
    fun `validator merges onto an existing tree when ForDB settles every conflict`() {
        withConnection { connection ->
            insert(connection, 1, "sefaria", "sefaria long", title = "מילונים וספרי יעץ")
            insert(connection, 2, "same", "sefaria words", title = "מילונים", parentId = 1)
            insert(connection, 3, "grammar", "grammar", title = "דקדוק", parentId = 1)
            insert(connection, 11, "otzaria", "otzaria long", title = "ספרות עזר")
            insert(connection, 12, "same", "otzaria words", title = "מילונים", parentId = 11)
            insert(connection, 13, "grammar", "grammar", title = "דקדוק", parentId = 11)
            insertBook(connection, id = 100, categoryId = 2)
            val rename = CategoryRename("מילונים וספרי יעץ", "ספרות עזר", CategoryMatchMode.Exact)
            val replaceBoth = { path: String ->
                override(path, DescriptionEdit.Replace("short"), DescriptionEdit.Replace("long"))
            }

            connection.autoCommit = false
            val unsettled = collectForDbRuleFailures(
                connection, listOf(rename), emptyList(), emptyList(), emptyList(), Logger.withTag("test"),
                listOf(replaceBoth("ספרות עזר")),
            )
            assertEquals(listOf("category_renames.csv"), unsettled.map { it.section })
            assertTrue(categoryExists(connection, 1), "the failed rule must be rolled back")

            val failures = collectForDbRuleFailures(
                connection, listOf(rename), emptyList(), emptyList(), emptyList(), Logger.withTag("test"),
                listOf(replaceBoth("ספרות עזר"), replaceBoth("ספרות עזר/מילונים")),
            )
            assertEquals(emptyList(), failures)
            assertEquals(listOf("ספרות עזר", "ספרות עזר/דקדוק", "ספרות עזר/מילונים"), allPaths(connection))
            assertEquals(12, bookCategory(connection, 100))
            connection.rollback()
        }
    }

    private fun override(
        path: String,
        shortEdit: DescriptionEdit = DescriptionEdit.Keep,
        longEdit: DescriptionEdit = DescriptionEdit.Keep,
    ) = CategoryDescriptionOverride(csvRecordNumber = 2, canonicalPath = path, shortEdit = shortEdit, longEdit = longEdit)

    private fun overridesOf(vararg overrides: CategoryDescriptionOverride) =
        overrides.associateBy { it.canonicalPath }

    /** Every category's path from the root, sorted; a duplicate path appears twice. */
    private fun allPaths(connection: Connection): List<String> {
        val rows = connection.createStatement().use { statement ->
            statement.executeQuery("SELECT id, parentId, title FROM category").use { result ->
                buildList {
                    while (result.next()) {
                        val parent = result.getLong(2).let { if (result.wasNull()) null else it }
                        add(Triple(result.getLong(1), parent, result.getString(3)))
                    }
                }
            }
        }
        val byId = rows.associateBy { it.first }
        fun path(id: Long): String {
            val (_, parent, title) = byId.getValue(id)
            return if (parent == null) title else path(parent) + "/" + title
        }
        return rows.map { path(it.first) }.sorted()
    }

    private fun withConnection(block: (Connection) -> Unit) {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    "CREATE TABLE category (" +
                        "id INTEGER PRIMARY KEY, parentId INTEGER, title TEXT NOT NULL, " +
                        "heShortDesc TEXT, heDesc TEXT)",
                )
                statement.execute(
                    "CREATE TABLE book (id INTEGER PRIMARY KEY, categoryId INTEGER NOT NULL)",
                )
            }
            block(connection)
        }
    }

    private fun insert(
        connection: Connection,
        id: Long,
        short: String?,
        long: String?,
        title: String = "category $id",
        parentId: Long? = null,
    ) {
        connection.prepareStatement(
            "INSERT INTO category (id,parentId,title,heShortDesc,heDesc) VALUES (?, ?, ?, ?, ?)",
        ).use { statement ->
            statement.setLong(1, id)
            if (parentId == null) statement.setNull(2, java.sql.Types.INTEGER) else statement.setLong(2, parentId)
            statement.setString(3, title)
            statement.setString(4, short)
            statement.setString(5, long)
            statement.executeUpdate()
        }
    }

    private fun insertBook(connection: Connection, id: Long, categoryId: Long) {
        connection.prepareStatement("INSERT INTO book (id,categoryId) VALUES (?, ?)").use { statement ->
            statement.setLong(1, id)
            statement.setLong(2, categoryId)
            statement.executeUpdate()
        }
    }

    private fun categoryExists(connection: Connection, id: Long): Boolean =
        queryLong(connection, "SELECT COUNT(*) FROM category WHERE id = ?", id) == 1L

    private fun categoryParent(connection: Connection, id: Long): Long? =
        queryLong(connection, "SELECT parentId FROM category WHERE id = ?", id)

    private fun bookCategory(connection: Connection, id: Long): Long? =
        queryLong(connection, "SELECT categoryId FROM book WHERE id = ?", id)

    private fun queryLong(connection: Connection, sql: String, id: Long): Long? =
        connection.prepareStatement(sql).use { statement ->
            statement.setLong(1, id)
            statement.executeQuery().use { result ->
                if (!result.next()) return@use null
                result.getLong(1).let { if (result.wasNull()) null else it }
            }
        }

    private fun read(connection: Connection, id: Long): Pair<String?, String?> =
        connection.prepareStatement(
            "SELECT heShortDesc, heDesc FROM category WHERE id = ?",
        ).use { statement ->
            statement.setLong(1, id)
            statement.executeQuery().use { result ->
                check(result.next())
                result.getString(1) to result.getString(2)
            }
        }
}
