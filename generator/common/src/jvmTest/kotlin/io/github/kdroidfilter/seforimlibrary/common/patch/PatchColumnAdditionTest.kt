package io.github.kdroidfilter.seforimlibrary.common.patch

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * A column added to the schema WITHOUT bumping `db_schema_version` (the
 * 2026-07-16 `category.heShortDesc` incident) used to kill the whole patch fan
 * with `no such column: prev.heShortDesc`, because the producer built its diff
 * predicate from every column of the NEW schema.
 *
 * The producer now emits an `ALTER TABLE … ADD COLUMN` migration for such a
 * column and ships every row whose new value is non-NULL. The migration is a
 * plain SQL row in the pre-existing `migrations` table, so an already-released
 * [PatchApplier] runs it verbatim before the upserts — no applier change and no
 * patch-format bump.
 */
class PatchColumnAdditionTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    // ── the real incident: a nullable column appears in new only ────────────

    @Test
    fun `nullable column added without a schema bump becomes an ADD COLUMN migration`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate(
                """
                INSERT INTO category(id, parentId, title, level, orderIndex) VALUES
                    (1, NULL, 'Tanakh', 0, 1),
                    (2, NULL, 'Mishnah', 0, 2),
                    (3, NULL, 'Talmud', 0, 3),
                    (4, NULL, 'Halakhah', 0, 4)
                """.trimIndent(),
            )
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    heShortDesc TEXT DEFAULT NULL"))
            st.executeUpdate(
                """
                INSERT INTO category(id, parentId, title, level, orderIndex, heShortDesc) VALUES
                    (1, NULL, 'Tanakh',   0, 1, 'תנ״ך'),
                    (2, NULL, 'Mishnah',  0, 2, 'משנה'),
                    (3, NULL, 'Talmud',   0, 3, 'תלמוד'),
                    (4, NULL, 'Halakhah', 0, 4, NULL),
                    (5, NULL, 'Kabbalah', 0, 5, NULL)
                """.trimIndent(),
            )
        }

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 9, toVersion = 25)

        // Exactly the migration the incident needed, and nothing else.
        assertEquals(
            listOf("""ALTER TABLE "category" ADD COLUMN "heShortDesc" TEXT"""),
            migrationsOf(patch),
        )
        // 3 rows differ only in the new column + 1 brand-new row. Row 4 is
        // identical and its new value is NULL, so ADD COLUMN alone covers it.
        assertEquals(4, produced.upsertCounts.getValue("category"))
        assertEquals(0, produced.deleteCounts.getValue("category"))

        assertAppliesTo(prev, next, patch)
    }

    @Test
    fun `no ALTER migration is emitted when the column sets already agree`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")
        val ddl = categoryDdl(extraColumns = ",\n    heShortDesc TEXT DEFAULT NULL")

        buildDb(prev) { st ->
            st.executeUpdate(ddl)
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, NULL)")
        }
        buildDb(next) { st ->
            st.executeUpdate(ddl)
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 'תנ״ך')")
        }

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        assertEquals(emptyList(), migrationsOf(patch))
        assertEquals(1, produced.upsertCounts.getValue("category"))
        assertAppliesTo(prev, next, patch)
    }

    // ── NOT NULL columns ────────────────────────────────────────────────────

    @Test
    fun `NOT NULL column with a declared default carries that default into the migration`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1), (2, NULL, 'Mishnah', 0, 2)")
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    weight INTEGER NOT NULL DEFAULT 7"))
            st.executeUpdate(
                "INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 7), (2, NULL, 'Mishnah', 0, 2, 42)",
            )
        }

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        assertEquals(
            listOf("""ALTER TABLE "category" ADD COLUMN "weight" INTEGER NOT NULL DEFAULT 7"""),
            migrationsOf(patch),
        )
        // ADD COLUMN back-fills every existing client row with 7, so only the
        // row that actually differs from that default has to ship.
        assertEquals(1, produced.upsertCounts.getValue("category"))
        assertAppliesTo(prev, next, patch)
    }

    @Test
    fun `NOT NULL column without a default gets a synthesised default plus a full snapshot`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1), (2, NULL, 'Mishnah', 0, 2)")
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    slug TEXT NOT NULL"))
            st.executeUpdate(
                "INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 'tanakh'), (2, NULL, 'Mishnah', 0, 2, 'mishnah')",
            )
        }

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        // SQLite refuses ADD COLUMN … NOT NULL without a default, so the
        // producer synthesises the type's neutral value; the accompanying full
        // snapshot overwrites it on every row so it can never survive.
        assertEquals(
            listOf("""ALTER TABLE "category" ADD COLUMN "slug" TEXT NOT NULL DEFAULT ''"""),
            migrationsOf(patch),
        )
        assertEquals(2, produced.upsertCounts.getValue("category"))
        assertAppliesTo(prev, next, patch)
        // …and no synthetic '' leaked through.
        DriverManager.getConnection("jdbc:sqlite:${next.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT COUNT(*) FROM category WHERE slug = ''").use { rs ->
                    rs.next()
                    assertEquals(0L, rs.getLong(1))
                }
            }
        }
    }

    @Test
    fun `v26 to v27 adds dhDisplay and ships a full line_dh snapshot`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(
                "CREATE TABLE line_dh (bookId INTEGER NOT NULL, dhText TEXT NOT NULL, " +
                    "lineIndex INTEGER NOT NULL, PRIMARY KEY (bookId, dhText, lineIndex)) WITHOUT ROWID",
            )
            st.executeUpdate("INSERT INTO line_dh VALUES (1, 'מאימתי קורין', 0), (1, 'משעה', 1)")
        }
        buildDb(next) { st ->
            st.executeUpdate(
                "CREATE TABLE line_dh (bookId INTEGER NOT NULL, dhText TEXT NOT NULL, " +
                    "lineIndex INTEGER NOT NULL, dhDisplay TEXT NOT NULL, " +
                    "PRIMARY KEY (bookId, dhText, lineIndex)) WITHOUT ROWID",
            )
            st.executeUpdate(
                "INSERT INTO line_dh VALUES " +
                    "(1, 'מאימתי קורין', 0, 'מאימתי קורין'), (1, 'משעה', 1, 'משעה שהכהנים')",
            )
        }

        val produced = PatchDbProducer().produce(
            prev,
            next,
            patch,
            fromVersion = 26,
            toVersion = 27,
            fromSchemaVersion = 4,
            toSchemaVersion = 5,
        )

        assertEquals(
            listOf("""ALTER TABLE "line_dh" ADD COLUMN "dhDisplay" TEXT NOT NULL DEFAULT ''"""),
            migrationsOf(patch),
        )
        assertEquals(2, produced.upsertCounts.getValue("line_dh"))
        assertAppliesTo(prev, next, patch) { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT dhDisplay FROM line_dh ORDER BY lineIndex").use { rs ->
                    assertTrue(rs.next())
                    assertEquals("מאימתי קורין", rs.getString(1))
                    assertTrue(rs.next())
                    assertEquals("משעה שהכהנים", rs.getString(1))
                }
            }
        }
    }

    @Test
    fun `a CURRENT_TIMESTAMP default is not constant so it is synthesised away`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1), (2, NULL, 'Mishnah', 0, 2)")
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    touchedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"))
            st.executeUpdate(
                "INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, '2026-07-16 00:00:00'), " +
                    "(2, NULL, 'Mishnah', 0, 2, '2026-09-04 00:00:00')",
            )
        }

        val produced = PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        // SQLite evaluates CURRENT_TIMESTAMP per row and rejects it in ADD
        // COLUMN, so it is treated exactly like a missing default.
        assertEquals(
            listOf("""ALTER TABLE "category" ADD COLUMN "touchedAt" TEXT NOT NULL DEFAULT ''"""),
            migrationsOf(patch),
        )
        assertEquals(2, produced.upsertCounts.getValue("category"))
        assertAppliesTo(prev, next, patch)
    }

    // ── indexes that arrive with the new column ─────────────────────────────

    @Test
    fun `a non-unique index on the added column ships as a CREATE INDEX migration`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("CREATE INDEX idx_category_order ON category(orderIndex)")
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1)")
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    heShortDesc TEXT DEFAULT NULL"))
            st.executeUpdate("CREATE INDEX idx_category_order ON category(orderIndex)")
            st.executeUpdate("CREATE INDEX idx_category_heshortdesc ON category(heShortDesc)")
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 'תנ״ך')")
        }

        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        // ADD COLUMN first, then the index over it — migrations run in version order.
        assertEquals(
            listOf(
                """ALTER TABLE "category" ADD COLUMN "heShortDesc" TEXT""",
                "CREATE INDEX idx_category_heshortdesc ON category(heShortDesc)",
            ),
            migrationsOf(patch),
        )
        // The logical hash ignores indexes, so only sqlite_master proves this.
        assertAppliesTo(prev, next, patch) { conn ->
            assertEquals(
                listOf("idx_category_heshortdesc", "idx_category_order"),
                indexNames(conn, "category"),
            )
        }
    }

    @Test
    fun `a new UNIQUE index makes the anchor unpatchable`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1)")
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    slug TEXT"))
            st.executeUpdate("CREATE UNIQUE INDEX ux_category_slug ON category(slug)")
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 'tanakh')")
        }

        val ex = assertFailsWith<UnpatchableAnchorException> {
            PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        }
        assertEquals("category", ex.table)
        assertEquals(listOf("slug"), ex.columns)
        val msg = ex.message.orEmpty()
        assertTrue("UNIQUE index" in msg, "message must name the cause: $msg")
        assertTrue("migrations run before its upserts" in msg, "message must explain why: $msg")
        assertTrue("bump db_schema_version" in msg, "message must direct the operator: $msg")
    }

    @Test
    fun `a new table-level UNIQUE constraint makes the anchor unpatchable`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1)")
        }
        buildDb(next) { st ->
            // An inline UNIQUE is backed by an sqlite_autoindex_*, which is part
            // of CREATE TABLE and can never be added to an existing table.
            st.executeUpdate(categoryDdl(extraColumns = ",\n    slug TEXT UNIQUE"))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 'tanakh')")
        }

        val ex = assertFailsWith<UnpatchableAnchorException> {
            PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        }
        assertEquals("category", ex.table)
        assertEquals(listOf("slug"), ex.columns)
        assertTrue("UNIQUE constraint" in ex.message.orEmpty())
    }

    @Test
    fun `unique-collision guard skips groups prev lacks but still checks the rest`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        // `ux_topic_alt` keeps its NAME but moves onto the newly added column,
        // so the index name-diff sees nothing added and the anchor stays
        // patchable — the only way the guard's "column missing from prev" skip
        // is reachable. The (code) group must still be checked.
        buildDb(prev) { st ->
            st.executeUpdate(
                "CREATE TABLE topic (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, code TEXT NOT NULL)",
            )
            st.executeUpdate("CREATE UNIQUE INDEX ux_topic_code ON topic(code)")
            st.executeUpdate("CREATE UNIQUE INDEX ux_topic_alt ON topic(name)")
            st.executeUpdate("INSERT INTO topic VALUES (1, 'Shabbat', 'X')")
        }
        buildDb(next) { st ->
            st.executeUpdate(
                "CREATE TABLE topic (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL, " +
                    "code TEXT NOT NULL, slug TEXT)",
            )
            st.executeUpdate("CREATE UNIQUE INDEX ux_topic_code ON topic(code)")
            st.executeUpdate("CREATE UNIQUE INDEX ux_topic_alt ON topic(slug)")
            // Same natural key 'X' under a DIFFERENT id — the lineage break the
            // guard exists to catch.
            st.executeUpdate("INSERT INTO topic VALUES (2, 'Shabbat', 'X', 'shabbat')")
        }

        val ex = assertFailsWith<IllegalStateException> {
            PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        }
        val msg = ex.message.orEmpty()
        assertTrue(
            "Secondary UNIQUE collision detected in 'topic' on (code)" in msg,
            "the group prev still has must be checked: $msg",
        )
        // …and the (slug) group was skipped rather than blowing up on prev.slug.
        assertTrue("no such column" !in msg, "the missing-column group must be skipped: $msg")
    }

    // ── genuinely unpatchable anchors ───────────────────────────────────────

    @Test
    fun `missing primary key column makes the anchor unpatchable`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(
                """
                CREATE TABLE link_anchor (
                    linkId INTEGER NOT NULL,
                    side INTEGER NOT NULL,
                    label TEXT,
                    PRIMARY KEY (linkId, side)
                )
                """.trimIndent(),
            )
            st.executeUpdate("INSERT INTO link_anchor VALUES (1, 0, 'a')")
        }
        buildDb(next) { st ->
            st.executeUpdate(
                """
                CREATE TABLE link_anchor (
                    linkId INTEGER NOT NULL,
                    side INTEGER NOT NULL,
                    charStart INTEGER NOT NULL DEFAULT 0,
                    charEnd INTEGER,
                    label TEXT,
                    PRIMARY KEY (linkId, side, charStart)
                )
                """.trimIndent(),
            )
            st.executeUpdate("INSERT INTO link_anchor VALUES (1, 0, 5, 9, 'a')")
        }

        val ex = assertFailsWith<UnpatchableAnchorException> {
            PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        }
        assertEquals("link_anchor", ex.table)
        assertEquals(listOf("charStart"), ex.columns)
        val msg = ex.message.orEmpty()
        assertTrue("PRIMARY KEY" in msg, "message must name the cause: $msg")
        assertTrue("bump db_schema_version" in msg, "message must direct the operator: $msg")
        assertEquals(3, UnpatchableAnchorException.EXIT_CODE)
    }

    @Test
    fun `column dropped without a schema bump makes the anchor unpatchable`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    legacyNote TEXT"))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 'note')")
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1)")
        }

        val ex = assertFailsWith<UnpatchableAnchorException> {
            PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)
        }
        assertEquals("category", ex.table)
        assertEquals(listOf("legacyNote"), ex.columns)
        assertTrue("cannot drop a column" in ex.message.orEmpty())
    }

    // ── backward compatibility of the artefact ──────────────────────────────

    @Test
    fun `column migrations keep the patch format unchanged for old clients`() {
        val prev = newDbPath("prev.db")
        val next = newDbPath("next.db")
        val patch = newDbPath("patch.db")

        buildDb(prev) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ""))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1)")
        }
        buildDb(next) { st ->
            st.executeUpdate(categoryDdl(extraColumns = ",\n    heShortDesc TEXT DEFAULT NULL"))
            st.executeUpdate("INSERT INTO category VALUES (1, NULL, 'Tanakh', 0, 1, 'תנ״ך')")
        }
        PatchDbProducer().produce(prev, next, patch, fromVersion = 1, toVersion = 2)

        DriverManager.getConnection("jdbc:sqlite:${patch.toAbsolutePath()}").use { conn ->
            // No new patch_meta keys and no format bump: an already-released
            // applier accepts this patch exactly as it accepts today's.
            conn.createStatement().use { st ->
                st.executeQuery("SELECT value FROM patch_meta WHERE key='schema_version'").use { rs ->
                    rs.next()
                    assertEquals(PatchDbSchema.CURRENT_VERSION.toString(), rs.getString(1))
                }
            }
            assertEquals(
                listOf("blobs", "delete_category", "migrations", "patch_meta", "upsert_category"),
                conn.createStatement().use { st ->
                    st.executeQuery(
                        "SELECT name FROM sqlite_master WHERE type='table' " +
                            "AND name NOT LIKE 'sqlite_%' ORDER BY name",
                    ).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
                },
            )
        }
        // Every migration is plain SQL the existing runMigrations() executes as-is.
        assertTrue(migrationsOf(patch).all { it.startsWith("ALTER TABLE ") })
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private fun categoryDdl(extraColumns: String) =
        """
        CREATE TABLE category (
            id INTEGER PRIMARY KEY NOT NULL,
            parentId INTEGER,
            title TEXT NOT NULL,
            level INTEGER NOT NULL DEFAULT 0,
            orderIndex INTEGER NOT NULL DEFAULT 999$extraColumns
        )
        """.trimIndent()

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

    /** The e2e contract: prev + patch must hash exactly like new. */
    private fun assertAppliesTo(
        prev: Path,
        next: Path,
        patch: Path,
        afterApply: (Connection) -> Unit = {},
    ) {
        val target = tmp.newFolder().toPath().resolve("target.db")
        Files.copy(prev, target)
        DriverManager.getConnection("jdbc:sqlite:${target.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { it.execute("PRAGMA foreign_keys = ON") }
            PatchApplier().apply(conn, patch)
            assertEquals(logicalHash(next), LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).compute(conn))
            afterApply(conn)
        }
    }

    /** Names of the explicitly-created (non-auto) indexes on [table]. */
    private fun indexNames(conn: Connection, table: String): List<String> =
        conn.createStatement().use { st ->
            st.executeQuery(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='$table' " +
                    "AND sql IS NOT NULL ORDER BY name",
            ).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } }
        }

    private fun logicalHash(path: Path): String =
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use(::hash)

    private fun hash(conn: Connection): String = LogicalContentHasher.forSchemaVersion(CURRENT_DB_SCHEMA_VERSION).compute(conn)
}
