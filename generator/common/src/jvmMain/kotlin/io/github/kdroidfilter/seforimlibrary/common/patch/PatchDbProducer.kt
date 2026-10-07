package io.github.kdroidfilter.seforimlibrary.common.patch

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.db.LineContentCompression
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.Connection
import java.sql.DriverManager

/**
 * Produces a `patch.db` from two seforim.db snapshots (previous + current).
 *
 * For each table in the signed `toSchemaVersion` contract:
 *   1. Reads the column list + PK from the **new** DB.
 *   2. Creates `upsert_<table>` mirroring those columns + PK.
 *   3. Inserts every row of `new.<table>` that either isn't in `prev.<table>`
 *      (joined on the PK) or differs from `prev` on any non-PK column.
 *   4. Creates `delete_<table>` with just the PK columns and inserts every
 *      `(pk…)` tuple present in `prev` but missing from `new`.
 *
 * See `DELTA_UPDATE_PLAN.md` §6.6.
 *
 * A table promoted between schema contracts is rebuilt from the target DB's
 * full snapshot even when it already exists physically in the previous full
 * DB. Its unsigned physical state was never guaranteed to exist on clients
 * that reached that release through an older-schema patch.
 *
 * **Columns added without a schema bump.** `db_schema_version` guards the
 * *table* contract, not the column list, so a column can appear in new while
 * prev still lacks it (`category.heShortDesc`, 2026-07-16). The producer then
 * emits an `ALTER TABLE "<t>" ADD COLUMN "<c>" <type> [NOT NULL DEFAULT <d>]`
 * migration and widens the upsert predicate with `new."<c>" IS NOT <back-fill>`
 * (the migration's own default, `NULL` when it has none), so the anchor stays
 * patchable. Migrations are plain SQL rows in the existing
 * `migrations` table, which every released `PatchApplier` already executes
 * verbatim before the upserts — no applier change and no patch-format bump.
 * Non-unique indexes the drifted table gained ship as plain `CREATE INDEX`
 * alongside. A missing PRIMARY KEY column, a column dropped without a bump, or
 * a newly UNIQUE index/constraint makes an anchor genuinely unpatchable; that
 * raises [UnpatchableAnchorException].
 *
 * **Indexes removed from the schema.** An explicit index that prev has and new
 * lacks ships as `DROP INDEX IF EXISTS main."<name>"` through the same
 * `migrations` channel, so patch-updated clients converge with full downloads.
 */
class PatchDbProducer(
    private val logger: Logger = Logger.withTag("PatchDbProducer"),
) {

    data class Output(
        val path: Path,
        val fromVersion: Int,
        val toVersion: Int,
        val upsertCounts: Map<String, Int>,
        val deleteCounts: Map<String, Int>,
    )

    fun produce(
        prevDb: Path,
        newDb: Path,
        outputPath: Path,
        fromVersion: Int,
        toVersion: Int,
        migrations: List<Pair<Int, String>> = emptyList(),
        fromSchemaVersion: Int = CURRENT_DB_SCHEMA_VERSION,
        toSchemaVersion: Int = CURRENT_DB_SCHEMA_VERSION,
    ): Output {
        require(fromSchemaVersion <= toSchemaVersion) {
            "Schema downgrade $fromSchemaVersion -> $toSchemaVersion is not supported"
        }
        if (requiresFullRebase(fromSchemaVersion, toSchemaVersion)) {
            throw UnpatchableAnchorException(
                table = "line",
                columns = listOf("content"),
                message = "schema $fromSchemaVersion -> $toSchemaVersion moves line text into line_content; " +
                    "a delta cannot drop line.content, so this transition is a full rebase only",
            )
        }
        val fromTables = patchTablesForSchemaVersion(fromSchemaVersion)
        val targetTables = patchTablesForSchemaVersion(toSchemaVersion)
        val fromTableNames = fromTables.mapTo(HashSet()) { it.name }
        val promotedTables = targetTables.mapTo(HashSet()) { it.name } - fromTableNames

        Files.createDirectories(outputPath.toAbsolutePath().parent)
        val tmp = outputPath.resolveSibling("${outputPath.fileName}.tmp")
        if (Files.exists(tmp)) Files.delete(tmp)

        Class.forName("org.sqlite.JDBC")
        val upsertCounts = LinkedHashMap<String, Int>()
        val deleteCounts = LinkedHashMap<String, Int>()

        DriverManager.getConnection("jdbc:sqlite:${tmp.toAbsolutePath()}").use { conn ->
            conn.autoCommit = false
            applyBaseDdl(conn)
            // patch_meta.schema_version describes the patch artifact format
            // understood by PatchApplier. It is deliberately independent of
            // the target DB's logical schema version carried by the release
            // manifest (and may therefore be 4 for a 2 -> 3 DB transition).
            writeMetadata(conn, fromVersion, toVersion)
            attach(conn, "prev", prevDb)
            attach(conn, "new", newDb)
            requireSameLineContentDictionary(conn)
            val nextMigrationVersion = (migrations.maxOfOrNull { it.first } ?: 0) + 1
            val createTableMigrations = inferCreateTableMigrations(
                conn = conn,
                firstVersion = nextMigrationVersion,
                targetTables = targetTables,
                promotedTables = promotedTables,
            )
            // A column added to the new schema without a db_schema_version
            // bump (the 2026-07-16 category.heShortDesc incident) leaves prev
            // without that column. Ship an ALTER TABLE … ADD COLUMN migration
            // for it — plain SQL that even an old PatchApplier executes as-is —
            // instead of failing the anchor with "no such column: prev.<c>".
            val columnPlans = planColumnMigrations(conn, targetTables, promotedTables)
            val addColumnMigrations = buildAddColumnMigrations(
                firstVersion = (createTableMigrations.maxOfOrNull { it.first } ?: (nextMigrationVersion - 1)) + 1,
                plans = columnPlans,
            )
            val preDropMigrations = migrations + createTableMigrations + addColumnMigrations
            var dropVersion = (preDropMigrations.maxOfOrNull { it.first } ?: 0) + 1
            val dropIndexMigrations = planDroppedIndexMigrations(conn, targetTables)
                .map { dropVersion++ to it }
            writeMigrations(conn, preDropMigrations + dropIndexMigrations)
            if (dropIndexMigrations.isNotEmpty()) {
                logger.i { "Emitting ${dropIndexMigrations.size} DROP INDEX migration(s)" }
            }
            for ((name, plan) in columnPlans) {
                logger.i {
                    "Table '$name': prev lacks ${plan.missingColumns} — emitting ADD COLUMN migration(s)" +
                        if (plan.forceFullSnapshot) " and shipping a full snapshot (synthesised NOT NULL default)" else ""
                }
            }

            // Materialise upsert_/delete_ tables based on the new DB's actual
            // schema. The producer is generic — every table in our config list
            // is processed identically.
            for (table in targetTables) {
                if (!tableExists(conn, "new", table.name)) continue
                PatchDbSchema.createUpsertTable(conn, "new", table)
                PatchDbSchema.createDeleteTable(conn, "new", table)
            }

            for (table in targetTables) {
                val plan = columnPlans[table.name]
                upsertCounts[table.name] = scanUpserts(
                    conn,
                    table,
                    forceFullSnapshot = table.name in promotedTables || plan?.forceFullSnapshot == true,
                    missingInPrev = plan?.missing.orEmpty(),
                )
            }
            for (table in targetTables) {
                deleteCounts[table.name] = scanDeletes(
                    conn,
                    table,
                    ignorePrevious = table.name in promotedTables,
                )
            }

            // Fail fast on secondary-UNIQUE collisions: catches the case
            // where prev and new were generated from different build_state.db
            // lineages (e.g. same `topic.name` allocated under different ids),
            // which would otherwise blow up mid-transaction in the applier
            // with an opaque "UNIQUE constraint failed" error.
            assertNoSecondaryUniqueCollisions(
                conn,
                targetTables,
                missingInPrev = columnPlans.mapValues { (_, plan) -> plan.missingColumns.toSet() },
            )

            // Delta clients would otherwise never get the statistics a full
            // download carries (AnalyzeDbCli), and some app queries run ~10x slower.
            val statRows = PatchDbSchema.writeStat1Snapshot(conn, "new")
            if (statRows > 0) logger.i { "Shipping $statRows sqlite_stat1 row(s) as ${PatchDbSchema.STAT1_SNAPSHOT_TABLE}" }

            // Outside the schema contract: every patch replaces these tables whole.
            val optionalRows = PatchDbSchema.writeOptionalTables(conn, "new")
            if (optionalRows.isNotEmpty()) logger.i { "Shipping optional table snapshot(s): $optionalRows" }

            // Commit BEFORE detach so SQLite isn't holding locks on the
            // attached DBs through an open transaction.
            conn.commit()
            conn.autoCommit = true
            detach(conn, "new")
            detach(conn, "prev")
            conn.createStatement().use { it.execute("VACUUM") }
        }

        Files.move(tmp, outputPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        val totalUpserts = upsertCounts.values.sum()
        val totalDeletes = deleteCounts.values.sum()
        logger.i {
            "Produced patch.db at $outputPath — upserts=$totalUpserts, deletes=$totalDeletes " +
                "(from v$fromVersion to v$toVersion)"
        }
        return Output(outputPath, fromVersion, toVersion, upsertCounts, deleteCounts)
    }

    private fun applyBaseDdl(conn: Connection) {
        conn.createStatement().use { st ->
            PatchDbSchema.baseStatements.forEach { st.executeUpdate(it) }
        }
    }

    private fun writeMetadata(conn: Connection, from: Int, to: Int) {
        conn.prepareStatement("INSERT INTO patch_meta(key, value) VALUES (?, ?)").use { ps ->
            listOf(
                "schema_version" to PatchDbSchema.CURRENT_VERSION.toString(),
                "from_version" to from.toString(),
                "to_version" to to.toString(),
                "generated_at" to java.time.Instant.now().toString(),
            ).forEach { (k, v) -> ps.setString(1, k); ps.setString(2, v); ps.executeUpdate() }
        }
    }

    private fun writeMigrations(conn: Connection, migrations: List<Pair<Int, String>>) {
        if (migrations.isEmpty()) return
        conn.prepareStatement("INSERT INTO migrations(version, sql) VALUES (?, ?)").use { ps ->
            for ((v, sql) in migrations) {
                ps.setInt(1, v); ps.setString(2, sql); ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun inferCreateTableMigrations(
        conn: Connection,
        firstVersion: Int,
        targetTables: List<PatchTable>,
        promotedTables: Set<String>,
    ): List<Pair<Int, String>> {
        val out = ArrayList<Pair<Int, String>>()
        var version = firstVersion
        for (table in targetTables) {
            if (!tableExists(conn, "new", table.name)) continue
            val promoted = table.name in promotedTables
            if (!promoted && tableExists(conn, "prev", table.name)) continue

            // A table that existed physically in a previous DB but was not in
            // its signed schema contract may be absent (or carry arbitrary
            // stale rows) on clients that reached that release by delta. Reset
            // it and ship a full snapshot so both client shapes converge.
            if (promoted) out += version++ to "DROP TABLE IF EXISTS \"${table.name}\""

            readCreateSql(conn, "new", "table", table.name)?.let { sql ->
                out += version++ to sql
            }
            readIndexSqlForTable(conn, "new", table.name).forEach { sql ->
                out += version++ to sql
            }
        }
        return out
    }

    /**
     * A column new has and prev lacks, plus the value `ADD COLUMN` back-fills
     * into prev's existing rows — the SQL literal from the emitted migration,
     * or `null` for NULL (a nullable column is always added without a
     * DEFAULT). Rows whose new value equals [backfill] need no upsert.
     */
    private data class MissingColumn(val name: String, val backfill: String?) {
        val backfillLiteral: String get() = backfill ?: "NULL"
    }

    /**
     * What has to happen to one table whose physical column set drifted
     * between prev and new without a `db_schema_version` bump.
     *
     *  - [missing]     columns new has and prev lacks, in new's order.
     *  - [statements]  the `ALTER TABLE … ADD COLUMN` migrations to ship.
     *  - [forceFullSnapshot] set when a NOT NULL column had to be added with a
     *    *synthesised* neutral default (the new schema declares NOT NULL but
     *    no usable constant DEFAULT). Every row of the table is then shipped so
     *    the synthetic value is overwritten everywhere and cannot leak.
     */
    private data class ColumnPlan(
        val missing: List<MissingColumn>,
        val statements: List<String>,
        val forceFullSnapshot: Boolean,
    ) {
        val missingColumns: List<String> get() = missing.map { it.name }
    }

    /**
     * Diffs prev's physical columns against new's for every target table that
     * exists on both sides, and decides how to reconcile them.
     *
     * Throws [UnpatchableAnchorException] for the two shapes no patch can
     * express: a missing PRIMARY KEY column, and a column dropped from the new
     * schema without a bump.
     */
    private fun planColumnMigrations(
        conn: Connection,
        targetTables: List<PatchTable>,
        promotedTables: Set<String>,
    ): Map<String, ColumnPlan> {
        val out = LinkedHashMap<String, ColumnPlan>()
        for (table in targetTables) {
            if (!tableExists(conn, "new", table.name)) continue
            // Promoted / absent-in-prev tables are (re)created by
            // inferCreateTableMigrations and shipped as a full snapshot;
            // there is no prev column set to reconcile.
            if (table.name in promotedTables) continue
            if (!tableExists(conn, "prev", table.name)) continue

            val prevCols = PatchDbSchema.readTableInfo(conn, "prev", table.name)
            val newCols = PatchDbSchema.readTableInfo(conn, "new", table.name)
            val prevNames = prevCols.mapTo(LinkedHashSet()) { it.name }
            val newNames = newCols.mapTo(LinkedHashSet()) { it.name }

            val dropped = prevCols.map { it.name }.filter { it !in newNames }
            if (dropped.isNotEmpty()) {
                throw UnpatchableAnchorException(
                    table = table.name,
                    columns = dropped,
                    message = "Table '${table.name}' in the previous DB carries column(s) " +
                        "${dropped.joinToString(", ")} that the new schema no longer declares. " +
                        "A patch cannot drop a column, so this anchor is not patchable — " +
                        "bump db_schema_version when removing a column.",
                )
            }

            val missing = newCols.filter { it.name !in prevNames }
            if (missing.isEmpty()) continue

            val missingPk = missing.map { it.name }.filter { it in table.primaryKey }
            if (missingPk.isNotEmpty()) {
                throw UnpatchableAnchorException(
                    table = table.name,
                    columns = missingPk,
                    message = "Table '${table.name}' in the previous DB lacks PRIMARY KEY column(s) " +
                        "${missingPk.joinToString(", ")}. ADD COLUMN cannot extend a primary key and the " +
                        "upsert/delete join key does not exist in prev, so this anchor is not patchable — " +
                        "bump db_schema_version when changing a primary key.",
                )
            }

            var forceFullSnapshot = false
            val statements = ArrayList<String>(missing.size)
            val planned = ArrayList<MissingColumn>(missing.size)
            for (col in missing) {
                // SQLite only accepts a CONSTANT default in ADD COLUMN, so a
                // parenthesised expression default and the CURRENT_* time
                // keywords are both unusable here.
                val declared = col.defaultValue?.takeUnless { it.isNonConstantDefault() }
                val backfill: String?
                val suffix: String
                when {
                    // A nullable column MUST be added without a DEFAULT: SQLite
                    // back-fills existing rows with it, and rows whose new value
                    // is NULL are (correctly) not shipped, so any non-NULL
                    // default would survive and break the logical hash.
                    !col.notNull -> {
                        backfill = null
                        suffix = ""
                    }
                    declared != null -> {
                        backfill = declared
                        suffix = " NOT NULL DEFAULT $declared"
                    }
                    else -> {
                        // SQLite refuses `ADD COLUMN … NOT NULL` without a
                        // default. Synthesise the type's neutral value and pay
                        // for it with a full snapshot of the table.
                        forceFullSnapshot = true
                        backfill = null
                        suffix = " NOT NULL DEFAULT ${neutralDefaultFor(col.type)}"
                    }
                }
                statements += "ALTER TABLE \"${table.name}\" ADD COLUMN \"${col.name}\" ${col.type}$suffix"
                planned += MissingColumn(col.name, backfill)
            }
            // A new column usually arrives with an index. inferCreateTableMigrations
            // only ships indexes for tables it (re)creates, so without this the
            // client would get the column and never its index — invisible to
            // LogicalContentHasher, which compares data only.
            statements += planIndexMigrations(conn, table)
            out[table.name] = ColumnPlan(
                missing = planned,
                statements = statements,
                forceFullSnapshot = forceFullSnapshot,
            )
        }
        return out
    }

    private fun buildAddColumnMigrations(
        firstVersion: Int,
        plans: Map<String, ColumnPlan>,
    ): List<Pair<Int, String>> {
        var version = firstVersion
        return plans.values.flatMap { it.statements }.map { version++ to it }
    }

    /** `PRAGMA index_list` + `index_info` + the index's own DDL, for one table. */
    private data class IndexInfo(
        val name: String,
        val unique: Boolean,
        /** `c` = CREATE INDEX, `u` = UNIQUE table constraint, `pk` = primary key. */
        val origin: String,
        val columns: List<String>,
        val sql: String?,
    )

    /**
     * Ships the indexes a drifted table gained along with its new column(s).
     *
     * Only called for tables that already have an [ColumnPlan] (i.e. whose
     * columns drifted); a table whose columns did not change keeps the previous
     * behaviour, since a purely-new index there is a hash-invisible difference
     * that has never been patched and would add an unbounded CREATE INDEX cost
     * to every client apply.
     *
     * A **UNIQUE** index is refused rather than shipped. Migrations run BEFORE
     * the upserts, so the index would be built over rows that still carry their
     * pre-patch values (a freshly added column holds nothing but the migration's
     * own back-fill on every row) — it would either fail outright or constrain
     * data the patch has not written yet. Shipping a full snapshot does not help:
     * the ordering is fixed by the patch format. Such an anchor is therefore not
     * patchable and the release fan skips it.
     */
    private fun planIndexMigrations(conn: Connection, table: PatchTable): List<String> {
        val prevIndexes = readIndexes(conn, "prev", table.name)
        val newIndexes = readIndexes(conn, "new", table.name)

        // Table-level UNIQUE constraints live in CREATE TABLE and are backed by
        // an `sqlite_autoindex_<table>_<n>` whose name is positional, so they
        // are diffed by column set rather than by name.
        val prevConstraintGroups = prevIndexes
            .filter { it.origin != "c" }
            .mapTo(HashSet()) { it.columns.toSet() }
        val addedConstraints = newIndexes.filter {
            it.origin == "u" && it.columns.toSet() !in prevConstraintGroups
        }
        if (addedConstraints.isNotEmpty()) {
            val cols = addedConstraints.flatMap { it.columns }.distinct()
            throw UnpatchableAnchorException(
                table = table.name,
                columns = cols,
                message = "Table '${table.name}' gained a UNIQUE constraint on " +
                    "(${cols.joinToString(", ")}) that the previous DB does not have. " +
                    "A table-level constraint is part of CREATE TABLE and cannot be added by a " +
                    "patch, so this anchor is not patchable — bump db_schema_version when adding " +
                    "a UNIQUE constraint.",
            )
        }

        val prevNames = prevIndexes.mapTo(HashSet()) { it.name }
        val created = newIndexes.filter { it.origin == "c" && it.name !in prevNames }
        val addedUnique = created.filter { it.unique }
        if (addedUnique.isNotEmpty()) {
            throw UnpatchableAnchorException(
                table = table.name,
                columns = addedUnique.flatMap { it.columns }.distinct(),
                message = "Table '${table.name}' gained UNIQUE index(es) " +
                    addedUnique.joinToString(", ") { "${it.name}(${it.columns.joinToString(", ")})" } +
                    " that the previous DB does not have. A patch's migrations run before its " +
                    "upserts, so the index would be built over rows still holding their pre-patch " +
                    "values — this anchor is not patchable; bump db_schema_version when adding a " +
                    "UNIQUE index.",
            )
        }
        return created.mapNotNull { it.sql }
    }

    /**
     * Explicit indexes that prev has and new lacks. The drop is idempotent, so a
     * client that never had the index (full download) runs a no-op.
     */
    private fun planDroppedIndexMigrations(conn: Connection, targetTables: List<PatchTable>): List<String> =
        targetTables
            .filter { tableExists(conn, "prev", it.name) && tableExists(conn, "new", it.name) }
            .flatMap { table ->
                val newNames = readIndexes(conn, "new", table.name).mapTo(HashSet()) { it.name }
                readIndexes(conn, "prev", table.name)
                    .filter { it.origin == "c" && it.name !in newNames }
                    .map { "DROP INDEX IF EXISTS main.\"${it.name.replace("\"", "\"\"")}\"" }
            }

    private fun readIndexes(conn: Connection, schemaAlias: String, table: String): List<IndexInfo> {
        data class ListRow(val name: String, val unique: Boolean, val origin: String)
        val rows = ArrayList<ListRow>()
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA $schemaAlias.index_list(\"$table\")").use { rs ->
                while (rs.next()) {
                    rows += ListRow(
                        name = rs.getString("name"),
                        unique = rs.getInt("unique") == 1,
                        origin = rs.getString("origin") ?: "c",
                    )
                }
            }
        }
        return rows.map { row ->
            val cols = ArrayList<String>()
            conn.createStatement().use { st ->
                st.executeQuery("PRAGMA $schemaAlias.index_info(\"${row.name}\")").use { rs ->
                    // `name` is NULL for an expression term of an index.
                    while (rs.next()) cols += rs.getString("name") ?: "<expr>"
                }
            }
            IndexInfo(
                name = row.name,
                unique = row.unique,
                origin = row.origin,
                columns = cols,
                sql = readCreateSql(conn, schemaAlias, "index", row.name),
            )
        }
    }

    /**
     * `true` for a `dflt_value` SQLite would refuse in `ADD COLUMN`: an explicit
     * NULL, a parenthesised expression, or one of the CURRENT_* time keywords.
     */
    private fun String.isNonConstantDefault(): Boolean {
        val v = trim()
        return v.equals("NULL", ignoreCase = true) ||
            v.startsWith("(") ||
            v.uppercase() in NON_CONSTANT_DEFAULT_KEYWORDS
    }

    /**
     * Neutral literal for a column's declared type, per SQLite's affinity
     * rules — the value a synthesised `NOT NULL DEFAULT` uses. It is always
     * overwritten by the full snapshot that accompanies it.
     */
    private fun neutralDefaultFor(declaredType: String): String {
        val t = declaredType.uppercase()
        return when {
            t.contains("INT") -> "0"
            t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") -> "''"
            t.contains("BLOB") -> "x''"
            t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") -> "0.0"
            else -> "0" // NUMERIC affinity
        }
    }

    private fun readCreateSql(conn: Connection, schemaAlias: String, type: String, name: String): String? {
        conn.prepareStatement(
            "SELECT sql FROM $schemaAlias.sqlite_master WHERE type=? AND name=? AND sql IS NOT NULL",
        ).use { ps ->
            ps.setString(1, type)
            ps.setString(2, name)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getString(1) else null
            }
        }
    }

    private fun readIndexSqlForTable(conn: Connection, schemaAlias: String, table: String): List<String> {
        val out = ArrayList<String>()
        conn.prepareStatement(
            """
            SELECT sql
            FROM $schemaAlias.sqlite_master
            WHERE type='index' AND tbl_name=? AND sql IS NOT NULL
            ORDER BY name
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, table)
            ps.executeQuery().use { rs ->
                while (rs.next()) out += rs.getString(1)
            }
        }
        return out
    }

    private fun attach(conn: Connection, alias: String, path: Path) {
        conn.prepareStatement("ATTACH DATABASE ? AS $alias").use { ps ->
            ps.setString(1, path.toAbsolutePath().toString())
            ps.executeUpdate()
        }
    }

    private fun detach(conn: Connection, alias: String) {
        conn.createStatement().use { it.execute("DETACH DATABASE $alias") }
    }

    private fun scanUpserts(
        conn: Connection,
        table: PatchTable,
        forceFullSnapshot: Boolean = false,
        missingInPrev: List<MissingColumn> = emptyList(),
    ): Int {
        val cols = PatchDbSchema.readTableInfo(conn, "new", table.name).map { it.name }
        if (cols.isEmpty()) return 0
        val colsCsv = cols.joinToString(",") { "\"$it\"" }
        if (forceFullSnapshot || !tableExists(conn, "prev", table.name)) {
            val sql = """
                INSERT INTO "upsert_${table.name}" ($colsCsv)
                SELECT $colsCsv
                FROM new."${table.name}"
            """.trimIndent()
            return conn.createStatement().use { it.executeUpdate(sql) }
        }
        val joinCond = table.primaryKey.joinToString(" AND ") { "new.\"$it\" = prev.\"$it\"" }
        // Columns prev does not have yet (added without a schema bump, see
        // planColumnMigrations) cannot be referenced as prev."c" at all. The
        // ADD COLUMN migration back-fills every existing client row with the
        // migration's own default (NULL when there is none), so we ship exactly
        // the rows whose NEW value differs from that back-fill.
        val missingSet = missingInPrev.mapTo(HashSet()) { it.name }
        val comparableNonPk = cols.filter { it !in table.primaryKey && it !in missingSet }
        // Use SQLite's `IS NOT` so NULL is treated as a distinct value from
        // '' and from any other column value. The previous COALESCE-based
        // comparison conflated NULL and empty string, silently dropping
        // upserts where a column toggled between NULL and '' (or vice
        // versa) — caught by the real-data v1→v2 e2e on book.heShortDesc.
        val diffTerms = comparableNonPk.map { "new.\"$it\" IS NOT prev.\"$it\"" } +
            missingInPrev.map { "new.\"${it.name}\" IS NOT ${it.backfillLiteral}" }
        val diffPredicate = if (diffTerms.isEmpty()) "FALSE" else diffTerms.joinToString(" OR ")
        // First PK column is enough to detect "prev row absent".
        val firstPk = table.primaryKey.first()
        val sql = """
            INSERT INTO "upsert_${table.name}" ($colsCsv)
            SELECT ${cols.joinToString(",") { "new.\"$it\"" }}
            FROM new."${table.name}" AS new
            LEFT JOIN prev."${table.name}" AS prev ON $joinCond
            WHERE prev."$firstPk" IS NULL OR ($diffPredicate)
        """.trimIndent()
        return conn.createStatement().use { it.executeUpdate(sql) }
    }

    private fun scanDeletes(conn: Connection, table: PatchTable, ignorePrevious: Boolean = false): Int {
        if (table.primaryKey.isEmpty()) return 0
        if (ignorePrevious) return 0
        if (!tableExists(conn, "prev", table.name)) return 0
        val pkCsv = table.primaryKey.joinToString(",") { "\"$it\"" }
        val joinCond = table.primaryKey.joinToString(" AND ") { "new.\"$it\" = prev.\"$it\"" }
        val firstPk = table.primaryKey.first()
        val sql = """
            INSERT INTO "delete_${table.name}" ($pkCsv)
            SELECT ${table.primaryKey.joinToString(",") { "prev.\"$it\"" }}
            FROM prev."${table.name}" AS prev
            LEFT JOIN new."${table.name}" AS new ON $joinCond
            WHERE new."$firstPk" IS NULL
        """.trimIndent()
        return conn.createStatement().use { it.executeUpdate(sql) }
    }

    /**
     * For every patch table that has at least one secondary UNIQUE index
     * (i.e. a UNIQUE on a non-PK column set), verify that no row in
     * `upsert_<table>` would collide with an existing row in `prev.<table>`
     * on those unique columns at a different primary-key value.
     *
     * Such collisions are almost always caused by feeding the producer two
     * `seforim.db` files that didn't share an `IdAllocator` lineage —
     * surfacing them here gives the operator a clear, actionable error
     * instead of a mid-transaction crash in [PatchApplier].
     */
    private fun assertNoSecondaryUniqueCollisions(
        conn: Connection,
        tables: List<PatchTable>,
        missingInPrev: Map<String, Set<String>> = emptyMap(),
    ) {
        for (table in tables) {
            if (!tableExists(conn, "new", table.name)) continue
            if (!tableExists(conn, "prev", table.name)) continue
            if (!tableExists(conn, "main", "upsert_${table.name}")) continue
            val pkCols = table.primaryKey
            if (pkCols.isEmpty()) continue
            val missingCols = missingInPrev[table.name].orEmpty()
            val uniqueGroups = readSecondaryUniqueGroups(conn, "new", table.name, pkCols.toSet())
            for (uniqueCols in uniqueGroups) {
                // A unique column prev does not have yet cannot collide with
                // anything there, and prev."c" would not even parse. Reached
                // only when prev carries an index of the SAME NAME over other
                // columns (so planIndexMigrations saw nothing added); a plainly
                // new UNIQUE index has already made the anchor unpatchable.
                // Other unique groups on the table are still checked.
                if (uniqueCols.any { it in missingCols }) continue
                val firstUnique = uniqueCols.first()
                val joinUnique = uniqueCols.joinToString(" AND ") { "new.\"$it\" = prev.\"$it\"" }
                val pkDiffers = pkCols.joinToString(" OR ") {
                    "new.\"$it\" IS NOT prev.\"$it\""
                }
                val selectCols = (pkCols + uniqueCols).joinToString(",") { "new.\"$it\" AS new_$it" } +
                    "," + pkCols.joinToString(",") { "prev.\"$it\" AS prev_$it" }
                val sql = """
                    SELECT $selectCols
                    FROM main."upsert_${table.name}" AS new
                    JOIN prev."${table.name}" AS prev ON $joinUnique
                    WHERE prev."$firstUnique" IS NOT NULL AND ($pkDiffers)
                    LIMIT 1
                """.trimIndent()
                conn.createStatement().use { st ->
                    st.executeQuery(sql).use { rs ->
                        if (rs.next()) {
                            val meta = rs.metaData
                            val sample = (1..meta.columnCount).joinToString(", ") {
                                "${meta.getColumnLabel(it)}=${rs.getObject(it)}"
                            }
                            error(
                                "Secondary UNIQUE collision detected in '${table.name}' on " +
                                    "(${uniqueCols.joinToString(", ")}): a row exists in prev " +
                                    "with a different PK than the row being upserted. " +
                                    "This usually means prev and new were generated from " +
                                    "different build_state.db lineages. Sample: $sample",
                            )
                        }
                    }
                }
            }
        }
    }

    /**
     * Reads `PRAGMA index_list` / `PRAGMA index_info` for a table and
     * returns the column-name sets of every secondary (non-PK) UNIQUE
     * index. Auto-indexes named `sqlite_autoindex_<table>_1` that back
     * the PRIMARY KEY are filtered out.
     */
    private fun readSecondaryUniqueGroups(
        conn: Connection,
        schemaAlias: String,
        table: String,
        pkColSet: Set<String>,
    ): List<List<String>> {
        val out = ArrayList<List<String>>()
        val indexNames = ArrayList<String>()
        conn.createStatement().use { st ->
            st.executeQuery("PRAGMA $schemaAlias.index_list(\"$table\")").use { rs ->
                while (rs.next()) {
                    val unique = rs.getInt("unique") == 1
                    if (unique) indexNames += rs.getString("name")
                }
            }
        }
        for (idx in indexNames) {
            val cols = ArrayList<String>()
            conn.createStatement().use { st ->
                st.executeQuery("PRAGMA $schemaAlias.index_info(\"$idx\")").use { rs ->
                    while (rs.next()) cols += rs.getString("name")
                }
            }
            if (cols.isEmpty()) continue
            // Skip the index that backs the primary key.
            if (cols.toSet() == pkColSet) continue
            out += cols
        }
        return out
    }

    /**
     * `zstd_dict` never changes through a patch: a client can only decode upserted rows
     * made with the dictionary it already holds, so any change of it is a full rebase.
     */
    private fun requireSameLineContentDictionary(conn: Connection) {
        val prev = LineContentCompression.storedDictionaries(conn, "prev")
        val new = LineContentCompression.storedDictionaries(conn, "new")
        val same = prev.keys == new.keys && prev.all { (id, dict) -> dict.contentEquals(new.getValue(id)) }
        if (!same) {
            throw UnpatchableAnchorException(
                table = LineContentCompression.DICT_TABLE,
                columns = listOf("dict"),
                message = "line text dictionary changed (prev ${prev.keys}, new ${new.keys}); " +
                    "rows compressed with it cannot reach clients that hold the old one",
            )
        }
    }

    private fun tableExists(conn: Connection, schema: String, name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM $schema.sqlite_master WHERE type='table' AND name=?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }

    companion object {
        // Kept for backwards-compat with callers (and the docs reference it).
        val TABLES_IN_FK_ORDER: List<String> = PATCH_TABLES_IN_FK_ORDER.map { it.name }

        /** Defaults SQLite evaluates per row — never valid in `ADD COLUMN`. */
        private val NON_CONSTANT_DEFAULT_KEYWORDS =
            setOf("CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME")
    }
}
