package io.github.kdroidfilter.seforimlibrary.common.patch

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Contract guard against the Dart `seforim_library_updater` package.
 *
 * Serializes [PATCH_TABLES_IN_FK_ORDER], [LogicalContentHasher.DEFAULT_TABLES]
 * and [CURRENT_DB_SCHEMA_VERSION] to a canonical JSON form and compares it
 * to a committed fixture. The identical fixture lives in the updater repo
 * (`test/patch_tables_contract.json`), where its own test asserts the Dart
 * lists produce byte-identical output — so the two table specs cannot drift.
 *
 * Canonical rules: fixed key order, FK/hash order preserved (no sorting),
 * UTF-8, trailing newline.
 */
class PatchTablesContractTest {

    private fun fixture(path: String): String = javaClass.getResourceAsStream(path)
        ?.readBytes()?.toString(Charsets.UTF_8)?.replace("\r\n", "\n")
        ?: error("fixture $path missing from test resources")

    @Test
    fun `frozen schema contracts derive from the current list by dropping additions`() {
        assertEquals(
            PATCH_TABLES_SCHEMA_4.filterNot { it.name in setOf("line_ref", "line_dh") },
            PATCH_TABLES_SCHEMA_3,
        )
        assertEquals(
            PATCH_TABLES_IN_FK_ORDER.filterNot { it.name == "line_content" || it.name == "zstd_dict" },
            PATCH_TABLES_SCHEMA_5,
        )
        assertEquals(
            PATCH_TABLES_SCHEMA_5.map { table ->
                if (table.name == "line_dh") table.copy(updatable = false) else table
            },
            PATCH_TABLES_SCHEMA_4,
        )
        assertEquals(
            PATCH_TABLES_SCHEMA_3.filterNot { it.name == "link_suppressed_side" },
            PATCH_TABLES_SCHEMA_2,
        )
        assertEquals(
            PATCH_TABLES_SCHEMA_2.filterNot { it.name == "book_base_text" },
            PATCH_TABLES_SCHEMA_1,
        )
        assertEquals(
            LogicalContentHasher.TABLES_SCHEMA_2.filterNot { it == "book_base_text" },
            LogicalContentHasher.TABLES_SCHEMA_1,
        )
        assertEquals(
            listOf(
                "source", "author", "topic", "pub_place", "pub_date", "connection_type", "generation",
                "category", "category_closure", "tocText", "book", "book_topic", "book_author",
                "book_base_text", "book_pub_place", "book_pub_date", "book_generation", "tocEntry", "line",
                "line_toc", "link", "link_anchor", "link_range", "link_coverage", "book_has_links",
                "book_version", "version_line", "book_acronym", "alt_toc_structure", "alt_toc_entry",
                "line_alt_toc", "default_commentator", "default_targum", "schema_meta",
            ),
            LogicalContentHasher.TABLES_SCHEMA_2,
        )
        assertEquals(
            LogicalContentHasher.TABLES_SCHEMA_2.toMutableList().apply {
                add(indexOf("link_coverage") + 1, "link_suppressed_side")
            },
            LogicalContentHasher.TABLES_SCHEMA_3,
        )
        assertEquals(
            LogicalContentHasher.TABLES_SCHEMA_3.toMutableList().apply {
                add(indexOf("line_toc") + 1, "line_ref")
                add(indexOf("line_ref") + 1, "line_dh")
            },
            LogicalContentHasher.TABLES_SCHEMA_4,
        )
        assertEquals(LogicalContentHasher.TABLES_SCHEMA_4, LogicalContentHasher.TABLES_SCHEMA_5)
        assertEquals(
            LogicalContentHasher.TABLES_SCHEMA_5.toMutableList().apply {
                add(indexOf("line") + 1, "line_content")
                add(indexOf("line_content") + 1, "zstd_dict")
            },
            LogicalContentHasher.TABLES_SCHEMA_6,
        )
        assertEquals(PATCH_TABLES_IN_FK_ORDER, patchTablesForSchemaVersion(6))
        assertEquals(PATCH_TABLES_SCHEMA_5, patchTablesForSchemaVersion(5))
    }

    private fun canonicalContract(
        fkOrder: List<PatchTable>,
        hashOrder: List<String>,
        schemaVersion: Int,
        optionalTables: List<OptionalPatchTable>? = null,
    ): String {
        fun strings(xs: List<String>) = xs.joinToString(", ") { "\"$it\"" }
        val b = StringBuilder()
        b.append("{\n")
        b.append("  \"schemaVersion\": ").append(schemaVersion).append(",\n")
        b.append("  \"fkOrder\": [\n")
        for ((i, t) in fkOrder.withIndex()) {
            val pk = t.primaryKey.joinToString(", ") { "\"$it\"" }
            b.append("    { \"table\": \"").append(t.name)
                .append("\", \"pk\": [").append(pk)
                .append("], \"updatable\": ").append(t.updatable).append(" }")
            if (i != fkOrder.lastIndex) b.append(",")
            b.append("\n")
        }
        b.append("  ],\n")
        b.append("  \"hashOrder\": [\n")
        for ((i, name) in hashOrder.withIndex()) {
            b.append("    \"").append(name).append("\"")
            if (i != hashOrder.lastIndex) b.append(",")
            b.append("\n")
        }
        // Frozen pre-side-channel fixtures end at hashOrder.
        if (optionalTables == null) {
            b.append("  ]\n")
        } else {
            b.append("  ],\n")
            b.append("  \"optionalTables\": [\n")
            for ((i, t) in optionalTables.withIndex()) {
                b.append("    { \"table\": \"").append(t.name)
                    .append("\", \"pk\": [").append(strings(t.primaryKey))
                    .append("], \"columns\": [").append(strings(t.columns)).append("] }")
                if (i != optionalTables.lastIndex) b.append(",")
                b.append("\n")
            }
            b.append("  ]\n")
        }
        b.append("}\n")
        return b.toString()
    }

    @Test
    fun `canonical serialization matches committed fixture`() {
        val expected = fixture("/patch_tables_contract.json")
        val actual = canonicalContract(
            PATCH_TABLES_IN_FK_ORDER,
            LogicalContentHasher.DEFAULT_TABLES,
            CURRENT_DB_SCHEMA_VERSION,
            OPTIONAL_PATCH_TABLES,
        )
        assertEquals(expected, actual)
    }

    @Test
    fun `optional tables stay outside the FK order and every hash order`() {
        val names = OPTIONAL_PATCH_TABLES.map { it.name }.toSet()
        assertEquals(OPTIONAL_PATCH_TABLES.size, names.size)
        assertEquals(emptySet(), PATCH_TABLES_IN_FK_ORDER.map { it.name }.toSet() intersect names)
        for (v in 1..CURRENT_DB_SCHEMA_VERSION) {
            assertEquals(emptySet(), LogicalContentHasher.tablesForSchemaVersion(v).toSet() intersect names)
            assertEquals(emptySet(), patchTablesForSchemaVersion(v).map { it.name }.toSet() intersect names)
        }
    }

    @Test
    fun `schema 5 canonical contract remains byte frozen`() {
        val expected = fixture("/patch_tables_contract_schema_5.json")
        val actual = canonicalContract(
            PATCH_TABLES_SCHEMA_5,
            LogicalContentHasher.TABLES_SCHEMA_5,
            schemaVersion = 5,
        )
        assertEquals(expected, actual)
    }

    @Test
    fun `schema 4 canonical contract remains byte frozen`() {
        val expected = fixture("/patch_tables_contract_schema_4.json")
        val actual = canonicalContract(
            PATCH_TABLES_SCHEMA_4,
            LogicalContentHasher.TABLES_SCHEMA_4,
            schemaVersion = 4,
        )
        assertEquals(expected, actual)
    }

    @Test
    fun `schema 3 canonical contract remains byte frozen`() {
        val expected = fixture("/patch_tables_contract_schema_3.json")
        val actual = canonicalContract(
            PATCH_TABLES_SCHEMA_3,
            LogicalContentHasher.TABLES_SCHEMA_3,
            schemaVersion = 3,
        )
        assertEquals(expected, actual)
    }

    @Test
    fun `schema 1 canonical contract remains byte frozen`() {
        val expected = fixture("/patch_tables_contract_schema_1.json")
        val actual = canonicalContract(
            PATCH_TABLES_SCHEMA_1,
            LogicalContentHasher.TABLES_SCHEMA_1,
            schemaVersion = 1,
        )
        assertEquals(expected, actual)
    }
}
