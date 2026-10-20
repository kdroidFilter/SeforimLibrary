package io.github.kdroidfilter.seforimlibrary.common.patch

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.sql.Connection
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Shared oracle with the Dart client (`contract.yml` cmp's the two fixtures),
 * for the frozen schema 6 and for schema 7.
 * A failure here means the two hashers drifted and deltas would be rejected.
 */
class LogicalHashContractTest {

    private class Oracle(resource: String) {
        private val fixture = Json.parseToJsonElement(
            requireNotNull(LogicalHashContractTest::class.java.getResourceAsStream(resource)) {
                "$resource missing from test resources"
            }.bufferedReader(Charsets.UTF_8).readText(),
        ).jsonObject

        val schemaVersion = fixture.getValue("schemaVersion").jsonPrimitive.int
        val hashTableOrder = fixture.getValue("hashTableOrder").jsonArray.map { it.jsonPrimitive.content }
        val hashRowOrder = fixture["hashRowOrder"]?.jsonObject
            ?.mapValues { (_, pk) -> pk.jsonArray.map { it.jsonPrimitive.content } }
        val setupSql = fixture.getValue("setupSql").jsonArray.map { it.jsonPrimitive.content }
        val wholeHash = fixture.getValue("wholeHash").jsonPrimitive.content
        val tableHashes = fixture.getValue("tableHashes").jsonObject
            .mapValues { it.value.jsonPrimitive.content }

        fun <T> withFixtureDb(block: (Connection) -> T): T =
            DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
                conn.createStatement().use { st -> setupSql.forEach { st.execute(it) } }
                block(conn)
            }
    }

    private val oracles = listOf(
        Oracle("/logical_hash_contract.json"),
        Oracle("/logical_hash_contract_schema_7.json"),
    )

    @Test
    fun `the oracles cover the frozen schema 6 and schema 7`() {
        assertEquals(listOf(6, 7), oracles.map { it.schemaVersion })
    }

    @Test
    fun `hash table and row order match the fixture`() = oracles.forEach { o ->
        assertEquals(o.hashTableOrder, LogicalContentHasher.tablesForSchemaVersion(o.schemaVersion))
        assertEquals(o.hashRowOrder, LogicalContentHasher.rowOrderForSchemaVersion(o.schemaVersion))
        o.hashRowOrder?.let { assertEquals(o.hashTableOrder, it.keys.toList(), "row order keys follow the table order") }
    }

    @Test
    fun `whole hash and every per-table hash match the shared oracle`() = oracles.forEach { o ->
        val report = o.withFixtureDb {
            LogicalContentHasher.forSchemaVersion(o.schemaVersion).computeReport(it)
        }
        assertEquals(o.wholeHash, report.wholeHash, "whole hash, schema ${o.schemaVersion}")
        assertEquals(o.hashTableOrder, report.tableHashes.keys.toList(), "per-table key order")
        for (table in o.hashTableOrder) {
            assertEquals(o.tableHashes[table], report.tableHashes[table], "table hash for '$table', schema ${o.schemaVersion}")
        }
    }

    @Test
    fun `compute is unchanged by the report API`() = oracles.forEach { o ->
        val (whole, report) = o.withFixtureDb {
            val hasher = LogicalContentHasher.forSchemaVersion(o.schemaVersion)
            hasher.compute(it) to hasher.computeReport(it)
        }
        assertEquals(whole, report.wholeHash)
        assertEquals(o.wholeHash, whole)
    }

    @Test
    fun `absent tables still contribute a hash and distinct tables differ`() = oracles.forEach { o ->
        val report = o.withFixtureDb {
            LogicalContentHasher.forSchemaVersion(o.schemaVersion).computeReport(it)
        }
        // `topic` is absent from the fixture DB — it hashes its prefix only,
        // which must still differ from another absent table's prefix hash.
        assertTrue(report.tableHashes.getValue("topic").isNotEmpty())
        assertTrue(report.tableHashes.getValue("topic") != report.tableHashes.getValue("pub_place"))
    }

    @Test
    fun `a table's hash is the digest of its stream alone`() = oracles.forEach { o ->
        // A one-table hasher's whole hash must equal that table's entry in the
        // full run: wholeHash is the digest of the concatenated table streams.
        o.withFixtureDb { conn ->
            val full = LogicalContentHasher.forSchemaVersion(o.schemaVersion).computeReport(conn)
            val rowOrder = LogicalContentHasher.rowOrderForSchemaVersion(o.schemaVersion)
            for (table in o.hashTableOrder) {
                val single = LogicalContentHasher(listOf(table), rowOrder).computeReport(conn)
                assertEquals(full.tableHashes[table], single.wholeHash, "stream digest for '$table'")
                assertEquals(single.wholeHash, single.tableHashes.getValue(table))
            }
        }
    }
}
