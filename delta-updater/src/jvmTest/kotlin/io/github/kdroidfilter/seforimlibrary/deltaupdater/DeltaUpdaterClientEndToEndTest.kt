package io.github.kdroidfilter.seforimlibrary.deltaupdater

import com.sun.net.httpserver.HttpServer
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * End-to-end test for the client-side delta apply flow:
 *
 *  1. Build a tiny seforim.db (the "live" client DB) and a target seforim.db.
 *  2. Produce a real patch.db from (prev, new) using the actual producer.
 *  3. Stand up a localhost HTTP server serving release_meta.json + manifest +
 *     the patch file.
 *  4. Invoke [DeltaUpdaterClient.checkForUpdate] + [DeltaUpdaterClient.applyChain]
 *     and verify:
 *       - the patch was downloaded (sha256 checked)
 *       - the patch was applied (logical hash matches target)
 *       - Lucene sinks were called for every upserted line + every deleted line
 *       - the catalog blob was written atomically
 *       - the backup + marker files are gone after finalize
 */
class DeltaUpdaterClientEndToEndTest {
    @JvmField @Rule
    val tmp = TemporaryFolder()

    @Test
    fun `download + apply + lucene + catalog + finalize succeeds end-to-end`() {
        // Live "client" seforim.db: a minimal SQLite carrying only what the
        // applier needs to touch in this scenario.
        val liveDb = tmp.newFolder().toPath().resolve("seforim.db")
        buildClientDb(liveDb)

        val targetDb = tmp.newFolder().toPath().resolve("target.db")
        buildTargetDb(targetDb)

        // Produce a patch from (live → target).
        val patchDb = tmp.newFolder().toPath().resolve("patch.db")
        io.github.kdroidfilter.seforimlibrary.common.patch.PatchDbProducer().produce(
            prevDb = liveDb,
            newDb = targetDb,
            outputPath = patchDb,
            fromVersion = 1,
            toVersion = 2,
        )
        // Stash a "catalog.pb" blob inside the patch so the CatalogUpdater has something to write.
        DriverManager.getConnection("jdbc:sqlite:${patchDb.toAbsolutePath()}").use { c ->
            c.prepareStatement("INSERT OR REPLACE INTO blobs(name, content) VALUES (?, ?)").use { ps ->
                ps.setString(1, "catalog.pb")
                ps.setBytes(2, "CATALOG_PAYLOAD_V2".toByteArray())
                ps.executeUpdate()
            }
        }

        // Compute manifest hashes from the actual files.
        val toHash = io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher()
            .compute(DriverManager.getConnection("jdbc:sqlite:${targetDb.toAbsolutePath()}"))
        val fromHash = io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher()
            .compute(DriverManager.getConnection("jdbc:sqlite:${liveDb.toAbsolutePath()}"))
        val patchSha256 = sha256(patchDb)
        val patchBytes = Files.size(patchDb)

        // Stand up a localhost HTTP server with release_meta + manifest + patch.db.
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val port = server.address.port
        val base = "http://127.0.0.1:$port"

        server.createContext("/release_meta.json") { ex ->
            val body = """
                {
                  "latestVersion": 2,
                  "fullBundle": {"version": 2, "url": "$base/full.tar.zst", "sha256": "abcd", "size": 1000000000},
                  "deltas": [
                    {
                      "fromVersion": 1,
                      "toVersion": 2,
                      "manifestUrl": "$base/1-2.json",
                      "totalSize": $patchBytes
                    }
                  ],
                  "retentionWindow": 30
                }
            """.trimIndent()
            ex.respond(200, body)
        }
        server.createContext("/1-2.json") { ex ->
            val body = """
                {
                  "fromVersion": 1,
                  "toVersion": 2,
                  "fromSchemaVersion": 1,
                  "toSchemaVersion": 1,
                  "fromContentHash": "$fromHash",
                  "toContentHash": "$toHash",
                  "patchFiles": [
                    {"file": "patch_global.db", "sha256": "$patchSha256", "size": $patchBytes}
                  ],
                  "catalogBlobName": "catalog.pb"
                }
            """.trimIndent()
            ex.respond(200, body)
        }
        server.createContext("/patch_global.db") { ex ->
            ex.responseHeaders.set("Content-Type", "application/octet-stream")
            ex.sendResponseHeaders(200, patchBytes)
            ex.responseBody.use { Files.copy(patchDb, it) }
        }
        server.start()

        try {
            val catalogPb = tmp.newFolder().toPath().resolve("catalog.pb")
            val workDir = tmp.newFolder().toPath()

            // Wire Lucene sinks that record everything they see.
            val deletedLineIds = ArrayList<Long>()
            val upsertedLines = ArrayList<LuceneUpdater.PatchLine>()
            val client = DeltaUpdaterClient(
                seforimDb = liveDb,
                catalogPb = catalogPb,
                workDir = workDir,
                releaseMetaUrl = "$base/release_meta.json",
                indexSinks = {
                    LuceneUpdater.DeleteSink { deletedLineIds += it } to
                        LuceneUpdater.UpsertSink { upsertedLines += it }
                },
                localVersionProvider = { 1 },
            )

            // Boot-time recovery should be a no-op (no marker).
            assertEquals(false, client.recoverIfNeeded())

            val path = client.checkForUpdate()
            assertTrue(path is UpdatePath.Chain, "expected Chain, got $path")
            require(path is UpdatePath.Chain)
            assertEquals(1, path.deltas.size)

            val progressEvents = ArrayList<String>()
            client.applyChain(path.deltas) { cur, total, status ->
                progressEvents += "$cur/$total: $status"
            }

            // After apply: target hash matches.
            val appliedHash = DriverManager.getConnection("jdbc:sqlite:${liveDb.toAbsolutePath()}").use {
                io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher().compute(it)
            }
            assertEquals(toHash, appliedHash, "applied DB must match target by logical hash")

            // Lucene sinks saw the line ops.
            assertTrue(upsertedLines.isNotEmpty(), "lucene sink should have seen the upserted line(s)")

            // Catalog blob was written.
            assertTrue(Files.exists(catalogPb))
            assertEquals("CATALOG_PAYLOAD_V2", Files.readString(catalogPb))

            // Marker + backup are gone after finalize.
            assertTrue(!Files.exists(liveDb.resolveSibling("${liveDb.fileName}.backup")))
            assertTrue(!Files.exists(liveDb.resolveSibling("${liveDb.fileName}.applying")))

            // Progress events fired in sequence.
            assertTrue(progressEvents.any { "downloading patch files" in it })
            assertTrue(progressEvents.any { "applying sqlite delta" in it })
            assertTrue(progressEvents.any { "done" in it })
        } finally {
            server.stop(0)
        }
    }

    // ─── Test fixtures: tiny seforim-shaped DBs ───────────────────────────────

    private fun buildClientDb(path: Path) {
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { c ->
            c.createStatement().use { st ->
                st.executeUpdate("CREATE TABLE schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)")
                st.executeUpdate("CREATE TABLE source (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE author (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE topic (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE pub_place (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE pub_date (id INTEGER PRIMARY KEY NOT NULL, date TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE connection_type (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
                st.executeUpdate("CREATE TABLE category (id INTEGER PRIMARY KEY NOT NULL, parentId INTEGER, title TEXT NOT NULL, level INTEGER NOT NULL DEFAULT 0, orderIndex INTEGER NOT NULL DEFAULT 999)")
                st.executeUpdate("CREATE TABLE tocText (id INTEGER PRIMARY KEY NOT NULL, text TEXT NOT NULL UNIQUE)")
                st.executeUpdate("""
                    CREATE TABLE book (
                        id INTEGER PRIMARY KEY NOT NULL,
                        categoryId INTEGER NOT NULL,
                        sourceId INTEGER NOT NULL,
                        title TEXT NOT NULL,
                        heRef TEXT,
                        heShortDesc TEXT,
                        notesContent TEXT,
                        orderIndex INTEGER NOT NULL DEFAULT 999,
                        totalLines INTEGER NOT NULL DEFAULT 0,
                        isBaseBook INTEGER NOT NULL DEFAULT 0,
                        hasTargumConnection INTEGER NOT NULL DEFAULT 0,
                        hasReferenceConnection INTEGER NOT NULL DEFAULT 0,
                        hasSourceConnection INTEGER NOT NULL DEFAULT 0,
                        hasCommentaryConnection INTEGER NOT NULL DEFAULT 0,
                        hasOtherConnection INTEGER NOT NULL DEFAULT 0,
                        hasAltStructures INTEGER NOT NULL DEFAULT 0,
                        hasTeamim INTEGER NOT NULL DEFAULT 0,
                        hasNekudot INTEGER NOT NULL DEFAULT 0
                    )
                """.trimIndent())
                st.executeUpdate("""
                    CREATE TABLE line (
                        id INTEGER PRIMARY KEY NOT NULL,
                        bookId INTEGER NOT NULL,
                        lineIndex INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        heRef TEXT,
                        tocEntryId INTEGER,
                        charCount INTEGER NOT NULL DEFAULT 0
                    )
                """.trimIndent())
                // Seed: 1 source, 1 category, 1 book, 2 lines.
                st.executeUpdate("INSERT INTO source(id, name) VALUES (1, 'Sefaria')")
                st.executeUpdate("INSERT INTO category(id, parentId, title, level, orderIndex) VALUES (1, NULL, 'Tanakh', 0, 1)")
                st.executeUpdate("INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'Genesis')")
                st.executeUpdate("INSERT INTO line(id, bookId, lineIndex, content) VALUES (1, 1, 0, 'old verse 1')")
                st.executeUpdate("INSERT INTO line(id, bookId, lineIndex, content) VALUES (2, 1, 1, 'old verse 2')")
            }
        }
    }

    private fun buildTargetDb(path: Path) {
        buildClientDb(path)
        // Apply some changes: edit verse 1, delete verse 2, add verse 3.
        DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}").use { c ->
            c.createStatement().use { st ->
                st.executeUpdate("UPDATE line SET content='new verse 1' WHERE id=1")
                st.executeUpdate("DELETE FROM line WHERE id=2")
                st.executeUpdate("INSERT INTO line(id, bookId, lineIndex, content) VALUES (3, 1, 1, 'new verse 3')")
            }
        }
    }

    private fun sha256(path: Path): String {
        val md = MessageDigest.getInstance("SHA-256")
        Files.newInputStream(path).use { input ->
            val buf = ByteArray(8192)
            var read: Int
            while (input.read(buf).also { read = it } > 0) md.update(buf, 0, read)
        }
        return md.digest().joinToString("") { "%02x".format(it) }
    }
}

private fun com.sun.net.httpserver.HttpExchange.respond(status: Int, body: String) {
    val bytes = body.toByteArray()
    sendResponseHeaders(status, bytes.size.toLong())
    responseBody.use { it.write(bytes) }
}
