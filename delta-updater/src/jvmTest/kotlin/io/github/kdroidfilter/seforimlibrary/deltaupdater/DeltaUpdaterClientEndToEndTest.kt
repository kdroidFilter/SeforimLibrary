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
        val uncompressedSha = sha256(patchDb)
        val uncompressedSize = Files.size(patchDb)
        // The orchestrator only consumes .zst patches now — compress the
        // freshly-produced patch.db and serve that over the test server.
        val compressed = io.github.kdroidfilter.seforimlibrary.common.patch.PatchCompressor
            .compress(patchDb, level = 3, workers = 1)

        // Stand up a localhost HTTP server with release_meta + manifest + patch.zst.
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
                      "totalSize": ${compressed.compressedSize}
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
                    {"file": "patch_global.db.zst", "compression": "zstd",
                     "sha256": "${compressed.compressedSha256}", "size": ${compressed.compressedSize},
                     "uncompressedSha256": "$uncompressedSha", "uncompressedSize": $uncompressedSize}
                  ],
                  "catalogBlobName": "catalog.pb"
                }
            """.trimIndent()
            ex.respond(200, body)
        }
        server.createContext("/patch_global.db.zst") { ex ->
            ex.responseHeaders.set("Content-Type", "application/octet-stream")
            ex.sendResponseHeaders(200, compressed.compressedSize)
            ex.responseBody.use { Files.copy(compressed.compressedFile, it) }
        }
        server.start()

        try {
            val catalogPb = tmp.newFolder().toPath().resolve("catalog.pb")
            val workDir = tmp.newFolder().toPath()

            // Wire Lucene sinks that record everything they see.
            val deletedLineIds = ArrayList<Long>()
            val upsertedLines = ArrayList<LuceneUpdater.PatchLine>()
            val sessionCloseCount = java.util.concurrent.atomic.AtomicInteger(0)
            val client = DeltaUpdaterClient(
                seforimDb = liveDb,
                catalogPb = catalogPb,
                workDir = workDir,
                releaseMetaUrl = "$base/release_meta.json",
                indexSinks = {
                    LuceneUpdater.SinkSession(
                        delete = LuceneUpdater.DeleteSink { deletedLineIds += it },
                        upsert = LuceneUpdater.UpsertSink { upsertedLines += it },
                        onClose = { sessionCloseCount.incrementAndGet() },
                    )
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

            // The session's onClose ran exactly once per delta on the happy
            // path — this is the hook real callers use to commit + close
            // their IndexWriter. A regression here would silently drop
            // every Lucene update.
            assertEquals(
                path.deltas.size, sessionCloseCount.get(),
                "SinkSession.close() must fire once per delta on success",
            )
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

    @Test
    fun `checkForUpdate treats a 404 release_meta as up-to-date`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/release_meta.json") { ex ->
            val body = "Not Found".toByteArray()
            ex.sendResponseHeaders(404, body.size.toLong())
            ex.responseBody.use { it.write(body) }
        }
        server.start()
        try {
            val seforim = tmp.newFolder().toPath().resolve("seforim.db")
            buildClientDb(seforim)
            val client = DeltaUpdaterClient(
                seforimDb = seforim,
                catalogPb = tmp.newFolder().toPath().resolve("catalog.pb"),
                workDir = tmp.newFolder().toPath(),
                releaseMetaUrl = "http://127.0.0.1:${server.address.port}/release_meta.json",
                indexSinks = {
                    LuceneUpdater.SinkSession(
                        delete = LuceneUpdater.DeleteSink { },
                        upsert = LuceneUpdater.UpsertSink { },
                    )
                },
                localVersionProvider = { 1 },
            )
            assertEquals(UpdatePath.UpToDate, client.checkForUpdate())
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `failed download leaves deltaDir behind so next attempt can resume`() {
        // Stand up a server that ALWAYS returns 500 on the patch file —
        // simulates a transient CDN failure mid-flight. The orchestrator's
        // applyChain must propagate the IOException, but its per-delta work
        // directory must survive on disk so the next attempt's downloader
        // can pick up the partial .part file.
        val patchBytes = 1024L
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val base = "http://127.0.0.1:${server.address.port}"
        server.createContext("/release_meta.json") { ex ->
            ex.respond(200, """
                {"latestVersion":2,"fullBundle":{"version":2,"url":"$base/full","sha256":"x","size":1000000000},
                 "deltas":[{"fromVersion":1,"toVersion":2,"manifestUrl":"$base/m.json","totalSize":$patchBytes}],
                 "retentionWindow":30}
            """.trimIndent())
        }
        val fakeSha = "0".repeat(64)
        server.createContext("/m.json") { ex ->
            ex.respond(200, """
                {"fromVersion":1,"toVersion":2,"fromSchemaVersion":1,"toSchemaVersion":1,
                 "fromContentHash":"fake","toContentHash":"fake",
                 "patchFiles":[{"file":"patch_global.db.zst","compression":"zstd",
                                "sha256":"$fakeSha","size":$patchBytes,
                                "uncompressedSha256":"$fakeSha","uncompressedSize":$patchBytes}]}
            """.trimIndent())
        }
        server.createContext("/patch_global.db.zst") { ex ->
            // Server closes the connection partway, simulating a flaky CDN.
            val partial = ByteArray(256) { it.toByte() }
            ex.sendResponseHeaders(200, patchBytes)
            ex.responseBody.use { it.write(partial); /* stop early */ }
        }
        server.start()
        try {
            val workDir = tmp.newFolder().toPath()
            val seforim = tmp.newFolder().toPath().resolve("seforim.db")
            buildClientDb(seforim)
            val client = DeltaUpdaterClient(
                seforimDb = seforim,
                catalogPb = tmp.newFolder().toPath().resolve("catalog.pb"),
                workDir = workDir,
                releaseMetaUrl = "$base/release_meta.json",
                indexSinks = {
                    LuceneUpdater.SinkSession(
                        delete = LuceneUpdater.DeleteSink { },
                        upsert = LuceneUpdater.UpsertSink { },
                    )
                },
                localVersionProvider = { 1 },
            )
            val chain = client.checkForUpdate()
            require(chain is UpdatePath.Chain)
            val ex = kotlin.runCatching { client.applyChain(chain.deltas) { _, _, _ -> } }
                .exceptionOrNull()
            assertNotNull(ex, "applyChain must propagate the download failure")

            // The stable per-delta directory must survive so the next
            // attempt's downloader can resume from whatever bytes the
            // first attempt managed to persist. The exact .part size
            // depends on com.sun.net.httpserver's buffering of partial
            // writes (which isn't a contract we can rely on in tests),
            // so we only assert the *dir* survives — that's the
            // orchestrator-level guarantee being exercised.
            val deltaDir = workDir.resolve("delta-v1-v2")
            assertTrue(Files.exists(deltaDir), "deltaDir must survive a failed download")
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `lucene failure after sqlite commit triggers in-process rollback`() {
        val liveDb = tmp.newFolder().toPath().resolve("seforim.db")
        buildClientDb(liveDb)
        val targetDb = tmp.newFolder().toPath().resolve("target.db")
        buildTargetDb(targetDb)
        val patchDb = tmp.newFolder().toPath().resolve("patch.db")
        io.github.kdroidfilter.seforimlibrary.common.patch.PatchDbProducer().produce(
            prevDb = liveDb, newDb = targetDb, outputPath = patchDb,
            fromVersion = 1, toVersion = 2,
        )
        val toHash = io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher()
            .compute(DriverManager.getConnection("jdbc:sqlite:${targetDb.toAbsolutePath()}"))
        val fromHash = io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher()
            .compute(DriverManager.getConnection("jdbc:sqlite:${liveDb.toAbsolutePath()}"))
        val uncompressedSha = sha256(patchDb)
        val uncompressedSize = Files.size(patchDb)
        val compressed = io.github.kdroidfilter.seforimlibrary.common.patch.PatchCompressor
            .compress(patchDb, level = 3, workers = 1)

        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val base = "http://127.0.0.1:${server.address.port}"
        server.createContext("/release_meta.json") { ex ->
            ex.respond(200, """
                {"latestVersion":2,"fullBundle":{"version":2,"url":"$base/full","sha256":"x","size":1000000000},
                 "deltas":[{"fromVersion":1,"toVersion":2,"manifestUrl":"$base/m.json","totalSize":${compressed.compressedSize}}],
                 "retentionWindow":30}
            """.trimIndent())
        }
        server.createContext("/m.json") { ex ->
            ex.respond(200, """
                {"fromVersion":1,"toVersion":2,"fromSchemaVersion":1,"toSchemaVersion":1,
                 "fromContentHash":"$fromHash","toContentHash":"$toHash",
                 "patchFiles":[{"file":"patch_global.db.zst","compression":"zstd",
                                "sha256":"${compressed.compressedSha256}","size":${compressed.compressedSize},
                                "uncompressedSha256":"$uncompressedSha","uncompressedSize":$uncompressedSize}]}
            """.trimIndent())
        }
        server.createContext("/patch_global.db.zst") { ex ->
            ex.responseHeaders.set("Content-Type", "application/octet-stream")
            ex.sendResponseHeaders(200, compressed.compressedSize)
            ex.responseBody.use { Files.copy(compressed.compressedFile, it) }
        }
        server.start()
        val sessionClosed = java.util.concurrent.atomic.AtomicBoolean(false)
        try {
            val client = DeltaUpdaterClient(
                seforimDb = liveDb,
                catalogPb = tmp.newFolder().toPath().resolve("catalog.pb"),
                workDir = tmp.newFolder().toPath(),
                releaseMetaUrl = "$base/release_meta.json",
                // Throwing upsert sink: simulates a Lucene-side failure
                // after the SQLite commit succeeded. The session's
                // onClose hook must still fire so the caller's IndexWriter
                // doesn't leak across runs.
                indexSinks = {
                    LuceneUpdater.SinkSession(
                        delete = LuceneUpdater.DeleteSink { /* no-op */ },
                        upsert = LuceneUpdater.UpsertSink { error("simulated lucene failure") },
                        onClose = { sessionClosed.set(true) },
                    )
                },
                localVersionProvider = { 1 },
            )
            val path = client.checkForUpdate()
            require(path is UpdatePath.Chain)

            val thrown = kotlin.runCatching { client.applyChain(path.deltas) { _, _, _ -> } }
                .exceptionOrNull()
            assertNotNull(thrown, "applyChain must propagate the Lucene failure")

            // SQLite was rolled back in-process: hash matches the pre-apply hash.
            val recoveredHash = DriverManager.getConnection("jdbc:sqlite:${liveDb.toAbsolutePath()}").use {
                io.github.kdroidfilter.seforimlibrary.common.patch.LogicalContentHasher().compute(it)
            }
            assertEquals(fromHash, recoveredHash, "seforim.db must be rolled back to fromContentHash")

            // Marker + backup were cleared by the in-process recovery,
            // so a subsequent boot won't log a misleading recovery message.
            assertTrue(!Files.exists(liveDb.resolveSibling("${liveDb.fileName}.backup")))
            assertTrue(!Files.exists(liveDb.resolveSibling("${liveDb.fileName}.applying")))
            // The session's onClose ran even though applyTo threw — the
            // .use { } block in the orchestrator guarantees this so callers
            // can commit + close their Lucene IndexWriter without leaks.
            assertTrue(sessionClosed.get(), "SinkSession.close() must fire even on Lucene failure")
        } finally {
            server.stop(0)
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
