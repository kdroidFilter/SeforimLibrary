package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks.SefariaCorpusIndex
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** Real importer and manual-link index, with a fresh JVM for each pinned ForDB archive. */
class SefariaCanonicalAuthorImportTest {
    @Test
    fun malformedMappingAbortsTheImportInsteadOfSuccessfullyDroppingTheBook() = withFixture { root ->
        writeArchive(root, "$HEADER\nbroken-row\n")
        val result = runWorker(root, "import")
        assertTrue(result.exitCode != 0, result.output)
        assertTrue(AUTHOR_CHANGES_FILE in result.output, result.output)
        assertFalse("completed" in result.output)
        assertEquals(listOf("0"), query(root, "SELECT COUNT(*) FROM book"))
    }

    @Test
    fun anInvalidArchiveDigestAbortsTheImportInsteadOfSkippingAuthoredBooks() = withFixture { root ->
        writeArchive(root, null)
        val result = runWorker(root, "import", sha = "0".repeat(64))
        assertTrue(result.exitCode != 0, result.output)
        assertTrue("SHA-256 mismatch" in result.output, result.output)
        assertEquals(listOf("0"), query(root, "SELECT COUNT(*) FROM book"))
    }

    @Test
    fun aMappingFailureAlsoAbortsTheManualLinkIndex() = withFixture { root ->
        writeArchive(root, "$HEADER\nbroken-row\n")
        val result = runWorker(root, "index")
        assertTrue(result.exitCode != 0, result.output)
        assertTrue(AUTHOR_CHANGES_FILE in result.output, result.output)
        assertFalse(Files.exists(root.resolve("index.json")))
    }

    @Test
    fun aMissingOptionalMappingStillImportsAndIndexesEveryLine() = withFixture { root ->
        writeArchive(root, null)
        assertSucceeded(runWorker(root, "import"))
        assertSucceeded(runWorker(root, "index"))
        assertEquals(listOf("שם ראשון"), query(root, "SELECT name FROM author"))
        assertEquals(1, index(root).getValue("accepted").jsonPrimitive.int)
        assertEquals(3, index(root).getValue("firstRefIndex").jsonPrimitive.int)
        assertEquals(listOf("גוף ראשון"), query(root, "SELECT content FROM line WHERE lineIndex=2"))
    }

    @Test
    fun booksWithoutAuthorsDoNotLoadTheArchiveInEitherPipeline() = withFixture(emptyList()) { root ->
        writeArchive(root, null)
        // If either pipeline touches the archive this deliberately wrong pin fails.
        assertSucceeded(runWorker(root, "import", sha = "0".repeat(64)))
        assertSucceeded(runWorker(root, "index", sha = "0".repeat(64)))
        assertEquals(listOf("0"), query(root, "SELECT COUNT(*) FROM author"))
        assertEquals(listOf("גוף ראשון"), query(root, "SELECT content FROM line WHERE lineIndex=1"))
        assertEquals(2, index(root).getValue("firstRefIndex").jsonPrimitive.int)
    }

    @Test
    fun canonicalizationPreservesRefsHeadingsAndCleaningOffsets() =
        withFixture(listOf("שם ראשון", "שם שני")) { root ->
            val names = SefariaAuthorCanonicalNames.parse(
                listOf(HEADER, "שם ראשון,מחבר מאוחד", "שם שני,מחבר מאוחד"),
            )
            // Include a generated heading after the author prefix, so an
            // accidental prefix deletion would move both headings and refs.
            val schemaPath = root.resolve("schemas/AuthorFixture.json")
            val schema = Json.parseToJsonElement(Files.readString(schemaPath)).jsonObject
            val nested = Json.parseToJsonElement("""
                {"title":"AuthorFixture","heTitle":"$HE_TITLE","nodeType":"SchemaNode","nodes":[
                  {"key":"Part","title":"Part","heTitle":"חלק","nodeType":"JaggedArrayNode",
                   "depth":1,"sectionNames":["Paragraph"],"addressTypes":["Integer"]}]}
            """.trimIndent())
            Files.writeString(schemaPath, JsonObject(schema + ("schema" to nested)).toString())
            val textPath = root.resolve("json/AuthorFixture/merged.json")
            val merged = Json.parseToJsonElement(Files.readString(textPath)).jsonObject
            val text = Json.parseToJsonElement("""{"Part":["גוף ראשון","גוף שני"]}""")
            Files.writeString(textPath, JsonObject(merged + ("text" to text)).toString())
            val original = read(root).single()
            val mapped = read(root, names::canonical).single()
            assertEquals(listOf("מחבר מאוחד"), mapped.authors)
            assertEquals(original.lines.size, mapped.lines.size)
            assertEquals(original.lines.drop(3), mapped.lines.drop(3))
            assertEquals(original.refEntries, mapped.refEntries)
            assertTrue(original.headings.any { it.lineIndex > 0 })
            assertEquals(original.headings, mapped.headings)
            assertEquals(original.cleanShiftByLineIndex, mapped.cleanShiftByLineIndex)
            assertEquals(original.simanTopicLines, mapped.simanTopicLines)
            assertTrue(mapped.authorMatchKeys.containsAll(listOf("שם ראשון", "שם שני", "מחבר מאוחד")))
        }

    @Test
    fun collidingCanonicalAuthorsPreserveBodyCoordinatesAndUniqueDatabaseAuthors() =
        withFixture(listOf("שם ראשון", "שם שני")) { root ->
            writeArchive(root, "$HEADER\nשם ראשון,מחבר מאוחד\nשם שני,מחבר מאוחד\n")
            val original = read(root).single()
            assertSucceeded(runWorker(root, "import"))
            assertSucceeded(runWorker(root, "index"))

            assertEquals(listOf("מחבר מאוחד"), query(root, "SELECT name FROM author"))
            assertEquals(listOf("1"), query(root, "SELECT COUNT(*) FROM book_author"))
            val lines = query(root, "SELECT content FROM line ORDER BY lineIndex")
            assertEquals(original.lines.size, lines.size)
            assertEquals(listOf("מחבר מאוחד", "מחבר מאוחד"), lines.subList(1, 3))
            assertEquals(original.lines.drop(3), lines.drop(3))
            val refIndex = original.refEntries.single { it.ref == "AuthorFixture 1" }.lineIndex
            assertEquals(refIndex, index(root).getValue("firstRefIndex").jsonPrimitive.int)
            assertEquals("גוף ראשון", lines[refIndex - 1])
            assertEquals(listOf("גוף ראשון"), query(root, "SELECT content FROM line WHERE lineIndex=${refIndex - 1}"))
            // Historical proofs retain source author lines; body coordinates still agree.
            assertEquals("שם ראשון", index(root).getValue("firstAuthorLine").jsonPrimitive.content)
            assertEquals(original.lines.size, index(root).getValue("lineCount").jsonPrimitive.int)
        }

    @Test
    fun canonicalAndOriginalBlacklistNamesAreBlockedInBothPipelines() {
        // The first case needs the canonical key; the second needs the original
        // key after a rename to an unlisted name. Neither representation may be lost.
        for ((source, target) in listOf("שם ראשון" to "דניאל רוז", "דניאל רוז" to "שם מותר")) {
            withFixture(listOf(source)) { root ->
                writeArchive(root, "$HEADER\n$source,$target\n")
                assertSucceeded(runWorker(root, "import"))
                assertSucceeded(runWorker(root, "index"))
                assertEquals(listOf("0"), query(root, "SELECT COUNT(*) FROM book"))
                assertEquals(0, index(root).getValue("accepted").jsonPrimitive.int)
                assertEquals(1, index(root).getValue("blacklisted").jsonPrimitive.int)
            }
        }
    }

    @Test
    fun aTransientMappingFailureIsPropagatedBeforeAnyPartialResultCanEscape() = withFixture { root ->
        var attempts = 0
        val failure = IllegalStateException("temporary archive failure")
        val names by lazy {
            if (++attempts == 1) throw failure
            SefariaAuthorCanonicalNames.EMPTY
        }
        val reader = reader { names.canonical(it) }
        val schemas = root.resolve("schemas")
        val lookup = reader.buildSchemaLookup(schemas)
        val error = assertFailsWith<SefariaAuthorMappingException> {
            runBlocking { reader.readBooksInParallel(root.resolve("json"), schemas, lookup) }
        }
        assertSame(failure, error.cause)
        assertEquals(1, attempts)
        // An explicit retry is safe: the first call failed instead of returning
        // a successful result that silently omitted this book.
        assertEquals(1, runBlocking { reader.readBooksInParallel(root.resolve("json"), schemas, lookup) }.size)
        assertEquals(2, attempts)
    }

    @Test
    fun selectedBookReadsPropagateTheOriginalMappingFailure() = withFixture { root ->
        val failure = IllegalArgumentException("bad author mapping")
        val reader = reader { throw failure }
        val schemas = root.resolve("schemas")
        val error = assertFailsWith<SefariaAuthorMappingException> {
            reader.readSelectedBooks(root.resolve("json"), schemas, reader.buildSchemaLookup(schemas), setOf(HE_TITLE)) {
                error("A failed mapping must not emit a payload")
            }
        }
        assertSame(failure, error.cause)
    }

    private fun reader(canonical: (String) -> String = { it }) =
        SefariaBookPayloadReader(Json { ignoreUnknownKeys = true }, Logger.withTag("CanonicalAuthorTest"), canonicalAuthorName = canonical)

    private fun read(root: Path, canonical: (String) -> String = { it }): List<BookPayload> = runBlocking {
        val reader = reader(canonical)
        val schemas = root.resolve("schemas")
        reader.readBooksInParallel(root.resolve("json"), schemas, reader.buildSchemaLookup(schemas))
    }

    private fun <T> withFixture(authors: List<String> = listOf("שם ראשון"), action: (Path) -> T): T {
        val root = Files.createTempDirectory("canonical-author-import")
        try {
            Files.createDirectories(root.resolve("schemas"))
            val dir = Files.createDirectories(root.resolve("json/AuthorFixture"))
            val authorJson = authors.joinToString(",") { "{\"he\":\"$it\"}" }
            Files.writeString(root.resolve("schemas/AuthorFixture.json"), """
                {"authors":[$authorJson],"heCategories":["בדיקה"],"categories":["Other"],
                 "schema":{"title":"AuthorFixture","heTitle":"$HE_TITLE","nodeType":"JaggedArrayNode",
                 "depth":1,"sectionNames":["Paragraph"],"addressTypes":["Integer"]}}
            """.trimIndent())
            Files.writeString(dir.resolve("merged.json"), """
                {"title":"AuthorFixture","heTitle":"$HE_TITLE","language":"he","text":["גוף ראשון","גוף שני"]}
            """.trimIndent())
            Files.writeString(root.resolve("table_of_contents.json"), "[]")
            return action(root)
        } finally {
            Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists) }
        }
    }

    private fun writeArchive(root: Path, mapping: String?) {
        ZipOutputStream(Files.newOutputStream(root.resolve("fordb.zip"))).use { zip ->
            for (name in FOR_DB_CSV_FILES.values) {
                zip.putNextEntry(ZipEntry("ForDB/$name"))
                zip.closeEntry()
            }
            if (mapping != null) {
                zip.putNextEntry(ZipEntry("ForDB/$AUTHOR_CHANGES_FILE"))
                zip.write(mapping.toByteArray(Charsets.UTF_8))
                zip.closeEntry()
            }
        }
    }

    private data class WorkerResult(val exitCode: Int, val output: String)

    private fun runWorker(root: Path, mode: String, sha: String? = null): WorkerResult {
        val archive = root.resolve("fordb.zip")
        val digest = sha ?: MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(archive))
            .joinToString("") { "%02x".format(it) }
        val urls = generateSequence(javaClass.classLoader) { it.parent }.filterIsInstance<URLClassLoader>()
            .flatMap { it.urLs.asSequence() }.filter { it.protocol == "file" }.map { Path.of(it.toURI()).toString() }
        val classpath = (urls + System.getProperty("java.class.path").split(java.io.File.pathSeparator).asSequence())
            .distinct().joinToString(java.io.File.pathSeparator)
        val log = root.resolve("$mode.log")
        val process = ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx512m",
            "--enable-native-access=ALL-UNNAMED", "-Dfile.encoding=UTF-8", "-DforDbArchive=$archive",
            "-DforDbSha256=$digest", "-cp", classpath, javaClass.name, mode, root.toString(),
        ).redirectErrorStream(true).redirectOutput(log.toFile()).start()
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly().waitFor()
            error("$mode timed out: ${Files.readString(log)}")
        }
        return WorkerResult(process.exitValue(), Files.readString(log))
    }

    private fun assertSucceeded(result: WorkerResult) = assertEquals(0, result.exitCode, result.output)

    private fun query(root: Path, sql: String): List<String> {
        val driver = JdbcSqliteDriver("jdbc:sqlite:${root.resolve("import.db")}")
        try {
            return driver.getConnection().createStatement().use { statement ->
                statement.executeQuery(sql).use { rows -> buildList { while (rows.next()) add(rows.getString(1)) } }
            }
        } finally {
            driver.close()
        }
    }

    private fun index(root: Path) = Json.parseToJsonElement(Files.readString(root.resolve("index.json")))
        .jsonObject

    companion object {
        private const val HEADER = "שם בספריא,שם קנוני"
        private const val HE_TITLE = "ספר בדיקת מחברים"

        @JvmStatic
        fun main(args: Array<String>) = runBlocking {
            val root = Path.of(args[1])
            when (args[0]) {
                "import" -> {
                    val db = root.resolve("import.db")
                    val repository = SeforimRepository(db.toString(), JdbcSqliteDriver("jdbc:sqlite:$db"))
                    try {
                        SefariaDirectImporter(root, repository, InMemoryIdAllocator.load(path = null)).import()
                    } finally {
                        repository.close()
                    }
                }
                "index" -> {
                    val index = SefariaCorpusIndex.load(
                        root, setOf(HE_TITLE), emptyMap(), setOf(HE_TITLE), Logger.withTag("CanonicalAuthorIndex"),
                    )
                    val result = buildJsonObject {
                        put("accepted", index.stats.payloadsAccepted)
                        put("blacklisted", index.stats.payloadsBlacklisted)
                        if (index.hasPrimaryHeTitle(HE_TITLE)) {
                            val book = index.bookByHeTitle(HE_TITLE)
                            put("firstRefIndex", index.resolveRef(book, "AuthorFixture 1").lineIndex)
                            put("firstAuthorLine", book.fullProofLines()[1])
                            put("lineCount", book.lineCount)
                        }
                    }
                    Files.writeString(root.resolve("index.json"), result.toString())
                }
                else -> error("Unknown worker mode: ${args[0]}")
            }
            println("completed")
        }
    }
}
