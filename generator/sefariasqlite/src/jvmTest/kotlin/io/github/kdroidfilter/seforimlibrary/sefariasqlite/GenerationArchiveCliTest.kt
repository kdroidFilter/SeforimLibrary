package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.InMemoryIdAllocator
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

/** Exercises the actual CLI processes, including their independent archive caches and preflight. */
class GenerationArchiveCliTest {
    private val header = "bookName,authorName,generationName,subGenerationName,startYear,endYear"
    private val bookInfo = "$header\n\"New title\",\"Author\nwith a quote \"\"\",\"New era\",\"\",\"\",\"\"\n"
    private val legacy = "שם ספר,קבוצת דור\nNew title,Legacy era\n"

    @Test
    fun `legacy archive passes rename preflight and seeds generations`() =
        succeeds(mapOf(GENERATIONS_FILE to legacy), "Legacy era")

    @Test
    fun `book info archive passes rename preflight and seeds generations`() =
        succeeds(mapOf(BOOK_INFO_FILE to bookInfo), "New era")

    @Test
    fun `book info wins when an archive also contains malformed legacy data`() =
        succeeds(mapOf(BOOK_INFO_FILE to bookInfo, GENERATIONS_FILE to "broken legacy"), "New era")

    @Test
    fun `malformed book info fails before rename writes even with valid legacy data`() =
        failsBeforeWrites(mapOf(BOOK_INFO_FILE to "$header\nNew title,Author,New era", GENERATIONS_FILE to legacy),
            "book_info.csv record 2 is malformed")

    @Test
    fun `missing generation assets fail before rename writes`() =
        failsBeforeWrites(emptyMap(), "ForDB archive has neither book_info.csv nor generations.csv")

    @Test
    fun `malformed legacy data fails before rename writes`() =
        failsBeforeWrites(mapOf(GENERATIONS_FILE to "שם ספר,קבוצת דור\nNew title"), "generations.csv row 1 is malformed")

    private fun succeeds(assets: Map<String, String>, expectedEra: String) = fixture(assets) { root, db, archive ->
        val rename = cli(root, db, archive, "RenameCategoriesPostProcessKt")
        assertEquals(0, rename.first, rename.second)
        assertEquals("New title", title(db))
        val seed = cli(root, db, archive, "SeedGenerationsPostProcessKt")
        assertEquals(0, seed.first, seed.second)
        val repeat = cli(root, db, archive, "SeedGenerationsPostProcessKt")
        assertEquals(0, repeat.first, repeat.second)
        DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT g.name FROM book_generation bg JOIN generation g ON g.id=bg.generationId").use { rs ->
                    val names = generateSequence { if (rs.next()) rs.getString(1) else null }.toList()
                    assertEquals(listOf(expectedEra), names)
                }
            }
        }
        assertContains(repeat.second, "book links=0")
    }

    private fun failsBeforeWrites(assets: Map<String, String>, message: String) = fixture(assets) { root, db, archive ->
        val before = Files.readAllBytes(db).toList()
        val state = Path.of("$db.buildstate")
        val beforeState = Files.readAllBytes(state).toList()
        val result = cli(root, db, archive, "RenameCategoriesPostProcessKt")
        assertNotEquals(0, result.first, result.second)
        assertContains(result.second, message)
        assertEquals(before, Files.readAllBytes(db).toList(), "Rejected input must not modify the database")
        assertEquals(beforeState, Files.readAllBytes(state).toList(), "Rejected input must not modify build state")
        assertEquals("Old title", title(db))
    }

    private fun fixture(assets: Map<String, String>, action: (Path, Path, Path) -> Unit) {
        val root = Files.createTempDirectory("generation-archive-cli-")
        try {
            val db = root.resolve("fixture.db")
            DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
                conn.createStatement().use { st ->
                    st.execute("CREATE TABLE book (id INTEGER PRIMARY KEY, title TEXT NOT NULL, heRef TEXT)")
                    st.execute("CREATE TABLE generation (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)")
                    st.execute("CREATE TABLE book_generation (bookId INTEGER, generationId INTEGER, PRIMARY KEY(bookId,generationId))")
                    st.execute("INSERT INTO book (id, title) VALUES (1, 'Old title')")
                }
            }
            // Newer rename stages require the same stable-id state that real generation creates.
            val allocator = InMemoryIdAllocator.load(path = null)
            assertEquals(1L, allocator.bookId("fixture", "Old title"))
            allocator.snapshotTo(Path.of("$db.buildstate"))
            val archive = root.resolve("fordb.zip")
            val files = mapOf(
                "category_renames.csv" to "",
                "category_moves.csv" to "Source path,Destination parent path\n",
                "book_renames.csv" to "Old title,New title\n",
                "book_moves.csv" to "name,Source path,Destination path\n",
                "all_metadata.json" to "[]\n",
                "sefaria_metadata_changes.csv" to "categoryPath,title,author,heShortDesc,heDesc,heDescNew\n",
                "sefaria_category_changes.csv" to "categoryPath,heShortDesc,heDesc,heShortDescNew,heDescNew\n",
                "book_banners.csv" to "id,sourceName,bookName,text\n",
                "book_protection.csv" to "sourceName,bookName,level\n",
            ) + assets
            ZipOutputStream(Files.newOutputStream(archive)).use { zip ->
                for ((name, text) in files) {
                    zip.putNextEntry(ZipEntry("ForDB/$name").apply { time = 0L })
                    zip.write(text.toByteArray(Charsets.UTF_8))
                    zip.closeEntry()
                }
            }
            action(root, db, archive)
        } finally {
            Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists) }
        }
    }

    private fun title(db: Path): String = DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery("SELECT title FROM book WHERE id=1").use { rs -> rs.next(); rs.getString(1) }
        }
    }

    private fun cli(root: Path, db: Path, archive: Path, mainClass: String): Pair<Int, String> {
        val sha = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(archive))
            .joinToString("") { "%02x".format(it) }
        val urls = generateSequence(javaClass.classLoader) { it.parent }
            .filterIsInstance<URLClassLoader>().flatMap { it.urLs.asSequence() }
            .filter { it.protocol == "file" }.map { Path.of(it.toURI()).toString() }
        val classpath = (urls + System.getProperty("java.class.path").split(java.io.File.pathSeparator).asSequence())
            .distinct().joinToString(java.io.File.pathSeparator)
        val log = root.resolve("$mainClass.log")
        val process = ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m",
            "--enable-native-access=ALL-UNNAMED", "-DforDbArchive=$archive", "-DforDbSha256=$sha",
            "-cp", classpath, "io.github.kdroidfilter.seforimlibrary.sefariasqlite.$mainClass", db.toString(),
        ).redirectErrorStream(true).redirectOutput(log.toFile()).start()
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly().waitFor()
            error("$mainClass timed out: ${Files.readString(log)}")
        }
        return process.exitValue() to Files.readString(log)
    }
}
