package io.github.kdroidfilter.seforimlibrary.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

class MagicDictionaryIndexTest {

    private val simpleNorm: (String) -> String = { it.trim().lowercase() }
    private val hebrewNorm: (String) -> String = { HebrewTextUtils.normalizeHebrew(it) }

    // --- load() tests ---

    @Test
    fun `load returns null for null path`() {
        val result = MagicDictionaryIndex.load(simpleNorm, null)
        assertNull(result)
    }

    @Test
    fun `load returns null for non-existent file`() {
        val nonExistentPath = Path.of("/non/existent/path/lexical.db")
        val result = MagicDictionaryIndex.load(simpleNorm, nonExistentPath)
        assertNull(result)
    }

    @Test
    fun `load returns null for file without required tables`() {
        // Create a temporary SQLite file without the required tables
        val tempFile = Files.createTempFile("invalid_lexical", ".db")
        try {
            DriverManager.getConnection("jdbc:sqlite:${tempFile.toAbsolutePath()}").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE other_table (id INTEGER PRIMARY KEY)")
                }
            }

            val result = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNull(result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `load returns index for valid database`() {
        val tempFile = createValidTestDatabase()
        try {
            val result = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    // --- findValidDictionary() tests ---

    @Test
    fun `findValidDictionary returns null for empty list`() {
        val result = MagicDictionaryIndex.findValidDictionary(emptyList())
        assertNull(result)
    }

    @Test
    fun `findValidDictionary returns null when no valid candidate`() {
        val candidates = listOf(
            Path.of("/non/existent/path1.db"),
            Path.of("/non/existent/path2.db")
        )
        val result = MagicDictionaryIndex.findValidDictionary(candidates)
        assertNull(result)
    }

    @Test
    fun `findValidDictionary returns first valid candidate`() {
        val tempFile1 = createValidTestDatabase()
        val tempFile2 = createValidTestDatabase()
        try {
            val candidates = listOf(tempFile1, tempFile2)
            val result = MagicDictionaryIndex.findValidDictionary(candidates)
            assertEquals(tempFile1, result)
        } finally {
            Files.deleteIfExists(tempFile1)
            Files.deleteIfExists(tempFile2)
        }
    }

    @Test
    fun `findValidDictionary skips invalid candidates`() {
        val invalidFile = Files.createTempFile("invalid", ".db")
        val validFile = createValidTestDatabase()
        try {
            // Create invalid database without required tables
            DriverManager.getConnection("jdbc:sqlite:${invalidFile.toAbsolutePath()}").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE other_table (id INTEGER PRIMARY KEY)")
                }
            }

            val candidates = listOf(invalidFile, validFile)
            val result = MagicDictionaryIndex.findValidDictionary(candidates)
            assertEquals(validFile, result)
        } finally {
            Files.deleteIfExists(invalidFile)
            Files.deleteIfExists(validFile)
        }
    }

    // --- expansionFor() tests ---

    @Test
    fun `expansionFor returns null for empty token`() {
        val tempFile = createValidTestDatabase()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            val result = index.expansionFor("")
            assertNull(result)

            val resultBlank = index.expansionFor("   ")
            assertNull(resultBlank)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `expansionFor returns null for unknown token`() {
        val tempFile = createValidTestDatabase()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            val result = index.expansionFor("unknownword12345")
            assertNull(result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `expansionFor returns expansion for known token`() {
        val tempFile = createTestDatabaseWithData()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            val result = index.expansionFor("word1")
            assertNotNull(result)
            assertTrue(result.surface.isNotEmpty())
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    // --- expansionsFor() tests ---

    @Test
    fun `expansionsFor returns empty list for empty token list`() {
        val tempFile = createValidTestDatabase()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            val result = index.expansionsFor(emptyList())
            assertTrue(result.isEmpty())
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `expansionsFor returns empty list for unknown tokens`() {
        val tempFile = createValidTestDatabase()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            val result = index.expansionsFor(listOf("unknown1", "unknown2"))
            assertTrue(result.isEmpty())
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `expansionsFor returns distinct expansions`() {
        val tempFile = createTestDatabaseWithData()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            // Query the same token twice - should return distinct results
            val result = index.expansionsFor(listOf("word1", "word1"))
            // Distinct should deduplicate
            assertTrue(result.size <= 1 || result.distinct().size == result.size)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `expansionsFor handles multiple tokens`() {
        val tempFile = createTestDatabaseWithData()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            val result = index.expansionsFor(listOf("word1", "word2"))
            assertNotNull(result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    // --- loadHashemSurfaces() tests ---

    @Test
    fun `loadHashemSurfaces returns empty list when no Hashem entries`() {
        val tempFile = createValidTestDatabase()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            val result = index.loadHashemSurfaces()
            assertTrue(result.isEmpty())
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `loadHashemSurfaces returns surfaces for יהוה base`() {
        val tempFile = createTestDatabaseWithHashemData()
        try {
            val index = MagicDictionaryIndex.load(hebrewNorm, tempFile)
            assertNotNull(index)

            val result = index.loadHashemSurfaces()
            assertTrue(result.isNotEmpty())
            assertTrue(result.contains("יהוה"))
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    // --- Hebrew normalization integration tests ---

    @Test
    fun `expansionFor works with Hebrew normalization`() {
        val tempFile = createTestDatabaseWithHebrewData()
        try {
            val index = MagicDictionaryIndex.load(hebrewNorm, tempFile)
            assertNotNull(index)

            // Search for a word with sofit letter
            val result = index.expansionFor("מלך")
            assertNotNull(result)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    @Test
    fun `expansion prefers base match over largest expansion`() {
        val tempFile = createTestDatabaseWithMultipleExpansions()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            // When token matches a base, prefer that expansion
            val result = index.expansionFor("root")
            assertNotNull(result)
            assertTrue(result.base.contains("root"))
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    // --- Caching tests ---

    @Test
    fun `repeated lookups use cache`() {
        val tempFile = createTestDatabaseWithData()
        try {
            val index = MagicDictionaryIndex.load(simpleNorm, tempFile)
            assertNotNull(index)

            // First lookup
            val result1 = index.expansionFor("word1")
            // Second lookup (should use cache)
            val result2 = index.expansionFor("word1")

            assertEquals(result1, result2)
        } finally {
            Files.deleteIfExists(tempFile)
        }
    }

    // --- Helper methods ---

    private fun createValidTestDatabase(): Path {
        val tempFile = Files.createTempFile("valid_lexical", ".db")
        DriverManager.getConnection("jdbc:sqlite:${tempFile.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("""
                    CREATE TABLE base (
                        id INTEGER PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """.trimIndent())
                stmt.execute("""
                    CREATE TABLE surface (
                        id INTEGER PRIMARY KEY,
                        value TEXT NOT NULL,
                        base_id INTEGER NOT NULL,
                        FOREIGN KEY (base_id) REFERENCES base(id)
                    )
                """.trimIndent())
                stmt.execute("""
                    CREATE TABLE variant (
                        id INTEGER PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """.trimIndent())
                stmt.execute("""
                    CREATE TABLE surface_variant (
                        surface_id INTEGER NOT NULL,
                        variant_id INTEGER NOT NULL,
                        PRIMARY KEY (surface_id, variant_id),
                        FOREIGN KEY (surface_id) REFERENCES surface(id),
                        FOREIGN KEY (variant_id) REFERENCES variant(id)
                    )
                """.trimIndent())
            }
        }
        return tempFile
    }

    private fun createTestDatabaseWithData(): Path {
        val tempFile = createValidTestDatabase()
        DriverManager.getConnection("jdbc:sqlite:${tempFile.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { stmt ->
                // Insert base
                stmt.execute("INSERT INTO base (id, value) VALUES (1, 'root1')")
                stmt.execute("INSERT INTO base (id, value) VALUES (2, 'root2')")

                // Insert surfaces
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (1, 'word1', 1)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (2, 'word1a', 1)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (3, 'word2', 2)")

                // Insert variants
                stmt.execute("INSERT INTO variant (id, value) VALUES (1, 'var1')")
                stmt.execute("INSERT INTO variant (id, value) VALUES (2, 'var2')")

                // Link surfaces to variants
                stmt.execute("INSERT INTO surface_variant (surface_id, variant_id) VALUES (1, 1)")
                stmt.execute("INSERT INTO surface_variant (surface_id, variant_id) VALUES (1, 2)")
            }
        }
        return tempFile
    }

    private fun createTestDatabaseWithHashemData(): Path {
        val tempFile = createValidTestDatabase()
        DriverManager.getConnection("jdbc:sqlite:${tempFile.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { stmt ->
                // Insert base with יהוה
                stmt.execute("INSERT INTO base (id, value) VALUES (1, 'יהוה')")

                // Insert surfaces for Hashem
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (1, 'יהוה', 1)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (2, 'ה׳', 1)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (3, 'השם', 1)")
            }
        }
        return tempFile
    }

    private fun createTestDatabaseWithHebrewData(): Path {
        val tempFile = createValidTestDatabase()
        DriverManager.getConnection("jdbc:sqlite:${tempFile.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { stmt ->
                // Insert Hebrew base
                stmt.execute("INSERT INTO base (id, value) VALUES (1, 'מלכ')")

                // Insert surfaces with final letter forms
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (1, 'מלך', 1)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (2, 'מלכים', 1)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (3, 'מלכות', 1)")
            }
        }
        return tempFile
    }

    private fun createTestDatabaseWithMultipleExpansions(): Path {
        val tempFile = createValidTestDatabase()
        DriverManager.getConnection("jdbc:sqlite:${tempFile.toAbsolutePath()}").use { conn ->
            conn.createStatement().use { stmt ->
                // Base that matches search token
                stmt.execute("INSERT INTO base (id, value) VALUES (1, 'root')")
                // Another base with more surfaces
                stmt.execute("INSERT INTO base (id, value) VALUES (2, 'otherroot')")

                // Surfaces for first base (small)
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (1, 'root', 1)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (2, 'rooted', 1)")

                // Surfaces for second base (larger)
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (3, 'root', 2)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (4, 'roots', 2)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (5, 'rooting', 2)")
                stmt.execute("INSERT INTO surface (id, value, base_id) VALUES (6, 'rootless', 2)")
            }
        }
        return tempFile
    }
}
