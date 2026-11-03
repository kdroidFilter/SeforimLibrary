package io.github.kdroidfilter.seforimlibrary.packaging

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CompactDbTest {
    private val texts =
        List(300) { i ->
            when (i % 3) {
                0 -> "<b>אמר רבי יוחנן</b> משום רבי שמעון בן יוחי, שורה $i: " + "ואמר רבא ".repeat(i % 20)
                1 -> "א"
                else -> "בראשית ברא אלהים את השמים ואת הארץ $i"
            }
        }

    @Test
    fun `the compact database reads as the generator's, its texts compressed`() {
        val dir = Files.createTempDirectory("compactdb")
        val source = dir.resolve("seforim.db")
        val target = dir.resolve("seforim.compact.db")
        // The generator's database: the schema, plain TEXT lines (as the databases built so far store them)
        JdbcSqliteDriver("jdbc:sqlite:$source").use { SeforimRepository(source.toString(), it) }
        DriverManager.getConnection("jdbc:sqlite:$source").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSERT INTO category(id, title, level) VALUES (1, 'תנ\"ך', 0)")
                st.execute("INSERT INTO source(id, name) VALUES (1, 'otzaria')")
                st.execute("INSERT INTO book(id, categoryId, sourceId, title, totalLines) VALUES (1, 1, 1, 'בראשית', ${texts.size})")
                st.execute("INSERT INTO connection_type(id, name) VALUES (1, 'COMMENTARY')")
            }
            conn.prepareStatement("INSERT INTO line(id, bookId, lineIndex, content, heRef, charCount) VALUES (?, 1, ?, ?, ?, ?)").use { ps ->
                texts.forEachIndexed { i, text ->
                    ps.setLong(1, i + 1L)
                    ps.setInt(2, i)
                    ps.setString(3, text)
                    ps.setString(4, "בראשית א, ${i + 1}")
                    ps.setInt(5, text.length)
                    ps.executeUpdate()
                }
            }
            conn.createStatement().use { st ->
                st.execute(
                    "INSERT INTO link(id, sourceBookId, targetBookId, sourceLineId, targetLineId, targetLineIndex, " +
                        "targetBookOrderIndex, connectionTypeId) VALUES (1, 1, 1, 1, 4, 3, 999, 1)",
                )
            }
        }

        compactDb(source, target, pageSize = 8192)

        DriverManager.getConnection("jdbc:sqlite:$target").use { conn ->
            fun strings(sql: String) = conn.createStatement().use { st -> st.executeQuery(sql).use { rs -> buildList { while (rs.next()) add(rs.getString(1)) } } }
            assertEquals(listOf("8192"), strings("PRAGMA page_size"))
            assertTrue("idx_line_heref" !in strings("SELECT name FROM sqlite_master WHERE type = 'index'"))
            assertTrue(strings("SELECT name FROM sqlite_master WHERE name LIKE 'sqlite_stat%'").isEmpty())
            assertEquals(listOf("blob"), strings("SELECT typeof(content) FROM line WHERE id = 1"))
            assertEquals(listOf("1"), strings("SELECT COUNT(*) FROM link"))
        }
        JdbcSqliteDriver("jdbc:sqlite:$target").use { driver ->
            val repository = SeforimRepository(target.toString(), driver)
            runBlocking {
                val lines = repository.getLines(1, 0, texts.size - 1)
                assertEquals(texts, lines.map { it.content })
                assertEquals("בראשית א, 1", lines.first().heRef)
                assertEquals(texts[3], repository.getCommentariesForLines(listOf(1)).single().targetText)
                assertEquals(texts.map { it.length }, repository.getBookCharCounts(1).toList())
            }
        }
        assertTrue(Files.size(target) > 0)
        dir.toFile().deleteRecursively()
    }
}
