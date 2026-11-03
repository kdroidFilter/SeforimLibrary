package io.github.kdroidfilter.seforimlibrary.dao.text

import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LineTextCodecTest {
    private val dictionary = "בראשית ברא אלהים את השמים ואת הארץ <b>אמר רבי יוחנן</b> ".repeat(40).encodeToByteArray()
    private val codec = LineTextCodec { mapOf(LineTextDictionaries.CONTENT to dictionary) }

    @Test
    fun `compressed texts read back, long ones too`() {
        val texts = listOf("", "א", "אמר רבי יוחנן משום רבי שמעון בן יוחי", "<b>וַיֹּאמֶר</b> ".repeat(50_000))
        for (text in texts) assertEquals(text, codec.decode(codec.compress(text, LineTextDictionaries.CONTENT)))
    }

    @Test
    fun `the dictionary shrinks short lines`() {
        val line = "אמר רבי יוחנן בראשית ברא אלהים את השמים"
        assertTrue(codec.compress(line, LineTextDictionaries.CONTENT).size * 2 < line.encodeToByteArray().size)
    }

    @Test
    fun `plain texts read as they are, with an Android cursor's terminating NUL`() {
        val text = "ואת הארץ"
        assertEquals(text, codec.decode(codec.encode(text)))
        assertEquals(text, codec.decode(text.encodeToByteArray() + 0))
        assertContentEquals(text.encodeToByteArray(), codec.encode(text))
    }

    @Test
    fun `the repository reads compressed, plain and TEXT lines alike`() =
        runBlocking {
            val driver = JdbcSqliteDriver(JdbcSqliteDriver.IN_MEMORY)
            // Creates the schema; the repository that reads opens the filled database, as the app does
            SeforimRepository(":memory:", driver)
            driver.execute(null, "INSERT INTO line_codec_dict(id, data) VALUES (1, ?)", 1) { bindBytes(0, dictionary) }
            driver.execute(null, "INSERT INTO category(id, title, level) VALUES (1, 'c', 0)", 0)
            driver.execute(null, "INSERT INTO source(id, name) VALUES (1, 's')", 0)
            driver.execute(null, "INSERT INTO book(id, categoryId, sourceId, title) VALUES (1, 1, 1, 'b')", 0)
            val texts = listOf("אמר רבי יוחנן", "בראשית ברא", "ואת הארץ")
            val stored = listOf(codec.compress(texts[0], LineTextDictionaries.CONTENT), codec.encode(texts[1]))
            stored.forEachIndexed { i, bytes ->
                driver.execute(null, "INSERT INTO line(id, bookId, lineIndex, content) VALUES (?, 1, ?, ?)", 3) {
                    bindLong(0, i + 1L)
                    bindLong(1, i.toLong())
                    bindBytes(2, bytes)
                }
            }
            driver.execute(null, "INSERT INTO line(id, bookId, lineIndex, content) VALUES (3, 1, 2, '${texts[2]}')", 0)
            assertEquals(texts, SeforimRepository(":memory:", driver).getLines(1, 0, 2).map { it.content })
            val stored0 = driver.executeQuery(null, "SELECT typeof(content) FROM line WHERE id = 1", { c ->
                QueryResult.Value(if (c.next().value) c.getString(0) else null)
            }, 0).value
            assertEquals("blob", stored0)
            driver.close()
        }
}
