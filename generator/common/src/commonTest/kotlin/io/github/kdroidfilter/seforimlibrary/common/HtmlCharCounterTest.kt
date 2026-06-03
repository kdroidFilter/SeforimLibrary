package io.github.kdroidfilter.seforimlibrary.common

import kotlin.test.Test
import kotlin.test.assertEquals

class HtmlCharCounterTest {
    @Test
    fun `empty string returns zero`() {
        assertEquals(0, countVisibleChars(""))
    }

    @Test
    fun `plain text length equals char count`() {
        assertEquals(5, countVisibleChars("hello"))
    }

    @Test
    fun `tag content is skipped`() {
        // "<b>hi</b>" renders as "hi"
        assertEquals(2, countVisibleChars("<b>hi</b>"))
    }

    @Test
    fun `nested tags are skipped`() {
        // "<p><b>ab</b> <i>cd</i></p>" renders as "ab cd"
        assertEquals(5, countVisibleChars("<p><b>ab</b> <i>cd</i></p>"))
    }

    @Test
    fun `void tags consume zero chars`() {
        // "<br/>" is a void tag; text across it counts normally
        assertEquals(3, countVisibleChars("a<br/>bc"))
    }

    @Test
    fun `html entity counts as a single visible character`() {
        // "&nbsp;" is one visible char
        assertEquals(3, countVisibleChars("a&nbsp;b"))
    }

    @Test
    fun `ampersand without terminating semicolon still counts`() {
        // "a & b" renders as 5 visible chars: 'a', ' ', '&', ' ', 'b'.
        // Lone & falls through and counts as 1.
        assertEquals(5, countVisibleChars("a & b"))
    }

    @Test
    fun `hebrew characters count each`() {
        // Hebrew is UTF-16 BMP, each character is one code unit.
        assertEquals(5, countVisibleChars("<p>שלום</p>".replace("<p>", "<p> ")))
    }

    @Test
    fun `long mixed content matches visible length`() {
        val html = "<h1>Title</h1><p>Hello <b>world</b> &amp; welcome!</p>"
        // Visible: "Title" (5) + "Hello " (6) + "world" (5) + " " (1) + "& welcome!" (10) = 27
        assertEquals(27, countVisibleChars(html))
    }
}
