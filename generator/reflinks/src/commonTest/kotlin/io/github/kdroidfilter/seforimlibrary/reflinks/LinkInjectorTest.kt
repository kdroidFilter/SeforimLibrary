package io.github.kdroidfilter.seforimlibrary.reflinks

import io.github.kdroidfilter.seforimlibrary.reflinks.LinkInjector.Link
import kotlin.test.Test
import kotlin.test.assertEquals

class LinkInjectorTest {

    private fun visible(html: String) = html.replace(Regex("<[^>]+>"), "")

    private fun linkOn(html: String, text: String, target: Long = 7, anchor: Boolean = false, llm: Boolean = false): Link {
        val start = visible(html).indexOf(text)
        return Link(start, start + text.length, target, anchor, llm)
    }

    @Test
    fun wrapsTheCitationInPlainText() {
        val html = "שנאמר (בראשית א, ד) וירא"
        val (out, n) = LinkInjector.inject(html, listOf(linkOn(html, "(בראשית א, ד)")))
        assertEquals("שנאמר <a class=\"ref\" href=\"seforim://line/7\">(בראשית א, ד)</a> וירא", out)
        assertEquals(1, n)
    }

    @Test
    fun keepsSurroundingTagsAndMarksArbitratedLinks() {
        val html = "<b>כדאיתא</b> (שבת כא:) <i>ע\"ש</i>"
        val (out, _) = LinkInjector.inject(html, listOf(linkOn(html, "(שבת כא:)", llm = true)))
        assertEquals("<b>כדאיתא</b> <a class=\"ref\" href=\"seforim://line/7\" data-llm=\"1\">(שבת כא:)</a> <i>ע\"ש</i>", out)
        assertEquals(visible(html), visible(out))
    }

    @Test
    fun rewritesAnExistingAnchor() {
        val html = "ראה <a href=\"otzaria://inline-link?path=x.txt&amp;index=3\">טז ז</a> שם"
        val (out, _) = LinkInjector.inject(html, listOf(linkOn(html, "טז ז", anchor = true)))
        assertEquals("ראה <a class=\"ref\" href=\"seforim://line/7\">טז ז</a> שם", out)
    }

    @Test
    fun skipsLinksCrossingATagAndOverlaps() {
        val html = "(שבת <b>כא:</b>) and (ב\"ק כב:)"
        val links = listOf(linkOn(html, "(שבת כא:)"), linkOn(html, "(ב\"ק כב:)"), linkOn(html, "(ב\"ק"))
        val (out, n) = LinkInjector.inject(html, links)
        assertEquals(1, n)
        assertEquals("(שבת <b>כא:</b>) and <a class=\"ref\" href=\"seforim://line/7\">(ב\"ק כב:)</a>", out)
    }
}
