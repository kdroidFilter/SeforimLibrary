package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist

internal actual fun escapedText(html: String): String = Jsoup.clean(html, Safelist.none())

internal actual fun <T> runBlockingIo(block: suspend CoroutineScope.() -> T): T = runBlocking(block = block)
