package io.github.kdroidfilter.seforimlibrary.search

import com.fleeksoft.ksoup.Ksoup
import com.fleeksoft.ksoup.safety.Safelist
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking

// Ksoup is jsoup's Kotlin port: the same cleaning as the JVM and Android
internal actual fun escapedText(html: String): String = Ksoup.clean(html, Safelist.none())

internal actual fun <T> runBlockingIo(block: suspend CoroutineScope.() -> T): T = runBlocking(block = block)
