package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndex

private const val SERVER_CLASS = "io.github.kdroidfilter.seforimlibrary.search.index.bridge.IndexBridgeServer"
private const val SERVER_METHOD = "call"

/** The calls to the Java runtime the iOS app embeds: `IndexBridgeServer.call`, through JNI, from the calling thread. */
fun EmbeddedJvm.bridgeTransport(): BridgeTransport = BridgeTransport { call(SERVER_CLASS, SERVER_METHOD, it) }

/** The [LuceneIndex] at [indexDir] on iOS: Lucene runs in the Java runtime the app embeds ([jvm]). */
fun JvmLuceneIndex(
    jvm: EmbeddedJvm,
    indexDir: String,
    cacheQueries: Boolean = false,
): LuceneIndex = BridgedLuceneIndex(jvm.bridgeTransport(), indexDir, cacheQueries)
