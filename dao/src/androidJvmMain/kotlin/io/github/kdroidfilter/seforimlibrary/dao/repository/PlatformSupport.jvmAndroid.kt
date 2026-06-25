package io.github.kdroidfilter.seforimlibrary.dao.repository

// Shared by the JVM and Android targets (both JVM-based): java.* is on the classpath.
internal actual fun <K, V> newConcurrentMap(): MutableMap<K, V> = java.util.concurrent.ConcurrentHashMap()
