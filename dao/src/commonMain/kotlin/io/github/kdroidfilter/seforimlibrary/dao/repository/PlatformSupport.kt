package io.github.kdroidfilter.seforimlibrary.dao.repository

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers

/** Dispatcher for blocking DB I/O. */
internal val ioDispatcher: CoroutineDispatcher = Dispatchers.IO

/** Thread-safe map (ConcurrentHashMap on JVM/Android, NSLock-backed map on iOS). */
internal expect fun <K, V> newConcurrentMap(): MutableMap<K, V>
