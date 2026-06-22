package io.github.kdroidfilter.seforimlibrary.dao.repository

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO

/** Dispatcher for blocking DB I/O. Testing whether Dispatchers.IO resolves directly in commonMain. */
internal val ioDispatcher: CoroutineDispatcher = Dispatchers.IO

/** Thread-safe map on JVM/Android (ConcurrentHashMap); plain map stub on native. */
internal expect fun <K, V> newConcurrentMap(): MutableMap<K, V>
