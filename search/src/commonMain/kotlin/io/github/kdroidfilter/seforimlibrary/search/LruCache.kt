package io.github.kdroidfilter.seforimlibrary.search

import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized

/** A thread-safe cache of the [capacity] most recently used entries. */
internal class LruCache<K, V : Any>(
    private val capacity: Int,
) {
    private val lock = SynchronizedObject()

    // Insertion order is kept: a hit moves its entry to the end, so the first entry is the least recently used
    private val map = LinkedHashMap<K, V>()

    operator fun get(key: K): V? =
        synchronized(lock) {
            map.remove(key)?.also { map[key] = it }
        }

    operator fun set(
        key: K,
        value: V,
    ) {
        synchronized(lock) { putLocked(key, value) }
    }

    /** The value of [key], else [create]'s, stored; [create] runs under the lock, so it must be cheap. */
    fun getOrPut(
        key: K,
        create: () -> V,
    ): V =
        synchronized(lock) {
            map.remove(key)?.also { map[key] = it } ?: create().also { putLocked(key, it) }
        }

    fun clear() {
        synchronized(lock) { map.clear() }
    }

    private fun putLocked(
        key: K,
        value: V,
    ) {
        map.remove(key)
        map[key] = value
        if (map.size > capacity) map.remove(map.keys.first())
    }
}
