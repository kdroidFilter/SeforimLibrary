package io.github.kdroidfilter.seforimlibrary.dao.repository

import platform.Foundation.NSLock

// Thread-safe map for native: backing map by delegation, with direct get/put operations
// guarded by an NSLock.
private class NsLockMutableMap<K, V>(
    private val backing: MutableMap<K, V> = LinkedHashMap(),
) : MutableMap<K, V> by backing {
    private val lock = NSLock()

    override fun get(key: K): V? {
        lock.lock()
        try {
            return backing[key]
        } finally {
            lock.unlock()
        }
    }

    override fun put(key: K, value: V): V? {
        lock.lock()
        try {
            return backing.put(key, value)
        } finally {
            lock.unlock()
        }
    }
}

internal actual fun <K, V> newConcurrentMap(): MutableMap<K, V> = NsLockMutableMap()
