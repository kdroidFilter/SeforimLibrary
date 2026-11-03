package io.github.kdroidfilter.seforimlibrary.cache

/** A cache read and written from several threads at once: concurrent lookups of the same key may both load it. */
interface ConcurrentCache<K : Any, V : Any> {
    operator fun get(key: K): V?

    operator fun set(
        key: K,
        value: V,
    )
}

/** On the JVM and Android, ConcurrentHashMap; on iOS (no java.util.concurrent), cachemap's lock-free reads. */
expect fun <K : Any, V : Any> concurrentCache(): ConcurrentCache<K, V>
