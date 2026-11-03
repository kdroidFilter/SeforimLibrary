package io.github.kdroidfilter.seforimlibrary.cache

import java.util.concurrent.ConcurrentHashMap

actual fun <K : Any, V : Any> concurrentCache(): ConcurrentCache<K, V> =
    object : ConcurrentCache<K, V> {
        private val map = ConcurrentHashMap<K, V>()

        override fun get(key: K): V? = map[key]

        override fun set(
            key: K,
            value: V,
        ) {
            map[key] = value
        }
    }
