package io.github.kdroidfilter.seforimlibrary.dao

import java.util.concurrent.ConcurrentHashMap

internal actual fun <K : Any, V : Any> concurrentCache(): ConcurrentCache<K, V> =
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
