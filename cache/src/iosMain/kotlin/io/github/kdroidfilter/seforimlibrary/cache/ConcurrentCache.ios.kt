package io.github.kdroidfilter.seforimlibrary.cache

import io.github.charlietap.cachemap.cacheMapOf

actual fun <K : Any, V : Any> concurrentCache(): ConcurrentCache<K, V> =
    object : ConcurrentCache<K, V> {
        private val map = cacheMapOf<K, V>()

        override fun get(key: K): V? = map[key]

        override fun set(
            key: K,
            value: V,
        ) {
            map[key] = value
        }
    }
