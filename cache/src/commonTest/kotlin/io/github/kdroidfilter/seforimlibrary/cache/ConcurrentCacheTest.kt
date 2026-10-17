package io.github.kdroidfilter.seforimlibrary.cache

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ConcurrentCacheTest {
    @Test
    fun storesAndReplacesValues() {
        val cache = concurrentCache<String, Int>()
        assertNull(cache["a"])
        cache["a"] = 1
        cache["a"] = 2
        assertEquals(2, cache["a"])
    }

    @Test
    fun keepsEveryValueWrittenFromSeveralThreads() =
        runBlocking {
            val cache = concurrentCache<Int, Int>()
            (0 until 8)
                .map { worker ->
                    async(Dispatchers.Default) { for (i in 0 until 1_000) cache[worker * 1_000 + i] = i }
                }.awaitAll()
            for (key in 0 until 8_000) assertEquals(key % 1_000, cache[key])
        }
}
