package io.github.kdroidfilter.seforimlibrary.search

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SeforimEmbedderTest {

    private fun cos(a: FloatArray, b: FloatArray): Float {
        var s = 0f
        for (i in a.indices) s += a[i] * b[i]
        return s
    }

    @Test
    fun embedderLoadsAndProducesUsableVectors() {
        val embedder = SeforimEmbedder.tryLoad()
        if (embedder == null) {
            println("[skip] no embedding model found (set -DseforimEmbedModelDir) — dense search disabled")
            return
        }
        embedder.use { e ->
            val q = e.embed("מה מברכים על אוכל")
            assertEquals(384, q.size, "embedding dim")

            // deterministic + normalized: same text twice -> cosine ~1
            val q2 = e.embed("מה מברכים על אוכל")
            assertTrue(cos(q, q2) > 0.999f, "self-cosine should be ~1.0")

            // sanity: a topically related text should be closer than an unrelated one
            val related = e.embed("ברכת הנהנין על פירות וירקות")
            val unrelated = e.embed("הלכות טומאה וטהרה של כלים")
            val cr = cos(q, related)
            val cu = cos(q, unrelated)
            println("[embedder] cos(related)=$cr  cos(unrelated)=$cu")
            assertTrue(cr > cu, "related ($cr) should be closer than unrelated ($cu)")
        }
    }
}
