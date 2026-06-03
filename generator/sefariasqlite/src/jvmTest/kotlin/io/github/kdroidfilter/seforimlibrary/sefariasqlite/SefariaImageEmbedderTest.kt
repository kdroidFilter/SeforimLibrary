package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit test for the in-memory replacement path of [SefariaImageEmbedder].
 * Network downloading + disk caching is covered end-to-end by the full
 * importer run (see SefariaDirectImporter.import).
 */
class SefariaImageEmbedderTest {
    @BeforeTest fun reset() = SefariaImageEmbedder.resetForTest()
    @AfterTest fun clean() = SefariaImageEmbedder.resetForTest()

    @Test
    fun substituteImagesReplacesKnownUrlsInImgTags() {
        val url = "https://textimages.sefaria.org/Tikkunei_Zohar/40.png"
        val dataUri = "data:image/png;base64,AAAA"
        SefariaImageEmbedder.seedForTest(url, dataUri)

        val input = """prefix <img src="$url"> middle <img src="$url" class="x"> end"""
        val out = cleanSefariaLine(input)

        assertTrue(out.contains("data:image/png;base64,AAAA"))
        assertFalse(out.contains(url), "original URL should be replaced")
    }

    @Test
    fun unknownUrlIsPreservedVerbatim() {
        // Embedder enabled but URL not in cache — leave it alone so it's
        // obvious (and fixable) instead of silently dropping content.
        SefariaImageEmbedder.seedForTest(
            "https://textimages.sefaria.org/known.png",
            "data:image/png;base64,Z"
        )
        val input = """<img src="https://textimages.sefaria.org/unknown.png">"""
        assertEquals(input, cleanSefariaLine(input))
    }

    @Test
    fun noOpWhenEmbedderDisabled() {
        // Without prefetch, substituteImages is a pass-through
        val input = """<img src="https://textimages.sefaria.org/x.png">"""
        assertEquals(input, cleanSefariaLine(input))
    }

    @Test
    fun compatibleWithOtherCleanSteps() {
        SefariaImageEmbedder.seedForTest(
            "https://textimages.sefaria.org/a.png",
            "data:image/png;base64,X"
        )
        // Otzar markup, <br>, and image embedding all in one line
        val input = """@04פתיחה} <br> <img src="https://textimages.sefaria.org/a.png"> text"""
        val out = cleanSefariaLine(input)
        assertTrue(out.startsWith("פתיחה"))
        assertFalse(out.contains("<br>"))
        assertTrue(out.contains("data:image/png;base64,X"))
    }
}
