package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SefariaCategoryOrderTest {
    @Test
    fun bavliReferenceCategoryIsOrderedAfterCommentators() {
        val root = Files.createTempDirectory("toc")
        Files.writeString(
            root.resolve("table_of_contents.json"),
            """
            [{"category":"Talmud","heCategory":"תלמוד","order":5,"contents":[
              {"category":"Bavli","heCategory":"בבלי","order":1,"contents":[
                {"category":"Reference","heCategory":"ספרות עזר","order":40},
                {"category":"Rishonim on Talmud","heCategory":"ראשונים","order":45},
                {"category":"Minor Tractates Commentaries","heCategory":"מפרשים על המסכתות הקטנות","order":60}
              ]}
            ]}]
            """.trimIndent(),
        )

        val orders = parseTableOfContentsMetadata(root, Json, Logger.withTag("test")).categoryOrders

        assertEquals(45, orders["תלמוד בבלי/ראשונים"])
        assertEquals(65, orders["תלמוד בבלי/ספרות עזר"])
        assertTrue(orders.getValue("תלמוד בבלי/ספרות עזר") > orders.getValue("תלמוד בבלי/מפרשים על המסכתות הקטנות"))
    }
}
