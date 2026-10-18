package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Issue Otzaria/otzaria#2407: same-tractate refs read as in the Vilna print, line ids unchanged. Mirrors otzaria-library's test_get_from_export. */
class SefariaSameTractateRefsTest {

    @Test
    fun `drops only the own tractate right after lekaman and leil`() {
        val tractate = "בבא מציעא"
        assertEquals(
            "כדאמרינן (לקמן דף ט.) וכן (לעיל ב, א)",
            SefariaSameTractateRefs.strip("כדאמרינן (לקמן בבא מציעא דף ט.) וכן (לעיל בבא  מציעא ב, א)", tractate),
        )
        val untouched = "(לקמן בבא קמא דף ב.) (לקמן פרק א' בבא מציעא דף ג.) (לקמן בבא מציעאי)"
        assertEquals(untouched, SefariaSameTractateRefs.strip(untouched, tractate))
        assertEquals("(לְקַמָּן דף ד.)", SefariaSameTractateRefs.strip("(לְקַמָּן בָּבָא מְצִיעָא דף ד.)", tractate))
        assertEquals(untouched, SefariaSameTractateRefs.strip(untouched, null))
    }

    @Test
    fun `a pointed title matches plain text and gershayim match either form`() {
        assertEquals("(לעיל דף ה:)", SefariaSameTractateRefs.strip("(לעיל מכות דף ה:)", "מַכּוֹת"))
        assertEquals("(לקמן דף ג.)", SefariaSameTractateRefs.strip("(לקמן ע\"ז דף ג.)", "ע״ז"))
    }

    @Test
    fun `drops the own tractate inside the bracket after lekaman`() {
        assertEquals(
            "דפריך לקמן (דף ה:) ולעיל (סז, ב) ובמתני' לקמן [לט ע\"ב]",
            SefariaSameTractateRefs.strip("דפריך לקמן (שבת דף ה:) ולעיל (שבת סז, ב) ובמתני' לקמן [שבת לט ע\"ב]", "שבת"),
        )
        assertEquals(
            "(כלקמן נט, ב) (וכדלקמן מט, א) (להלן כו,א)",
            SefariaSameTractateRefs.strip("(כלקמן שבת נט, ב) (וכדלקמן שבת מט, א) (להלן שבת כו,א)", "שבת"),
        )
        assertEquals("(לעיל דף מה. ולקמן דף ס:)", SefariaSameTractateRefs.strip("(לעיל שבת דף מה. ולקמן שבת דף ס:)", "שבת"))
    }

    @Test
    fun `drops the own tractate before a daf address`() {
        assertEquals(
            "בפרק כירה (דף מד) ובפ' המצניע (ד' צג:) ובסוף פירקין (קנ\"ז ע\"ב)",
            SefariaSameTractateRefs.strip("בפרק כירה (שבת דף מד) ובפ' המצניע (שבת ד' צג:) ובסוף פירקין (שבת קנ\"ז ע\"ב)", "שבת"),
        )
        assertEquals("(קנז.) (טו, א) (פב ב') (פב)", SefariaSameTractateRefs.strip("(שבת קנז.) (שבת טו, א) (שבת פב ב') (שבת פב)", "שבת"))
    }

    @Test
    fun `keeps a tractate name that is not a daf reference`() {
        for (line in listOf(
            "(שבת שנטמא) [שבת] (שבת בלעם) (שבת שם) (גזרו שבת) (שבתות דף ב.) (שבת פ\"ג ה\"א)",
            "(לקמן פרק א' שבת דף ג.) (לקמן שבת)",
            "(לקמן מכות\u05c3 דף ה:)",
            "(שבת זו) (שבת זה) [שבת זו.] (שבת יה.) (שבת ך)",
        )) assertEquals(line, SefariaSameTractateRefs.strip(line, if ("מכות" in line) "מכות" else "שבת"))
    }

    @Test
    fun `keeps yerushalmi and tosefta of the same name`() {
        for (line in listOf(
            "ובירושלמי (שבת ג,א) גרסינן", "לו.<br>ובירושלמי (שבת ג,א)", "בתוספתא (שבת ג, א)", "ירושלמי [שבת דף ג.]",
            "בירושלמי <b>(שבת ג, א)</b>", "<b>ירושלמי</b> (שבת ג, א)", "בתוספתא<br> <i>(שבת דף ג.)</i>",
        )) assertEquals(line, SefariaSameTractateRefs.strip(line, "שבת"))
    }

    @Test
    fun `long runs of spaces or tags do not depend on the stack`() {
        val cases = listOf(
            "בירושלמי" + " ".repeat(1_000_000) + "(שבת ג,א)" to null,
            "בירושלמי" + "<b> ".repeat(250_000) + "(שבת ג,א)" to null,
            "בירושלמי" + "<b>".repeat(250_000) + "x (שבת דף ג.)" to "בירושלמי" + "<b>".repeat(250_000) + "x (דף ג.)",
            "(שבת" + " ".repeat(1_000_000) + "דף ב.)" to "(דף ב.)",
            "(לקמן שבת" + " ".repeat(1_000_000) + "דף ב.)" to "(לקמן" + " ".repeat(1_000_000) + "דף ב.)",
        )
        for ((line, expected) in cases) {
            var result: Result<String>? = null
            // A small stack: a regex that recurses per character would overflow here.
            val worker = Thread(null, { result = runCatching { SefariaSameTractateRefs.strip(line, "שבת") } }, "strip", 256L * 1024)
            worker.start()
            worker.join()
            assertEquals(expected ?: line, result!!.getOrThrow())
        }
    }

    @Test
    fun `many references in one line`() {
        val line = "(שבת דף ב.) לקמן (שבת ג.) (לעיל שבת דף ד:) ".repeat(2000)
        assertEquals("(דף ב.) לקמן (ג.) (לעיל דף ד:) ".repeat(2000), SefariaSameTractateRefs.strip(line, "שבת"))
    }

    @Test
    fun `daf numbers are spelled the standard way and stop at the last daf of the Bavli`() {
        assertTrue(SefariaSameTractateRefs.isDafNumber("קע\"ו"))
        assertTrue(SefariaSameTractateRefs.isDafNumber("טז"))
        assertTrue(SefariaSameTractateRefs.isDafNumber("קט\"ו"))
        for (word in listOf("זו", "זה", "יה", "יו", "ך", "לך")) assertTrue(!SefariaSameTractateRefs.isDafNumber(word), word)
        assertTrue(!SefariaSameTractateRefs.isDafNumber("קעז"))
        assertTrue(!SefariaSameTractateRefs.isDafNumber("שם"))
        assertTrue(!SefariaSameTractateRefs.isDafNumber("בלע"))
    }

    private fun strings(values: List<String>) = JsonArray(values.map(::JsonPrimitive))

    private val comments = listOf(
        "כדאמרינן (לקמן בבא מציעא דף ט.) ושם",
        "דיבור – פירוש (לעיל בבא מציעא ב, א)",
        "בלי הפניה כלל",
    )

    private fun fixture(root: Path) {
        Files.createDirectories(root.resolve("schemas"))
        Files.createDirectories(root.resolve("json"))
        for ((en, he, categories, base) in listOf(
            listOf("Bava Metzia", "בבא מציעא", "Talmud/Bavli", null),
            listOf("Mishnah Bava Metzia", "משנה בבא מציעא", "Mishnah/Seder Nezikin", null),
            listOf("Fixture on Bava Metzia", "פירוש על בבא מציעא", "Talmud/Bavli/Rishonim on Talmud", "Bava Metzia"),
            listOf("Fixture on Mishnah Bava Metzia", "פירוש על משנה בבא מציעא", "Mishnah/Commentary", "Mishnah Bava Metzia"),
        )) {
            val schema = buildJsonObject {
                put("title", en); put("heTitle", he)
                put("schema", buildJsonObject {
                    put("title", en); put("heTitle", he); put("nodeType", "JaggedArrayNode"); put("depth", 2)
                    put("sectionNames", strings(listOf("Daf", "Comment")))
                    put("heSectionNames", strings(listOf("דף", "פירוש")))
                    put("addressTypes", strings(listOf("Talmud", "Integer")))
                })
                put("categories", strings(categories!!.split('/')))
                if (base != null) { put("dependence", "Commentary"); put("base_text_titles", strings(listOf(base))) }
            }
            Files.writeString(root.resolve("schemas/${en!!.replace(' ', '_')}.json"), schema.toString())
            val text = if (base == null) listOf("מקור") else comments
            val merged = buildJsonObject {
                put("title", en); put("heTitle", he); put("language", "he")
                put("versions", JsonArray(listOf(strings(listOf("Fixture Edition", "")))))
                put("text", JsonArray(listOf(JsonArray(emptyList()), JsonArray(emptyList()), strings(text))))
            }
            Files.writeString(Files.createDirectories(root.resolve("json/$en")).resolve("merged.json"), merged.toString())
        }
    }

    @Test
    fun `a Bavli commentary stores the Vilna form under its Sefaria line keys`() = runBlocking {
        val root = Files.createTempDirectory("sefaria-same-tractate")
        try {
            fixture(root)
            val json = Json { ignoreUnknownKeys = true }
            val reader = SefariaBookPayloadReader(json, Logger.withTag("SameTractateRefsTest"))
            val payloads = reader.readBooksInParallel(root.resolve("json"), root.resolve("schemas"), reader.buildSchemaLookup(root.resolve("schemas")))
            val bavli = payloads.single { it.enTitle == "Fixture on Bava Metzia" }
            val mishnah = payloads.single { it.enTitle == "Fixture on Mishnah Bava Metzia" }
            assertEquals("בבא מציעא", bavli.sameTractateHeTitle)
            assertNull(mishnah.sameTractateHeTitle)
            assertTrue(mishnah.lines.any { "(לקמן בבא מציעא דף ט.)" in it })

            // The pipeline before the fix: the same walk with nothing stripped.
            val schemaObj = json.parseToJsonElement(Files.readString(root.resolve("schemas/Fixture_on_Bava_Metzia.json"))).jsonObject["schema"]!!.jsonObject
            val old = reader.walkTextWithSchema(
                schemaObj, JsonArray(listOf(JsonArray(emptyList()), JsonArray(emptyList()), strings(comments))),
                bavli.heTitle, bavli.enTitle,
            )
            val oldPayload = bavli.copy(
                lines = old.lines, cleanShiftByLineIndex = old.cleanShifts, lineKeyHashOverrides = old.lineKeyHashOverrides,
            ).precomputeLineData()

            assertEquals(old.lines.size, bavli.lines.size)
            assertEquals(old.lines.map { it.replace("בבא מציעא ", "") }, bavli.lines)
            val newKeys = bavli.precomputed!!.lineKeyHashes!!
            val oldKeys = oldPayload.precomputed!!.lineKeyHashes!!
            for (idx in bavli.lines.indices) assertContentEquals(oldKeys[idx], newKeys[idx], "line $idx must keep its key")
            for (idx in bavli.lines.indices) {
                if (bavli.lines[idx] != old.lines[idx]) assertTrue(lineWasModifiedByCleaning(bavli.cleanShiftByLineIndex.getValue(idx)))
            }
        } finally {
            Files.walk(root).use { paths -> paths.sorted(java.util.Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }
}
