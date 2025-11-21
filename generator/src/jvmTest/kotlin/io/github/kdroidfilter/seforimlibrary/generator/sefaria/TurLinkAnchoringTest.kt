package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SefariaMergedText
import io.github.kdroidfilter.seforimlibrary.generator.sefaria.models.SefariaSchema
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Hardcoded regression checks for the first line(s) of Tur.
 *
 * The goal is to guarantee that CSV links targeting the very first
 * siman of the Tur (Orach Chayim 1:1) do not get attached to the
 * introduction line, and that the introduction continues to resolve
 * to line index 0.
 *
 * These tests run against the checked-in Sefaria dump under
 * build/sefaria, without having to regenerate the DB.
 */
class TurLinkAnchoringTest {
    private val sefariaRoot = File("build/sefaria")
    private val linksDir = sefariaRoot.resolve("links")
    private val mergedFile = sefariaRoot.resolve("json/Halakhah/Tur/Tur/merged.json")
    private val schemaFile = sefariaRoot.resolve("schemas/Tur.json")

    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
        coerceInputValues = true
    }

    private val merged: SefariaMergedText by lazy {
        assertTrue(mergedFile.exists(), "missing merged.json for Tur at ${mergedFile.absolutePath}")
        json.decodeFromString(SefariaMergedText.serializer(), mergedFile.readText())
    }

    private val schema: SefariaSchema by lazy {
        assertTrue(schemaFile.exists(), "missing schema for Tur at ${schemaFile.absolutePath}")
        json.decodeFromString(SefariaSchema.serializer(), schemaFile.readText())
    }

    private val textParser = SefariaTextParser()
    private val citationParser = SefariaCitationParser()

    // Flattened lines as the generator would produce them
    private val flattenedLines: List<String> by lazy {
        textParser.parse(bookId = 1L, mergedText = merged, schema = schema).lines.map { it.content }
    }

    // Manually verified expected content for the first Tur lines
    private val expectedIntroLine =
        "ברוך ה' אלהי ישראל אשר לו הגדולה והגבורה. לו נאה שיר ושבחה הלל וזמרה. עוז וממשלה. על כל אלהים מאוד נעלה. אם אומר אשמיע כל תהלתו ואהללה. לא אוכל כי אין בלשוני מלה. וכי הוא מרומם על כל ברכה ותהלה. ולו דומיה תהלה. שליט בכל דרי מטה ומעלה. כלם איש איש ממקומו ישתחוו לו בחילה. ראשון לראשונים אין לראשיתו ראש ותחילה. וגם את אחרונים הוא ואין לו קץ ותכלה. בראשית ברא את השמים ואת הארץ ובששת ימים עשה את מלאכתו עד אם כלה. וייצר את האדם עפר מאדמתו. את הכל עשה יפה בעתו בעבור ישראל ראשית תבואתו. ובעבור תורת קנינו ראשית נסיכתו. להנחיל לעם קדשו יעקב חבל נחלתו. וזווג ראשית עם ראשית. טוב אחרית דבר מראשיתו. זווגו יפה עלה. כי אמרת ה' צרופה מקושטת בעשרים וארבע ככלה:"

    private val expectedFirstSimanLine =
        " <i data-commentator=\"Bach\" data-order=\"1.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"1.1\"></i> יהודה בן תימא אומר הוי עז כנמר וקל כנשר רץ כצבי וגבור כארי לעשות רצון אביך שבשמים<i data-commentator=\"Hagahot\" data-order=\"1.1\"></i> <i data-commentator=\"Drisha\" data-order=\"1.1\"></i> פרט <i data-commentator=\"Prisha\" data-order=\"1.1\"></i> ארבעה דברים  <i data-commentator=\"Beit_Yosef\" data-order=\"2.1\"></i> בעבודת הבורא יתברך  <i data-commentator=\"Beit_Yosef\" data-order=\"3.1\"></i> והתחיל<i data-commentator=\"Hagahot\" data-order=\"2.1\"></i> בעז כנמר <i data-commentator=\"Prisha\" data-order=\"2.1\"></i> לפי שהוא כלל גדול בעבודת הבורא יתברך לפי שפעמים אדם חפץ לעשות מצוה ונמנע מלעשותה מפני בני אדם שמלעיגין עליו ועל כן הזהיר שתעיז פניך כנגד המלעיגין <i data-commentator=\"Prisha\" data-order=\"3.1\"></i> ואל תמנע מלעשות המצוה <i data-commentator=\"Prisha\" data-order=\"4.1\"></i>  <i data-commentator=\"Bach\" data-order=\"2.1\"></i> וכן א\"ר יוחנן בן זכאי לתלמידיו יהי רצון שתהא מורא שמים עליכם כמורא בשר ודם <i data-commentator=\"Prisha\" data-order=\"5.1\"></i> וכן <i data-commentator=\"Drisha\" data-order=\"2.1\"></i> הוא אומר לענין הבושה שפעמים אדם מתבייש מפני האדם יותר ממה שיתבייש מפני הבורא יתברך  <i data-commentator=\"Bach\" data-order=\"3.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"4.1\"></i> על כן הזהיר שתעיז מצחך כנגד המלעיגים ולא תבוש וכן אמר דוד ע\"ה ואדברה בעדותיך נגד מלכים ולא אבוש אף כי היה נרדף ובורח מן העו\"ג  <i data-commentator=\"Beit_Yosef\" data-order=\"5.1\"></i> היה מחזיק בתורתו ולומד אף כי היו מלעיגים עליו <i data-commentator=\"Prisha\" data-order=\"6.1\"></i>  <i data-commentator=\"Bach\" data-order=\"4.1\"></i> ואמר  <i data-commentator=\"Beit_Yosef\" data-order=\"6.1\"></i> קל כנשר כנגד ראות העין ודמה אותו לנשר כי כאשר הנשר שט באויר כך הוא ראות העין לומר שתעצים עיניך מראות ברע  <i data-commentator=\"Beit_Yosef\" data-order=\"7.1\"></i> כי היא תחלת העבירה שהעין רואה והלב חומד וכלי המעשה גומרין <i data-commentator=\"Drisha\" data-order=\"3.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"8.1\"></i> ואמר <i data-commentator=\"Prisha\" data-order=\"7.1\"></i> גבור כארי כנגד הלב  <i data-commentator=\"Beit_Yosef\" data-order=\"9.1\"></i> כי הגבורה בעבודת הבורא יתברך היא בלב ואמר שתחזק לבך בעבודתו <i data-commentator=\"Drisha\" data-order=\"4.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"10.1\"></i> ואמר רץ כצבי כנגד הרגלים <i data-commentator=\"Prisha\" data-order=\"8.1\"></i>  <i data-commentator=\"Bach\" data-order=\"5.1\"></i> שרגליך לטוב ירוצו <i data-commentator=\"Prisha\" data-order=\"9.1\"></i>  <i data-commentator=\"Bach\" data-order=\"6.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"11.1\"></i> וכן <i data-commentator=\"Drisha\" data-order=\"5.1\"></i> דוד המלך ע\"ה היה מתפלל על שלשתם<i data-commentator=\"Hagahot\" data-order=\"3.1\"></i> אלא ששינה הסדר ואמר הדריכני בנתיב מצותיך על הרגלים ואמר אח\"כ הט לבי ואמר אח\"כ העבר עיני מראות שוא  <i data-commentator=\"Bach\" data-order=\"7.1\"></i> והזכיר בלב הטיה ובעין העברה כי הלב הוא ברשותו להטותו בדרך הטובה או לרעה אף אחר שראה מעשה השוא על כן התפלל שיעזרנו להטותו לדרך הטובה אבל ראות השוא אינו ברשותו כי אפשר שיפגע בו פתאום ויראנו לכן התפלל שיעביר עיניו מראות שוא ולא יזמינהו לפניו כלל  <i data-commentator=\"Bach\" data-order=\"8.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"12.1\"></i> לכן צריך האדם להתגבר כארי לעמוד <i data-commentator=\"Darchei Moshe\" data-order=\"1.1\"></i> בבקר לעבודת בוראו ואף אם ישיאנו יצרו בחורף לאמר איך תעמוד בבקר כי הקור גדול או ישיאנו בקיץ לאמר איך תעמוד ממטתך ועדיין לא שבעת משנתך התגבר עליו לקום <i data-commentator=\"Prisha\" data-order=\"10.1\"></i> שתהא אתה מעורר השחר ולא יהא הוא מעירך כמו שאמר דוד ע\"ה <i data-commentator=\"Prisha\" data-order=\"11.1\"></i> עורה כבודי עורה הנבל וכנור אעירה שחר אני מעיר השחר ואין השחר מעיר אותי וכ\"ש אם ישכים קודם אור הבוקר לקום להתחנן לפני בוראו מה יופיו ומה טובו <i data-commentator=\"Prisha\" data-order=\"12.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"13.1\"></i> וטוב למי שמקדים שיכוין לשעות שמשתנות המשמרות שהן בשליש הלילה ולסוף ב' שלישי הלילה ולסוף הליל' שבאלו הזמנים הקב\"ה נזכר לחורבן הבית ופיזור ישראל בין העו\"ג והתפלה שיתפלל אדם באותה שעה על החורבן והפיזור רצוייה וקרובה להתקבל ויפיל תחנתו לפני המקו' א' המרב' וא' הממעיט ובלבד שיכוין לבו בתחנוניו כי טוב מעט בכוונה מהרבות בהם שלא בכוונה <i data-commentator=\"Prisha\" data-order=\"13.1\"></i>  <i data-commentator=\"Bach\" data-order=\"9.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"14.1\"></i> וטוב לו' פרשת העקידה <i data-commentator=\"Prisha\" data-order=\"14.1\"></i> ופרשת המן <i data-commentator=\"Drisha\" data-order=\"6.1\"></i> ועשרת <i data-commentator=\"Prisha\" data-order=\"15.1\"></i> הדברות <i data-commentator=\"Prisha\" data-order=\"16.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"15.1\"></i> ופרשת הקרבנות כגון פרשת העולה ומנחה ושלמים וחטאת ואשם  <i data-commentator=\"Bach\" data-order=\"10.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"16.1\"></i> אמנם פרשת הקרבנות טוב יותר לאומרה ביום שהם במקום הקרבת הקרבן שזמנו ביום וכשיסיים פרשת העולה <i data-commentator=\"Drisha\" data-order=\"7.1\"></i> יאמר רבון העולמים י\"ר מלפניך שיהא זה חשוב ומקובל לפניך כאילו הקרבתי עולה בזמנה <i data-commentator=\"Prisha\" data-order=\"17.1\"></i>  <i data-commentator=\"Bach\" data-order=\"11.1\"></i> וכן יאמר בפרשת המנחה והשלמים והאשם <i data-commentator=\"Prisha\" data-order=\"18.1\"></i>  <i data-commentator=\"Bach\" data-order=\"12.1\"></i>  <i data-commentator=\"Beit_Yosef\" data-order=\"17.1\"></i> ואחר פרשת החטאת <i data-commentator=\"Darchei Moshe\" data-order=\"3.1\"></i> לא יאמר כן לפי שאינה באה נדבה <i data-commentator=\"Drisha\" data-order=\"8.1\"></i>. ואם אינו יכול להשכים קודם אור הבוקר מכל מקום התפלה אשר היא מועד לכל חי אל יאחר אותה ויחשוב בלבו אילו היה בעבודת מלך בשר ודם וציוהו להשכים באור הבוקר לעבודתו <i data-commentator=\"Prisha\" data-order=\"19.1\"></i> היה זהיר וזריז לעמוד לעבודתו כאשר ציוהו כ\"ש וק\"ו בנו של קל וחומר לפני מלך מלכי המלכים הקב\"ה: "

    @Test
    fun `introduction stays on line zero and is linked as such`() {
        val targetCitation = "Tur, Orach Chayim, Introduction"
        assertNotNull(findCitationRow(targetCitation), "expected at least one link targeting $targetCitation in CSVs")

        val citation = citationParser.parse(targetCitation)
        assertNotNull(citation, "parser should handle introduction citation")

        val idx = lineIndexFromMerged(merged.text, citation)
        assertEquals(0, idx, "introduction must remain at line index 0")
        assertTrue(flattenedLines[idx].startsWith("ברוך"), "intro line should still be the opening bracha text")
        assertEquals(expectedIntroLine, flattenedLines[idx], "intro line text should match the verified Sefaria content")
    }

    @Test
    fun `first siman does not collapse onto intro line`() {
        val targetCitation = "Tur, Orach Chayim 1:1"
        assertNotNull(findCitationRow(targetCitation), "expected at least one link targeting $targetCitation in CSVs")

        val citation = citationParser.parse(targetCitation)
        assertNotNull(citation, "parser should handle siman citation")

        val idx = lineIndexFromMerged(merged.text, citation)
        // Eight introduction lines precede the first siman; enforce the offset.
        assertEquals(8, idx, "siman 1 should start after the 8 intro lines, not at line 0")
        assertTrue(flattenedLines[idx].contains("יהודה בן תימא"), "first siman line should contain the opening Tur text")
        assertEquals(expectedFirstSimanLine, flattenedLines[idx], "first siman line text should match the verified Sefaria content")
    }

    @Test
    fun `intro links match expected commentator set`() {
        val targetCitation = "Tur, Orach Chayim, Introduction"
        val links = collectLinks(targetCitation)
        assertEquals(1, links.size, "intro should only have one Sefaria link")
        assertEquals(setOf("Psalms"), links.keys, "intro link should originate from Psalms only")
        assertEquals(1, links["Psalms"])
    }

    @Test
    fun `first siman links match expected commentators and counts`() {
        val targetCitation = "Tur, Orach Chayim 1:1"
        val counts = collectLinks(targetCitation)

        val expected = mapOf(
            "Prisha, Orach Chayim" to 19,
            "Beit Yosef, Orach Chayim" to 17,
            "Bach, Orach Chayim" to 12,
            "Drisha, Orach Chayim" to 8,
            "Psalms" to 4,
            "Darkhei Moshe, Orach Chayim" to 2,
            "Pesachim" to 1,
            "Arukh HaShulchan, Orach Chaim" to 1,
            "Pirkei Avot" to 1,
            "Berakhot" to 1,
            "Shulchan Arukh, Orach Chayim" to 1
        )

        assertEquals(67, counts.values.sum(), "total links to Tur OC 1:1 should remain stable")
        assertEquals(expected, counts)
    }

    /**
     * Locate the first CSV row whose second column (Citation 2) matches the target.
     */
    private fun findCitationRow(targetCitation: String): List<String>? {
        assertTrue(linksDir.exists(), "links directory is missing at ${linksDir.absolutePath}")
        linksDir.listFiles { f -> f.isFile && f.name.endsWith(".csv") }?.forEach { file ->
            file.useLines { seq ->
                seq.drop(1).forEach { line ->
                    val cols = parseCsvLine(line)
                    if (cols.size >= 2 && cols[1] == targetCitation) {
                        return cols
                    }
                }
            }
        }
        return null
    }

    private fun collectLinks(targetCitation: String): Map<String, Int> {
        assertTrue(linksDir.exists(), "links directory is missing at ${linksDir.absolutePath}")
        val counts = linkedMapOf<String, Int>()
        linksDir.listFiles { f -> f.isFile && f.name.endsWith(".csv") }?.forEach { file ->
            file.useLines { seq ->
                seq.drop(1).forEach { line ->
                    val cols = parseCsvLine(line)
                    if (cols.size >= 4 && cols[1] == targetCitation) {
                        val source = cols[3]
                        counts[source] = counts.getOrDefault(source, 0) + 1
                    }
                }
            }
        }
        return counts
    }

    /**
     * Minimal CSV splitter matching the generator's link parsing.
     */
    private fun parseCsvLine(line: String): List<String> {
        val result = mutableListOf<String>()
        val current = StringBuilder()
        var inQuotes = false

        line.forEach { ch ->
            when {
                ch == '"' -> inQuotes = !inQuotes
                ch == ',' && !inQuotes -> {
                    result += current.toString()
                    current.clear()
                }
                else -> current.append(ch)
            }
        }
        result += current.toString()
        return result
    }

    /**
     * A test-only copy of the generator's merged.json traversal to get a line index.
     */
    private fun lineIndexFromMerged(
        root: JsonElement,
        citation: SefariaCitationParser.Citation
    ): Int {
        val sectionElement: JsonElement = if (citation.section != null && root is JsonObject) {
            root[citation.section] ?: run {
                if (citation.section.contains(",")) {
                    val parts = citation.section.split(",", limit = 2)
                    val mainSection = parts[0].trim()
                    val subSection = parts[1].trim()
                    val mainElement = root[mainSection] as? JsonObject
                    mainElement?.get(subSection)
                } else null
            }
        } else {
            root
        } ?: error("could not locate section ${citation.section} in merged.json")

        val offsetBeforeSubsection = if (citation.section?.contains(",") == true && root is JsonObject) {
            val parts = citation.section.split(",", limit = 2)
            val mainSection = parts[0].trim()
            val subSection = parts[1].trim()
            val mainElement = root[mainSection] as? JsonObject
            if (mainElement != null) calculateOffsetBeforeKey(mainElement, subSection) else 0
        } else 0

        val (targetArray, introLines) = unwrapSectionElement(sectionElement)
            ?: error("unexpected section structure for ${citation.section}")
        val idx = lineIndexInArray(targetArray, citation.references)
            ?: error("could not map refs ${citation.references} to an index")
        return offsetBeforeSubsection + introLines + idx
    }

    private fun calculateOffsetBeforeKey(obj: JsonObject, targetKey: String): Int {
        var offset = 0
        for ((key, value) in obj) {
            if (key == targetKey) break
            offset += countLinesInElement(value)
        }
        return offset
    }

    private fun unwrapSectionElement(element: JsonElement): Pair<JsonArray, Int>? {
        return when (element) {
            is JsonArray -> element to 0
            is JsonObject -> {
                if (element.containsKey("")) {
                    val introLines = element
                        .filterKeys { it.isNotEmpty() }
                        .values
                        .sumOf { countLinesInElement(it) }
                    val defaultVal = element[""] as? JsonArray ?: return null
                    defaultVal to introLines
                } else null
            }
            else -> null
        }
    }

    private fun lineIndexInArray(array: JsonArray, refs: List<Int>): Int? {
        if (refs.isEmpty()) return 0
        val targetIdx = refs.first() - 1
        if (targetIdx !in array.indices) return null
        val prefix = array.take(targetIdx).sumOf { countLinesInElement(it) }
        val child = array[targetIdx]
        if (refs.size == 1) return prefix

        return when (child) {
            is JsonArray -> {
                val inner = lineIndexInArray(child, refs.drop(1)) ?: return null
                prefix + inner
            }
            is JsonObject -> {
                val (innerArray, introLines) = unwrapSectionElement(child) ?: return null
                val inner = lineIndexInArray(innerArray, refs.drop(1)) ?: return null
                prefix + introLines + inner
            }
            else -> null
        }
    }

    private fun countLinesInElement(element: JsonElement): Int {
        return when (element) {
            is JsonPrimitive -> if (element.isString && element.content.isNotBlank()) 1 else 0
            is JsonArray -> element.sumOf { countLinesInElement(it) }
            is JsonObject -> {
                if (element.containsKey("")) {
                    val intro = element.filterKeys { it.isNotEmpty() }.values.sumOf { countLinesInElement(it) }
                    val defaultLines = element[""]?.let { countLinesInElement(it) } ?: 0
                    intro + defaultLines
                } else {
                    element.values.sumOf { countLinesInElement(it) }
                }
            }
            else -> 0
        }
    }
}
