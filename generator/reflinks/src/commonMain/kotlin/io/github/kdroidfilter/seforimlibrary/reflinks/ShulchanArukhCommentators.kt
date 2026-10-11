package io.github.kdroidfilter.seforimlibrary.reflinks

/**
 * Commentaries on the Tur / Shulchan Arukh, addressed by siman. A citation like "הט\"ז (או\"ח סי' רנא)" names the
 * commentary just before the parentheses: its candidate is offered to the arbitration next to the Shulchan Arukh.
 */
object ShulchanArukhCommentators {

    /**
     * How a commentary's heRef starts for a given part: [title] is the book title, and when [partSection] is true the
     * part is the first heRef section ("בית יוסף, אורח חיים, לו, …") rather than part of the title.
     */
    data class Target(val title: String, val partSection: Boolean = false)

    private data class Commentator(val names: List<String>, val targets: Map<String, Target>)

    private const val OC = "אורח חיים"
    private const val YD = "יורה דעה"
    private const val EH = "אבן העזר"
    private const val CM = "חושן משפט"
    private val allParts = listOf(OC, YD, EH, CM)

    private fun perPart(vararg parts: String, title: (String) -> String) = parts.associateWith { Target(title(it)) }
    private fun sectioned(title: String, vararg parts: String = allParts.toTypedArray()) =
        parts.associateWith { Target(title, partSection = true) }

    private val commentators = listOf(
        Commentator(listOf("ט\"ז", "טו\"ז", "טורי זהב"), perPart(OC, YD, EH, CM) { "טורי זהב על שולחן ערוך $it" }),
        Commentator(listOf("ש\"ך", "שפתי כהן"), perPart(YD, CM) { "שפתי כהן על שולחן ערוך $it" }),
        Commentator(listOf("מג\"א", "מגן אברהם"), mapOf(OC to Target("מגן אברהם"))),
        Commentator(listOf("מ\"ב", "משנ\"ב", "משנה ברורה"), mapOf(OC to Target("משנה ברורה"))),
        Commentator(listOf("ב\"י", "בית יוסף"), sectioned("בית יוסף")),
        Commentator(listOf("ד\"מ", "דרכי משה"), sectioned("דרכי משה")),
        Commentator(listOf("ב\"ח"), sectioned("ב\"ח")),
        Commentator(listOf("פרישה"), sectioned("פרישה")),
        Commentator(listOf("דרישה"), sectioned("דרישה")),
        Commentator(listOf("ב\"ש", "בית שמואל"), mapOf(EH to Target("בית שמואל"))),
        Commentator(listOf("ח\"מ", "חלקת מחוקק"), mapOf(EH to Target("חלקת מחוקק"))),
        Commentator(listOf("סמ\"ע", "מאירת עיניים"), mapOf(CM to Target("מאירת עיניים על שולחן ערוך חושן משפט"))),
        Commentator(listOf("גר\"א", "ביאור הגר\"א"), perPart(OC, YD, EH, CM) { "ביאור הגר\"א על שולחן ערוך $it" }),
        Commentator(listOf("פ\"ת", "פתחי תשובה"), perPart(YD, EH, CM) { "פתחי תשובה על שולחן ערוך $it" }),
        Commentator(listOf("שע\"ת", "שערי תשובה"), mapOf(OC to Target("שערי תשובה על שולחן ערוך אורח חיים"))),
        Commentator(
            listOf("באה\"ט", "באר היטב"),
            mapOf(OC to Target("באר היטב אורח חיים"), YD to Target("באר היטב יורה דעה"), EH to Target("באר היטב אבן העזר"), CM to Target("באר היטב חשן משפט")),
        ),
        Commentator(listOf("ערוה\"ש", "ערוך השולחן"), sectioned("ערוך השולחן")),
        Commentator(listOf("א\"ר", "אליה רבה"), mapOf(OC to Target("אליה רבה על שלחן ערוך אורח חיים"))),
        Commentator(listOf("כה\"ח", "כף החיים"), mapOf(OC to Target("כף החיים על שולחן ערוך אורח חיים"), YD to Target("כף החיים על שולחן ערוך יורה דעה"))),
        Commentator(listOf("קצוה\"ח", "קצות החושן", "קצות"), mapOf(CM to Target("קצות החושן על שולחן ערוך חושן משפט"))),
        Commentator(listOf("נתיבות", "נתה\"מ", "נה\"מ"), mapOf(CM to Target("נתיבות המשפט, ביאורים על שולחן ערוך חושן משפט"))),
    )

    private val byName: Map<String, Commentator> =
        commentators.flatMap { c -> c.names.map { HebrewText.nameKey(it) to c } }.toMap()
    private val maxWords = commentators.maxOf { c -> c.names.maxOf { it.split(' ').size } }
    private val prefixes = listOf("", "ה", "ב", "וב", "ו", "וה", "ד", "כ", "מ", "בה", "וכ", "כה", "מה")

    /** The commentary named at the end of [wordsBefore] (the words just before the citation), for [part]. */
    fun named(wordsBefore: List<String>, part: String): Target? {
        for (k in minOf(maxWords, wordsBefore.size) downTo 1) {
            val phrase = wordsBefore.takeLast(k).joinToString(" ").trimEnd(',', ':', '.')
            for (p in prefixes) {
                if (!phrase.startsWith(p) || phrase.length <= p.length + 1) continue
                byName[HebrewText.nameKey(phrase.removePrefix(p))]?.targets?.get(part)?.let { return it }
            }
        }
        return null
    }
}
