package io.github.kdroidfilter.seforimlibrary.reflinks.arbitration

/**
 * Arbitration prompts. Bump [VERSION] whenever a prompt changes: it is part of the cache key, so old decisions are kept
 * but no longer reused. "v4" is the version measured on the holdout set (98.5 % link precision, 91.4 % recall).
 */
object Prompts {
    const val VERSION = "v4"

    private const val TAIL = """Abbreviations: או"ח/יו"ד/אה"ע/חו"מ = the four parts of Tur / Shulchan Arukh (also used by other books, e.g. responsa);
מ"א = מלכים א or מגן אברהם; מ"ב = מלכים ב or משנה ברורה.
Reply with JSON only: {"choice": <int>, "confidence": "high"|"medium"|"low"}"""

    val reference = """You resolve ONE explicit citation in a Hebrew rabbinic text (Talmud, Halakha, Midrash, commentaries).
You get a passage with the citation marked ⟦…⟧ and a numbered list of candidate target passages.
Decide which candidate the citation refers to, or 0 if none.

How candidates work: a candidate points to the FIRST segment of the unit named by the citation (the verse, or the chapter
when only a chapter is cited, the folio side, the siman/seif) and its snippet shows only that segment. A quote that
appears later in the same unit is still a match.

Rules, in order:
1. Commentator before the citation. If the words just before ⟦…⟧ name a commentator or a later work
   (רע"א, רעק"א, גר"א, ש"ך, ט"ז, סמ"ע, מג"א, פמ"ג, ב"ח, ב"י, ב"ש, רד"ק, רש"י, תוס', רשב"ם, בחיי, רמב"ן, מאירי, ר"ן, רא"ש,
   פיה"מ, פתחי תשובה, ערוך השולחן, any responsa), the citation points to THAT work: choose its candidate if offered, else 0.
   Exception: רמ"א, מחבר, שו"ע, ש"ע are the Shulchan Arukh itself.
2. Book name before the parentheses. When the citation relies on a book name written just before it, make sure that name
   is the biblical book itself and not part of another work's title (ספרא ויקרא, ספרי דברים, מדרש תהלים, זוהר ויקרא,
   נחלת יהושע, a commentary on the verse…). If it is another work, answer 0.
3. Talmud folio written פ"X. After a tractate name, "פ"X" (פ"ב, פ"ה., פ"ח:) is almost always folio 80+X (פ"ה = 85),
   not chapter X. Choose a Mishna-chapter candidate only when the text explicitly says פרק / בפרק / מתני' / משנה / תנן.
4. If quoted words clearly come from another place than every candidate, answer 0.
5. Otherwise use the topic and the source book genre.
$TAIL"""

    val ibid = """You resolve ONE "שם" (ibid) citation in a Hebrew rabbinic text.
You get a passage with the citation marked ⟦…⟧, the previous explicit citation found earlier in the text (a guess of what
שם refers to), and numbered candidate target passages. Decide which candidate the שם refers to, or 0 if none.

How candidates work: a candidate points to the FIRST segment of a unit (verse, chapter, folio side) and its snippet shows
only that segment; a quote later in the same unit still matches.

Rules, in order:
1. שם refers to the work cited most recently IN THE PASSAGE ITSELF before ⟦…⟧. If the passage names or cites another work
   after the given previous citation (another book, a Talmud page, a commentator like רש"י/תוס'/רמב"ם/רמ"א), שם refers to
   that other work → answer 0 unless a candidate is that work.
2. Require positive evidence for a candidate: a quote that belongs to it, or a clear continuation of the same discussion
   of the same work. In anthology entries ending with "(שם …)" the שם points to the source quoted in that entry, which is
   often not the previous citation. Without such evidence answer 0.
3. Bare (שם): choose the exact-place candidate if the passage refers to that very verse/place; choose the "same work,
   exact place unknown" candidate if it is the same work but another place; 0 if another work, or if "שם" is not a
   citation at all (e.g. part of a phrase or acronym).
$TAIL"""

    fun system(case: ArbitrationCase): String = if (case.kind == CaseKind.IBID) ibid else reference

    fun user(case: ArbitrationCase): String = buildString {
        appendLine("Source book: ${case.sourceBook}")
        appendLine("Passage: ${case.context}")
        case.antecedent?.let {
            appendLine("Previous citation in the text (what שם may refer to): ${it.citation} — in: ${it.excerpt}")
        }
        appendLine()
        appendLine("Candidates:")
        case.candidates.forEachIndexed { i, c ->
            val note = c.note?.let { " ($it)" }.orEmpty()
            appendLine("${i + 1}. ${c.heRef}$note: ${c.snippet}")
        }
        append("0. none of the above")
    }
}
