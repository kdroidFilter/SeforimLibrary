package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.notes.InlineNotes
import io.github.kdroidfilter.seforimlibrary.core.text.normalizeCategoryPathSegment

internal fun sanitizeFolder(name: String?): String {
    if (name.isNullOrBlank()) return ""
    return normalizeCategoryPathSegment(name)
}

// Legacy Otzar HaChochma style/format markers that Sefaria did not strip when
// ingesting some books (e.g. Maaseh Rokeach, Malbim). Pattern is `@NN<text>}`
// where NN is a two-digit style code. We keep the inner text, drop the marker.
private val OTZAR_MARKUP_REGEX = Regex("""@\d{2}([^}]*)\}""")

private val REPEATED_SPACES_REGEX = Regex(" {2,}")
private val HTML_LINE_BREAK_REGEX = Regex("""<br\s*/?>""", RegexOption.IGNORE_CASE)

// In almost every Sefaria book an inline `<br>` is real structure (paragraphs,
// a bold heading's own line). These books break mid-sentence instead, into short
// lines that read as broken prose, so their inline breaks collapse to spaces.
private val COLLAPSE_INLINE_BREAK_BOOKS = setOf(
    "Tikkunei Zohar",
    // Koren's print line layout (~50 characters a line).
    "The Koren Rosh HaShana Mahzor; Ashkenaz",
    "The Koren Yom Kippur Mahzor; Ashkenaz",
    "The Koren Shalem Siddur; Ashkenaz",
)

internal fun collapsesInlineBreaks(bookEnTitle: String): Boolean =
    bookEnTitle in COLLAPSE_INLINE_BREAK_BOOKS

// A `<br>` at the *end* of a line is not an in-paragraph break: Sefaria's MAM
// Tanach emits it after `{פ}` to mark an open parasha (פרשה פתוחה), which the
// reader renders as the break before the next verse. Collapsing it swallowed
// that break, so only breaks with text after them are collapsed.
private val TRAILING_HTML_LINE_BREAK_REGEX =
    Regex("""(?:\s*<br\s*/?>)+\s*$""", RegexOption.IGNORE_CASE)

// Ref prefixes of sections whose footnotes are a translator's apparatus rather
// than part of the printed sefer. Noda BiYehudah I's Orach Chaim comes from
// Harold Landa's edition, whose footnotes mix English notes with Hebrew
// sources followed by Sefaria's English translations (kdroidFilter/Zayit#439).
private val TRANSLATOR_FOOTNOTE_REF_PREFIXES = listOf("Noda BiYehudah I, Orach Chaim,")

internal fun hasTranslatorFootnotes(ref: String): Boolean =
    TRANSLATOR_FOOTNOTE_REF_PREFIXES.any { ref.startsWith(it) }

// Books whose segments already open with their printed marker ("(יד) נהגו לתייג"),
// so the generated "(letter) " segment label doubles it. The Mishnah Berurah's
// kuntresim (e.g. Mishnat Soferim in siman 36) have no seif katan at all, so a
// generated label there is wrong rather than redundant (kdroidFilter/SeforimLibrary#73, #74).
private val SELF_LABELED_SEGMENT_BOOKS = setOf("Mishnah Berurah")

internal fun hasSelfLabeledSegments(bookEnTitle: String): Boolean = bookEnTitle in SELF_LABELED_SEGMENT_BOOKS

// Books whose unnamed ("Integer") segments are the printed numbered paragraphs (אותיות)
// readers navigate by: each segment gets its "(letter) " label and its own TOC entry
// (kdroidFilter/Zayit#509).
private val NUMBERED_PARAGRAPH_BOOKS = setOf(
    "Bereshit Rabbah",
    "Shemot Rabbah",
    "Vayikra Rabbah",
    "Bamidbar Rabbah",
    "Devarim Rabbah",
    "Ruth Rabbah",
    "Esther Rabbah",
    "Eikhah Rabbah",
    "Kohelet Rabbah",
    "Shir HaShirim Rabbah",
)

internal fun hasNumberedParagraphs(bookEnTitle: String): Boolean = bookEnTitle in NUMBERED_PARAGRAPH_BOOKS

// Some simanim of the Mishnah Berurah (494-529) mark the seif katan as "{א}"
// instead of the printed "(א)".
private val BRACED_SEGMENT_MARKER_REGEX = Regex("""^\{([א-ת]{1,4})\}""")

internal fun normalizeSegmentMarker(line: String): String = BRACED_SEGMENT_MARKER_REGEX.replace(line, "($1)")

/** Drops footnote markers and bodies; bodies may nest `<i>` tags, so closing tags are matched by depth. */
internal fun stripSefariaFootnotes(line: String): String = InlineNotes.stripMarkersAndBodies(line)

internal fun cleanSefariaLine(
    raw: String,
    collapseInlineBreaks: Boolean = false,
    stripFootnotes: Boolean = false,
): String {
    var s = if (raw.contains('\n')) raw.replace("\n", "") else raw
    if (stripFootnotes) {
        s = stripSefariaFootnotes(s)
    }
    if (OTZAR_MARKUP_REGEX.containsMatchIn(s)) {
        s = OTZAR_MARKUP_REGEX.replace(s, "$1")
    }
    s = normalizeLineBreaks(s, inlineBreak = if (collapseInlineBreaks) " " else "<br>")
    // Inline any Sefaria textimages as base64 data URIs (no-op if the embedder
    // hasn't been prefetched or the line contains no such URL).
    s = SefariaImageEmbedder.substituteImages(s)
    return s
}

/** [line] with every inline `<br>` as a space, retaining a terminal break. */
internal fun collapseInlineLineBreaks(line: String): String = normalizeLineBreaks(line, inlineBreak = " ")

private fun normalizeLineBreaks(line: String, inlineBreak: String): String {
    if ('<' !in line || !HTML_LINE_BREAK_REGEX.containsMatchIn(line)) return line
    // Kotlin's trim() treats Unicode separators (for example NBSP) as
    // whitespace, while java.util.regex \s does not. Preserve the old cleaner's
    // behavior for a break-only line before retaining any structural <br>.
    if (HTML_LINE_BREAK_REGEX.replace(line, "").isBlank()) return ""
    val trailing = TRAILING_HTML_LINE_BREAK_REGEX.find(line)
    val body = if (trailing != null) line.substring(0, trailing.range.first) else line
    val s = HTML_LINE_BREAK_REGEX.replace(body, inlineBreak).replace(REPEATED_SPACES_REGEX, " ").trim()
    return if (trailing != null && s.isNotEmpty()) "$s<br>" else s
}

// Hebrew label Sefaria's aliyah section name maps to. Named so the alt-TOC
// builder can recognise an aliyah level and re-label it by ordinal.
internal const val ALIYAH_SECTION_LABEL = "עליה"

// The seven weekly aliyot are read by their ordinal position (ראשון, שני, …),
// which is how a חומש names them, rather than "עליה" + gematria. Returns the
// ordinal label for a 1-based aliyah index, or null outside the customary range
// so the caller falls back to its generic "<base> <gematria>" label.
private val ALIYAH_ORDINALS = listOf(
    "ראשון", "שני", "שלישי", "רביעי", "חמישי", "שישי", "שביעי"
)

internal fun aliyahOrdinalLabel(index: Int): String? = ALIYAH_ORDINALS.getOrNull(index - 1)

/**
 * Maps common Sefaria English section/address names to their Hebrew equivalents.
 * Used when a schema only exposes `sectionNames` (English) without the
 * matching `heSectionNames`, so the generated TOC labels fall back gracefully
 * instead of showing English strings mid-Hebrew UI.
 *
 * Returns the original string if no mapping applies, or `null` for blank input
 * and for unnamed levels: Sefaria uses "Integer" as a placeholder name and shows
 * those sections by their number alone (its Hebrew refs read "א׳:ב׳", and
 * "Integer" has no Hebrew term in its dictionary).
 */
internal fun mapSectionNameToHebrew(base: String?): String? {
    if (base.isNullOrBlank()) return null
    val norm = base.lowercase().trim('\'', '"', ' ')
    return when {
        // Short tokens are matched exactly: as substrings they would hit unrelated names.
        norm == "integer" -> null
        norm == "ot" -> "אות"                // מדרש ילמדנו
        norm == "dh" -> "דיבור המתחיל"
        norm == "vav" -> "וו"                // ספר יראים (ווים ועמודים)
        "aliyah" in norm || "aliya" in norm -> ALIYAH_SECTION_LABEL
        "daf" in norm -> "דף"
        "chapter" in norm -> "פרק"
        "perek" in norm -> "פרק"
        "siman" in norm -> "סימן"
        // Sefaria ships this as "Se'if" (with an apostrophe); keep the plain form too.
        "seif" in norm || "se'if" in norm -> "סעיף"
        "section" in norm -> "סימן"
        "klal" in norm -> "כלל"
        "psalm" in norm -> "מזמור"
        "day" in norm -> "יום"
        "tikkun" in norm -> "תיקון"
        "parasha" in norm || "parsha" in norm -> "פרשה"
        "mishna" in norm -> "משנה"
        "halakha" in norm || "halacha" in norm -> "הלכה"
        "volume" in norm -> "כרך"
        "part" in norm -> "חלק"
        "verse" in norm || "pasuk" in norm -> "פסוק"
        "teshuva" in norm || "responsum" in norm || "responsa" in norm -> "תשובה"
        // Section names that Sefaria exposes only in English (the book's
        // heSectionNames is blank/absent), so the TOC/alt-TOC would otherwise
        // show English mid-Hebrew. Each Hebrew value is verified against the
        // book's schema and source text — see SefariaEnglishSectionNamesTest.
        "sheilta" in norm -> "שאילתא"       // שאילתות דרב אחאי גאון, העמק שאלה
        "maayan" in norm -> "מעיין"          // חסד לאברהם
        "nahar" in norm -> "נהר"             // חסד לאברהם
        "shoket" in norm -> "שוקת"           // חסד לאברהם
        "shaar" in norm -> "שער"             // שערי קדושה
        "treatise" in norm -> "שער"          // יסוד מורא ("אסדר שנים עשר שערים")
        "shorash" in norm -> "שורש"          // ביאור על ספר המצוות לרס"ג
        "manuscript" in norm -> "כתב יד"     // ליקוטי מוהר"ן
        "epistle" in norm -> "אגרת"          // נועם אלימלך (אגרת הקודש)
        "comment" in norm -> "פירוש"         // נועם אלימלך
        "question" in norm -> "שאלה"         // אגרת רב שרירא גאון
        "principle" in norm -> "עיקר"        // ספר הבחור ("שלש עשרה עקרים")
        "page" in norm -> "עמוד"             // גנזי מצרים, הלכות ספר תורה
        "word" in norm -> "מילה"             // מחברת מנחם
        "room" in norm -> "חדר"              // רב פנינים על משלי
        "paragraph" in norm -> "פסקה"
        "line" in norm -> "שורה"
        "column" in norm -> "טור"
        "folio" in norm -> "דף"
        "segment" in norm -> "קטע"
        "footnote" in norm -> "הערה"
        "pararaph" in norm -> "פסקה"
        "passuk" in norm -> "פסוק"
        "tosefta" in norm -> "תוספתא"
        "midrash" in norm -> "מדרש"
        "drush" in norm -> "דרוש"
        "remez" in norm -> "רמז"
        "inyan" in norm -> "ענין"
        "mitzvah" in norm -> "מצוה"
        "piyyut" in norm -> "פיוט"
        "hadran" in norm -> "הדרן"
        "kovetz" in norm -> "קובץ"
        // Generic English words are matched exactly so they cannot catch longer names.
        norm == "gate" || "sha'ar" in norm -> "שער"
        norm == "essay" -> "מאמר"
        norm == "statement" -> "סימן"
        norm == "letter" -> "אות"
        norm == "chamber" -> "חדר"
        norm == "window" -> "חלון"
        norm == "book" -> "ספר"
        else -> base
    }
}

internal fun normalizeTitleKey(value: String?): String? {
    if (value.isNullOrBlank()) return null

    // Normalize various quote styles (ASCII and Hebrew) so titles that differ
    // only by גרש/גרשיים or straight quotes map to the same key.
    val withoutQuotes = value
        .replace("\"", "")
        .replace("'", "")
        .replace("\u05F3", "") // Hebrew geresh
        .replace("\u05F4", "") // Hebrew gershayim

    return withoutQuotes
        .lowercase()
        .replace("\\s+".toRegex(), " ")
        .replace('_', ' ')
        .trim()
}
