package io.github.kdroidfilter.seforimlibrary.sefariasqlite

internal fun sanitizeFolder(name: String?): String {
    if (name.isNullOrBlank()) return ""
    return name.replace("\"", "״").trim()
}

// Legacy Otzar HaChochma style/format markers that Sefaria did not strip when
// ingesting some books (e.g. Maaseh Rokeach, Malbim). Pattern is `@NN<text>}`
// where NN is a two-digit style code. We keep the inner text, drop the marker.
private val OTZAR_MARKUP_REGEX = Regex("""@\d{2}([^}]*)\}""")

// Sefaria's merged.json for some books (most notably Tikkunei Zohar from daf
// יז onward) uses `<br>` tags to mark internal line breaks inside what is
// logically a single paragraph — typically piyut/poetry sections. The app
// renders each `<br>`-delimited fragment as its own short line, which
// regresses the legacy plain-text Otzaria experience where the paragraph was
// continuous. Collapse them to a single space so paragraphs read as prose.
private val HTML_LINE_BREAK_REGEX = Regex("""<br\s*/?>""", RegexOption.IGNORE_CASE)

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

// Some simanim of the Mishnah Berurah (494-529) mark the seif katan as "{א}"
// instead of the printed "(א)".
private val BRACED_SEGMENT_MARKER_REGEX = Regex("""^\{([א-ת]{1,4})\}""")

internal fun normalizeSegmentMarker(line: String): String = BRACED_SEGMENT_MARKER_REGEX.replace(line, "($1)")

private val FOOTNOTE_MARKER_REGEX = Regex("""<sup class="footnote-marker">.*?</sup>""")
private const val FOOTNOTE_OPEN = """<i class="footnote">"""
private val ITALIC_TAG_REGEX = Regex("""<i\b[^>]*>|</i>""", RegexOption.IGNORE_CASE)

/** Drops footnote markers and bodies; bodies may nest `<i>` tags, so closing tags are matched by depth. */
internal fun stripSefariaFootnotes(line: String): String {
    if (!line.contains(FOOTNOTE_OPEN)) return line
    val withoutMarkers = FOOTNOTE_MARKER_REGEX.replace(line, "")
    val out = StringBuilder(withoutMarkers.length)
    var cursor = 0
    while (true) {
        val start = withoutMarkers.indexOf(FOOTNOTE_OPEN, cursor)
        if (start < 0) break
        out.append(withoutMarkers, cursor, start)
        var depth = 0
        var end = withoutMarkers.length
        for (tag in ITALIC_TAG_REGEX.findAll(withoutMarkers, start)) {
            depth += if (tag.value.startsWith("</")) -1 else 1
            if (depth == 0) {
                end = tag.range.last + 1
                break
            }
        }
        cursor = end
    }
    out.append(withoutMarkers, cursor, withoutMarkers.length)
    return out.toString()
}

internal fun cleanSefariaLine(raw: String, stripFootnotes: Boolean = false): String {
    var s = if (raw.contains('\n')) raw.replace("\n", "") else raw
    if (stripFootnotes) {
        s = stripSefariaFootnotes(s)
    }
    if (OTZAR_MARKUP_REGEX.containsMatchIn(s)) {
        s = OTZAR_MARKUP_REGEX.replace(s, "$1")
    }
    if (HTML_LINE_BREAK_REGEX.containsMatchIn(s)) {
        s = HTML_LINE_BREAK_REGEX.replace(s, " ")
        // Collapse any double spaces we just introduced
        s = s.replace(Regex(" {2,}"), " ").trim()
    }
    // Inline any Sefaria textimages as base64 data URIs (no-op if the embedder
    // hasn't been prefetched or the line contains no such URL).
    s = SefariaImageEmbedder.substituteImages(s)
    return s
}

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
        norm == "ot" -> "אות"
        norm == "dh" -> "דיבור המתחיל"
        "aliyah" in norm || "aliya" in norm -> "עליה"
        "daf" in norm -> "דף"
        "chapter" in norm -> "פרק"
        "perek" in norm -> "פרק"
        "siman" in norm -> "סימן"
        "seif" in norm -> "סעיף"
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
        "paragraph" in norm -> "פסקה"
        "line" in norm -> "שורה"
        "column" in norm -> "טור"
        "folio" in norm -> "דף"
        "segment" in norm -> "קטע"
        "question" in norm -> "שאלה"
        "comment" in norm -> "פירוש"
        "footnote" in norm -> "הערה"
        "pararaph" in norm -> "פסקה"
        "se'if" in norm -> "סעיף"
        "passuk" in norm -> "פסוק"
        "sheilta" in norm -> "שאילתא"
        "tosefta" in norm -> "תוספתא"
        "midrash" in norm -> "מדרש"
        "drush" in norm -> "דרוש"
        "remez" in norm -> "רמז"
        "inyan" in norm -> "ענין"
        "mitzvah" in norm -> "מצוה"
        "shorash" in norm -> "שורש"
        "piyyut" in norm -> "פיוט"
        "hadran" in norm -> "הדרן"
        "kovetz" in norm -> "קובץ"
        "gate" in norm || "shaar" in norm || "sha'ar" in norm -> "שער"
        "essay" in norm || "treatise" in norm -> "מאמר"
        "statement" in norm -> "סימן"
        "principle" in norm -> "עיקר"
        "epistle" in norm -> "אגרת"
        "letter" in norm -> "אות"
        "page" in norm -> "עמוד"
        "word" in norm -> "ערך"
        "room" in norm || "chamber" in norm -> "חדר"
        "window" in norm -> "חלון"
        "book" in norm -> "ספר"
        "nahar" in norm -> "נהר"
        "maayan" in norm -> "מעין"
        "shoket" in norm -> "שוקת"
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
