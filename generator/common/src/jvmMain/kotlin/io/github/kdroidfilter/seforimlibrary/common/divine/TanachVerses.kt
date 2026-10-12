package io.github.kdroidfilter.seforimlibrary.common.divine

/**
 * The 39 Tanach base books as titled in seforim.db (Sefaria import). The override
 * list is keyed by these titles; the CLI refuses to run unless every one resolves
 * to exactly one base book, so a renamed book can never silently disable a
 * profane override.
 */
internal val TANACH_TITLES: List<String> = listOf(
    "בראשית", "שמות", "ויקרא", "במדבר", "דברים",
    "יהושע", "שופטים", "שמואל א", "שמואל ב", "מלכים א", "מלכים ב",
    "ישעיהו", "ירמיהו", "יחזקאל", "הושע", "יואל", "עמוס", "עובדיה", "יונה", "מיכה",
    "נחום", "חבקוק", "צפניה", "חגי", "זכריה", "מלאכי",
    "רות", "תהילים", "איוב", "משלי", "שיר השירים", "קהלת", "איכה", "אסתר",
    "דניאל", "עזרא", "נחמיה", "דברי הימים א", "דברי הימים ב",
)

private val NUMERAL_VALUES: Map<Char, Int> = mapOf(
    'א' to 1, 'ב' to 2, 'ג' to 3, 'ד' to 4, 'ה' to 5, 'ו' to 6, 'ז' to 7, 'ח' to 8, 'ט' to 9,
    'י' to 10, 'כ' to 20, 'ך' to 20, 'ל' to 30, 'מ' to 40, 'ם' to 40, 'נ' to 50, 'ן' to 50,
    'ס' to 60, 'ע' to 70, 'פ' to 80, 'ף' to 80, 'צ' to 90, 'ץ' to 90,
    'ק' to 100, 'ר' to 200, 'ש' to 300, 'ת' to 400,
)

/** Hebrew numeral (`קיט`, `ט"ו`, `טז`) → its value, or null when anything else is in it. */
internal fun parseHebrewNumeral(text: String): Int? {
    val letters = text.trim().filterNot { it == '"' || it == '\'' || it == '׳' || it == '״' }
    if (letters.isEmpty()) return null
    var sum = 0
    for (c in letters) sum += NUMERAL_VALUES[c] ?: return null
    return sum
}

/**
 * A Tanach line's verse from its heRef (`בראשית, יט, יח`). The book part must equal
 * the line's own book title; anything unparsable is null (the caller then treats
 * the line fail-closed).
 */
internal fun parseTanachHeRef(heRef: String, bookTitle: String): VerseRef? {
    val parts = heRef.split(',').map(String::trim)
    if (parts.size != 3 || parts[0] != bookTitle) return null
    val chapter = parseHebrewNumeral(parts[1]) ?: return null
    val verse = parseHebrewNumeral(parts[2]) ?: return null
    return VerseRef(bookTitle, chapter, verse)
}
