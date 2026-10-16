package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType

// Canonical "Shulchan Arukh, Orach Chayim 93:1": group 1 = section, group 2 = siman.
private val SHULCHAN_ARUKH_SEIF_ONE = Regex("""shulchan arukh (.+) (\d+):1""")

/**
 * Sefaria ties every Tur siman to the same Shulchan Arukh siman with one `commentary` row whose
 * Shulchan Arukh side is seif 1 alone ("Shulchan Arukh, Orach Chayim 93:1" ↔ "Tur, Orach Chayim
 * 93:1"; in Choshen Mishpat "Tur, … 100:1-7"). The two are parallel codes, so neither is the
 * other's commentary, and the tie holds for the whole siman (Otzaria issue #2481).
 *
 * Returns the Shulchan Arukh side of such a row widened to the whole siman, when [canonical] is
 * that side and [other] the Tur side; null for any other row. The caller stores the row as
 * [ConnectionType.RELATED].
 */
internal fun turShulchanArukhSimanRange(
    type: ConnectionType,
    canonical: String,
    other: String,
): CitationRange? {
    if (type != ConnectionType.COMMENTARY) return null
    val (section, siman) = SHULCHAN_ARUKH_SEIF_ONE.matchEntire(canonical)?.destructured ?: return null
    val turSiman = "tur $section $siman"
    if (other != turSiman && !other.startsWith("$turSiman:")) return null
    return CitationRange(startCanonical = canonical, endCanonical = "shulchan arukh $section $siman")
}
