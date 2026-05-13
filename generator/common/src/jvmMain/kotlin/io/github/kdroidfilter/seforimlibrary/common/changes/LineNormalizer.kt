package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.core.text.HebrewTextUtils
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist

/**
 * Pipeline that turns a raw line (HTML, with nikud/teamim, with sofit, with
 * extra whitespace) into a normalised string suitable for fuzzy line matching
 * across builds. Mirrors the Lucene "default" analyzer used by the client
 * search index, so two builds whose line ids changed only because of cosmetic
 * reformatting still produce identical normalised forms.
 *
 * Steps (in order):
 *   1. Strip HTML tags (jsoup, no allowed tags).
 *   2. Remove teamim (cantillation marks) and nikud (vowel points).
 *   3. Replace maqaf and gershayim/geresh with neutral chars.
 *   4. Normalise Hebrew final letters (sofit) to their base form.
 *   5. Collapse all whitespace runs to a single space.
 *
 * See DELTA_UPDATE_PLAN.md §3.6 (Patience Diff post-normalisation).
 */
object LineNormalizer {

    /**
     * Returns the normalised form of [content], or an empty string if the
     * input is null/blank/HTML-only.
     */
    fun normalize(content: String?): String {
        if (content.isNullOrBlank()) return ""
        // 1. HTML strip
        val noHtml = Jsoup.clean(content, "", Safelist.none())
            // Jsoup adds &nbsp; etc.; decode entities and unescape.
            .let { org.jsoup.parser.Parser.unescapeEntities(it, false) }
        if (noHtml.isBlank()) return ""

        // 2. Diacritics
        var s = HebrewTextUtils.removeAllDiacritics(noHtml)

        // 3. Maqaf / gershayim / geresh → space / strip
        s = HebrewTextUtils.replaceMaqaf(s, replacement = " ")
        s = s.replace("״", "").replace("׳", "")

        // 4. Sofit (final letters) → base form
        s = replaceFinalLettersWithBase(s)

        // 5. Whitespace collapse (also catches NBSP  , ZWSP ​, etc.)
        s = s.replace("[\\s\\u00A0\\u200B\\u2028\\u2029]+".toRegex(), " ").trim()
        return s
    }

    private fun replaceFinalLettersWithBase(text: String): String = text
        .replace('ך', 'כ')  // ך → כ
        .replace('ם', 'מ')  // ם → מ
        .replace('ן', 'נ')  // ן → נ
        .replace('ף', 'פ')  // ף → פ
        .replace('ץ', 'צ')  // ץ → צ
}
