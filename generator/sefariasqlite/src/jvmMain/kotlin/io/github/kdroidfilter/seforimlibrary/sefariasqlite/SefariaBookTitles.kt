package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Author prefixes for Sefaria collections whose Hebrew title doesn't name the author,
 * keyed by English collective title. The app shows a commentary by its title without
 * " על <base book>", so "חידושי אגדות" alone doesn't tell the reader it's the Maharsha.
 */
private val authorPrefixesByCollectiveTitle = mapOf(
    "Chidushei Agadot" to "מהרש\"א",
    "Chidushei Halachot" to "מהרש\"א",
)

/**
 * Title displayed for a Sefaria book: its Hebrew title, prefixed with the author when
 * its collection needs it. Only the display title changes; `heTitle` stays the natural key.
 */
internal fun sefariaDisplayTitle(heTitle: String, collectiveTitleEn: String?): String {
    val prefix = collectiveTitleEn?.let { authorPrefixesByCollectiveTitle[it] } ?: return heTitle
    return if (heTitle.startsWith(prefix)) heTitle else "$prefix - $heTitle"
}
