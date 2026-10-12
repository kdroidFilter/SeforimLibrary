package io.github.kdroidfilter.seforimlibrary.common.divine

/**
 * A fixed phrase in which one word is a divine name: words compared on the consonant
 * skeleton, adjacent (spaces, maqaf, paseq, inline tags only). [nameIndex] is the name
 * word; its prefix letters (the part before [nameBody]) stay outside the span.
 */
internal data class NamePhrase(
    val words: List<String>,
    val nameIndex: Int,
    val nameBody: String,
    /** Unvocalized the phrase means something else (אל בנה = "to her son"). */
    val vocalizedOnly: Boolean = false,
) {
    companion object {
        /** `"*האל הקדוש"`: the word marked with `*` is the name, spelled [body] at its end. */
        fun of(phrase: String, body: String, vocalizedOnly: Boolean = false): NamePhrase {
            val parts = phrase.trim().split(' ').filter(String::isNotEmpty)
            val index = parts.indexOfFirst { it.startsWith('*') }
            require(index >= 0) { "phrase '$phrase' marks no name word" }
            val words = parts.map { it.removePrefix("*") }
            require(words[index].endsWith(body)) { "phrase '$phrase': name word does not end with $body" }
            return NamePhrase(words, index, body, vocalizedOnly)
        }
    }
}

/**
 * Phrases of the prayer book (root category "סדר התפילה") in which אֵל is certainly the
 * Name. Derived from the real occurrences in the library (frequency + review); the NIKUD
 * rule for אֵל stays limited to Tanach verses. Pending rabbinic approval.
 * Deliberately absent: quotes of verses the override list holds profane (מי אל בשמים,
 * כהררי אל, לפניו לא נוצר אל) and ואל הבקר רץ (a vocalization error for אֶל).
 */
internal val PRAYER_EL_PHRASES: List<NamePhrase> = listOf(
    "*אל רחום וחנון",
    "*אל חנון ורחום",
    "*אל רחום שמך",
    "*אל חנון שמך",
    "*אל הורית לנו",
    "*אל הורתנו לומר",
    "*אל מלך יושב על כסא רחמים",
    "*אל מלך חנון ורחום",
    "*אל מלך גדול וקדוש",
    "*אל מלך נאמן",
    "*אל מלך רופא נאמן",
    "*האל המלך הגדול",
    "*לאל מלך יוצר",
    "*האל ישועתנו",
    "*אל ארך אפים",
    "*אל רם ונשא",
    "*אל גדול יהוה",
    "*אל חי וקים",
    "*אל חי העולמים",
    "*אל נקמות",
    "*האל הנאמן",
    "*אל טוב וסלח",
    "*אל טוב ומטיב",
    "*האל הקדוש",
    "*האל הטוב",
    "*אל מלא רחמים",
    "*אל ההודאות",
    "*אל נערץ בסוד",
    "*אל הכבוד הרעים",
    "*אל שומע תפלות",
    "*אל גואל חזק",
    "*אל שומרנו ומצילנו",
    "*אל פועל ישועות",
    "*אל רופא רחמן",
    "*אל זן ומפרנס",
    "*לאל ברוך נעימות",
    "*אל ברוך גדול דעה",
    "*לאל אשר שבת",
    "*אל אדון על כל המעשים",
    "*אל אמונה ואין עול",
    "*אל נשא היית",
    "מי *אל כמוך",
    "הנה *אל ישועתי",
    "*אל יהוה ויאר לנו",
    "הללו *אל בקדשו",
    "*לאל לבב חוקר",
    "*אל נא רפא נא",
    "*אל דעות יהוה",
    "*אל נורא עלילה",
    "*אל אלהי הרוחות",
).map { NamePhrase.of(it, "אל") } + NamePhrase.of("*אל בנה", "אל", vocalizedOnly = true)

/**
 * Phrases in which אֱלוֹהַּ is certainly the Name outside Tanach (Tanach verses come from
 * the override list). Generic "a god" (אין אלוה מבלעדיך, מי אלוה מבלעדי ה') is profane and
 * never listed. Pending rabbinic approval.
 */
internal val PRAYER_ELOAH_PHRASES: List<NamePhrase> = listOf(
    "*אלוה סליחות" to "אלוה",
    "*אלוה ממעל" to "אלוה",
    "מלפני *אלוה יעקב" to "אלוה",
    "*אלוה מתימן" to "אלוה",
    "אמרת *אלוה" to "אלוה",
    "*אלוה כל בריות" to "אלוה",
    "*אלוה כל הנפשות" to "אלוה",
    "*אלוה כפר לנו" to "אלוה",
    "*אלה כל בשר" to "אלה",
    "*אלוה עושי" to "אלוה",
    "*אלוה עשי" to "אלוה",
    "ויטש *אלוה עשהו" to "אלוה",
    "אל *אלוה דלפה עיני" to "אלוה",
).map { (phrase, body) -> NamePhrase.of(phrase, body) }

/**
 * Contexts in which a nikud-proved יָהּ is not the Name. Pending rabbinic approval.
 * Zohar: לְאִתְדְּנָא בְּיָהּ, בְּיָהּ חַיָּיבֵי (Aramaic "in it", the fire).
 * Bereshit Rabbah 92:1: יָהּ יָהּ דַּי דַּי (wordplay on שַׁדַּי).
 */
internal val YAH_SUPPRESS_PREV: Set<String> = setOf("לאתדנא")
internal val YAH_SUPPRESS_NEXT: Set<String> = setOf("חייבי")
internal val YAH_SUPPRESS_NEXT_PAIRS: Set<Pair<String?, String?>> = setOf("יה" to "די", "די" to "די")
