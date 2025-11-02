package io.github.kdroidfilter.seforimlibrary.analysis

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.charfilter.MappingCharFilter
import org.apache.lucene.analysis.charfilter.NormalizeCharMap
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.pattern.PatternCaptureGroupTokenFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.synonym.SynonymGraphFilter
import org.apache.lucene.analysis.synonym.SynonymMap
import org.apache.lucene.util.CharsRef
import java.util.regex.Pattern

/**
 * Standard-based analyzer for Hebrew that emits additional variants for:
 *  - Leading clitic letters (ד/ה/ו/ב/ל/מ/כ/ש) stripped
 *  - Common suffixes (possessive/object endings) stripped
 *  - Common ktiv male/chaser spelling variants via synonyms (bidirectional)
 */
class HebrewCliticAwareAnalyzer : Analyzer() {
    private val singleVav: Pattern = Pattern.compile("^[\u05D5](.+)$")
    private val anyClitics: Pattern = Pattern.compile("^[\u05D3\u05D4\u05D5\u05D1\u05DC\u05DE\u05DB\u05E9]+(.+)$")

    // Suffix stripping disabled to reduce false positives; rely on clitics + synonyms only.

    override fun createComponents(fieldName: String): TokenStreamComponents {
        val src = StandardTokenizer()
        var ts: TokenStream = src
        ts = LowerCaseFilter(ts)
        ts = SynonymGraphFilter(ts, buildSynonymMap(), true)
        ts = PatternCaptureGroupTokenFilter(ts, /*preserveOriginal=*/true, singleVav, anyClitics)
        return TokenStreamComponents(src, ts)
    }

    override fun initReader(fieldName: String, reader: java.io.Reader): java.io.Reader {
        // Normalize Hebrew final forms (sofit) to base letters so that e.g., סימן → סימנ
        val b = NormalizeCharMap.Builder()
        b.add("\u05DA", "\u05DB") // ך -> כ
        b.add("\u05DD", "\u05DE") // ם -> מ
        b.add("\u05DF", "\u05E0") // ן -> נ
        b.add("\u05E3", "\u05E4") // ף -> פ
        b.add("\u05E5", "\u05E6") // ץ -> צ
        // Normalize maqaf (U+05BE) to space to separate compounds
        b.add("\u05BE", " ") // maqaf → space
        // Drop gershayim (U+05F4) and geresh (U+05F3)
        b.add("\u05F4", "") // gershayim → remove
        b.add("\u05F3", "") // geresh → remove
        return MappingCharFilter(b.build(), reader)
    }

    private fun buildSynonymMap(): SynonymMap {
        // Common ktiv male/chaser and traditional vs modern spellings.
        // Keep pairs conservative to reduce noise; add bidirectionally.
        val pairs = listOf(
            // Core religious/archaic forms
            "שולחן" to "שלחן",
            "שמים" to "שמיים",
            "מצוה" to "מצווה",
            "מצות" to "מצוות",
            "תקוה" to "תקווה",
            "מקוה" to "מקווה",
            "תפלה" to "תפילה",
            "חכמה" to "חוכמה",
            "דבור" to "דיבור",
            "ספור" to "סיפור",
            "אמתי" to "אמיתי",
            "קושיא" to "קושיה",

            // Male/chaser yod/vav inside word
            "גליון" to "גיליון",
            "ניקיון" to "נקיון",
            "בנין" to "בניין",
            "ענין" to "עניין",
            "רעיון" to "ראיון", // common misspelling
            "דמיון" to "דימיון",
            "כשרון" to "כישרון",
            "זכרון" to "זיכרון",
            "כח" to "כוח",
            "כונה" to "כוונה",
            "מראית" to "מראיית", // rare, but seen in texts

            // Male/chaser with final -יה endings
            "עליה" to "עלייה",
            "ראיה" to "ראייה",
            "פניה" to "פנייה",
            "בניה" to "בנייה",
            "עשיה" to "עשייה",
            "שתיה" to "שתייה",
            "קריה" to "קרייה",
            "עיריה" to "עירייה",
            "תושיה" to "תושייה",

            // Dagesh/niqqud influenced variants (safe modern pairs)
            "דוגמה" to "דוגמא",
            "תכנית" to "תוכנית",
            "תיאור" to "תאור",
            "תיאוריה" to "תאוריה",
            "תיאבון" to "תאבון",
            "תיאטרון" to "תאטרון",
            "תיאום" to "תאום", // note: תאום also means "twin"
            "אויר" to "אוויר",
            "אוירה" to "אווירה",
            "מאורע" to "אירוע",
            "נלווה" to "נלוה",
            "לוויה" to "לויה",
            "צהריים" to "צהרים",
            "צהריים" to "צוהריים",

            // Liaison/function-like words (classical vs modern or frequent confusions)
            "בן" to "בין",
            "כל" to "כול",
            "לאמר" to "לומר",
            "כהן" to "כוהן",
            "גירעון" to "גרעון",
            "רבינו" to "רבנו",
            "אלקינו" to "אלוקינו",
            "כיון" to "כיוון"
        )
        val builder = SynonymMap.Builder(true)
        fun add(a: String, b: String) {
            builder.add(CharsRef(a), CharsRef(b), true)
            builder.add(CharsRef(b), CharsRef(a), true)
        }
        for ((a, b) in pairs) add(a, b)
        return builder.build()
    }
}
