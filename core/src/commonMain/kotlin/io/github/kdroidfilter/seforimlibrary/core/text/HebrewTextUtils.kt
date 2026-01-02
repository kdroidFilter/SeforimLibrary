package io.github.kdroidfilter.seforimlibrary.core.text

/**
 * Utility class for processing Hebrew text by removing diacritical marks (nikud/niqqud).
 *
 * This class provides functions to clean Hebrew text from various diacritical marks including:
 * - Nikud (vowel points)
 * - Teamim (biblical cantillation marks)
 * - Maqaf (Hebrew hyphen)
 *
 * Based on the Unicode ranges and character mappings used in Hebrew text processing.
 */
object HebrewTextUtils {

    /**
     * Hebrew nikud (vowel point) characters mapping.
     * These are the diacritical marks used in Hebrew to indicate vowels.
     */
    private val NIKUD_SIGNS = mapOf(
        "HATAF_SEGOL" to "ֱ",    // U+05B1
        "HATAF_PATAH" to "ֲ",     // U+05B2
        "HATAF_QAMATZ" to "ֳ",    // U+05B3
        "HIRIQ" to "ִ",           // U+05B4
        "TSERE" to "ֵ",           // U+05B5
        "SEGOL" to "ֶ",           // U+05B6
        "PATAH" to "ַ",           // U+05B7
        "QAMATZ" to "ָ",          // U+05B8
        "SIN_DOT" to "ׂ",         // U+05C2
        "SHIN_DOT" to "ׁ",        // U+05C1
        "HOLAM" to "ֹ",           // U+05B9
        "DAGESH" to "ּ",          // U+05BC
        "QUBUTZ" to "ֻ",          // U+05BB
        "SHEVA" to "ְ",           // U+05B0
        "QAMATZ_QATAN" to "ׇ"     // U+05C7
    )

    /**
     * Meteg character (silluq) - U+05BD.
     * A vertical line placed to the left of a vowel to indicate stress.
     */
    private const val METEG = "ֽ"

    /**
     * Regular expression pattern for removing all nikud signs including meteg.
     */
    private val NIKUD_WITH_METEG_REGEX = "[${NIKUD_SIGNS.values.joinToString("")}$METEG]".toRegex()

    /**
     * Regular expression pattern for removing nikud signs only (excluding meteg).
     */
    private val NIKUD_ONLY_REGEX = "[${NIKUD_SIGNS.values.joinToString("")}]".toRegex()

    /**
     * Regular expression pattern for biblical cantillation marks (teamim).
     * Covers Unicode range U+0591 to U+05AF.
     */
    private val TEAMIM_REGEX = "[\u0591-\u05AF]".toRegex()

    /**
     * Hebrew maqaf character (hyphen) - U+05BE.
     */
    private const val MAQAF_CHAR = "־"

    /**
     * Removes all nikud (vowel points) from Hebrew text.
     *
     * This function strips vowel diacritical marks from Hebrew text, making it suitable
     * for applications that need unpointed Hebrew text.
     *
     * @param text The Hebrew text containing nikud marks, or null
     * @param includeMeteg Whether to also remove meteg marks (default: true)
     * @return The text with nikud removed, or empty string if input is null/empty
     *
     * @sample
     * ```kotlin
     * val pointed = "בְּרֵאשִׁית"
     * val unpointed = HebrewTextUtils.removeNikud(pointed)
     * // Result: "בראשית"
     * ```
     */
    fun removeNikud(text: String?, includeMeteg: Boolean = true): String {
        if (text.isNullOrEmpty()) return ""

        return if (includeMeteg) {
            text.replace(NIKUD_WITH_METEG_REGEX, "")
        } else {
            text.replace(NIKUD_ONLY_REGEX, "")
        }
    }

    /**
     * Removes biblical cantillation marks (teamim) from Hebrew text.
     *
     * Teamim are accent marks used in biblical Hebrew to indicate musical notation
     * and syntactic relationships. This function removes these marks while preserving
     * the base text and nikud.
     *
     * @param text The Hebrew text containing teamim, or null
     * @return The text with teamim removed, or empty string if input is null/empty
     *
     * @sample
     * ```kotlin
     * val withTeamim = "בְּרֵאשִׁ֖ית"
     * val withoutTeamim = HebrewTextUtils.removeTeamim(withTeamim)
     * // Result: "בְּרֵאשִׁית"
     * ```
     */
    fun removeTeamim(text: String?): String {
        if (text.isNullOrEmpty()) return ""
        return text.replace(TEAMIM_REGEX, "")
    }

    /**
     * Removes all diacritical marks from Hebrew text.
     *
     * This function removes both nikud (vowel points) and teamim (cantillation marks),
     * resulting in plain Hebrew consonantal text.
     *
     * @param text The Hebrew text containing diacritical marks, or null
     * @return The text with all diacritical marks removed, or empty string if input is null/empty
     *
     * @sample
     * ```kotlin
     * val fullyMarked = "בְּרֵאשִׁ֖ית בָּרָ֣א"
     * val plain = HebrewTextUtils.removeAllDiacritics(fullyMarked)
     * // Result: "בראשית ברא"
     * ```
     */
    fun removeAllDiacritics(text: String?): String {
        if (text.isNullOrEmpty()) return ""
        return removeTeamim(removeNikud(text, true))
    }

    /**
     * Checks whether the given text contains nikud marks.
     *
     * @param text The text to examine, or null
     * @return `true` if the text contains any nikud marks, `false` otherwise
     *
     * @sample
     * ```kotlin
     * val hasNikud = HebrewTextUtils.containsNikud("בְּרֵאשִׁית") // true
     * val noNikud = HebrewTextUtils.containsNikud("בראשית")     // false
     * ```
     */
    fun containsNikud(text: String?): Boolean {
        if (text.isNullOrEmpty()) return false
        return NIKUD_WITH_METEG_REGEX.containsMatchIn(text)
    }

    /**
     * Checks whether the given text contains teamim (cantillation marks).
     *
     * @param text The text to examine, or null
     * @return `true` if the text contains any teamim, `false` otherwise
     *
     * @sample
     * ```kotlin
     * val hasTeamim = HebrewTextUtils.containsTeamim("בְּרֵאשִׁ֖ית") // true
     * val noTeamim = HebrewTextUtils.containsTeamim("בְּרֵאשִׁית")   // false
     * ```
     */
    fun containsTeamim(text: String?): Boolean {
        if (text.isNullOrEmpty()) return false
        return TEAMIM_REGEX.containsMatchIn(text)
    }

    /**
     * Checks whether the given text contains maqaf (Hebrew hyphen).
     *
     * @param text The text to examine, or null
     * @return `true` if the text contains any maqaf characters, `false` otherwise
     *
     * @sample
     * ```kotlin
     * val hasMaqaf = HebrewTextUtils.containsMaqaf("אֵל־שַׁדַּי") // true
     * val noMaqaf = HebrewTextUtils.containsMaqaf("אל שדי")     // false
     * ```
     */
    fun containsMaqaf(text: String?): Boolean {
        if (text.isNullOrEmpty()) return false
        return text.contains(MAQAF_CHAR)
    }

    /**
     * Replaces Hebrew maqaf characters with a specified replacement string.
     *
     * Maqaf is the Hebrew hyphen character (־) used to connect words or parts of words
     * in Hebrew text. This function allows replacing it with other characters like
     * space, dash, or any other string.
     *
     * @param text The text containing maqaf characters, or null
     * @param replacement The string to replace maqaf with (default: single space)
     * @return The text with maqaf characters replaced, or empty string if input is null/empty
     *
     * @sample
     * ```kotlin
     * val withMaqaf = "אֵל־שַׁדַּי"
     * val withSpace = HebrewTextUtils.replaceMaqaf(withMaqaf)        // "אֵל שַׁדַּי"
     * val withDash = HebrewTextUtils.replaceMaqaf(withMaqaf, "-")    // "אֵל-שַׁדַּי"
     * ```
     */
    fun replaceMaqaf(text: String?, replacement: String = " "): String {
        if (text.isNullOrEmpty()) return ""
        return text.replace(MAQAF_CHAR, replacement)
    }
}

