package io.github.kdroidfilter.seforimlibrary.search

/**
 * Utility functions for Hebrew text processing.
 * Includes normalization, diacritic removal, and final letter handling.
 */
object HebrewTextUtils {

    /**
     * Map of Hebrew final letters (sofit) to their base forms.
     */
    val SOFIT_MAP = mapOf(
        'ך' to 'כ',  // U+05DA -> U+05DB
        'ם' to 'מ',  // U+05DD -> U+05DE
        'ן' to 'נ',  // U+05DF -> U+05E0
        'ף' to 'פ',  // U+05E3 -> U+05E4
        'ץ' to 'צ'   // U+05E5 -> U+05E6
    )

    /**
     * Normalizes Hebrew text by:
     * - Removing teamim (cantillation marks) U+0591–U+05AF
     * - Removing nikud (vowel points) U+05B0–U+05BD and related
     * - Replacing maqaf U+05BE with space
     * - Removing gershayim/geresh
     * - Normalizing final letters to base forms
     * - Collapsing whitespace
     *
     * @param input The input string to normalize
     * @return The normalized string
     */
    fun normalizeHebrew(input: String): String {
        if (input.isBlank()) return ""
        var s = input.trim()

        // Remove biblical cantillation marks (teamim) U+0591–U+05AF
        s = s.replace("[\u0591-\u05AF]".toRegex(), "")
        // Remove nikud signs including meteg and qamatz qatan
        s = s.replace("[\u05B0\u05B1\u05B2\u05B3\u05B4\u05B5\u05B6\u05B7\u05B8\u05B9\u05BB\u05BC\u05BD\u05C1\u05C2\u05C7]".toRegex(), "")
        // Replace maqaf U+05BE with space
        s = s.replace('\u05BE', ' ')
        // Remove gershayim/geresh
        s = s.replace("\u05F4", "").replace("\u05F3", "")
        // Normalize Hebrew final letters (sofit) to base forms
        s = replaceFinalsWithBase(s)
        // Collapse whitespace
        s = s.replace("\\s+".toRegex(), " ").trim()
        return s
    }

    /**
     * Replaces Hebrew final letters (sofit) with their base forms.
     *
     * @param text The input text
     * @return Text with final letters replaced
     */
    fun replaceFinalsWithBase(text: String): String = text
        .replace('\u05DA', '\u05DB') // ך -> כ
        .replace('\u05DD', '\u05DE') // ם -> מ
        .replace('\u05DF', '\u05E0') // ן -> נ
        .replace('\u05E3', '\u05E4') // ף -> פ
        .replace('\u05E5', '\u05E6') // ץ -> צ

    /**
     * Checks if a character is a Hebrew diacritic (nikud or teamim).
     *
     * @param c The character to check
     * @return true if the character is a diacritic
     */
    fun isNikudOrTeamim(c: Char): Boolean {
        val code = c.code
        return (code in 0x0591..0x05AF) || // teamim
               (code in 0x05B0..0x05BD) || // nikud + meteg
               (c == '\u05C1') || (c == '\u05C2') || (c == '\u05C7')
    }

    /**
     * Strips Hebrew diacritics (nikud and teamim) from text and returns
     * both the plain text and an index map from plain indices to original indices.
     *
     * @param src The source string
     * @return Pair of (plain text, index map)
     */
    fun stripDiacriticsWithMap(src: String): Pair<String, IntArray> {
        val out = StringBuilder(src.length)
        val map = ArrayList<Int>(src.length)
        var i = 0
        while (i < src.length) {
            val ch = src[i]
            if (!isNikudOrTeamim(ch)) {
                out.append(ch)
                map.add(i)
            }
            i++
        }
        val arr = IntArray(map.size) { map[it] }
        return out.toString() to arr
    }

    /**
     * Strips Hebrew diacritics from text without preserving index mapping.
     *
     * @param text The input text
     * @return Text without diacritics
     */
    fun stripDiacritics(text: String): String {
        if (text.isEmpty()) return text
        val sb = StringBuilder(text.length)
        for (ch in text) {
            if (!isNikudOrTeamim(ch)) {
                sb.append(ch)
            }
        }
        return sb.toString()
    }

    /**
     * Maps a plain text index back to the original text index.
     *
     * @param mapToOrig The index map from stripDiacriticsWithMap
     * @param plainIndex The index in the plain text
     * @return The corresponding index in the original text
     */
    fun mapToOrigIndex(mapToOrig: IntArray, plainIndex: Int): Int {
        if (mapToOrig.isEmpty()) return plainIndex
        val idx = plainIndex.coerceIn(0, mapToOrig.size - 1)
        return mapToOrig[idx]
    }
}
