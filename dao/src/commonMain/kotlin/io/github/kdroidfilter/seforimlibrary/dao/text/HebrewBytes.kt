package io.github.kdroidfilter.seforimlibrary.dao.text

// One byte per character of the corpus' alphabet, where UTF-8 takes two for each Hebrew letter, nikud and taam:
// 0x00-0x7F ASCII, 0x80-0xEF U+0590-U+05FF, 0xF0-0xFD the commonest other characters, 0xFE then a UTF-16 unit
private const val HEBREW_FIRST = 0x0590
private const val HEBREW_LAST = 0x05FF
private const val HEBREW_BYTE = 0x80
private const val COMMON_BYTE = 0xF0
private const val ESCAPE = 0xFE

// Dashes, quotes, the bidi marks, no-break space
private val COMMON_CHARS =
    charArrayOf(
        '\u2013', '\u2014', '\u201D', '\u201C', '\u2019', '\u2018', '\u200E',
        '\u00A0', '\u202C', '\u202B', '\u200F', '\u202A', '\u2026', '\u00B7',
    )

private val BYTE_CHARS =
    CharArray(ESCAPE) { b ->
        when {
            b < HEBREW_BYTE -> b.toChar()
            b < COMMON_BYTE -> (HEBREW_FIRST + b - HEBREW_BYTE).toChar()
            else -> COMMON_CHARS[b - COMMON_BYTE]
        }
    }

/** [text] in Hebrew bytes. */
internal fun encodeHebrewBytes(text: String): ByteArray {
    val out = ByteArray(text.length * 3)
    var n = 0
    for (c in text) {
        val code = c.code
        when {
            code < HEBREW_BYTE -> out[n++] = code.toByte()
            code in HEBREW_FIRST..HEBREW_LAST -> out[n++] = (HEBREW_BYTE + code - HEBREW_FIRST).toByte()
            else -> {
                val common = COMMON_CHARS.indexOf(c)
                if (common >= 0) {
                    out[n++] = (COMMON_BYTE + common).toByte()
                } else {
                    out[n++] = ESCAPE.toByte()
                    out[n++] = (code ushr 8).toByte()
                    out[n++] = code.toByte()
                }
            }
        }
    }
    return out.copyOf(n)
}

/** The text of [length] Hebrew bytes of [bytes]. */
internal fun decodeHebrewBytes(bytes: ByteArray, length: Int): String {
    val chars = CharArray(length)
    var n = 0
    var i = 0
    while (i < length) {
        val b = bytes[i++].toInt() and 0xFF
        if (b == ESCAPE) {
            chars[n++] = (((bytes[i].toInt() and 0xFF) shl 8) or (bytes[i + 1].toInt() and 0xFF)).toChar()
            i += 2
        } else {
            chars[n++] = BYTE_CHARS[b]
        }
    }
    return chars.concatToString(0, n)
}
