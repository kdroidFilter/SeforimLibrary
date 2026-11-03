package io.github.kdroidfilter.seforimlibrary.dao.text

import app.cash.sqldelight.ColumnAdapter

// First byte of a compressed value: 0xFF never starts (or appears in) UTF-8 text
private const val COMPRESSED_MARKER: Byte = 0xFF.toByte()

/** The dictionaries of the line texts: a value's header names the one it was compressed with. */
object LineTextDictionaries {
    const val CONTENT: Int = 1
}

/**
 * The line texts as the database stores them: plain UTF-8, or compressed against a dictionary of the database's
 * `line_codec_dict` table: 0xFF, the dictionary's id, the text's length in Hebrew bytes (a varint), then [Lz]'s
 * sequences over the text in [Hebrew bytes][encodeHebrewBytes].
 *
 * Lines are short (750 bytes of UTF-8 on average, many of 200). One byte per Hebrew letter, nikud or taam halves them;
 * LZ against 60 KiB of the corpus' own text brings them to 3.1x in all, about as much as deflate with a dictionary, while
 * decoding is copies only: no entropy decoding, no state to set up, no native call (the same code on every platform).
 * The generator writes plain text; the shipped database is compressed by the packaging (`:packaging:compactDb`), and
 * both read the same way.
 *
 * @param dictionaries loads the dictionaries (in Hebrew bytes) by id, on [loadDictionaries] or the first value read
 */
class LineTextCodec(dictionaries: () -> Map<Int, ByteArray>) {
    private val dictionaries by lazy(dictionaries)
    private val compressionDictionaries by lazy { this.dictionaries.mapValues { Lz.Dictionary(it.value) } }

    /**
     * Loads the dictionaries now: a loader that queries the database must not run inside another query's cursor
     * (SQLDelight's JDBC driver closes the thread's connection under it).
     */
    fun loadDictionaries() {
        dictionaries.size
    }

    /** Plain UTF-8: what the generator writes. */
    fun encode(text: String): ByteArray = text.encodeToByteArray()

    /** [text] compressed against the dictionary [dictionaryId]. */
    fun compress(text: String, dictionaryId: Int): ByteArray {
        val dictionary = compressionDictionaries[dictionaryId] ?: missing(dictionaryId)
        val hebrew = encodeHebrewBytes(text)
        val sequences = Lz.compress(hebrew, dictionary)
        val out = ByteArray(2 + VARINT_MAX + sequences.size)
        out[0] = COMPRESSED_MARKER
        out[1] = dictionaryId.toByte()
        val n = writeVarint(out, 2, hebrew.size)
        sequences.copyInto(out, n)
        return out.copyOf(n + sequences.size)
    }

    fun decode(bytes: ByteArray): String {
        if (bytes.isEmpty()) return ""
        if (bytes[0] == COMPRESSED_MARKER) {
            val dictionaryId = bytes[1].toInt() and 0xFF
            val dictionary = dictionaries[dictionaryId] ?: missing(dictionaryId)
            var at = 2
            var length = 0
            var shift = 0
            while (true) {
                val b = bytes[at++].toInt() and 0xFF
                length = length or ((b and 0x7F) shl shift)
                if (b < 0x80) break
                shift += 7
            }
            val hebrew = ByteArray(length)
            Lz.decompress(bytes, at, hebrew, dictionary)
            return decodeHebrewBytes(hebrew, length)
        }
        // Android's cursors read a TEXT value (an older database's) as a blob with its terminating NUL
        val end = if (bytes.last() == 0.toByte()) bytes.size - 1 else bytes.size
        return bytes.decodeToString(0, end)
    }

    private fun missing(id: Int): Nothing = error("Line text dictionary $id is missing from line_codec_dict")

    /** SQLDelight's adapter of the line's text. */
    val columnAdapter: ColumnAdapter<String, ByteArray> =
        object : ColumnAdapter<String, ByteArray> {
            override fun decode(databaseValue: ByteArray): String = this@LineTextCodec.decode(databaseValue)

            override fun encode(value: String): ByteArray = this@LineTextCodec.encode(value)
        }

    companion object {
        // Below LZ's reach (65,535 bytes back) by a line's length, so that most lines still see all of it
        const val DICTIONARY_SIZE: Int = 60 * 1024

        private const val VARINT_MAX = 5

        /** A dictionary for [compress]: the tail of [sample] (texts of the corpus, end to end) in Hebrew bytes. */
        fun buildDictionary(sample: List<String>, size: Int = DICTIONARY_SIZE): ByteArray {
            val bytes = encodeHebrewBytes(sample.joinToString(""))
            return bytes.copyOfRange((bytes.size - size).coerceAtLeast(0), bytes.size)
        }

        private fun writeVarint(out: ByteArray, offset: Int, value: Int): Int {
            var at = offset
            var rest = value
            while (rest >= 0x80) {
                out[at++] = ((rest and 0x7F) or 0x80).toByte()
                rest = rest ushr 7
            }
            out[at++] = rest.toByte()
            return at
        }
    }
}
