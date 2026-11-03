package io.github.kdroidfilter.seforimlibrary.dao.text

import app.cash.sqldelight.ColumnAdapter

// First byte of a compressed value: 0xFF never starts (or appears in) UTF-8 text
private const val COMPRESSED_MARKER: Byte = 0xFF.toByte()
private const val HEADER_SIZE = 2

/** The dictionaries of the line texts: a value's header names the one it was deflated with. */
object LineTextDictionaries {
    const val CONTENT: Int = 1
}

/** Raw deflate (no zlib header) with a preset dictionary, at the best compression. */
internal expect fun deflateRaw(input: ByteArray, dictionary: ByteArray): ByteArray

/** The UTF-8 text of [length] bytes of [input] from [offset], deflated by [deflateRaw] with the same [dictionary]. */
internal expect fun inflateRawToString(input: ByteArray, offset: Int, length: Int, dictionary: ByteArray): String

/**
 * The line texts as the database stores them: plain UTF-8, or deflated with a dictionary of the
 * database's `line_codec_dict` table, behind a two-byte header (0xFF, the dictionary's id).
 *
 * Lines are short (750 bytes on average): a dictionary of the corpus' own text compresses them about 3.2x where
 * deflate alone gets 2.5x, while each line still reads on its own. The generator writes plain text; the shipped
 * database is compressed by the packaging (`:packaging:compactDb`), and both read the same way.
 *
 * @param dictionaries loads the dictionaries by id, on [loadDictionaries] or the first compressed value read
 */
class LineTextCodec(dictionaries: () -> Map<Int, ByteArray>) {
    private val dictionaries by lazy(dictionaries)

    /**
     * Loads the dictionaries now: a loader that queries the database must not run inside another query's cursor
     * (SQLDelight's JDBC driver closes the thread's connection under it).
     */
    fun loadDictionaries() {
        dictionaries.size
    }

    /** Plain UTF-8: what the generator writes. */
    fun encode(text: String): ByteArray = text.encodeToByteArray()

    /** [text] deflated with the dictionary [dictionaryId]. */
    fun compress(text: String, dictionaryId: Int): ByteArray {
        val deflated = deflateRaw(text.encodeToByteArray(), dictionary(dictionaryId))
        return ByteArray(HEADER_SIZE + deflated.size).also {
            it[0] = COMPRESSED_MARKER
            it[1] = dictionaryId.toByte()
            deflated.copyInto(it, HEADER_SIZE)
        }
    }

    fun decode(bytes: ByteArray): String {
        if (bytes.isEmpty()) return ""
        if (bytes[0] == COMPRESSED_MARKER) {
            val dictionary = dictionary(bytes[1].toInt() and 0xFF)
            return inflateRawToString(bytes, HEADER_SIZE, bytes.size - HEADER_SIZE, dictionary)
        }
        // Android's cursors read a TEXT value (an older database's) as a blob with its terminating NUL
        val end = if (bytes.last() == 0.toByte()) bytes.size - 1 else bytes.size
        return bytes.decodeToString(0, end)
    }

    private fun dictionary(id: Int): ByteArray =
        dictionaries[id] ?: error("Line text dictionary $id is missing from line_codec_dict")

    /** SQLDelight's adapter of the line's text. */
    val columnAdapter: ColumnAdapter<String, ByteArray> =
        object : ColumnAdapter<String, ByteArray> {
            override fun decode(databaseValue: ByteArray): String = this@LineTextCodec.decode(databaseValue)

            override fun encode(value: String): ByteArray = this@LineTextCodec.encode(value)
        }
}
