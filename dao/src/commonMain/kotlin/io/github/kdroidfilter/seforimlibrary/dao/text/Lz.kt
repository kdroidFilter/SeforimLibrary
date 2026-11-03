package io.github.kdroidfilter.seforimlibrary.dao.text

/**
 * LZ77 without entropy coding, LZ4's block format, behind a dictionary: a match may reach back into the dictionary
 * as if it preceded the data. Decoding is copies only, with nothing to set up per value: the dictionary is read where
 * it lies.
 *
 * A sequence is a token (literal count in the high nibble, match length - 4 in the low one, 15 extended by bytes
 * adding up to 255 each), the literals, then the match's offset on 2 bytes (little endian) and the length's
 * extension; the last sequence has literals only.
 */
internal object Lz {
    const val MIN_MATCH = 4
    const val MAX_OFFSET = 65_535

    private const val HASH_BITS = 15
    private const val CHAIN_DEPTH = 64

    /** Decodes [src] from [offset] into [out], exactly filled. */
    fun decompress(src: ByteArray, offset: Int, out: ByteArray, dictionary: ByteArray) {
        val end = src.size
        val dictSize = dictionary.size
        var ip = offset
        var op = 0
        while (ip < end) {
            val token = src[ip++].toInt() and 0xFF
            var literals = token ushr 4
            if (literals == 15) {
                var b: Int
                do {
                    b = src[ip++].toInt() and 0xFF
                    literals += b
                } while (b == 255)
            }
            src.copyInto(out, op, ip, ip + literals)
            ip += literals
            op += literals
            if (ip >= end) break

            val distance = (src[ip].toInt() and 0xFF) or ((src[ip + 1].toInt() and 0xFF) shl 8)
            ip += 2
            var length = (token and 15) + MIN_MATCH
            if (token and 15 == 15) {
                var b: Int
                do {
                    b = src[ip++].toInt() and 0xFF
                    length += b
                } while (b == 255)
            }
            var from = op - distance
            if (from < 0) {
                // The match starts in the dictionary, and may run on into the data
                val n = minOf(-from, length)
                dictionary.copyInto(out, op, dictSize + from, dictSize + from + n)
                op += n
                length -= n
                from = 0
            }
            if (length > 0) {
                if (op - from >= length) {
                    out.copyInto(out, op, from, from + length)
                } else {
                    for (k in 0 until length) out[op + k] = out[from + k]
                }
                op += length
            }
        }
    }

    /** The dictionary's hash chains, built once and shared by every [compress]. */
    class Dictionary(val bytes: ByteArray) {
        internal val head = IntArray(1 shl HASH_BITS) { -1 }
        internal val prev = IntArray(bytes.size)

        init {
            for (i in 0..bytes.size - MIN_MATCH) {
                val h = hash(bytes, i)
                prev[i] = head[h]
                head[h] = i
            }
        }
    }

    /** [data] compressed against [dictionary]: greedy matching, with one step of lazy evaluation. */
    fun compress(data: ByteArray, dictionary: Dictionary): ByteArray {
        val dict = dictionary.bytes
        val dictSize = dict.size
        val n = data.size
        // Positions are virtual: the dictionary's first, then the data's from dictSize
        val head = dictionary.head.copyOf()
        val prev = IntArray(n)
        val out = ByteArray(n + n / 255 + 16)
        var op = 0
        var anchor = 0
        var i = 0

        fun byteAt(p: Int): Byte = if (p < dictSize) dict[p] else data[p - dictSize]

        fun insert(at: Int) {
            if (at + MIN_MATCH <= n) {
                val h = hash(data, at)
                prev[at] = head[h]
                head[h] = dictSize + at
            }
        }

        // The longest match at data position [at]: (length shl 16) or distance, 0 if none
        fun find(at: Int): Long {
            if (at + MIN_MATCH > n) return 0
            val v = dictSize + at
            var candidate = head[hash(data, at)]
            var best = 0
            var bestDistance = 0
            var steps = 0
            while (candidate >= 0 && steps++ < CHAIN_DEPTH && v - candidate <= MAX_OFFSET) {
                var length = 0
                while (at + length < n && byteAt(candidate + length) == data[at + length]) length++
                if (length > best) {
                    best = length
                    bestDistance = v - candidate
                    if (at + length == n) break
                }
                candidate = if (candidate < dictSize) dictionary.prev[candidate] else prev[candidate - dictSize]
            }
            return if (best >= MIN_MATCH) (best.toLong() shl 16) or bestDistance.toLong() else 0
        }

        fun writeLength(value: Int) {
            var rest = value
            while (rest >= 255) {
                out[op++] = 255.toByte()
                rest -= 255
            }
            out[op++] = rest.toByte()
        }

        fun emit(literalEnd: Int, length: Int, distance: Int) {
            val literals = literalEnd - anchor
            val matchCode = if (length == 0) 0 else minOf(length - MIN_MATCH, 15)
            out[op++] = ((minOf(literals, 15) shl 4) or matchCode).toByte()
            if (literals >= 15) writeLength(literals - 15)
            data.copyInto(out, op, anchor, literalEnd)
            op += literals
            if (length == 0) return
            out[op++] = distance.toByte()
            out[op++] = (distance ushr 8).toByte()
            if (length - MIN_MATCH >= 15) writeLength(length - MIN_MATCH - 15)
        }

        while (i < n) {
            var match = find(i)
            if (match == 0L) {
                insert(i)
                i++
                continue
            }
            // A longer match one byte on wins over this one
            insert(i)
            val next = find(i + 1)
            if (next ushr 16 > match ushr 16) {
                i++
                match = next
                insert(i)
            }
            val length = (match ushr 16).toInt()
            emit(i, length, (match and 0xFFFF).toInt())
            for (p in i + 1 until i + length) insert(p)
            i += length
            anchor = i
        }
        if (anchor < n || op == 0) emit(n, 0, 0)
        return out.copyOf(op)
    }

    private fun hash(bytes: ByteArray, at: Int): Int {
        val v =
            (bytes[at].toInt() and 0xFF) or ((bytes[at + 1].toInt() and 0xFF) shl 8) or
                ((bytes[at + 2].toInt() and 0xFF) shl 16) or ((bytes[at + 3].toInt() and 0xFF) shl 24)
        return (v * -1640531535) ushr (32 - HASH_BITS)
    }
}
