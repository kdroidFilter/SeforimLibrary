package io.github.kdroidfilter.seforimlibrary.dao.text

import java.io.ByteArrayOutputStream
import java.util.zip.Deflater
import java.util.zip.Inflater

// One inflater per thread: reading a page of lines inflates each of them, and a new one allocates its native state
// (ThreadLocal.withInitial needs Android 8)
private val inflaters = object : ThreadLocal<Inflater>() {
    override fun initialValue() = Inflater(true)
}
private val buffers = object : ThreadLocal<ByteArray>() {
    override fun initialValue() = ByteArray(64 * 1024)
}

// A buffer grown for one long line is kept only up to this size (small devices)
private const val MAX_RETAINED_BUFFER = 256 * 1024
private val DUMMY_BYTE = ByteArray(1)

internal actual fun deflateRaw(input: ByteArray, dictionary: ByteArray): ByteArray {
    val deflater = Deflater(Deflater.BEST_COMPRESSION, true)
    try {
        deflater.setDictionary(dictionary)
        deflater.setInput(input)
        deflater.finish()
        val out = ByteArrayOutputStream(input.size / 2 + 64)
        val buffer = ByteArray(16 * 1024)
        while (!deflater.finished()) out.write(buffer, 0, deflater.deflate(buffer))
        return out.toByteArray()
    } finally {
        deflater.end()
    }
}

internal actual fun inflateRawToString(input: ByteArray, offset: Int, length: Int, dictionary: ByteArray): String {
    val inflater = inflaters.get()!!
    inflater.reset()
    inflater.setDictionary(dictionary)
    inflater.setInput(input, offset, length)
    var buffer = buffers.get()!!
    var size = 0
    var paddedInput = false
    while (!inflater.finished()) {
        if (size == buffer.size) buffer = buffer.copyOf(buffer.size * 2).also { if (it.size <= MAX_RETAINED_BUFFER) buffers.set(it) }
        val n = inflater.inflate(buffer, size, buffer.size - size)
        if (n == 0 && inflater.needsInput()) {
            // Raw inflate may want one byte past the stream to see its end (Inflater's "nowrap" note)
            check(!paddedInput) { "Truncated line text" }
            inflater.setInput(DUMMY_BYTE)
            paddedInput = true
        }
        size += n
    }
    return String(buffer, 0, size, Charsets.UTF_8)
}
