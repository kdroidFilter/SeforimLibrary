package io.github.kdroidfilter.seforimlibrary.dao.text

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.UByteVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import platform.zlib.ZLIB_VERSION
import platform.zlib.Z_BEST_COMPRESSION
import platform.zlib.Z_DEFAULT_STRATEGY
import platform.zlib.Z_DEFLATED
import platform.zlib.Z_FINISH
import platform.zlib.Z_NO_FLUSH
import platform.zlib.Z_OK
import platform.zlib.Z_STREAM_END
import platform.zlib.deflate
import platform.zlib.deflateBound
import platform.zlib.deflateEnd
import platform.zlib.deflateInit2_
import platform.zlib.deflateSetDictionary
import platform.zlib.inflate
import platform.zlib.inflateEnd
import platform.zlib.inflateInit2_
import platform.zlib.inflateSetDictionary
import platform.zlib.z_stream

// Raw deflate: no zlib header, the dictionary set before the first byte
private const val RAW_WINDOW_BITS = -15

@OptIn(ExperimentalForeignApi::class)
internal actual fun deflateRaw(input: ByteArray, dictionary: ByteArray): ByteArray =
    memScoped {
        val stream = alloc<z_stream>()
        check(
            deflateInit2_(
                stream.ptr, Z_BEST_COMPRESSION, Z_DEFLATED, RAW_WINDOW_BITS, 9, Z_DEFAULT_STRATEGY,
                ZLIB_VERSION, sizeOf<z_stream>().convert(),
            ) == Z_OK,
        )
        try {
            dictionary.usePinned { d ->
                check(deflateSetDictionary(stream.ptr, d.addressOf(0).reinterpret(), dictionary.size.convert()) == Z_OK)
            }
            val out = ByteArray(deflateBound(stream.ptr, input.size.convert()).toInt() + 16)
            input.usePinned { src ->
                out.usePinned { dst ->
                    stream.next_in = if (input.isEmpty()) null else src.addressOf(0).reinterpret<UByteVar>()
                    stream.avail_in = input.size.convert()
                    stream.next_out = dst.addressOf(0).reinterpret()
                    stream.avail_out = out.size.convert()
                    check(deflate(stream.ptr, Z_FINISH) == Z_STREAM_END)
                }
            }
            out.copyOf(stream.total_out.toInt())
        } finally {
            deflateEnd(stream.ptr)
        }
    }

@OptIn(ExperimentalForeignApi::class)
internal actual fun inflateRawToString(input: ByteArray, offset: Int, length: Int, dictionary: ByteArray): String =
    memScoped {
        val stream = alloc<z_stream>()
        check(inflateInit2_(stream.ptr, RAW_WINDOW_BITS, ZLIB_VERSION, sizeOf<z_stream>().convert()) == Z_OK)
        try {
            dictionary.usePinned { d ->
                check(inflateSetDictionary(stream.ptr, d.addressOf(0).reinterpret(), dictionary.size.convert()) == Z_OK)
            }
            var out = ByteArray(maxOf(length * 4, 256))
            var size = 0
            input.usePinned { src ->
                stream.next_in = src.addressOf(offset).reinterpret()
                stream.avail_in = length.convert()
                while (true) {
                    if (size == out.size) out = out.copyOf(out.size * 2)
                    val status =
                        out.usePinned { dst ->
                            stream.next_out = dst.addressOf(size).reinterpret()
                            stream.avail_out = (out.size - size).convert()
                            inflate(stream.ptr, Z_NO_FLUSH).also { size = out.size - stream.avail_out.toInt() }
                        }
                    if (status == Z_STREAM_END) break
                    check(status == Z_OK) { "Corrupt line text (zlib $status)" }
                }
            }
            out.decodeToString(0, size)
        } finally {
            inflateEnd(stream.ptr)
        }
    }
