package io.github.kdroidfilter.seforimlibrary.packaging

import io.github.kdroidfilter.seforimlibrary.dao.text.LineTextCodec
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Every line of a compact database against the generator's, byte for byte. Runs only with SOURCE_DB and COMPACT_DB:
 *
 *   SOURCE_DB=build/seforim.db COMPACT_DB=build/seforim.compact.db ./gradlew :packaging:jvmTest --tests '*CompactDbVerification*' --rerun
 */
class CompactDbVerification {
    @Test
    fun linesReadBack() {
        val source = System.getenv("SOURCE_DB") ?: return
        val compact = System.getenv("COMPACT_DB") ?: return
        val sql = "SELECT id, bookId, lineIndex, content, heRef, tocEntryId, charCount FROM line ORDER BY id"
        DriverManager.getConnection("jdbc:sqlite:file:$source?mode=ro").use { src ->
            DriverManager.getConnection("jdbc:sqlite:file:$compact?mode=ro").use { dst ->
                val codec =
                    LineTextCodec {
                        dst.createStatement().use { st ->
                            st.executeQuery("SELECT id, data FROM line_codec_dict").use { rs ->
                                buildMap { while (rs.next()) put(rs.getInt(1), rs.getBytes(2)) }
                            }
                        }
                    }
                codec.loadDictionaries()
                var lines = 0L
                var mismatches = 0L
                src.createStatement().use { a ->
                    dst.createStatement().use { b ->
                        val ra = a.executeQuery(sql)
                        val rb = b.executeQuery(sql)
                        while (ra.next()) {
                            check(rb.next()) { "The compact database has fewer lines" }
                            val same =
                                ra.getLong(1) == rb.getLong(1) && ra.getLong(2) == rb.getLong(2) && ra.getLong(3) == rb.getLong(3) &&
                                    ra.getString(4) == codec.decode(rb.getBytes(4)) && ra.getString(5) == rb.getString(5) &&
                                    ra.getObject(6) == rb.getObject(6) && ra.getLong(7) == rb.getLong(7)
                            if (!same) mismatches++
                            lines++
                        }
                        check(!rb.next()) { "The compact database has more lines" }
                    }
                }
                println("VERIFY lines $lines, mismatches $mismatches")
                assertEquals(0, mismatches)
            }
        }
    }
}
