package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import java.util.concurrent.ConcurrentHashMap

/**
 * Sefaria's Bavli commentaries name their own tractate in references the Vilna print
 * leaves unnamed (Rashi and Tosafot in the Orayta edition carry none):
 * "(לקמן מכות דף ט.)", "לקמן (מכות דף ט.)" and "בפרק כירה (שבת דף מד)". Only that
 * tractate is dropped:
 * - right after לקמן/לעיל/להלן (with ו/ד/כ/כד) inside an open bracket;
 * - right after a bracket, when a daf address follows: "דף"/"ד'", or a daf number
 *   (standard spelling, up to קעו) with . : , ע"א א' or a closing bracket.
 * Kept: a bracket after ירושלמי/תוספתא (any prefix letters, abbreviations, even across
 * a formatting tag — another work of the same name), a name not followed by a daf
 * address, and the name as a plain word.
 *
 * Display only: a changed line keeps the key of its Sefaria form and is marked as
 * cleaned, so line ids, links and the raw-offset gate behave as before.
 * Mirrors otzaria-library's get_from_export.drop_same_tractate.
 */
internal object SefariaSameTractateRefs {

    // Combining points and cantillation only; maqaf, paseq, sof pasuq and nun hafukha are punctuation.
    private const val NIKUD = "[\u0591-\u05BD\u05BF\u05C1\u05C2\u05C4\u05C5\u05C7]*"
    // Bava Batra, the longest tractate, ends at daf קעו.
    private const val LAST_DAF = 176
    private const val QUOTES = "\"'״׳"
    // Python's \s: Unicode spaces and the separators U+001C–U+001F, which occur in the export.
    private const val WS = """[\s\x1c-\x1f]"""
    private val gematria: Map<Char, Int> = "אבגדהוזחטיכלמנסעפצק".toList().zip(
        listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
    ).toMap()
    // Hundred, tens, units; ט"ו/ט"ז for 15/16. "זו" or "זה" is a word, not a number.
    private val dafNumber = Regex("""ק?(?:ט[וז]|י[א-דז-ט]?|[כלמנסעפצ]?[א-ט]?)""")
    private val dafAddress = Regex(
        """(?U)(?:דף|ד['׳])$WS""" +
            """|(?<num>[א-ת](?:["'״׳]?[א-ת]){0,2}["'״׳]?)""" +
            """(?:$WS*[.:]|$WS*,$WS*[אב](?![א-ת])|$WS+ע["'״׳]?[אב](?![א-ת])|$WS+[אב]['׳]?(?=[\s\x1c-\x1f)\],.;:])|(?=[)\]]))""",
    )
    // ירושלמי/תוספתא after any prefix letters, or abbreviated; then spaces, direction marks or tags.
    // Possessive: a backtracking group of alternatives recurses per repetition in java.util.regex.
    private const val OTHER_WORK = """(?<![א-ת])[ובדהכלמש]{0,4}(?:ירושלמי|ירושל['׳]|ירוש['׳]|ירו['׳]|תוספתא|תוספת['׳]|תוספ['׳])""" +
        """(?:[\s\x1c-\x1f\u200e\u200f]|<[^<>]*>)*+"""

    private class Patterns(val relative: Regex, val bracketed: Regex)
    private val patterns = ConcurrentHashMap<String, Patterns>()

    private fun patternsFor(tractateHe: String): Patterns = patterns.getOrPut(tractateHe) {
        val name = tractateHe.replace(Regex(NIKUD), "").map { c ->
            when (c) {
                ' ' -> "$WS+"
                '"', '״' -> "[\"״]"
                '\'', '׳' -> "['׳]"
                else -> Regex.escape(c.toString())
            }
        }.joinToString(NIKUD)
        val words = listOf("לקמן", "לעיל", "להלן").joinToString("|") { word -> word.map { it.toString() }.joinToString(NIKUD) }
        Patterns(
            relative = Regex("""(?U)(?<![א-ת])((?:ו?(?:כד|כ|ד)?)(?:$words)$NIKUD)$WS+$name$NIKUD(?=$WS)"""),
            bracketed = Regex("""(?U)(?<other>$OTHER_WORK)?([(\[])$name$NIKUD$WS+"""),
        )
    }

    internal fun isDafNumber(token: String): Boolean {
        val letters = token.filter { it !in QUOTES }
        return dafNumber.matches(letters) && letters.sumOf { gematria.getValue(it) } <= LAST_DAF
    }

    private fun startsWithDafAddress(text: String, at: Int): Boolean {
        val m = dafAddress.toPattern().matcher(text).region(at, text.length).useTransparentBounds(true)
        if (!m.lookingAt()) return false
        val num = m.group("num") ?: return true
        return isDafNumber(num)
    }

    /** [line] without the commentary's own tractate in its references; unchanged when [tractateHe] is null. */
    fun strip(line: String, tractateHe: String?): String {
        if (tractateHe == null || ('(' !in line && '[' !in line)) return line
        val p = patternsFor(tractateHe)
        // The last bracket before each match, scanned once along the line.
        var scanned = 0
        var inside = false
        val relative = p.relative.replace(line) { m ->
            for (i in scanned until m.range.first) {
                when (line[i]) {
                    '(', '[' -> inside = true
                    ')', ']' -> inside = false
                }
            }
            scanned = m.range.first
            if (inside) m.groupValues[1] else m.value
        }
        return p.bracketed.replace(relative) { m ->
            val keep = m.groups["other"] != null || !startsWithDafAddress(relative, m.range.last + 1)
            if (keep) m.value else m.groupValues[2]
        }
    }

    /**
     * Strips every line of [built]. A changed line gets a key override hashing its unstripped
     * raw segment (unless it already has one) and a cleaned shift, keeping its generated prefix.
     */
    fun apply(built: SefariaBookPayloadReader.BuiltBookContent, tractateHe: String?): SefariaBookPayloadReader.BuiltBookContent {
        if (tractateHe == null) return built
        val lines = built.lines.toMutableList()
        val shifts = HashMap(built.cleanShifts)
        val overrides = HashMap(built.lineKeyHashOverrides)
        var changed = false
        for ((idx, line) in built.lines.withIndex()) {
            val stripped = strip(line, tractateHe)
            if (stripped == line) continue
            val shift = shifts[idx]
            overrides.getOrPut(idx) { IdAllocatorBindings.lineNaturalKeyHash(rawSegmentForKey(line, shift)) }
            shifts[idx] = cleanedLineShift(shift?.let(::generatedPrefixLength) ?: 0)
            lines[idx] = stripped
            changed = true
        }
        return if (!changed) built else built.copy(lines = lines, cleanShifts = shifts, lineKeyHashOverrides = overrides)
    }
}
