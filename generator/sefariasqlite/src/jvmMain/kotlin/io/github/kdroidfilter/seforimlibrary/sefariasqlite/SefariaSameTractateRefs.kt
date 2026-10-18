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
 *   (descending letters, up to קעו) with . : , ע"א א' or a closing bracket.
 * Kept: a bracket after ירושלמי/תוספתא (another work of the same name), a name not
 * followed by a daf address, and the name as a plain word.
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
    private val gematria: Map<Char, Int> = "אבגדהוזחטיכךלמםנןסעפףצץקרשת".toList().zip(
        listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 20, 30, 40, 40, 50, 50, 60, 70, 80, 80, 90, 90, 100, 200, 300, 400),
    ).toMap()
    private val dafAddress = Regex(
        """(?U)(?:דף|ד['׳])\s""" +
            """|(?<num>[א-ת](?:["'״׳]?[א-ת]){0,2}["'״׳]?)""" +
            """(?:\s*[.:]|\s*,\s*[אב](?![א-ת])|\s+ע["'״׳]?[אב](?![א-ת])|\s+[אב]['׳]?(?=[\s)\],.;:])|(?=[)\]]))""",
    )
    private val otherWork = Regex("""(?U)(?<![א-ת])(?:ו|ב|ה|וב|וה|ובה|ד|וד)?(?:ירושלמי|ירוש['׳]|תוספתא|תוספת['׳])\s*\z""")

    private class Patterns(val relative: Regex, val bracketed: Regex)
    private val patterns = ConcurrentHashMap<String, Patterns>()

    private fun patternsFor(tractateHe: String): Patterns = patterns.getOrPut(tractateHe) {
        val name = tractateHe.replace(Regex(NIKUD), "").map { c ->
            when (c) {
                ' ' -> """\s+"""
                '"', '״' -> "[\"״]"
                '\'', '׳' -> "['׳]"
                else -> Regex.escape(c.toString())
            }
        }.joinToString(NIKUD)
        val words = listOf("לקמן", "לעיל", "להלן").joinToString("|") { word -> word.map { it.toString() }.joinToString(NIKUD) }
        Patterns(
            relative = Regex("""(?U)(?<![א-ת])((?:ו?(?:כד|כ|ד)?)(?:$words)$NIKUD)\s+$name$NIKUD(?=\s)"""),
            bracketed = Regex("""(?U)([(\[])$name$NIKUD\s+"""),
        )
    }

    internal fun isDafNumber(token: String): Boolean {
        val letters = token.filter { it !in QUOTES }
        if (letters == "טו" || letters == "טז") return true
        val values = letters.map { gematria.getValue(it) }
        return values.zipWithNext().all { (a, b) -> a > b } && values.sum() <= LAST_DAF
    }

    internal fun insideBracket(text: String, pos: Int): Boolean {
        val opened = maxOf(text.lastIndexOf('(', pos - 1), text.lastIndexOf('[', pos - 1))
        return opened > maxOf(text.lastIndexOf(')', pos - 1), text.lastIndexOf(']', pos - 1))
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
        val relative = p.relative.replace(line) { m -> if (insideBracket(line, m.range.first)) m.groupValues[1] else m.value }
        return p.bracketed.replace(relative) { m ->
            val keep = !startsWithDafAddress(relative, m.range.last + 1) ||
                otherWork.containsMatchIn(relative.substring(0, m.range.first))
            if (keep) m.value else m.groupValues[1]
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
