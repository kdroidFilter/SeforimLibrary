package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import java.util.concurrent.ConcurrentHashMap

/**
 * Sefaria's Bavli commentaries insert the commentary's own tractate after לקמן/לעיל:
 * "(לקמן מכות דף ט.)" where the Vilna print reads "(לקמן דף ט.)". Only that tractate
 * is dropped, and only right after the word: לקמן/לעיל never point to another tractate.
 *
 * Display only: a changed line keeps the key of its Sefaria form and is marked as
 * cleaned, so line ids, links and the raw-offset gate behave as before.
 */
internal object SefariaSameTractateRefs {

    private const val NIKUD = "[֑-ׇ]*"
    private val patterns = ConcurrentHashMap<String, Regex>()

    /** Matches "(לקמן <tractate>" / "(לעיל <tractate>", with or without nikud, before a space. */
    fun patternFor(tractateHe: String): Regex = patterns.getOrPut(tractateHe) {
        val name = tractateHe.replace(Regex(NIKUD), "").map { c ->
            when (c) {
                ' ' -> """\s+"""
                '"', '״' -> "[\"״]"
                '\'', '׳' -> "['׳]"
                else -> Regex.escape(c.toString())
            }
        }.joinToString(NIKUD)
        val words = listOf("לקמן", "לעיל").joinToString("|") { word -> word.map { it.toString() }.joinToString(NIKUD) }
        Regex("""(?U)\(($words)$NIKUD\s+$name$NIKUD(?=\s)""")
    }

    /** [line] without the commentary's own tractate after לקמן/לעיל; unchanged when [tractateHe] is null. */
    fun strip(line: String, tractateHe: String?): String =
        if (tractateHe == null || '(' !in line) line else patternFor(tractateHe).replace(line, "($1")

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
