package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import java.util.concurrent.ConcurrentHashMap

internal fun trimTrailingSeparators(value: String): String =
    value.trimEnd(':', ' ', ',')

// Pre-computed gematria lookup table for common values
private val gematriaCache = ConcurrentHashMap<Int, String>()

// Hot-path regex constants: these were being recompiled on every CSV row in
// the Sefaria links phase (~7.5 M rows × 2-3 calls/row = tens of millions of
// recompilations). JFR 2026-05-13 showed normalizeCitation + canonicalBase
// near the top of the post-optimisation hot list.
private val WHITESPACE_REGEX = "\\s+".toRegex()
private val COLON_VERSE_TAIL_REGEX = Regex(":\\d+[ab]?(?:-\\d+[ab]?)?$")
private val TRAILING_SPACES_NUMBER_REGEX = Regex(" +(\\d+[ab]?)$")

// Memoize canonicalCitation: the same source strings ("Genesis 1:1",
// "Berakhot 2a", …) get canonicalised millions of times across the links
// phase. Bounded so a pathological corpus can't blow up the heap.
private const val CANONICAL_CACHE_MAX = 500_000
private val canonicalCitationCache = ConcurrentHashMap<String, String>()

/** The form [toGematria] printed for 1000–1199 before "תתר" ("א ", "א א"); line ids built from it stay. */
internal fun legacyThousandsGematria(num: Int): String? =
    if (num in 1000..1199) "${toGematria(num / 1000)} ${if (num % 1000 > 0) toGematria(num % 1000) else ""}" else null

internal fun toGematria(num: Int): String {
    if (num <= 0) return num.toString()

    // Check cache first
    gematriaCache[num]?.let { return it }

    // Below 1200 the print repeats tav ("תתר" for a thousand), like Sefaria does.
    val thousands = if (num < 1200) 0 else num / 1000
    var remainder = if (num < 1200) num else num % 1000
    val builder = StringBuilder()
    if (thousands > 0) {
        builder.append(toGematria(thousands)).append(' ')
    }

    val hundredsMap = listOf(
        400 to "ת",
        300 to "ש",
        200 to "ר",
        100 to "ק"
    )
    for ((value, letter) in hundredsMap) {
        while (remainder >= value) {
            builder.append(letter)
            remainder -= value
        }
    }

    if (remainder == 15) {
        builder.append("טו")
        remainder = 0
    } else if (remainder == 16) {
        builder.append("טז")
        remainder = 0
    }

    val tensMap = listOf(
        90 to "צ",
        80 to "פ",
        70 to "ע",
        60 to "ס",
        50 to "נ",
        40 to "מ",
        30 to "ל",
        20 to "כ",
        10 to "י"
    )
    for ((value, letter) in tensMap) {
        if (remainder >= value) {
            builder.append(letter)
            remainder -= value
        }
    }

    val unitsMap = listOf(
        9 to "ט",
        8 to "ח",
        7 to "ז",
        6 to "ו",
        5 to "ה",
        4 to "ד",
        3 to "ג",
        2 to "ב",
        1 to "א"
    )
    for ((value, letter) in unitsMap) {
        if (remainder >= value) {
            builder.append(letter)
            remainder -= value
        }
    }

    val result = builder.toString()
    // Cache only small values to avoid memory bloat
    if (num < 10000) {
        gematriaCache[num] = result
    }
    return result
}

internal fun toDaf(index: Int): String {
    val i = index + 1
    return if (i % 2 == 0) "${toGematria(i / 2)}." else "${toGematria(i / 2)}:"
}

internal fun toEnglishDaf(index: Int): String {
    val i = index + 1
    return if (i % 2 == 0) "${i / 2}a" else "${i / 2}b"
}

internal fun parseCsvLine(line: String): List<String> {
    val result = mutableListOf<String>()
    val sb = StringBuilder()
    var inQuotes = false
    var i = 0
    val len = line.length
    while (i < len) {
        val c = line[i]
        if (inQuotes) {
            if (c == '"') {
                if (i + 1 < len && line[i + 1] == '"') {
                    sb.append('"')
                    i++
                } else {
                    inQuotes = false
                }
            } else {
                sb.append(c)
            }
        } else {
            when (c) {
                '"' -> inQuotes = true
                ',' -> {
                    result += sb.toString()
                    sb.setLength(0)
                }

                else -> sb.append(c)
            }
        }
        i++
    }
    result += sb.toString()
    return result
}

internal fun normalizeCitation(raw: String): String =
    raw.trim().trim('"', '\'').replace(WHITESPACE_REGEX, " ")

internal fun canonicalCitation(raw: String): String {
    // Fast cache hit on the typical "same citation seen N times" pattern.
    canonicalCitationCache[raw]?.let { return it }
    val computed = normalizeCitation(raw).replace(",", "").lowercase()
    // Bound the cache: drop everything if it crosses the cap. Simpler than
    // an LRU and good enough since the corpus has a bounded set of unique
    // citations (~hundreds of thousands).
    if (canonicalCitationCache.size > CANONICAL_CACHE_MAX) canonicalCitationCache.clear()
    canonicalCitationCache[raw] = computed
    return computed
}

internal fun canonicalTail(raw: String): String {
    val canonical = canonicalCitation(raw)
    val tokens = canonical.split(' ').filter { it.isNotBlank() }
    val startIdx = tokens.indexOfFirst { token ->
        token.any { it.isDigit() } || token.contains(':') || token.contains('-')
    }
    return if (startIdx >= 0) tokens.drop(startIdx).joinToString(" ") else canonical
}

internal fun stripBookAlias(canonical: String, aliases: Set<String>): String {
    var result = canonical
    for (alias in aliases) {
        if (alias.isBlank()) continue
        if (result == alias) {
            result = ""
            break
        }
        if (result.startsWith("$alias ")) {
            result = result.removePrefix(alias).trimStart()
            break
        }
    }
    return result.ifBlank { canonical }
}

internal fun canonicalBase(citation: String): String {
    val normalized = canonicalCitation(citation)
    val stripAfterColon = normalized.replace(COLON_VERSE_TAIL_REGEX, "")
    return stripAfterColon
        .replace(TRAILING_SPACES_NUMBER_REGEX, " $1")
        .trim()
}

internal fun citationRangeStart(citation: String): String? {
    val dashParts = citation.split('-', limit = 2)
    val start = dashParts.firstOrNull()?.trim().orEmpty()
    if (start.isBlank()) return null
    return canonicalCitation(start)
}

internal fun resolveRefs(
    citation: String,
    refsByCanonical: Map<String, List<RefEntry>>,
    refsByBase: Map<String, RefEntry>
): List<RefEntry> {
    val canonical = canonicalCitation(citation)
    refsByCanonical[canonical]?.let { if (it.isNotEmpty()) return it }

    val rangeStart = citationRangeStart(canonical)
    if (rangeStart != null) {
        refsByCanonical[rangeStart]?.let { if (it.isNotEmpty()) return it }
        // A range start at an intermediate level (e.g. "Ibn Ezra on Genesis 6:1"
        // where leaves are "6:1:X") IS a base key — look it up verbatim before
        // canonicalBase(), which would strip one component too many and miss.
        refsByBase[rangeStart]?.let { return listOf(it) }
        refsByBase[canonicalBase(rangeStart)]?.let { return listOf(it) }
        if (!rangeStart.contains(":")) {
            val baseWithOne = canonicalBase("$rangeStart 1")
            refsByBase[baseWithOne]?.let { return listOf(it) }
        }
    }

    if (canonical.count { it == ':' } == 1) {
        val canonicalWithOne = "$canonical:1"
        refsByCanonical[canonicalWithOne]?.let { if (it.isNotEmpty()) return it }
        refsByBase[canonicalBase(canonicalWithOne)]?.let { return listOf(it) }
    }

    refsByBase[canonicalBase(canonical)]?.let { return listOf(it) }
    if (!canonical.contains(":")) {
        val baseWithOne = canonicalBase("$canonical 1")
        refsByBase[baseWithOne]?.let { return listOf(it) }
    }
    return emptyList()
}

/**
 * Reconstructs the full end ref of a dashed citation ("exodus 29:43-44" → "exodus 29:44",
 * "exodus 29:43-30:2" → "exodus 30:2", "berakhot 2a-2b" → "berakhot 2b"): the end part
 * replaces the same number of trailing components it spells out. Null when there is no
 * usable end part.
 */
internal fun rangeEndRef(rangeStart: String, canonical: String): String? {
    val endPart = canonical.split('-', limit = 2).getOrNull(1)?.trim().orEmpty()
    if (endPart.isBlank()) return null
    if (endPart.contains(' ')) return canonicalCitation(endPart)
    val endComponents = endPart.split(':')
    if (rangeStart.contains(':')) {
        val startComponents = rangeStart.split(':')
        if (endComponents.size >= startComponents.size) {
            // The end spells out the full address ("29:43-30:2") — the book name
            // still lives in component 0 of the start ("exodus 29"); keep it.
            val lastSpace = startComponents.first().lastIndexOf(' ')
            if (lastSpace < 0) return canonicalCitation(endPart)
            return canonicalCitation(
                startComponents.first().substring(0, lastSpace + 1) + endPart
            )
        }
        return canonicalCitation(
            (startComponents.dropLast(endComponents.size) + endComponents).joinToString(":")
        )
    }
    val lastSpace = rangeStart.lastIndexOf(' ')
    if (lastSpace < 0) return null
    return canonicalCitation(rangeStart.substring(0, lastSpace + 1) + endPart)
}

/**
 * The LAST segment of the scope [citation] denotes, or null when it names a single exact
 * segment. Mirrors [resolveRefs]' branch order — whichever branch resolves the start also
 * fixes the scope: an exact canonical hit is one segment (no range); a dashed citation ends
 * at its reconstructed end ref (itself section-level → that base's last entry); a base /
 * ":1"-completion fallback covers the whole base, so its scope ends at [lastByBase]'s entry.
 * [lastByBase] must be built like refsByBase but keeping the HIGHEST lineIndex per base.
 * Callers must still validate same-path and end>start before trusting the result.
 */
internal fun resolveRefEnd(
    citation: String,
    refsByCanonical: Map<String, List<RefEntry>>,
    refsByBase: Map<String, RefEntry>,
    lastByBase: Map<String, RefEntry>,
): RefEntry? {
    val canonical = canonicalCitation(citation)
    if (refsByCanonical[canonical]?.isNotEmpty() == true) return null

    val rangeStart = citationRangeStart(canonical)
    if (rangeStart != null && rangeStart != canonical) {
        val endRef = rangeEndRef(rangeStart, canonical) ?: return null
        refsByCanonical[endRef]?.let { list ->
            list.maxByOrNull { it.lineIndex }?.let { return it }
        }
        lastByBase[endRef]?.let { return it }
        lastByBase[canonicalBase(endRef)]?.let { return it }
        return null
    }

    if (canonical.count { it == ':' } == 1) {
        val canonicalWithOne = "$canonical:1"
        if (refsByCanonical[canonicalWithOne]?.isNotEmpty() == true ||
            refsByBase[canonicalBase(canonicalWithOne)] != null
        ) {
            return lastByBase[canonicalBase(canonicalWithOne)]
        }
    }

    refsByBase[canonicalBase(canonical)]?.let { return lastByBase[canonicalBase(canonical)] }
    if (!canonical.contains(":")) {
        val baseWithOne = canonicalBase("$canonical 1")
        refsByBase[baseWithOne]?.let { return lastByBase[baseWithOne] }
    }
    return null
}

