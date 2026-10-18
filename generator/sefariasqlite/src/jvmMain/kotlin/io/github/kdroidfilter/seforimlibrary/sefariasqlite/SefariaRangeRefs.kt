package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Exact resolution of ranged Sefaria citations ("Exodus 1:1-6:1",
 * "Rashi on Genesis 18:10:2-3", "Berakhot 2a:1-2b:4").
 *
 * A range is a dash inside the citation's trailing numeric address run —
 * dashes inside title words never make a range. The end address inherits the
 * start's leading components: "13:11-13" → start 13:11, end 13:13.
 */

// Trailing "<start>-<end>" address run, preceded by a space: group 1 = start
// components, group 2 = end components. Anchored at end of the citation.
private val CITATION_RANGE_REGEX =
    Regex(" ((?:\\d+[ab]?)(?::\\d+[ab]?)*)-((?:\\d+[ab]?)(?::\\d+[ab]?)*)$")

// Trailing address run of a non-ranged canonical ref ("title 13:11:1" → "13:11:1").
private val TRAILING_ADDRESS_REGEX =
    Regex(" ((?:\\d+[ab]?)(?::\\d+[ab]?)*)$")

internal data class CitationRange(
    /** Canonical citation of the range's first address, e.g. "rashi on genesis 13:11". */
    val startCanonical: String,
    /** Canonical citation of the range's last address, e.g. "rashi on genesis 13:13". */
    val endCanonical: String,
)

/**
 * Parses a canonical citation into a [CitationRange], or null when the
 * citation has no trailing address range. The end side may cite fewer
 * components than the start ("18:10:2-3"); missing leading components are
 * inherited from the start. An end with MORE components than the start is
 * malformed and returns null.
 */
internal fun parseCitationRange(canonical: String): CitationRange? {
    val m = CITATION_RANGE_REGEX.find(canonical) ?: return null
    val titlePart = canonical.substring(0, m.range.first)
    if (titlePart.isBlank()) return null
    val startComps = m.groupValues[1].split(':')
    val endComps = m.groupValues[2].split(':')
    if (endComps.size > startComps.size) return null
    val fullEnd = startComps.dropLast(endComps.size) + endComps
    if (fullEnd == startComps) return null
    return CitationRange(
        startCanonical = "$titlePart ${startComps.joinToString(":")}",
        endCanonical = "$titlePart ${fullEnd.joinToString(":")}",
    )
}

/**
 * Per-book-path index: canonical citation prefix → the LAST (max lineIndex)
 * leaf entry under that prefix. Prefix keys are emitted at every address
 * depth ("title 13", "title 13:11", "title 13:11:1"), so a range end cited at
 * any level resolves to the last line it covers. Refs without a trailing
 * address run are keyed by their full canonical form only.
 */
internal class PathRefPrefixIndex private constructor(
    private val lastByPrefix: Map<String, RefEntry>,
) {
    fun lastUnder(canonicalPrefix: String): RefEntry? = lastByPrefix[canonicalPrefix]

    companion object {
        fun build(entries: List<RefEntry>): PathRefPrefixIndex {
            val last = HashMap<String, RefEntry>(entries.size * 2)
            for (entry in entries) {
                val canonical = canonicalCitation(entry.ref)
                for (prefix in addressPrefixes(canonical)) {
                    val current = last[prefix]
                    if (current == null || entry.lineIndex > current.lineIndex) {
                        last[prefix] = entry
                    }
                }
            }
            return PathRefPrefixIndex(last)
        }
    }
}

/**
 * Corpus-wide index: canonical citation of a whole section → its first and last
 * segment. Keys are every proper address prefix ("tosafot on gittin 51a",
 * "jerusalem talmud megillah 1") and, for an amud, its whole daf ("gittin 51",
 * Sefaria's normal form of "gittin 51a-51b"). A key shared by two books is dropped.
 */
internal class SectionRefIndex private constructor(
    private val first: Map<String, RefEntry>,
    private val last: Map<String, RefEntry>,
) {
    fun firstUnder(section: String): RefEntry? = first[section]

    fun lastUnder(section: String): RefEntry? = last[section]

    companion object {
        fun build(entries: List<RefEntry>): SectionRefIndex {
            val first = HashMap<String, RefEntry>()
            val last = HashMap<String, RefEntry>()
            val shared = HashSet<String>()
            for (entry in entries) {
                for (key in sectionKeys(canonicalCitation(entry.ref))) {
                    val lowest = first[key]
                    if (lowest != null && lowest.path != entry.path) shared += key
                    if (lowest == null || entry.lineIndex < lowest.lineIndex) first[key] = entry
                    val highest = last[key]
                    if (highest == null || entry.lineIndex > highest.lineIndex) last[key] = entry
                }
            }
            first.keys.removeAll(shared)
            last.keys.removeAll(shared)
            return SectionRefIndex(first, last)
        }
    }
}

/**
 * First and last segment of a citation naming one whole section ("gittin 51").
 * A dashed citation is not one section: the linker reads "פ"ב-ח" notation as a range.
 */
internal fun sectionTarget(citation: String, sections: SectionRefIndex): Pair<RefEntry, RefEntry>? {
    val canonical = canonicalCitation(citation)
    val start = sections.firstUnder(canonical) ?: return null
    val end = sections.lastUnder(canonical) ?: return null
    return start to end
}

private val AMUD_REGEX = Regex("(\\d+)[ab]")

/** "rashi on gittin 51a:3:1" → ["rashi on gittin 51a", "rashi on gittin 51a:3", "rashi on gittin 51"]. */
internal fun sectionKeys(canonical: String): List<String> {
    if (TRAILING_ADDRESS_REGEX.find(canonical) == null) return emptyList()
    val prefixes = addressPrefixes(canonical)
    val keys = prefixes.dropLast(1).toMutableList()
    val amud = AMUD_REGEX.matchEntire(prefixes[0].substringAfterLast(' '))
    if (amud != null) keys += prefixes[0].substringBeforeLast(' ') + " " + amud.groupValues[1]
    return keys
}

/** "title 13:11:1" → ["title 13", "title 13:11", "title 13:11:1"]. */
internal fun addressPrefixes(canonical: String): List<String> {
    val m = TRAILING_ADDRESS_REGEX.find(canonical) ?: return listOf(canonical)
    val titlePart = canonical.substring(0, m.range.first)
    if (titlePart.isBlank()) return listOf(canonical)
    val comps = m.groupValues[1].split(':')
    val prefixes = ArrayList<String>(comps.size)
    val sb = StringBuilder(titlePart)
    for (comp in comps) {
        sb.append(if (prefixes.isEmpty()) " " else ":").append(comp)
        prefixes += sb.toString()
    }
    return prefixes
}
