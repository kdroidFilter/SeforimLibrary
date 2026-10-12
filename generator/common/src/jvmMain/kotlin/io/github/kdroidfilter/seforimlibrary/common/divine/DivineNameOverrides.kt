package io.github.kdroidfilter.seforimlibrary.common.divine

enum class OverrideDecision { SACRED, PROFANE }

/**
 * One reviewed verse-level decision. [kind] null means every kind except HVYH
 * (written `*`). A SACRED entry names one verse and one kind; it forces every word
 * of that verse whose body spells the kind (source EXCEPTION_LIST).
 */
data class DivineOverride(
    val from: VerseRef,
    val to: VerseRef,
    val kind: DivineKind?,
    val decision: OverrideDecision,
    val source: String,
    val note: String,
) {
    fun covers(verse: VerseRef): Boolean =
        verse.book == from.book && verse >= from && verse <= to

    fun appliesTo(kind: DivineKind): Boolean = this.kind == null || this.kind == kind
}

/**
 * The curated verse list — otzaria-library `ForDB/divine_name_overrides.csv`
 * (`ref,kind,decision,source,note`). PROFANE wins over every rule; a SACRED entry
 * for the exact verse and kind wins over a PROFANE range (Gen 19 is profane except
 * 19:18 ADNUT). Applies to Tanach base-book lines only.
 *
 * Without a usable list the build treats every Tanach base line as HVYH-only
 * (see rebuildLineDivineNameIndex); this class never has a built-in default.
 */
class DivineNameOverrides(val entries: List<DivineOverride>) {
    private val byBook: Map<String, List<DivineOverride>> = entries.groupBy { it.from.book }

    fun sacredKinds(verse: VerseRef): Set<DivineKind> =
        byBook[verse.book].orEmpty()
            .filter { it.decision == OverrideDecision.SACRED && it.covers(verse) }
            .mapNotNullTo(LinkedHashSet()) { it.kind }

    fun isProfane(verse: VerseRef, kind: DivineKind): Boolean {
        if (kind == DivineKind.HVYH) return false
        if (kind in sacredKinds(verse)) return false
        return byBook[verse.book].orEmpty()
            .any { it.decision == OverrideDecision.PROFANE && it.covers(verse) && it.appliesTo(kind) }
    }

    /** [overrides] is null when any row was not understood — the caller must not use a partial list. */
    data class Parsed(val overrides: DivineNameOverrides?, val problems: List<String>)

    companion object {
        const val FILE_NAME = "divine_name_overrides.csv"
        val HEADER = listOf("ref", "kind", "decision", "source", "note")

        private val REF = Regex("""^(.+?)\s+(\d+):(\d+)(?:\s*-\s*(?:(\d+):)?(\d+))?$""")

        /**
         * `שמות 3:14`, `שופטים 17:1-18:31`, `שמות 22:7-8` → a verse range of one Tanach book,
         * or null when the book is not one of [TANACH_TITLES] or the range is empty.
         */
        fun parseRef(ref: String): Pair<VerseRef, VerseRef>? {
            val m = REF.matchEntire(ref.trim()) ?: return null
            val book = m.groupValues[1].trim()
            if (book !in TANACH_TITLES) return null
            val chapter = m.groupValues[2].toInt()
            val from = VerseRef(book, chapter, m.groupValues[3].toInt())
            val to = when {
                m.groupValues[5].isEmpty() -> from
                m.groupValues[4].isEmpty() -> VerseRef(book, chapter, m.groupValues[5].toInt())
                else -> VerseRef(book, m.groupValues[4].toInt(), m.groupValues[5].toInt())
            }
            if (from.chapter < 1 || from.verse < 1 || to < from) return null
            return from to to
        }

        /**
         * Parses CSV records (header first, as `parseForDbCsvRecords` yields them). Every
         * problem is reported; any problem makes the whole list unusable (fail-closed: a
         * silently dropped profane row would let a profane word through).
         */
        fun parse(records: List<List<String>>): Parsed {
            if (records.isEmpty()) return Parsed(null, listOf("$FILE_NAME is empty"))
            if (records.first().map(String::trim) != HEADER) {
                return Parsed(null, listOf("$FILE_NAME must start with the header ${HEADER.joinToString(",")}"))
            }
            val problems = ArrayList<String>()
            val out = ArrayList<DivineOverride>()
            records.drop(1).forEachIndexed { index, fields ->
                val where = "$FILE_NAME record ${index + 2}"
                if (fields.size != HEADER.size) { problems += "$where has ${fields.size} fields, expected ${HEADER.size}"; return@forEachIndexed }
                val range = parseRef(fields[0])
                if (range == null) { problems += "$where: unknown or malformed ref '${fields[0]}'"; return@forEachIndexed }
                val kindText = fields[1].trim()
                val kind = if (kindText == "*") null else runCatching { DivineKind.parse(kindText) }.getOrNull()
                if (kind == null && kindText != "*") { problems += "$where: unknown kind '$kindText'"; return@forEachIndexed }
                if (kind == DivineKind.HVYH || kind == DivineKind.YAH) {
                    problems += "$where: $kind is never overridden by the list"; return@forEachIndexed
                }
                val decision = runCatching { OverrideDecision.valueOf(fields[2].trim().uppercase()) }.getOrNull()
                if (decision == null) { problems += "$where: unknown decision '${fields[2]}'"; return@forEachIndexed }
                if (decision == OverrideDecision.SACRED && (kind == null || range.first != range.second)) {
                    problems += "$where: a sacred entry names one verse and one kind"; return@forEachIndexed
                }
                if (fields[4].isBlank()) { problems += "$where: note is required"; return@forEachIndexed }
                out += DivineOverride(range.first, range.second, kind, decision, fields[3].trim(), fields[4].trim())
            }
            val sacred = out.filter { it.decision == OverrideDecision.SACRED }
            sacred.groupBy { it.from to it.kind }.filterValues { it.size > 1 }.keys.forEach {
                problems += "$FILE_NAME repeats the sacred entry ${it.first} ${it.second}"
            }
            for (s in sacred) {
                if (out.any { it.decision == OverrideDecision.PROFANE && it.from == s.from && it.to == s.to && it.kind == s.kind }) {
                    problems += "$FILE_NAME marks ${s.from} ${s.kind} both sacred and profane"
                }
            }
            return Parsed(if (problems.isEmpty()) DivineNameOverrides(out) else null, problems)
        }
    }
}
