package io.github.kdroidfilter.seforimlibrary.reflinks

import io.github.kdroidfilter.seforimlibrary.reflinks.HebrewText.gematria
import io.github.kdroidfilter.seforimlibrary.reflinks.HebrewText.markerNumber
import io.github.kdroidfilter.seforimlibrary.reflinks.HebrewText.nameKey
import io.github.kdroidfilter.seforimlibrary.reflinks.HebrewText.normalizeQuotes

/** Known ambiguities the deterministic parser cannot settle; such citations go to the LLM arbitration. */
enum class Ambiguity {
    /** "שבת פ\"ב": chapter 2 of the Mishna, or (much more often) folio 82. */
    PEREK_OR_DAF,

    /** "מ\"א ב, ו": Kings I, or Magen Avraham (מ\"ב: Kings II or Mishna Berura). */
    KINGS_OR_COMMENTARY,

    /** "או\"ח סי' רנא": Shulchan Arukh, Tur, Mishna Berura, or another book organized by the Tur's parts. */
    TUR_PART_ONLY,
}

/** A parse result: the most likely reading, plus the alternatives when the citation is [ambiguity]. */
data class ParsedCitation(
    val citation: Citation,
    val ambiguity: Ambiguity? = null,
    val alternatives: List<Citation> = emptyList(),
)

/**
 * Parses the text of one parenthesized group ("בראשית א, ד", "ב\"ק כב:", "רמב\"ם פ\"ב מהל' שבת ה\"ג") into a citation.
 * Only the first ';'-separated segment is read, and only the longest prefix that forms a citation is kept, so
 * "רש\"י סוכה מ ב ד\"ה אדם" links "רש\"י סוכה מ ב".
 *
 * @param rambamTitles the `book.title` of every Mishneh Torah book ("משנה תורה, הלכות שבת"), to map hilchot names.
 */
class CitationParser(rambamTitles: Collection<String>) {

    private val aliases: Map<String, List<Pair<AliasKind, String?>>> = buildAliases()
    private val maxAliasWords: Int = aliases.keys.maxOf { it.split(' ').size }
    private val hilchot: Map<String, String> = buildHilchot(rambamTitles)

    private enum class AliasKind {
        TANAKH, TRACTATE, MISHNA, YERUSHALMI, TOSEFTA, RASHI_BAVLI, TOSAFOT, RABBA, SA, TUR, RAMBAM, MISC, ZOHAR,
    }

    /** Parses the inside of a "( … )" / "[ … ]" group, without the brackets. */
    fun parse(group: String): ParsedCitation? {
        val body = HebrewText.collapseSpaces(normalizeQuotes(group)).trim()
        val segment = body.split(Regex("\\s*;\\s*")).first()
        return parseSegment(stripIntro(segment))
    }

    /** Parses a citation whose book name is written before the parentheses: name "בישעיה", location "מ, ג". */
    fun parseWithName(name: String, location: String): ParsedCitation? {
        val n = normalizeQuotes(name).trim(' ', ',', ':')
        // Prepositions glued to the name: בישעיה, ובחולין, כדברי…, שבשבת
        return listOf("", "ב", "ו", "וב", "ד", "וד", "ל", "מ", "כ", "וכ", "שב")
            .filter { n.startsWith(it) && n.length > it.length + 1 }
            .firstNotNullOfOrNull { prefix ->
                val bare = n.removePrefix(prefix)
                parse("$bare $location")?.takeIf {
                    it.citation.alias == bare && it.citation.trailingTokens == 0 && it.citation.precision != Precision.BOOK
                }
            }
    }

    fun stripIntro(segment: String): String {
        var s = segment.trim()
        var changed = true
        while (changed) {
            changed = false
            for (w in BookAliases.introWords) {
                if (s.startsWith("$w ")) {
                    s = s.substring(w.length + 1).trim()
                    changed = true
                }
            }
        }
        return s
    }

    private fun parseSegment(body: String): ParsedCitation? {
        val (kinds, rest, alias) = matchName(body) ?: return null
        val tokens = cleanLocation(rest).split(' ').filter { it.isNotBlank() }
        val results = kinds.mapNotNull { (kind, canon) ->
            // Longest prefix of the location that parses; an empty location yields a book-level citation.
            (tokens.size downTo 0).firstNotNullOfOrNull { k ->
                tryKind(kind, canon, tokens.take(k).joinToString(" ").trim(' ', ','), alias)
                    ?.takeIf { k > 0 || tokens.isEmpty() }
                    ?.let { it.copy(citation = it.citation.copy(trailingTokens = tokens.size - k)) }
            }
        }.distinctBy { it.citation.family to it.citation.title }
        return results.maxByOrNull { it.citation.precision.ordinal }
    }

    // ─── Name matching ────────────────────────────────────────────────────────────────────────────

    private fun matchName(body: String): Triple<List<Pair<AliasKind, String?>>, String, String>? {
        val words = body.split(' ').filter { it.isNotEmpty() }
        if (words.isEmpty()) return null
        val variants = buildList {
            add(words)
            val first = words[0]
            // Prefixed prepositions: בשבת, ובשבת, דשבת
            if (first.length > 2 && first[0] in "בוד") add(listOf(first.substring(1)) + words.drop(1))
            if (first.length > 3 && first.take(2) in setOf("וב", "וד")) add(listOf(first.substring(2)) + words.drop(1))
        }
        for (ws in variants) {
            for (k in minOf(maxAliasWords, ws.size) downTo 1) {
                val alias = ws.take(k).joinToString(" ")
                aliases[nameKey(alias)]?.let { return Triple(it, ws.drop(k).joinToString(" ").trim(' ', ','), alias) }
            }
        }
        return null
    }

    private fun buildAliases(): Map<String, List<Pair<AliasKind, String?>>> {
        val map = mutableMapOf<String, MutableList<Pair<AliasKind, String?>>>()
        fun add(alias: String, kind: AliasKind, canon: String?) {
            val list = map.getOrPut(nameKey(alias)) { mutableListOf() }
            if (kind to canon !in list) list += kind to canon
        }
        BookAliases.tanakh.forEach { (canon, al) -> al.forEach { add(it, AliasKind.TANAKH, canon) } }
        BookAliases.tractates.forEach { (canon, al) ->
            al.forEach { a ->
                add(a, AliasKind.TRACTATE, canon)
                listOf("מסכת", "גמ'", "גמרא", "בבלי").forEach { add("$it $a", AliasKind.TRACTATE, canon) }
                listOf("משנה", "מתני'").forEach { add("$it $a", AliasKind.MISHNA, canon) }
                listOf("ירושלמי", "ירוש'").forEach { add("$it $a", AliasKind.YERUSHALMI, canon) }
                listOf("תוספתא", "תוספתא דמסכת").forEach { add("$it $a", AliasKind.TOSEFTA, canon) }
                add("רש\"י $a", AliasKind.RASHI_BAVLI, canon)
                listOf("תוס'", "תוספות").forEach { add("$it $a", AliasKind.TOSAFOT, canon) }
            }
        }
        BookAliases.rabba.forEach { (canon, al) ->
            al.forEach { add(it, AliasKind.RABBA, canon); add("מדרש $it", AliasKind.RABBA, canon) }
        }
        BookAliases.shulchanArukhParts.forEach { (canon, al) ->
            al.forEach { a ->
                add(a, AliasKind.SA, canon)
                listOf("שו\"ע", "שולחן ערוך", "ש\"ע", "טוש\"ע").forEach { add("$it $a", AliasKind.SA, canon) }
                add("טור $a", AliasKind.TUR, canon)
            }
        }
        listOf("שו\"ע", "שולחן ערוך").forEach { add(it, AliasKind.SA, null) }
        add("טור", AliasKind.TUR, null)
        listOf("רמב\"ם", "הרמב\"ם", "רמבם", "משנה תורה", "יד החזקה").forEach { add(it, AliasKind.RAMBAM, null) }
        BookAliases.misc.forEach { (canon, al) -> al.forEach { add(it, AliasKind.MISC, canon) } }
        listOf(1 to "א", 2 to "ב", 3 to "ג").forEach { (vol, l) ->
            listOf("זח\"$l", "זוהר ח\"$l", "זהר ח\"$l", "זוה\"ק ח\"$l").forEach { add(it, AliasKind.ZOHAR, vol.toString()) }
        }
        return map
    }

    private fun buildHilchot(titles: Collection<String>): Map<String, String> {
        val map = mutableMapOf<String, String>()
        for (title in titles) {
            val name = title.substringAfter("הלכות", "").trim().ifEmpty { continue }
            map.putIfAbsent(nameKey(name), title)
            BookAliases.rambamExtra[name]?.forEach { map.putIfAbsent(nameKey(it), title) }
            // First member of a compound name: "תפילין ומזוזה וספר תורה" → "תפילין"
            map.putIfAbsent(nameKey(name.split(" ו").first()), title)
        }
        return map
    }

    // ─── Location grammars ────────────────────────────────────────────────────────────────────────

    private fun cleanLocation(rest: String): String = HebrewText.collapseSpaces(normalizeQuotes(rest)).trim(' ', ',', ';')

    private fun num(token: String?): Int? = token?.let { gematria(it) }

    private data class ChapterVerse(val chapter: Int, val verse: Int?, val verseEnd: Int?)

    private fun chapterVerse(rest: String): ChapterVerse? {
        val r = rest.replace(Regex("^(פרק|פ')\\s*"), "").replace(Regex("\\s*(פסוק|פס')\\s*"), " ").trim()
        val m = Regex("^($NUM)(?:$SEP($NUM)(?:\\s*[-–]\\s*($NUM))?)?\\.?$").find(r) ?: return null
        val groups = m.groupValues.drop(1).filter { it.isNotEmpty() }
        if (groups.any { it == "ע\"א" || it == "ע\"ב" }) return null
        val chapter = num(m.groupValues[1]) ?: return null
        val verse = m.groupValues[2].takeIf { it.isNotEmpty() }?.let { num(it) ?: return null }
        return ChapterVerse(chapter, verse, num(m.groupValues[3].ifEmpty { null }))
    }

    private data class DafLocation(val daf: Section.Daf, val explicitSide: Boolean)

    private fun daf(rest: String): DafLocation? {
        val r = rest.replace(Regex("^(דף|ד')\\s*"), "").trim()
        val m = Regex("^((?:$NUM)?[א-ת]'?)\\s*([.:])?(?:\\s*,?\\s*(?:ע\"|עמוד\\s*|ס?ע\")([אב]))?(?:\\s*(?:ד\"ה|בד\"ה).*)?\\.?$").find(r)
        if (m == null) {
            val m2 = Regex("^($NUM)\\s*[,.]?\\s*([אב])'?\\.?$").find(r) ?: return null
            val folio = num(m2.groupValues[1])?.takeIf { it in 2..180 } ?: return null
            return DafLocation(Section.Daf(folio, if (m2.groupValues[2] == "א") Section.Side.A else Section.Side.B), true)
        }
        val folio = num(m.groupValues[1])?.takeIf { it in 2..180 } ?: return null
        val sideB = m.groupValues[2] == ":" || m.groupValues[3] == "ב"
        return DafLocation(
            Section.Daf(folio, if (sideB) Section.Side.B else Section.Side.A),
            explicitSide = m.groupValues[2].isNotEmpty() || m.groupValues[3].isNotEmpty(),
        )
    }

    /** "פ\"ג …" / "פי\"ג …" / "פרק ג …" → (3, rest). */
    private fun perekToken(r: String): Pair<Int, String>? {
        val ws = r.split(' ', limit = 2)
        markerNumber(ws[0], "פ")?.let { return it to ws.getOrElse(1) { "" } }
        val m = Regex("^(?:פרק|פ'|פר')\\s*($NUM)(?=\\s|$|,)\\s*,?\\s*(.*)$").find(r) ?: return null
        return num(m.groupValues[1])?.let { it to m.groupValues[2] }
    }

    /** "מ\"ב" / "ה\"ג" / "משנה ב" / "הלכה ג" → (n, rest). */
    private fun subToken(r: String, markers: String): Pair<Int, String>? {
        val ws = r.split(' ', limit = 2)
        markerNumber(ws[0], markers)?.let { return it to ws.getOrElse(1) { "" } }
        val m = Regex("^(?:משנה|הלכה|הל'|מ'|ה'|סימן|סי'|אות|ס')\\s*($NUM)(?=\\s|$|,)\\s*(.*)$").find(r) ?: return null
        return num(m.groupValues[1])?.let { it to m.groupValues[2] }
    }

    private fun strictPerekMishna(rest: String): Pair<Int, Int?>? {
        val (perek, after) = perekToken(rest) ?: return null
        val r = after.trim(' ', ',')
        if (r.isEmpty()) return perek to null
        val (mishna, tail) = subToken(r, "מה") ?: return null
        return if (tail.trim(' ', '.').isEmpty()) perek to mishna else null
    }

    private data class Siman(val siman: Int, val seif: Int?)

    /** "סי' רנא ס\"ב" / "סימן רנא סעיף ב" / "רנא, ב". Sub-seif (ס\"ק) references are rejected: they cite a commentary. */
    private fun siman(rest: String): Siman? {
        val r = rest.trim()
        val m = Regex("^(?:סי'|סימן|ס'|סי)\\s*($NUM)(?=\\s|$|,)\\s*,?\\s*(.*)$").find(r)
        if (m == null) {
            val m2 = Regex("^($NUM)(?:$SEP($NUM))?$").find(r) ?: return null
            val si = num(m2.groupValues[1]) ?: return null
            return Siman(si, m2.groupValues[2].ifEmpty { null }?.let { num(it) ?: return null })
        }
        val si = num(m.groupValues[1]) ?: return null
        val after = m.groupValues[2].trim()
        if (after.isEmpty()) return Siman(si, null)
        if (after.startsWith("ס\"ק") || after.startsWith("סק\"") || after.startsWith("סעיף קטן")) return null
        Regex("^(?:סעיף|סעי'|סע')\\s*($NUM)\\.?$").find(after)?.let { return num(it.groupValues[1])?.let { s -> Siman(si, s) } }
        markerNumber(after, "ס")?.let { return Siman(si, it) }
        Regex("^($NUM)\\.?$").find(after)?.let { return num(it.groupValues[1])?.let { s -> Siman(si, s) } }
        return null
    }

    private fun matchHilchot(words: List<String>): Pair<String, List<String>>? {
        for (k in minOf(5, words.size) downTo 1) {
            hilchot[nameKey(words.take(k).joinToString(" "))]?.let { return it to words.drop(k) }
        }
        return null
    }

    /** "הל' שבת פ\"ב ה\"ג" | "פ\"ב מהל' שבת ה\"ג" | "הלכות שבת ב, ג". */
    private fun rambam(rest: String): Triple<String, Int?, Int?>? {
        val r = rest.trim(' ', ',')
        perekToken(r)?.let { (perek, after) ->
            val m = Regex("^(?:מהל'|מהלכות|מה'|הל'|הלכות|בהל')\\s*(.*)$").find(after.trim()) ?: return null
            val (title, ws) = matchHilchot(m.groupValues[1].split(' ')) ?: return null
            val tail = ws.joinToString(" ").trim(' ', ',')
            if (tail.isEmpty()) return Triple(title, perek, null)
            val (halacha, left) = halachaToken(tail) ?: return null
            return if (left.isBlank()) Triple(title, perek, halacha) else null
        }
        val m = Regex("^(?:הל'|הלכות|הלכ'|ה')\\s*(.*)$").find(r) ?: return null
        val (title, ws) = matchHilchot(m.groupValues[1].split(' ')) ?: return null
        val tail = ws.joinToString(" ").trim(' ', ',')
        if (tail.isEmpty()) return Triple(title, null, null)
        perekToken(tail)?.let { (perek, after) ->
            val a = after.trim(' ', ',')
            if (a.isEmpty()) return Triple(title, perek, null)
            val (halacha, left) = halachaToken(a) ?: return null
            return if (left.isBlank()) Triple(title, perek, halacha) else null
        }
        return chapterVerse(tail)?.let { Triple(title, it.chapter, it.verse) }
    }

    /** "ה\"ג", "הי\"א", and the "הל\"X" spelling where ל only abbreviates הלכה (הל\"ט = halacha 9). */
    private fun halachaToken(r: String): Pair<Int, String>? {
        val ws = r.split(' ', limit = 2)
        val t = normalizeQuotes(ws[0]).trimEnd('.', ',')
        if (t.startsWith("הל\"") || t.startsWith("הלכ\"")) {
            num(t.substringAfter('"'))?.let { return it to ws.getOrElse(1) { "" } }
        }
        return subToken(r, "ה")
    }

    // ─── Families ─────────────────────────────────────────────────────────────────────────────────

    private fun cite(family: Family, title: String, sections: List<Section>, alias: String, rangeEnd: Int? = null) =
        Citation(
            family = family, title = title, sections = sections, alias = alias, rangeEnd = rangeEnd,
            precision = when {
                sections.isEmpty() -> Precision.BOOK
                family == Family.TANAKH && sections.size == 1 -> Precision.CHAPTER
                else -> Precision.FULL
            },
        )

    private fun tryKind(kind: AliasKind, canon: String?, rest: String, alias: String): ParsedCitation? = when (kind) {
        AliasKind.TANAKH -> {
            val title = canon!!
            if (rest.isEmpty()) {
                ParsedCitation(cite(Family.TANAKH, title, emptyList(), alias))
            } else {
                chapterVerse(rest)?.let { cv ->
                    val secs = listOfNotNull(Section.Num(cv.chapter), cv.verse?.let { Section.Num(it) })
                    val main = cite(Family.TANAKH, title, secs, alias, cv.verseEnd)
                    val commentary = when (alias) {
                        "מ\"א" -> "מגן אברהם"
                        "מ\"ב" -> "משנה ברורה"
                        else -> null
                    }
                    if (commentary != null && cv.verse != null) {
                        ParsedCitation(
                            main, Ambiguity.KINGS_OR_COMMENTARY,
                            listOf(cite(Family.MISC, commentary, secs, alias)),
                        )
                    } else {
                        ParsedCitation(main)
                    }
                }
            }
        }

        AliasKind.TRACTATE -> tractate(canon!!, rest, alias)

        AliasKind.MISHNA, AliasKind.YERUSHALMI, AliasKind.TOSEFTA -> {
            val (family, prefix) = when (kind) {
                AliasKind.MISHNA -> Family.MISHNA to "משנה "
                AliasKind.YERUSHALMI -> Family.YERUSHALMI to "תלמוד ירושלמי "
                else -> Family.TOSEFTA to "תוספתא "
            }
            val title = prefix + canon
            if (rest.isEmpty()) {
                ParsedCitation(cite(family, title, emptyList(), alias))
            } else {
                (strictPerekMishna(rest) ?: chapterVerse(rest)?.let { it.chapter to it.verse })?.let { (p, m) ->
                    ParsedCitation(cite(family, title, listOfNotNull(Section.Num(p), m?.let { Section.Num(it) }), alias))
                }
            }
        }

        AliasKind.RASHI_BAVLI, AliasKind.TOSAFOT -> {
            daf(rest)?.takeIf { canon in BookAliases.bavli }?.let {
                val (family, prefix) = if (kind == AliasKind.RASHI_BAVLI) Family.RASHI_BAVLI to "רש\"י על " else Family.TOSAFOT to "תוספות על "
                ParsedCitation(cite(family, prefix + canon, listOf(it.daf), alias))
            }
        }

        AliasKind.RABBA -> rabba(canon!!, rest, alias)

        AliasKind.SA, AliasKind.TUR -> turFamily(kind, canon, rest, alias)

        AliasKind.RAMBAM -> rambam(rest)?.let { (title, perek, halacha) ->
            ParsedCitation(cite(Family.RAMBAM, title, listOfNotNull(perek?.let { Section.Num(it) }, halacha?.let { Section.Num(it) }), alias))
        }

        AliasKind.MISC -> misc(canon!!, rest, alias)

        AliasKind.ZOHAR -> daf(rest)?.let {
            ParsedCitation(
                Citation(
                    family = Family.ZOHAR_DAF, title = "ספר הזהר", sections = emptyList(), precision = Precision.FULL,
                    alias = alias, zoharVolume = canon!!.toInt(), zoharDaf = it.daf,
                ),
            )
        }
    }

    private fun tractate(canon: String, rest: String, alias: String): ParsedCitation? {
        if (rest.isEmpty()) return ParsedCitation(cite(Family.BAVLI, canon, emptyList(), alias))
        val first = rest.split(' ').first()
        val bareChapterMarker = first.getOrNull(1) == '"' && markerNumber(first, "פ") != null
        val d = daf(rest)
        if (d != null && canon in BookAliases.bavli && !(bareChapterMarker && !d.explicitSide)) {
            return ParsedCitation(cite(Family.BAVLI, canon, listOf(d.daf), alias))
        }
        val pm = strictPerekMishna(rest) ?: chapterVerse(rest)?.takeIf { it.verse != null }?.let { it.chapter to it.verse }
        pm ?: return null
        val (perek, mishna) = pm
        val mishnaCitation = cite(Family.MISHNA, "משנה $canon", listOfNotNull(Section.Num(perek), mishna?.let { Section.Num(it) }), alias)
        // "שבת פ\"ב" without side and without mishna: chapter 2, or (more often) folio 82.
        if (bareChapterMarker && mishna == null && canon in BookAliases.bavli) {
            val folio = gematria(first.trimEnd('.', ','))
            if (folio != null && folio in 2..180) {
                return ParsedCitation(
                    mishnaCitation, Ambiguity.PEREK_OR_DAF,
                    listOf(cite(Family.BAVLI, canon, listOf(Section.Daf(folio, Section.Side.A)), alias)),
                )
            }
        }
        return ParsedCitation(mishnaCitation)
    }

    private fun rabba(canon: String, rest: String, alias: String): ParsedCitation? {
        if (rest.isEmpty()) return ParsedCitation(cite(Family.RABBA, canon, emptyList(), alias))
        perekToken(rest.replace(Regex("^(?:פרשה|פר')\\s*"), "פרק "))?.let { (parasha, after) ->
            val a = after.trim(' ', ',')
            if (a.isEmpty()) return ParsedCitation(cite(Family.RABBA, canon, listOf(Section.Num(parasha)), alias))
            val sub = subToken(a, "סא") ?: Regex("^($NUM)\\.?$").find(a)?.let { m -> num(m.groupValues[1])?.let { it to "" } }
            return sub?.takeIf { it.second.isBlank() }?.let {
                ParsedCitation(cite(Family.RABBA, canon, listOf(Section.Num(parasha), Section.Num(it.first)), alias))
            }
        }
        return chapterVerse(rest)?.let {
            ParsedCitation(cite(Family.RABBA, canon, listOfNotNull(Section.Num(it.chapter), it.verse?.let { v -> Section.Num(v) }), alias))
        }
    }

    private fun turFamily(kind: AliasKind, canon: String?, rest: String, alias: String): ParsedCitation? {
        var part = canon
        var r = rest
        if (part == null) {
            loop@ for ((pc, al) in BookAliases.shulchanArukhParts) {
                for (a in al) {
                    if (r.startsWith(a)) {
                        part = pc
                        r = r.substring(a.length).trim(' ', ',')
                        break@loop
                    }
                }
            }
        }
        part ?: return null
        val s = siman(r) ?: return null
        val simanSecs = listOfNotNull(Section.Num(s.siman), s.seif?.let { Section.Num(it) })
        if (kind == AliasKind.TUR) {
            return ParsedCitation(cite(Family.TUR, "טור", listOf(Section.Name(part), Section.Num(s.siman)), alias))
        }
        val main = cite(Family.SHULCHAN_ARUKH, "שולחן ערוך, $part", simanSecs, alias)
        // A bare part name ("או\"ח סי' רנא") is also how the Tur, the Mishna Berura and many responsa are cited.
        if (alias in BookAliases.shulchanArukhParts.getValue(part)) {
            val alternatives = buildList {
                add(cite(Family.TUR, "טור", listOf(Section.Name(part), Section.Num(s.siman)), alias))
                if (part == "אורח חיים") add(cite(Family.MISC, "משנה ברורה", listOf(Section.Num(s.siman)), alias))
            }
            return ParsedCitation(main, Ambiguity.TUR_PART_ONLY, alternatives)
        }
        return ParsedCitation(main)
    }

    private fun misc(canon: String, rest: String, alias: String): ParsedCitation? {
        if (rest.isEmpty()) return ParsedCitation(cite(Family.MISC, canon, emptyList(), alias))
        return when (canon) {
            "משנה ברורה" -> siman(rest)?.let {
                ParsedCitation(cite(Family.MISC, canon, listOfNotNull(Section.Num(it.siman), it.seif?.let { s -> Section.Num(s) }), alias))
            }

            "ילקוט שמעוני על התורה" -> Regex("^(?:רמז|רמ')\\s*($NUM)\\.?$").find(rest)?.let { m ->
                num(m.groupValues[1])?.let { ParsedCitation(cite(Family.MISC, canon, listOf(Section.Num(it)), alias)) }
            }

            "ספר החינוך" -> Regex("^(?:מצוה|מצוות|מצ')\\s*($NUM)\\.?$").find(rest)?.let { m ->
                num(m.groupValues[1])?.let { ParsedCitation(cite(Family.MISC, canon, listOf(Section.Num(it)), alias)) }
            }

            "מדרש תנחומא" -> {
                val ws = rest.split(' ')
                val n = num(ws.last())
                if (ws.size >= 2 && n != null && ws.dropLast(1).none { num(it) != null }) {
                    ParsedCitation(cite(Family.MISC, canon, listOf(Section.Name(ws.dropLast(1).joinToString(" ")), Section.Num(n)), alias))
                } else {
                    null
                }
            }

            else -> chapterVerse(rest)?.let {
                ParsedCitation(cite(Family.MISC, canon, listOfNotNull(Section.Num(it.chapter), it.verse?.let { v -> Section.Num(v) }), alias))
            }
        }
    }

    private companion object {
        /** A Hebrew numeral token: up to 4 letters, optional geresh/gershayim, optional last letter. */
        const val NUM = "[א-ת]{1,4}['\"]?[א-ת]?"
        const val SEP = "\\s*[,:]?\\s*"
    }
}
