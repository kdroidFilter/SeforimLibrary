package io.github.kdroidfilter.seforimlibrary.reflinks.corpus

import io.github.kdroidfilter.seforimlibrary.reflinks.Ambiguity
import io.github.kdroidfilter.seforimlibrary.reflinks.BookAliases
import io.github.kdroidfilter.seforimlibrary.reflinks.Citation
import io.github.kdroidfilter.seforimlibrary.reflinks.CitationParser
import io.github.kdroidfilter.seforimlibrary.reflinks.Family
import io.github.kdroidfilter.seforimlibrary.reflinks.HebrewText
import io.github.kdroidfilter.seforimlibrary.reflinks.ParsedCitation
import io.github.kdroidfilter.seforimlibrary.reflinks.Precision
import io.github.kdroidfilter.seforimlibrary.reflinks.SefariaRefParser
import io.github.kdroidfilter.seforimlibrary.reflinks.Section
import io.github.kdroidfilter.seforimlibrary.reflinks.ShulchanArukhCommentators
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.Antecedent
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.ArbitrationCase
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.Candidate
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.CaseKind
import java.net.URLDecoder

/** How a link was decided, kept with the link so the app can filter by confidence without a rebuild. */
enum class Method {
    /** Book and location inside the parentheses. */
    EXPLICIT,

    /** Book name written just before the parentheses: "בביצה (דף יז)". */
    NAME_BEFORE,

    /** Bavli chapter name before a folio: "בפ' מרובה (דף עד)". */
    CHAPTER_NAME,

    /** Folio alone inside a commentary on a tractate: "(דף כא:)" in "רש\"י על שבת". */
    BASE_TRACTATE,

    /** "(שם …)" whose antecedent is in the same line. */
    IBID_SAME_LINE,

    /** `<a href="otzaria://inline-link?path=…&index=…">` already in the text. */
    OTZARIA_ANCHOR,

    /** `<a class="refLink" data-ref="…">` left by Sefaria in its texts. */
    SEFARIA_ANCHOR,
}

/** Which kind of ambiguity produced an arbitration case (reporting only). */
enum class CaseClass { PEREK_OR_DAF, KINGS_OR_COMMENTARY, TUR_PART_ONLY, TANAKH_NAME_BEFORE, IBID_WITH_LOCATION, IBID_BARE }

sealed interface Occurrence {
    val lineId: Long
    val bookId: Long

    /** [start]/[end]: offsets of the parenthesized group in the line's tag-stripped text. */
    val start: Int
    val end: Int

    data class Linked(
        override val lineId: Long, override val bookId: Long, override val start: Int, override val end: Int,
        val target: LineRef, val method: Method,
    ) : Occurrence

    data class Ambiguous(
        override val lineId: Long, override val bookId: Long, override val start: Int, override val end: Int,
        val case: ArbitrationCase, val caseClass: CaseClass,
    ) : Occurrence
}

/**
 * Walks the corpus line by line and turns every parenthesized citation into an [Occurrence]: a deterministic link, or an
 * ambiguous case for the LLM. Rules measured on the corpus:
 * - ibid (שם) is only trusted inside the line of its antecedent; across lines it goes to the arbitration;
 * - a folio alone resolves through the name written before it, a chapter name, or the commentary's own tractate,
 *   never through "the last tractate cited" in previous lines.
 */
class CitationScanner(private val index: HeRefIndex, private val sefariaRefs: SefariaRefParser? = null) {

    private val parser = CitationParser(index.rambamTitles())
    private val group = Regex("[(\\[]([^()\\[\\]]{1,80})[)\\]]")
    private val tags = Regex("<[^>]+>")
    private val anchor = Regex("<a\\b([^>]*)>(.*?)</a>", RegexOption.DOT_MATCHES_ALL)
    private val otzariaHref = Regex("href=\"otzaria://inline-link\\?([^\"]+)\"")
    private val sefariaDataRef = Regex("data-ref=\"([^\"]+)\"")

    /** "(דף עד)", "(קיח.)", "(כא ע\"ב)": a bare "(ב)" after a chapter name is a note marker, not a folio. */
    private val explicitFolio = Regex("^(?:דף|ד')\\s|[.:](?:\\s|$)|ע\"[אב]")
    private val baseTractate = Regex("על (?:מסכת )?(" + BookAliases.bavli.sortedByDescending { it.length }.joinToString("|") + ")$")

    private data class Ante(val citation: Citation, val target: LineRef?, val lineId: Long, val raw: String, val excerpt: String)

    fun scan(onOccurrence: (Occurrence) -> Unit) {
        var currentBook = -1L
        var last: Ante? = null
        index.forEachLine { bookId, lineId, content ->
            if (bookId != currentBook) {
                currentBook = bookId
                last = null
            }
            if ('(' !in content && '[' !in content && "<a" !in content) return@forEachLine
            val text = tags.replace(content, "")
            val bookTitle = index.bookTitles[bookId].orEmpty()
            // Links already in the text win over the citation written inside them
            val anchored = if ("<a" in content) anchors(content, lineId, bookId).onEach(onOccurrence).map { it.start until it.end } else emptyList()
            for (m in group.findAll(text)) {
                if (anchored.any { it.first < m.range.last + 1 && m.range.first < it.last + 1 }) continue
                val inside = HebrewText.stripNikud(m.groups[1]!!.value).trim()
                if (inside.none { it in 'א'..'ת' } || inside.split(' ').size > 8) continue
                val occ = OccurrenceContext(text, m.range.first, m.range.last + 1, inside, lineId, bookId, bookTitle)
                last = handle(occ, last, onOccurrence) ?: last
            }
        }
    }

    /** Otzaria inline links and Sefaria refLinks; offsets are those of the anchor text in the tag-stripped line. */
    private fun anchors(content: String, lineId: Long, bookId: Long): List<Occurrence.Linked> = anchor.findAll(content).mapNotNull { m ->
        val attributes = m.groupValues[1].replace("&amp;", "&")
        val target = otzariaTarget(attributes) ?: sefariaTarget(attributes) ?: return@mapNotNull null
        val start = tags.replace(content.substring(0, m.groups[2]!!.range.first), "").length
        val end = start + tags.replace(m.groupValues[2], "").length
        Occurrence.Linked(lineId, bookId, start, end, target.first, target.second)
    }.toList()

    /** "otzaria://inline-link?path=<book>.txt&index=<1-based line>" */
    private fun otzariaTarget(attributes: String): Pair<LineRef, Method>? {
        val href = otzariaHref.find(attributes)?.groupValues?.get(1) ?: return null
        val params = href.split('&').associate { it.substringBefore('=') to URLDecoder.decode(it.substringAfter('='), Charsets.UTF_8) }
        val title = params["path"]?.substringAfterLast('/')?.removeSuffix(".txt") ?: return null
        val lineIndex = params["index"]?.toIntOrNull()?.minus(1) ?: return null
        val bookId = index.bookIds[title] ?: return null
        return index.lineAt(bookId, lineIndex)?.let { it to Method.OTZARIA_ANCHOR }
    }

    private fun sefariaTarget(attributes: String): Pair<LineRef, Method>? {
        val dataRef = sefariaDataRef.find(attributes)?.groupValues?.get(1) ?: return null
        val ref = sefariaRefs?.parse(dataRef) ?: return null
        val target = if (ref.sections.isEmpty()) {
            index.bookIds[ref.heTitle]?.let { index.lineAt(it, 0) }
        } else {
            // A Sefaria ref may stop at a deeper level than the heRef (or the reverse): take the longest known prefix
            val book = index.book(ref.heTitle)
            (ref.sections.size downTo 1).firstNotNullOfOrNull { book[ref.sections.take(it)] }
        }
        return target?.let { it to Method.SEFARIA_ANCHOR }
    }

    private data class OccurrenceContext(
        val text: String, val start: Int, val end: Int, val inside: String,
        val lineId: Long, val bookId: Long, val bookTitle: String,
    )

    /** Returns the new antecedent when this group is itself a citation of a work. */
    private fun handle(o: OccurrenceContext, last: Ante?, emit: (Occurrence) -> Unit): Ante? {
        val body = parser.stripIntro(HebrewText.normalizeQuotes(o.inside))

        if (body == "שם" || body.startsWith("שם ") || body.startsWith("שם,")) {
            ibid(o, body.removePrefix("שם").trim(' ', ','), last, emit)
            return null
        }

        parser.parse(o.inside)?.takeIf { it.citation.precision != Precision.BOOK }?.let { parsed ->
            return explicit(o, parsed, Method.EXPLICIT, emit)
        }

        nameBefore(o)?.let { (parsed, method) -> return explicit(o, parsed, method, emit) }
        return null
    }

    private fun explicit(o: OccurrenceContext, parsed: ParsedCitation, method: Method, emit: (Occurrence) -> Unit): Ante {
        val target = index.resolve(parsed.citation)
        val needsArbitration = parsed.ambiguity != null ||
            (method == Method.NAME_BEFORE && parsed.citation.family == Family.TANAKH)
        if (needsArbitration) {
            val candidates = (listOf(parsed.citation) + parsed.alternatives).mapNotNull { c -> index.resolve(c)?.let { candidate(it) } } +
                listOfNotNull(namedCommentary(o, parsed))
            if (candidates.isNotEmpty()) {
                val cls = parsed.ambiguity?.let { CaseClass.valueOf(it.name) } ?: CaseClass.TANAKH_NAME_BEFORE
                emit(Occurrence.Ambiguous(o.lineId, o.bookId, o.start, o.end, ArbitrationCase(CaseKind.REFERENCE, o.bookTitle, context(o), candidates), cls))
            }
        } else if (target != null) {
            emit(Occurrence.Linked(o.lineId, o.bookId, o.start, o.end, target, method))
        }
        return Ante(parsed.citation, target, o.lineId, o.text.substring(o.start, o.end), context(o).takeLast(300))
    }

    private fun ibid(o: OccurrenceContext, rest: String, last: Ante?, emit: (Occurrence) -> Unit) {
        val ante = last ?: return
        val anteTarget = ante.target ?: return
        val sameLine = ante.lineId == o.lineId
        if (rest.isEmpty()) {
            val chapter = ante.citation.sections.takeIf { it.size > 1 }?.let { index.book(ante.citation.title)[it.take(1)] }
            val candidates = listOfNotNull(
                candidate(anteTarget),
                chapter?.let { candidate(it, note = "same work, exact place unknown: link to the chapter/folio") },
            )
            emit(Occurrence.Ambiguous(o.lineId, o.bookId, o.start, o.end, ibidCase(o, ante, candidates), CaseClass.IBID_BARE))
            return
        }
        val citation = ibidCitation(ante.citation, rest) ?: return
        val target = index.resolve(citation) ?: return
        if (sameLine) {
            emit(Occurrence.Linked(o.lineId, o.bookId, o.start, o.end, target, Method.IBID_SAME_LINE))
        } else {
            emit(Occurrence.Ambiguous(o.lineId, o.bookId, o.start, o.end, ibidCase(o, ante, listOf(candidate(target))), CaseClass.IBID_WITH_LOCATION))
        }
    }

    /** "(שם ע\"ב)" after a folio → other side of the same folio; otherwise re-parse the location with the antecedent's name. */
    private fun ibidCitation(ante: Citation, rest: String): Citation? {
        if (ante.family == Family.BAVLI && Regex("^(?:ע\"[אב]|עמוד [אב])\\.?$").matches(rest)) {
            val daf = ante.sections.firstOrNull() as? Section.Daf ?: return null
            val side = if ('א' in rest) Section.Side.A else Section.Side.B
            return ante.copy(sections = listOf(daf.copy(side = side)))
        }
        val parsed = parser.parse("${ante.alias} $rest")?.citation ?: return null
        return parsed.takeIf { it.title == ante.title && it.trailingTokens == 0 && it.sections.isNotEmpty() }
    }

    /** Book name or Bavli chapter name written just before the parentheses. */
    private fun nameBefore(o: OccurrenceContext): Pair<ParsedCitation, Method>? {
        val words = wordsBefore(o)
        if (words.isEmpty()) return null

        // "בפ' מרובה (דף עד)" / "בפרק כל כתבי (קיח.)"
        val chapterAt = words.indexOfLast { it.trimStart('ו', 'ב') in setOf("פרק", "פ'", "פר'") }
        if (chapterAt >= 0 && chapterAt < words.size - 1 && explicitFolio.containsMatchIn(o.inside)) {
            val name = HebrewText.nameKey(words.drop(chapterAt + 1).joinToString(" ").trimEnd(',', ':'))
            index.bavliChapterNames[name]?.let { tractate ->
                parser.parse("$tractate ${o.inside}")?.takeIf { it.citation.family == Family.BAVLI && it.citation.trailingTokens == 0 }
                    ?.let { return it to Method.CHAPTER_NAME }
            }
        }
        for (k in minOf(3, words.size) downTo 1) {
            parser.parseWithName(words.takeLast(k).joinToString(" "), o.inside)?.let { return it to Method.NAME_BEFORE }
        }

        // A folio alone in a commentary on a tractate
        val tractate = o.bookTitle.takeIf { it in BookAliases.bavli } ?: baseTractate.find(o.bookTitle)?.groupValues?.get(1)
        if (tractate != null && Regex("^(?:דף|ד') ").containsMatchIn(o.inside)) {
            parser.parse("$tractate ${o.inside}")?.takeIf { it.citation.family == Family.BAVLI && it.citation.trailingTokens == 0 }
                ?.let { return it to Method.BASE_TRACTATE }
        }
        return null
    }

    /** "הט\"ז (או\"ח סי' רנא)": the commentary named before a bare Tur/Shulchan Arukh citation, at that siman. */
    private fun namedCommentary(o: OccurrenceContext, parsed: ParsedCitation): Candidate? {
        if (parsed.ambiguity != Ambiguity.TUR_PART_ONLY) return null
        val part = parsed.citation.title.substringAfter("שולחן ערוך, ", "").ifEmpty { return null }
        val siman = parsed.citation.sections.firstOrNull() ?: return null
        val target = ShulchanArukhCommentators.named(wordsBefore(o), part) ?: return null
        val sections = listOfNotNull(Section.Name(part).takeIf { target.partSection }, siman)
        return index.book(target.title)[sections]?.let { candidate(it) }
    }

    private fun wordsBefore(o: OccurrenceContext): List<String> =
        HebrewText.normalizeQuotes(HebrewText.stripNikud(o.text.substring(0, o.start))).trimEnd()
            .split(' ').filter { it.isNotEmpty() }.takeLast(4)

    private fun candidate(ref: LineRef, note: String? = null): Candidate {
        val title = index.bookTitles.getValue(ref.bookId)
        return Candidate(title = title, heRef = ref.heRef, snippet = index.snippet(ref.lineId), note = note)
    }

    private fun ibidCase(o: OccurrenceContext, ante: Ante, candidates: List<Candidate>) = ArbitrationCase(
        kind = CaseKind.IBID,
        sourceBook = o.bookTitle,
        context = context(o),
        candidates = candidates,
        antecedent = Antecedent(ante.raw, ante.excerpt),
    )

    /** ~35 words on each side of the citation, plain text, the citation wrapped in ⟦ ⟧. */
    private fun context(o: OccurrenceContext): String {
        val before = HebrewText.plain(o.text.substring(0, o.start)).split(' ').takeLast(35).joinToString(" ")
        val after = HebrewText.plain(o.text.substring(o.end)).split(' ').take(35).joinToString(" ")
        val cited = HebrewText.plain(o.text.substring(o.start, o.end))
        return "$before ⟦$cited⟧ $after".trim()
    }
}
