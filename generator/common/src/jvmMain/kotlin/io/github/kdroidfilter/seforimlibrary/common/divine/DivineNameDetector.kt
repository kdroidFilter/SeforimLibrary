package io.github.kdroidfilter.seforimlibrary.common.divine

/**
 * Finds the VERIFIED sacred divine names of one stored line. Every rule is
 * fail-closed: a name is emitted only when a rule proves it sacred and nothing
 * argues against it; replacing a profane word would corrupt the text, while a
 * missed name only leaves the line as it is today.
 *
 * Sources (see the shared contract):
 *  - HVYH_RULE: the app's own regex and its 3-letter look-behind, unchanged.
 *  - NIKUD_RULE: vocalized words whose points decide (אֲדֹנָי, שַׁדַּי, אֵל, אֱלוֹהַּ).
 *  - COLLOCATION: fixed phrases on the consonant skeleton (ה' אלהינו, אלהי ישראל, אל שדי…).
 *  - EXCEPTION_LIST: reviewed verses in `divine_name_overrides.csv`, which also suppress.
 */
internal class DivineNameDetector(private val overrides: DivineNameOverrides?) {

    data class Result(
        val spans: List<DivineSpan>,
        /** Raw ranges the app regex replaces in this line (the HVYH superset gate). */
        val appHvyh: List<IntRange>,
        /** Why a candidate was not emitted, keyed `KIND/SOURCE:reason`. */
        val dropped: Map<String, Int>,
    )

    private data class Candidate(val rawStart: Int, val rawEnd: Int, val kind: DivineKind, val source: DivineSource)

    /**
     * [verse] is the Tanach verse of a base-text line (overrides apply), [nonHvyh]
     * false limits the line to the HVYH rule (a Tanach line whose verse is unknown,
     * or any Tanach line when no override list is available). [elScope] admits the
     * NIKUD rules for אֵל and אֱלוֹהַּ, which rabbinic prose also uses for "a god"
     * (Mishnah Sanhedrin 7:6): only Tanach verses and the prayer book.
     */
    fun detect(
        content: String,
        verse: VerseRef? = null,
        nonHvyh: Boolean = true,
        elScope: Boolean = verse != null,
        prayerBook: Boolean = false,
    ): Result {
        val dropped = LinkedHashMap<String, Int>()
        fun drop(kind: DivineKind, source: DivineSource, why: String) {
            dropped.merge("${kind.name}/${source.name}:$why", 1, Int::plus)
        }

        val appHvyh = appHvyhMatches(content)
        val candidates = ArrayList<Candidate>()
        appHvyh.mapTo(candidates) { Candidate(it.first, it.last + 1, DivineKind.HVYH, DivineSource.HVYH_RULE) }

        val line = ScannedLine(content)
        // A verse without its override list cannot honour the profane entries.
        if (nonHvyh && !(verse != null && overrides == null)) {
            val words = line.words
            for (i in words.indices) nikudRules(words, i, elScope, candidates, ::drop)
            collocations(words, candidates, ::drop)
            if (prayerBook) prayerPhrases(words, candidates, ::drop)
            if (verse != null && overrides != null) {
                for (kind in overrides.sacredKinds(verse)) {
                    for (word in words) {
                        if (word.glued || word.hasPresentation) continue
                        val body = exceptionBody(word, kind) ?: continue
                        candidates += Candidate(body.first().start, body.last().end, kind, DivineSource.EXCEPTION_LIST)
                    }
                }
            }
        }

        val accepted = ArrayList<DivineSpan>()
        val ordered = candidates.sortedWith(compareBy({ PRIORITY.indexOf(it.source) }, { it.rawStart }))
        for (c in ordered) {
            if (verse != null && c.source != DivineSource.EXCEPTION_LIST && overrides?.isProfane(verse, c.kind) == true) {
                drop(c.kind, c.source, "profane-override"); continue
            }
            val visible = line.visibleRange(c.rawStart, c.rawEnd)
            if (visible == null) { drop(c.kind, c.source, "not-visible-text"); continue }
            // The app re-checks every span; anything it would reject is never written.
            if (!appAcceptsSpan(content, c.rawStart, c.rawEnd, c.kind)) { drop(c.kind, c.source, "app-boundary"); continue }
            if (accepted.any { it.rawStart < c.rawEnd && c.rawStart < it.rawEnd }) {
                // Same word found by a stronger source: not a loss, nothing to report.
                continue
            }
            accepted += DivineSpan(c.rawStart, c.rawEnd, visible.first, visible.last + 1, c.kind, c.source)
        }
        accepted.sortBy { it.rawStart }
        return Result(accepted, appHvyh, dropped)
    }

    // ── NIKUD rules ──────────────────────────────────────────────────────────

    private fun nikudRules(
        words: List<Word>,
        i: Int,
        elScope: Boolean,
        out: MutableList<Candidate>,
        drop: (DivineKind, DivineSource, String) -> Unit,
    ) {
        val word = words[i]
        if (word.glued || word.hasPresentation || !word.hasVowels) return
        for (k in prefixLengths(word, PREFIXES_NO_HEH)) {
            // In a vocalized word a real prefix carries its own vowel (שׁוֹאֵל, שְׁמוּאֵל do not).
            if (!word.letters.take(k).all(::isPrefixVocalized)) break
            val body = word.letters.subList(k, word.letters.size)
            val shinPrefix = word.letters.take(k).any { it.ch == 'ש' }
            val kind = when {
                isAdnut(body) -> DivineKind.ADNUT
                isShaddai(body) -> DivineKind.SHADDAI
                isVocalizedEl(body) && !shinPrefix -> DivineKind.EL
                isYah(body) && isYahPrefix(word.letters.take(k)) -> DivineKind.YAH
                isEloah(body) -> DivineKind.ELOAH
                else -> null
            } ?: continue
            if (kind == DivineKind.ELOAH && !ELOAH_NIKUD_ENABLED) {
                drop(kind, DivineSource.NIKUD_RULE, "disabled-pending-approval")
                return
            }
            if (kind == DivineKind.EL && !elScope) {
                drop(kind, DivineSource.NIKUD_RULE, "outside-scope")
                return
            }
            val guard = profaneContext(words, i, kind)
            if (guard != null) {
                drop(kind, DivineSource.NIKUD_RULE, guard)
            } else {
                out += Candidate(body.first().start, body.last().end, kind, DivineSource.NIKUD_RULE)
            }
            return
        }
    }

    /** אֲדֹנָי: qamats (never patach) under the nun, no vowel on the final yod. */
    private fun isAdnut(body: List<WordLetter>): Boolean =
        skeletonOf(body) == "אדני" &&
            QAMATS in body[2].marks && PATAH !in body[2].marks &&
            body[3].vowels.isEmpty()

    /** שַׁדַּי: shin dot (never sin dot) and patach alone (שֶׁדַּי is "that it suffices"), dagesh + patach/qamats in the dalet, bare yod. */
    private fun isShaddai(body: List<WordLetter>): Boolean =
        skeletonOf(body) == "שדי" &&
            SHIN_DOT in body[0].marks && SIN_DOT !in body[0].marks && body[0].vowels == PATAH.toString() &&
            DAGESH in body[1].marks && (PATAH in body[1].marks || QAMATS in body[1].marks) &&
            body[2].vowels.isEmpty()

    /** A prefix letter's own vowel: sheva, hataf, hiriq, tsere, segol, patach, qamats, or ו as shuruk. */
    private fun isPrefixVocalized(letter: WordLetter): Boolean =
        letter.marks.any { it in PREFIX_VOWELS } || (letter.ch == 'ו' && letter.vowels.isEmpty() && DAGESH in letter.marks)

    /** אֵל / אֵלִי: tsere alone under the alef (אֶל is the preposition, אֵלַי "to me"); no dagesh (אֵלּוּ). */
    private fun isVocalizedEl(body: List<WordLetter>): Boolean = body.size in 2..3 && DAGESH !in body[1].marks && when (skeletonOf(body)) {
        "אל" -> body[0].vowels == TSERE.toString() && body[1].vowels.isEmpty()
        "אלי" -> body[0].vowels == TSERE.toString() && body[1].vowels == HIRIQ.toString() && body[2].vowels.isEmpty()
        else -> false
    }

    /**
     * אֱלוֹהַּ / אֱלֹהַּ: hataf segol (or nothing, after a prefix) under the alef, holam on the
     * lamed or its vav, mapiq and furtive patach in the final heh. Rules out אֵלֶּה, אָלָה,
     * the Aramaic אֱלָהּ and בִּשְׂמֹאלָהּ.
     */
    private fun isEloah(body: List<WordLetter>): Boolean {
        val s = skeletonOf(body)
        if (s != "אלוה" && s != "אלה") return false
        val heh = body.last()
        fun hasHolam(l: WordLetter) = HOLAM in l.marks || '\u05BA' in l.marks
        val holam = if (s == "אלוה") hasHolam(body[2]) && body[1].vowels.isEmpty() else hasHolam(body[1])
        return body[0].vowels.all { it == HATAF_SEGOL } && holam && QAMATS !in body[1].marks &&
            DAGESH in heh.marks && PATAH in heh.marks
    }

    // ── COLLOCATIONS ─────────────────────────────────────────────────────────

    private fun collocations(
        words: List<Word>,
        out: MutableList<Candidate>,
        drop: (DivineKind, DivineSource, String) -> Unit,
    ) {
        fun usable(j: Int) = j in words.indices && !words[j].glued && !words[j].hasPresentation
        fun joined(j: Int) = usable(j) && words[j].softBefore
        fun add(body: List<WordLetter>, kind: DivineKind, at: Int) {
            val guard = if (kind in GUARDED) profaneContext(words, at, kind) else null
            if (guard != null) drop(kind, DivineSource.COLLOCATION, guard)
            else out += Candidate(body.first().start, body.last().end, kind, DivineSource.COLLOCATION)
        }

        for (i in words.indices) {
            if (!usable(i)) continue
            val w = words[i]

            // Anchor (יהוה, ה', יי) + אלהי… [+ צבאות], or anchor + צבאות.
            if (isAnchor(words, i) && joined(i + 1)) {
                val elohim = elohimBody(words[i + 1], allowVav = true)
                if (elohim != null) {
                    add(elohim, DivineKind.ELOHIM, i + 1)
                    if (joined(i + 2)) tzevaotBody(words[i + 2])?.let { add(it, DivineKind.TZEVAOT, i + 2) }
                } else {
                    // יְהוִה הַצְּבָאוֹת (Amos 9:5): the article stays outside the span.
                    tzevaotBody(words[i + 1])?.let { add(it, DivineKind.TZEVAOT, i + 1) }
                }
            }

            // Unvocalized יה only in closed phrases (pending rabbinic approval). Never a bare יה,
            // never ביה/ליה + ה' (Aramaic "in/to him": דכתב ביה ה', מהדרו ליה ה').
            yahBody(w)?.let { yah ->
                if (w.hasVowels) return@let
                val context = WordContext(words)
                val prev = context.skeleton(i, -1)
                val next = if (joined(i + 1)) words[i + 1].skeleton else null
                val proven = when (w.skeleton) {
                    // יה ה' צבאות, עזי וזמרת יה ה' (Isa 12:2); שם יה / בשם יה.
                    "יה" -> (joined(i + 1) && isAnchor(words, i + 1)) ||
                        (w.softBefore && prev in YAH_SHEM_WORDS) ||
                        // לשכן יה אלהים (Ps 68:19).
                        (next == "אלהים" && w.softBefore && prev in YAH_SHOCHEN_WORDS)
                    // כי ביה ה' צור עולמים (Isa 26:4).
                    "ביה" -> w.softBefore && prev == "כי" &&
                        (next in YAH_AFTER_KI_BEYAH || (joined(i + 1) && isAnchor(words, i + 1)))
                    else -> false
                }
                // סימן יה ה', יד יה ה': the numeral fifteen.
                val numeral = prev != null && (
                    isNumeralLike(prev) || prev.length <= 2 || inWithPrefix(prev, REFERENCE_WORDS, "ובלמ", 2)
                    ) && prev != "כי" && prev !in YAH_SHEM_WORDS
                when {
                    !proven -> Unit
                    numeral -> drop(DivineKind.YAH, DivineSource.COLLOCATION, "numeral-context")
                    else -> add(yah, DivineKind.YAH, i)
                }
            }

            // אלהי + a patriarch / ישראל / השמים / העברים; אלהי(ם) [ה]צבאות.
            val construct = elohimBody(w, allowVav = true)
            if (construct != null && joined(i + 1)) {
                val form = skeletonOf(construct)
                val next = words[i + 1]
                if ((form == "אלהי" || form == "אלוהי") && next.skeleton in PATRONS) {
                    add(construct, DivineKind.ELOHIM, i)
                }
                if (form in setOf("אלהי", "אלוהי", "אלהים", "אלוהים")) {
                    tzevaotBody(next)?.let {
                        add(construct, DivineKind.ELOHIM, i)
                        add(it, DivineKind.TZEVAOT, i + 1)
                    }
                }
            }

            // אל שדי / אל עליון.
            val el = elBody(w)
            if (el != null && joined(i + 1)) {
                val next = words[i + 1]
                if (next.skeleton == "שדי" && (!next.hasVowels || isShaddai(next.letters))) {
                    // Unvocalized, אל may be "to": להשיבו אל שדי אמו (to his mother's breasts).
                    // At most one prefix letter: שמואל שדי ליה ("Shmuel threw it") parses as ש-מ-ו + אל.
                    val prefix = w.letters.size - el.size
                    val proven = prefix <= 1 && (
                        w.hasVowels || next.hasVowels || prefix == 1 || !w.softBefore ||
                            (WordContext(words).skeleton(i, -1) in setOf("אני", "ואני", "אנכי", "שם", "בשם", "ושם"))
                        )
                    if (proven) {
                        add(el, DivineKind.EL, i)
                        add(next.letters, DivineKind.SHADDAI, i + 1)
                    } else {
                        drop(DivineKind.SHADDAI, DivineSource.COLLOCATION, "el-shaddai-unproven")
                    }
                } else if (next.skeleton == "עליון") {
                    val prefix = w.letters.size - el.size
                    val unvocalizedProof = (prefix == 1 && w.letters[0].ch == 'ל') ||
                        (WordContext(words).skeleton(i, -1) in setOf("ברוך", "וברוך", "כהן")) ||
                        (joined(i + 2) && words[i + 2].skeleton in setOf("קנה", "קונה"))
                    if (w.hasVowels || unvocalizedProof) add(el, DivineKind.EL, i)
                    else drop(DivineKind.EL, DivineSource.COLLOCATION, "el-elyon-unproven")
                }
            }

            // האל הגדול הגבור והנורא: the article is not part of the name.
            if (w.skeleton == "האל" && joined(i + 1) && joined(i + 2) && joined(i + 3) &&
                words[i + 1].skeleton in setOf("הגדול", "הגדל") &&
                words[i + 2].skeleton in setOf("הגבור", "הגבר") &&
                words[i + 3].skeleton == "והנורא"
            ) {
                val body = w.letters.subList(1, 3)
                if (!w.hasVowels || isVocalizedEl(body)) add(body, DivineKind.EL, i)
            }
        }
    }

    /** `אל` with an optional non-heh prefix; when vocalized it must read אֵל. */
    private fun elBody(word: Word): List<WordLetter>? {
        for (k in prefixLengths(word, PREFIXES_NO_HEH)) {
            val body = word.letters.subList(k, word.letters.size)
            if (skeletonOf(body) != "אל") continue
            if (word.hasVowels && !isVocalizedEl(body)) return null
            return body
        }
        return null
    }

    private fun tzevaotBody(word: Word): List<WordLetter>? = when (word.skeleton) {
        "צבאות" -> word.letters
        "הצבאות" -> word.letters.subList(1, word.letters.size)
        else -> null
    }

    /** An אלהים-family word, optionally after ו; the body is everything after the ו. */
    private fun elohimBody(word: Word, allowVav: Boolean): List<WordLetter>? {
        if (word.glued) return null
        val starts = if (allowVav && word.letters.firstOrNull()?.ch == 'ו') listOf(0, 1) else listOf(0)
        for (k in starts) {
            val body = word.letters.subList(k, word.letters.size)
            if (skeletonOf(body) in ELOHIM_FORMS) return body
        }
        return null
    }

    private fun isAnchor(words: List<Word>, i: Int): Boolean {
        val word = words[i]
        for (k in prefixLengths(word, PREFIXES)) {
            val rest = skeletonOf(word.letters.subList(k, word.letters.size))
            if (rest !in ANCHORS) continue
            if (rest.startsWith('ה') && i > 0) {
                // ה' after a reference word or another numeral is the number five.
                val prev = WordContext(words).skeleton(i, -1) ?: return true
                if (isNumeralLike(prev) || inWithPrefix(prev, REFERENCE_WORDS, "ובלמ", 2)) return false
            }
            return true
        }
        return false
    }

    /** יָהּ: qamats alone under the yod, mapiq in the heh (בֵּיהּ, לֵיהּ have neither). */
    private fun isYah(body: List<WordLetter>): Boolean =
        skeletonOf(body) == "יה" && body[0].vowels == QAMATS.toString() && DAGESH in body[1].marks

    /**
     * A numeral or abbreviation (ד', סי', ס"ק): a quote at the end or a gershayim before the last
     * letter. A quote followed by a word (ל'את, מ'ויצו, a quotation mark a tag split off) is not one.
     */
    private fun isNumeralLike(skeleton: String): Boolean {
        if (skeleton.isEmpty()) return false
        if (isWordQuote(skeleton.last())) return true
        val gershayim = skeleton.indexOfFirst { it == '"' || it == '״' }
        return gershayim >= 0 && gershayim >= skeleton.length - 2
    }

    /**
     * Prefixes יָהּ really takes: one of ב ל כ מ ו, or ו + one of ב ל כ מ (וּלְיָהּ). Longer or other
     * runs are a word ending in ־יָהּ: בְּשָׁוְיָהּ, שִׁבְיָהּ, בִּכְיָהּ, כֶּלְיָהּ. A tsere prefix is
     * never יָהּ (בֵיָהּ = בָּהּ, "in it").
     */
    private fun isYahPrefix(prefix: List<WordLetter>): Boolean = prefix.none { TSERE in it.vowels } && when (prefix.size) {
        0 -> true
        1 -> prefix[0].ch in YAH_PREFIXES || prefix[0].ch == 'ו'
        2 -> prefix[0].ch == 'ו' && prefix[1].ch in YAH_PREFIXES
        else -> false
    }

    /** The יה of a word that is only [isYahPrefix] letters and יה; never part of a longer word. */
    private fun yahBody(word: Word): List<WordLetter>? {
        if (word.glued || word.hasPresentation) return null
        if (word.skeleton.length !in 2..4 || !word.skeleton.endsWith("יה")) return null
        val split = word.letters.size - 2
        if (!isYahPrefix(word.letters.take(split))) return null
        return word.letters.subList(split, word.letters.size)
    }

    /** Whitelisted prayer-book phrases ([PRAYER_EL_PHRASES], [PRAYER_ELOAH_PHRASES]). */
    private fun prayerPhrases(
        words: List<Word>,
        out: MutableList<Candidate>,
        drop: (DivineKind, DivineSource, String) -> Unit,
    ) {
        fun usable(j: Int) = j in words.indices && !words[j].glued && !words[j].hasPresentation
        for (i in words.indices) {
            for ((kind, phrases) in listOf(DivineKind.EL to PRAYER_EL_PHRASES, DivineKind.ELOAH to PRAYER_ELOAH_PHRASES)) {
                for (phrase in phrases) {
                    val matches = phrase.words.indices.all { k ->
                        val j = i + k
                        usable(j) && words[j].skeleton == phrase.words[k] && (k == 0 || words[j].softBefore)
                    }
                    if (!matches) continue
                    val nameWord = words[i + phrase.nameIndex]
                    val body = nameWord.letters.subList(nameWord.letters.size - phrase.nameBody.length, nameWord.letters.size)
                    if (phrase.vocalizedOnly && !nameWord.hasVowels) continue
                    val shapeOk = !nameWord.hasVowels || when (kind) {
                        DivineKind.EL -> isVocalizedEl(body)
                        // A listed phrase decides; MAM spells Deut 32:15 אֱלוֹהַ without the mapiq.
                        else -> skeletonOf(body) == "אלוה" || isEloah(body)
                    }
                    if (!shapeOk) { drop(kind, DivineSource.COLLOCATION, "prayer-phrase-vocalization"); continue }
                    val guard = profaneContext(words, i + phrase.nameIndex, kind)
                    if (guard != null) drop(kind, DivineSource.COLLOCATION, guard)
                    else out += Candidate(body.first().start, body.last().end, kind, DivineSource.COLLOCATION)
                }
            }
        }
    }

    private fun exceptionBody(word: Word, kind: DivineKind): List<WordLetter>? {
        val bodies = when (kind) {
            DivineKind.EHYEH -> setOf("אהיה")
            DivineKind.ADNUT -> setOf("אדני")
            DivineKind.EL -> setOf("אל", "אלי")
            DivineKind.ELOAH -> setOf("אלוה", "אלה")
            DivineKind.ELOHIM -> ELOHIM_FORMS
            DivineKind.SHADDAI -> setOf("שדי")
            DivineKind.TZEVAOT -> setOf("צבאות")
            DivineKind.YAH -> setOf("יה")
            DivineKind.HVYH -> return null
        }
        val prefixes = if (kind == DivineKind.EHYEH) emptySet() else PREFIXES_NO_HEH
        for (k in prefixLengths(word, prefixes)) {
            val body = word.letters.subList(k, word.letters.size)
            if (skeletonOf(body) !in bodies) continue
            // A sacred verse must not mark אֵלֶּה ("these") or בֵּיהּ: these kinds need the mapiq.
            val needsMapiq = kind == DivineKind.YAH || (kind == DivineKind.ELOAH && skeletonOf(body) == "אלה")
            if (needsMapiq && DAGESH !in body.last().marks) return null
            return body
        }
        return null
    }

    // ── Guards ───────────────────────────────────────────────────────────────

    /** Why the context makes this occurrence profane or doubtful, or null when it does not. */
    private fun profaneContext(words: List<Word>, i: Int, kind: DivineKind): String? {
        val context = WordContext(words)
        val next = context.skeleton(i, 1)
        val prev = context.skeleton(i, -1)
        if (next != null && next in NEXT_PROFANE) return "next-other-god"
        // אֵל לֹא יוֹשִׁיעַ (Aleinu, Isa 45:20), לֹא אֱלֹהַּ.
        if ((kind == DivineKind.EL || kind == DivineKind.ELOAH) && next != null && next in setOf("לא", "ולא")) return "next-negation"
        // בֶּן־לָאֵל (Num 3:24): a person's name.
        if (kind == DivineKind.EL && prev != null && prev in setOf("בן", "בת", "ובן")) return "patronymic"
        if (prev != null && prev in PREV_NEGATING) return "prev-negation-or-kol"
        for (d in -3..3) {
            if (d == 0) continue
            val near = context.skeleton(i, d) ?: continue
            if (inWithPrefix(near, NEAR_PROFANE, "ו", 1)) return "near-idolatry-word"
        }
        when (kind) {
            // צוּרִישַׁדָּי (Num 1:6): a personal name, also split as צורי שדי.
            DivineKind.SHADDAI -> if (prev != null && prev in setOf("צורי", "וצורי", "בצורי")) return "personal-name"
            // הַלְלוּ־יָהּ is one word (Pesachim 117a); שַׁלְהֶבֶתְיָה (Song 8:6) likewise.
            DivineKind.YAH -> {
                if (prev != null && prev in setOf("הללו", "והללו", "שלהבת")) return "compound-word"
                if (prev in YAH_SUPPRESS_PREV || next in YAH_SUPPRESS_NEXT) return "suppressed-phrase"
                if (next to context.skeleton(i, 2) in YAH_SUPPRESS_NEXT_PAIRS) return "suppressed-phrase"
            }
            DivineKind.EL -> {
                if (next == "אלים") return "el-elim"
                if (prev != null && inWithPrefix(prev, setOf("בית"), "ובלמ", 2)) return "beit-el-place"
                val word = words[i].skeleton
                if (word.startsWith("לאל") && next != null && next.startsWith("יד")) return "leel-yad-power"
            }
            DivineKind.ELOAH -> if (next != null && next in setOf("נכר", "מעזים", "מעוזים")) return "eloah-foreign"
            else -> Unit
        }
        return null
    }

    companion object {
        // Pending rabbinic approval: ~25% of nikud-proved אֱלוֹהַּ are generic ("אין אלוה מבלעדיך").
        const val ELOAH_NIKUD_ENABLED = false

        // Pending rabbinic approval: אֵל by nikud in the prayer book (מי אל בשמים, כהררי אל,
        // ואל הבקר רץ) contradicts the verse list; EL by nikud is limited to Tanach base verses.
        const val EL_NIKUD_IN_PRAYER_BOOK = false

        private val PRIORITY = listOf(
            DivineSource.EXCEPTION_LIST, DivineSource.HVYH_RULE, DivineSource.NIKUD_RULE, DivineSource.COLLOCATION,
        )

        /** Prefix letters (Rambam Yesodei HaTorah 6:3-4) — excluded from the span. */
        private val PREFIXES: Set<Char> = "ובכלמשה".toSet()

        /** הָאֵל may be "these" (Gen 19:8), so a heh prefix never proves a name. */
        private val PREFIXES_NO_HEH: Set<Char> = "ובכלמש".toSet()

        private val YAH_PREFIXES: Set<Char> = "בלכמ".toSet()
        private val YAH_SHEM_WORDS = setOf("שם", "בשם", "ושם")
        private val YAH_SHOCHEN_WORDS = setOf("לשכן", "לשכון", "ולשכון", "ולשכן", "בשכון")
        private val YAH_AFTER_KI_BEYAH = setOf("צור", "ד'", "ד׳", "הויה", "הוי'", "הוי׳", "הוי\"ה", "הוי״ה")

        /** Sheva, hataf segol/patach/qamats, hiriq, tsere, segol, patach, qamats. */
        private val PREFIX_VOWELS: Set<Char> = "\u05B0\u05B1\u05B2\u05B3\u05B4\u05B5\u05B6\u05B7\u05B8".toSet()

        private val ANCHORS = setOf("יהוה", "ה'", "ה׳", "ה’", "יי")

        private val GUARDED = setOf(DivineKind.ELOHIM, DivineKind.EL, DivineKind.ELOAH, DivineKind.ADNUT, DivineKind.YAH)

        val ELOHIM_FORMS: Set<String> = listOf("אלהי", "אלוהי").flatMap { base ->
            listOf("ם", "נו", "כם", "ך", "ו", "הם", "", "כן").map { base + it }
        }.toSet()

        /** אלהי + one of these is always the God of Israel. Conservative on purpose. */
        val PATRONS: Set<String> = setOf(
            "ישראל", "אברהם", "יצחק", "יעקב",
            "אבותינו", "אבתינו", "אבותיכם", "אבתיכם", "אבותיהם", "אבתיהם", "אבותם", "אבתם",
            "אבותי", "אבתי", "אבי", "השמים", "העברים",
        )

        private val NEXT_PROFANE = setOf("אחרים", "האחרים", "אחר", "האחר", "נכר", "הנכר", "זר", "זרים", "הזרים")

        private val PREV_NEGATING = setOf("לא", "ולא", "בלא", "כל", "וכל", "בכל", "לכל", "מכל", "ככל")

        /** Within three words of the name; compared after one leading ו is dropped. */
        private val NEAR_PROFANE = setOf(
            "העמים", "הגוים", "גוים", "מצרים", "כסף", "זהב", "מסכה", "הנכר", "נכר",
            "פסל", "פסלי", "אלילים", "האלילים", "הבעל", "הבעלים", "עשתרת", "עשתרות", "עץ", "אבן",
        )

        /** Words after which `ה'` is the numeral five, not the Name. */
        private val REFERENCE_WORDS = setOf(
            "סימן", "סימנים", "סעיף", "סעיפים", "פרק", "פרקים", "הלכה", "הלכות", "משנה", "אות", "אותיות",
            "דף", "עמוד", "שורה", "שער", "מאמר", "חלק", "פסוק", "פסוקים", "כלל", "שאלה", "תשובה", "דרוש",
            "ספר", "מסכת", "פרשה", "פרשת", "סעיף", "יום", "ימים", "שנה", "שנים", "מספר",
        )

        /** The app regex, ported literally from `text_manipulation.dart` (`_holyName`). */
        private val APP_HVYH = Regex("י(\\p{Mn}*)ה(\\p{Mn}*)ו(\\p{Mn}*)ה(\\p{Mn}*)")

        /** Raw ranges the app replaces: regex match not preceded by 3 letters (marks skipped). */
        fun appHvyhMatches(text: String): List<IntRange> =
            APP_HVYH.findAll(text).filterNot { threeLettersBefore(text, it.range.first) }.map { it.range }.toList()

        private fun threeLettersBefore(text: String, start: Int): Boolean {
            var letters = 0
            var k = start - 1
            while (k >= 0) {
                val c = text[k]
                if (isMark(c)) { k--; continue }
                if (c in 'א'..'ת') {
                    letters++
                    if (letters >= 3) return true
                    k--
                    continue
                }
                break
            }
            return false
        }

        private val APP_BOUNDARY_TAGS = setOf(
            "sup", "sub", "br", "hr", "p", "div", "li", "ul", "ol", "table", "tr",
            "td", "th", "blockquote", "h1", "h2", "h3", "h4", "h5", "h6",
        )
        private const val APP_PREFIX_LETTERS = "והבכלמש"

        /** `isDivineNameMark` of the app: combining marks except maqaf, paseq, sof pasuq, nun hafukha. */
        private fun isAppMark(c: Char): Boolean =
            c in '\u0300'..'\u036F' || (c in '\u0591'..'\u05C7' && c != '\u05BE' && c != '\u05C0' && c != '\u05C3' && c != '\u05C6')

        private fun isAppBoundaryTag(html: String, lt: Int): Boolean {
            var i = lt + 1
            if (i < html.length && html[i] == '/') i++
            val start = i
            while (i < html.length && (html[i].lowercaseChar() in 'a'..'z' || html[i] in '0'..'9')) i++
            return html.substring(start, i).lowercase() in APP_BOUNDARY_TAGS
        }

        /**
         * Port of the app's `divineNameHasWordBoundaries` + skeleton validation: no Hebrew letter
         * after the span; before it at most three prefix letters, then a word boundary; and the
         * span itself is letters and marks spelling [kind].
         */
        fun appAcceptsSpan(html: String, start: Int, end: Int, kind: DivineKind): Boolean {
            val skeleton = StringBuilder()
            for (k in start until end) {
                val c = html[k]
                when {
                    isHebrewLetter(c) -> skeleton.append(c)
                    isAppMark(c) -> Unit
                    else -> return false
                }
            }
            val s = skeleton.toString()
            val skeletonOk = when (kind) {
                DivineKind.HVYH -> s == "יהוה"
                DivineKind.ADNUT -> s == "אדני"
                DivineKind.EL -> s == "אל" || s == "אלי"
                DivineKind.ELOAH -> s == "אלוה" || s == "אלה"
                DivineKind.ELOHIM -> s.startsWith("אלה") || s.startsWith("אלוה")
                DivineKind.EHYEH -> s == "אהיה"
                DivineKind.SHADDAI -> s == "שדי"
                DivineKind.TZEVAOT -> s == "צבאות"
                DivineKind.YAH -> s == "יה"
            }
            if (!skeletonOk) return false

            var i = end
            while (i < html.length) {
                val c = html[i]
                if (c == '<') {
                    val gt = html.indexOf('>', i)
                    if (gt < 0 || isAppBoundaryTag(html, i)) break
                    i = gt + 1
                } else if (isAppMark(c)) {
                    i++
                } else {
                    if (isHebrewLetter(c)) return false
                    break
                }
            }
            var prefixes = 0
            i = start - 1
            while (i >= 0) {
                val c = html[i]
                if (c == '>') {
                    val lt = html.lastIndexOf('<', i)
                    if (lt < 0 || isAppBoundaryTag(html, lt)) break
                    i = lt - 1
                } else if (isAppMark(c)) {
                    i--
                } else if (isHebrewLetter(c)) {
                    if (c !in APP_PREFIX_LETTERS) return false
                    if (++prefixes > 3) return false
                    i--
                } else {
                    break
                }
            }
            return true
        }

        private fun skeletonOf(letters: List<WordLetter>): String = letters.joinToString("") { it.ch.toString() }

        /** Prefix lengths 0..3 whose letters all come from [allowed], always leaving a body. */
        private fun prefixLengths(word: Word, allowed: Set<Char>): List<Int> {
            val out = arrayListOf(0)
            var k = 0
            while (k < 3 && k < word.letters.size - 1 && word.letters[k].ch in allowed) {
                k++
                out += k
            }
            return out
        }

        /** [skeleton] is one of [targets] after at most [max] leading letters from [letters]. */
        private fun inWithPrefix(skeleton: String, targets: Set<String>, letters: String, max: Int): Boolean {
            for (k in 0..max) {
                if (k > skeleton.length) return false
                if (k > 0 && skeleton[k - 1] !in letters) return false
                if (skeleton.substring(k) in targets) return true
            }
            return false
        }
    }
}
