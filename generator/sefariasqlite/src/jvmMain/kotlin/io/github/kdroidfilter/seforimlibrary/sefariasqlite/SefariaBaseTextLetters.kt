package io.github.kdroidfilter.seforimlibrary.sefariasqlite

/**
 * Commentaries whose notes the base text letters in a series of its own (Otzaria/otzaria#1979).
 * Sefaria keeps those letters only in the base text's itags: `data-order` is the note's number
 * in its siman and `data-label` the letter printed on it, an alphabet that restarts every siman
 * and skips the unlettered notes ("•"). Sefaria's client labels the note with that same
 * `data-label` (TextRange.jsx). Yoreh De'ah's itags carry no `data-label`, so it is not here.
 */
internal data class BaseTextLettering(
    val commentary: String,
    val baseText: String,
    val commentator: String,
)

internal val BASE_TEXT_LETTERINGS = listOf(
    BaseTextLettering("Be'er HaGolah on Shulchan Arukh, Orach Chayim", "Shulchan Arukh, Orach Chayim", "Be'er HaGolah"),
    BaseTextLettering("Be'er HaGolah on Shulchan Arukh, Even HaEzer", "Shulchan Arukh, Even HaEzer", "Be'er HaGolah"),
    BaseTextLettering("Be'er HaGolah on Shulchan Arukh, Choshen Mishpat", "Shulchan Arukh, Choshen Mishpat", "Be'er HaGolah"),
)

internal val BASE_TEXT_LETTERING_BASES: Set<String> = BASE_TEXT_LETTERINGS.mapTo(HashSet()) { it.baseText }

/** The label the base text prints on an unlettered note. */
internal const val UNLETTERED_NOTE_LABEL = "•"

/** Commentary title -> canonical note ref -> the letter its base text prints on it. */
internal class BaseTextLetters(private val byCommentary: Map<String, Map<String, String>>) {
    /** Null for a book no base text letters; fails when its base text was never read. */
    fun forBook(enTitle: String): Map<String, String>? {
        val lettering = BASE_TEXT_LETTERINGS.firstOrNull { it.commentary == enTitle } ?: return null
        return byCommentary[enTitle] ?: throw SefariaSchemaException(
            "$enTitle takes its letters from ${lettering.baseText}, which was not read"
        )
    }

    companion object {
        val EMPTY = BaseTextLetters(emptyMap())

        fun from(bases: Collection<BookPayload>): BaseTextLetters {
            val byTitle = bases.groupBy { it.enTitle }.mapValues { (title, books) ->
                books.singleOrNull() ?: throw SefariaSchemaException("$title read from ${books.size} files")
            }
            return BaseTextLetters(
                BASE_TEXT_LETTERINGS.mapNotNull { lettering ->
                    byTitle[lettering.baseText]?.let { lettering.commentary to lettersOf(lettering, it) }
                }.toMap()
            )
        }
    }
}

private fun lettersOf(lettering: BaseTextLettering, base: BookPayload): Map<String, String> {
    val letters = HashMap<String, String>()
    for (entry in base.refEntries) {
        for (itag in parseInlineItags(base.lines[entry.lineIndex - 1])) {
            if (itag.commentator != lettering.commentator) continue
            val order = itag.order ?: throw SefariaSchemaException("${entry.ref}: ${lettering.commentator} itag without data-order")
            val label = itag.label ?: throw SefariaSchemaException("${entry.ref}: ${lettering.commentator} itag without data-label")
            val ref = buildCommentRef(entry.ref, base.enTitle, lettering.commentary, order)
                ?: throw SefariaSchemaException("${entry.ref}: not a ref of ${base.enTitle}")
            val previous = letters.putIfAbsent(canonicalCitation(ref), label)
            if (previous != null && previous != label) {
                throw SefariaSchemaException("$ref: lettered both $previous and $label in ${base.enTitle}")
            }
        }
    }
    return letters
}

// A Choshen Mishpat note that opens with its own printed letter: "ב) כן כ' הטור".
private val OWN_PRINTED_LETTER = Regex("""^\s*(?:<[^>]+>\s*)*[א-ת]\)""")

/** The prefix a lettered note gets: its letter, nothing when unlettered or already printing one. */
internal fun baseTextLetterPrefix(label: String, note: String): String = when {
    label == UNLETTERED_NOTE_LABEL -> ""
    OWN_PRINTED_LETTER.containsMatchIn(note) -> ""
    else -> "($label) "
}
