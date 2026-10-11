package io.github.kdroidfilter.seforimlibrary.reflinks

/** One level of a heRef address ("בראשית א, ד" = [Num(1), Num(4)], "שבת כא:" = [Daf(21, B)]). */
sealed interface Section {
    data class Num(val value: Int) : Section
    data class Daf(val folio: Int, val side: Side) : Section
    data class Name(val text: String) : Section

    enum class Side { A, B }
}

enum class Family {
    TANAKH, BAVLI, MISHNA, YERUSHALMI, TOSEFTA, RASHI_BAVLI, TOSAFOT,
    RABBA, SHULCHAN_ARUKH, TUR, RAMBAM, MISC, ZOHAR_DAF,
}

/** How precise the parsed location is. BOOK means the name was recognized but no usable location. */
enum class Precision { BOOK, CHAPTER, FULL }

/**
 * A recognized citation: [title] is the target book title as stored in `book.title`, [sections] its heRef address.
 * [trailingTokens] counts the words after the location that were not part of the citation (e.g. "ד\"ה …").
 */
data class Citation(
    val family: Family,
    val title: String,
    val sections: List<Section>,
    val precision: Precision,
    val alias: String,
    val trailingTokens: Int = 0,
    val rangeEnd: Int? = null,
    /** Zohar only: volume (1-3) and folio, resolved through the alt TOC rather than heRef. */
    val zoharVolume: Int? = null,
    val zoharDaf: Section.Daf? = null,
)
