package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Link between two texts (commentary, reference, etc.)
 *
 * Storage convention: links are persisted in a single canonical direction
 * `source → target` (base book → dependant book when applicable). The reverse
 * `SOURCE` view is synthesized at read time from links where the line appears
 * as `targetLineId`. `ConnectionType.SOURCE` therefore never appears as a
 * stored row — it is a virtual type produced by the repository.
 *
 * @property id The unique identifier of the link
 * @property sourceBookId The identifier of the source book
 * @property targetBookId The identifier of the target book
 * @property sourceLineId The identifier of the source line
 * @property targetLineId The identifier of the target line
 * @property targetLineIndex The 0-based index of the target line within its book.
 *           Denormalized from `line.lineIndex` so that commentaries can be ordered
 *           by their natural position in the target book without an extra JOIN.
 * @property connectionType The type of connection between the texts
 */
@Serializable
data class Link(
    val id: Long = 0,
    val sourceBookId: Long,
    val targetBookId: Long,
    val sourceLineId: Long,
    val targetLineId: Long,
    val targetLineIndex: Int,
    val connectionType: ConnectionType,
    /**
     * `true` when the orientation of this link was determined by an explicit
     * Sefaria-declared `base_text_titles` match (i.e. the target book's schema
     * declares the source book as its base text). `false` for orientations
     * inferred via density chaining, isBaseBook/priorityRank fallback, or
     * unoriented types.
     *
     * Used by the SOURCE virtual view's ORDER BY to surface Sefaria-declared
     * bases above lateral citations in books that cite Tanakh extensively
     * while having a smaller declared base (e.g. Nachalat Avot on Pirkei Avot
     * where Mishnah Avot has 93 links but Tehillim citations dominate at 371).
     */
    val isDeclaredBase: Boolean = false,
)

/**
 * Types of connections between texts.
 *
 * Persisted types are everything except [SOURCE]. [SOURCE] is a virtual type
 * exposed by the repository when answering "what does this line comment on?"
 * — it is derived by querying `targetLineId` of stored COMMENTARY/TARGUM/etc.
 * links and swapping source/target columns at read time.
 */
@Serializable
enum class ConnectionType {
    COMMENTARY,
    SUPER_COMMENTARY,
    TARGUM,
    REFERENCE,

    /** Virtual: never stored. Derived by the repository from reverse-direction links. */
    SOURCE,

    MIDRASH,
    QUOTATION,
    MESORAT_HASHAS,
    EIN_MISHPAT,
    DIBUR_HAMATCHIL,
    PARSHANUT,
    MISHNAH_IN_TALMUD,
    RELATED,
    OTHER,
    ;

    companion object {
        /**
         * Creates a ConnectionType from a string value.
         *
         * Accepts Sefaria's `Conection Type` (sic) CSV values verbatim — case,
         * whitespace and underscore/space variations are normalized. Unknown
         * values fall back to [OTHER].
         */
        fun fromString(value: String): ConnectionType {
            val v = value.trim().lowercase().replace(' ', '_')
            return when (v) {
                "commentary" -> COMMENTARY
                "super_commentary", "supercommentary" -> SUPER_COMMENTARY
                "targum" -> TARGUM
                "reference" -> REFERENCE
                "source" -> SOURCE
                "midrash" -> MIDRASH
                "quotation", "quotation_auto", "quotation_auto_tanakh" -> QUOTATION
                "mesorat_hashas" -> MESORAT_HASHAS
                "ein_mishpat", "ein_mishpat_/_ner_mitsvah", "ein_mishpat_/_ner_mitzvah" -> EIN_MISHPAT
                "dibur_hamatchil" -> DIBUR_HAMATCHIL
                "parshanut" -> PARSHANUT
                "mishnah_in_talmud" -> MISHNAH_IN_TALMUD
                "related", "related_passage" -> RELATED
                "", "none" -> OTHER
                else -> OTHER
            }
        }
    }
}
