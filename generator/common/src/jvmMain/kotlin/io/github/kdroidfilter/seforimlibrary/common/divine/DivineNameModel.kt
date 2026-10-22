package io.github.kdroidfilter.seforimlibrary.common.divine

/**
 * The divine name a `line_divine_name` row marks. [code] is stored in `span` ([DivineSpanCode])
 * and is part of the contract with the Otzaria app — never renumber.
 */
enum class DivineKind(val code: Int) {
    HVYH(1),
    ADNUT(2),
    EL(3),
    ELOAH(4),
    ELOHIM(5),
    EHYEH(6),
    SHADDAI(7),
    TZEVAOT(8),

    /** יָהּ as a word of its own (with prefixes); never inside a word such as הללויה. */
    YAH(9),
    ;

    companion object {
        fun parse(name: String): DivineKind = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException("unknown divine-name kind '$name'")
    }
}

/** Which rule proved the occurrence sacred. [code] is stored in `span` ([DivineSpanCode], contract). */
enum class DivineSource(val code: Int) {
    HVYH_RULE(1),
    NIKUD_RULE(2),
    COLLOCATION(3),
    EXCEPTION_LIST(4),
}

/**
 * The stored `span` INTEGER (contract): `length << 7 | source << 4 | kind`, where
 * length = charEnd - charStart. Encode and decode reject any value outside its field.
 */
object DivineSpanCode {
    private const val KIND_BITS = 4
    private const val SOURCE_BITS = 3
    private const val LENGTH_SHIFT = KIND_BITS + SOURCE_BITS
    private const val KIND_MASK = (1 shl KIND_BITS) - 1
    private const val SOURCE_MASK = (1 shl SOURCE_BITS) - 1
    const val MAX_LENGTH: Int = Int.MAX_VALUE ushr LENGTH_SHIFT

    data class Decoded(val charEnd: Int, val kind: DivineKind, val source: DivineSource)

    fun encode(charStart: Int, charEnd: Int, kind: DivineKind, source: DivineSource): Int {
        val length = charEnd.toLong() - charStart
        require(charStart >= 0 && length in 1..MAX_LENGTH) { "divine span [$charStart, $charEnd) out of range" }
        require(kind.code in 1..KIND_MASK) { "divine kind code ${kind.code} does not fit $KIND_BITS bits" }
        require(source.code in 1..SOURCE_MASK) { "divine source code ${source.code} does not fit $SOURCE_BITS bits" }
        return (length.toInt() shl LENGTH_SHIFT) or (source.code shl KIND_BITS) or kind.code
    }

    fun decode(charStart: Int, span: Long): Decoded {
        require(charStart >= 0 && span in 0..Int.MAX_VALUE) { "divine span $span at $charStart out of range" }
        val length = (span ushr LENGTH_SHIFT).toInt()
        val kindCode = (span and KIND_MASK.toLong()).toInt()
        val sourceCode = ((span ushr KIND_BITS) and SOURCE_MASK.toLong()).toInt()
        val kind = DivineKind.entries.firstOrNull { it.code == kindCode }
        val source = DivineSource.entries.firstOrNull { it.code == sourceCode }
        require(length >= 1 && charStart.toLong() + length <= Int.MAX_VALUE && kind != null && source != null) {
            "undecodable divine span $span at $charStart"
        }
        return Decoded(charStart + length, kind, source)
    }
}

/**
 * One verified sacred occurrence. [rawStart]/[rawEnd] index the stored HTML;
 * [charStart]/[charEnd] are the contract's visible-char coordinates of that
 * HTML (a stored line has no note bodies, unlike `link_anchor`; see `countVisibleChars`).
 */
data class DivineSpan(
    val rawStart: Int,
    val rawEnd: Int,
    val charStart: Int,
    val charEnd: Int,
    val kind: DivineKind,
    val source: DivineSource,
)

/** A Tanach verse, resolved from a base-text line's heRef. */
data class VerseRef(val book: String, val chapter: Int, val verse: Int) : Comparable<VerseRef> {
    override fun compareTo(other: VerseRef): Int =
        compareValuesBy(this, other, { it.book }, { it.chapter }, { it.verse })

    override fun toString(): String = "$book $chapter:$verse"
}
