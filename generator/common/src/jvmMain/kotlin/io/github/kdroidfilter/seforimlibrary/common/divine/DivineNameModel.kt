package io.github.kdroidfilter.seforimlibrary.common.divine

/**
 * The divine name a `line_divine_name` row marks. [code] is the stored INTEGER and
 * is part of the contract with the Otzaria app — never renumber.
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

    /** Reserved, never emitted in wave 1 (halachic dispute over יה). */
    YAH(9),
    ;

    companion object {
        fun parse(name: String): DivineKind = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException("unknown divine-name kind '$name'")
    }
}

/** Which rule proved the occurrence sacred. [code] is the stored INTEGER (contract). */
enum class DivineSource(val code: Int) {
    HVYH_RULE(1),
    NIKUD_RULE(2),
    COLLOCATION(3),
    EXCEPTION_LIST(4),
}

/**
 * One verified sacred occurrence. [rawStart]/[rawEnd] index the stored HTML;
 * [charStart]/[charEnd] are the contract's visible-char coordinates
 * (same space as `link_anchor`, see `countVisibleChars`).
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
