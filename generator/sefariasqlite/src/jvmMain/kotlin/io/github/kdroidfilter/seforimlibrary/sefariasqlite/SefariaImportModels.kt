package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import io.github.kdroidfilter.seforimlibrary.common.ids.LegacyLineKey
import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import kotlinx.serialization.Serializable

internal object SefariaImportTuning {
    const val LINE_BATCH_SIZE = 5_000
    const val LINK_BATCH_SIZE = 2_000
    const val FILE_PARALLELISM = 8
}

/**
 * Sefaria's `dependence` flag — the canonical signal for "this book is a
 * dependant text of another book". `null` means the book stands on its own
 * (Tanakh, Talmud, etc.).
 *
 * `OTHER_DEPENDANT` covers values present in the schema that don't have an
 * exact mapping (e.g. "Guides", "Sub-Commentary"). Treated identically to
 * COMMENTARY for orientation purposes — what matters is "dependant or not".
 */
internal enum class Dependence { COMMENTARY, TARGUM, MIDRASH, OTHER_DEPENDANT }

internal data class BookMeta(
    val isBaseBook: Boolean,
    val categoryLevel: Int,
    val priorityRank: Int?,
    // Schema-derived: dependant kind ("Commentary"/"Targum"/...), null = base book.
    val dependence: Dependence? = null,
    // All known base bookIds: starts from Sefaria's declared `base_text_titles`,
    // then gets extended by `inferPrimaryBasesForEmptyDeclaredBookmeta` and by
    // density-based sibling chaining. Used by the resolver to orient links.
    val baseTextBookIds: Set<Long> = emptySet(),
    // **Strict** subset of `baseTextBookIds` — only the bookIds that came from
    // Sefaria's own `base_text_titles` declaration in the schema. Inference and
    // density chaining never mutate this set. Used by the SOURCE virtual view
    // to boost Sefaria-confirmed bases above lateral citations (e.g. Mishnah
    // Avot above Tehillim for Nachalat Avot on Pirkei Avot). → baseProvenance=2.
    val sefariaDeclaredBaseTextBookIds: Set<Long> = emptySet(),
    // Bases recovered from the title's "X on Y" pattern (no `base_text_titles`
    // in schema). Disjoint from the declared set; density chaining never
    // mutates it. → baseProvenance=1 (declared wins if a book is in both).
    val inferredBaseTextBookIds: Set<Long> = emptySet(),
    // Schema-derived: Sefaria's `collective_title.en` — the commentator name shared
    // across all volumes of a multi-volume work (e.g. "Rashi" for "Rashi on Genesis",
    // "Rashi on Exodus"…). Used by the density chain to aggregate per-collective
    // signal so volume-level noise doesn't tip the per-pair ratio.
    val collectiveTitleEn: String? = null,
    // Schema `categories[0]` ("Tanakh", "Midrash"…), for books whose kind the schema
    // doesn't state through `dependence`.
    val topCategoryEn: String? = null,
    // השמות הקנוניים של מחברי הספר (כמו בטבלת author). משמשים להשלמת בסיסים של
    // פירוש-על-פירוש: "אבן עזרא על שמות; פירוש הקצר" הוא של אותו מחבר כמו
    // "אבן עזרא על בראשית" שמחוקקי יהודה מצהיר עליו (ר' completeDeclaredBaseFamilies).
    val authorKeys: Set<String> = emptySet(),
    // Sefaria `Reference` work (dictionary, lexicon…). It explains words of other
    // texts without being their base or their commentary, so a link between a
    // reference work and a non-reference book is never oriented.
    val isReferenceWork: Boolean = false,
)

/// Base encoded value in [BookPayload.cleanShiftByLineIndex] for a line whose
/// content was modified by cleanSefariaLine and has no generated prefix.
/// A modified line with a prefix of length N is encoded as `CLEAN_MODIFIED - N`.
internal const val CLEAN_MODIFIED = -1

/** Encodes "cleaned content, generated prefix of [prefixLength] UTF-16 chars". */
internal fun cleanedLineShift(prefixLength: Int): Int {
    require(prefixLength >= 0) { "prefixLength must not be negative: $prefixLength" }
    return CLEAN_MODIFIED - prefixLength
}

/** Negative shift values mean raw char offsets are no longer exact. */
internal fun lineWasModifiedByCleaning(encodedShift: Int): Boolean = encodedShift < 0

/** Recovers the generated-prefix length without conflating it with cleaning. */
internal fun generatedPrefixLength(encodedShift: Int): Int =
    if (lineWasModifiedByCleaning(encodedShift)) -(encodedShift + 1) else encodedShift

internal data class BookPayload(
    val heTitle: String,
    val enTitle: String,
    val categoriesHe: List<String>,
    val lines: List<String>,
    val refEntries: List<RefEntry>,
    val headings: List<Heading>,
    val authors: List<String>,
    // Every name form these authors are known by: the bare schema names plus
    // the honorific/acronym variants from authors.json. `authors` holds only
    // the chosen display name, so blacklist matching must use this instead —
    // otherwise a titled display name walks past an entry listed bare.
    val authorMatchKeys: List<String> = emptyList(),
    // Long description (Sefaria heDesc) → book.heDesc
    val description: String?,
    // Short one-line summary (Sefaria heShortDesc) → book.heShortDesc
    val heShortDesc: String?,
    val pubDates: List<PubDate>,
    val altStructures: List<AltStructurePayload>,
    // Schema metadata used for link orientation. The *Keys lists hold the
    // *normalized* titles (en+he) of base texts; resolution to bookIds happens
    // in a second pass once all books have been inserted.
    val dependence: Dependence? = null,
    // Raw `dependence` value (trim + lowercase), persisted to book.dependenceType.
    val rawDependence: String? = null,
    // From schema `base_text_titles` — provenance SEFARIA_DECLARED.
    val declaredBaseTextTitleKeys: List<String> = emptyList(),
    // Recovered from the "X on Y" title pattern — provenance INFERRED_TITLE.
    val inferredBaseTextTitleKeys: List<String> = emptyList(),
    val collectiveTitleHe: String? = null,
    val collectiveTitleEn: String? = null,
    // All Sefaria-known aliases for the book (titleVariants + heTitleVariants),
    // normalized. Indexed alongside the primary titles in normalizedTitleToBookId
    // so that title-pattern base parsing ("X on Y") can resolve "Y" to a bookId
    // when "Y" is a Sefaria-recognised alias (e.g. "Avot" → Pirkei Avot).
    val titleAliasKeys: List<String> = emptyList(),
    // merged.json versions: when the book was merged from exactly ONE version,
    // the merged text is that version verbatim — the precondition for exact
    // charLevelData offset import. null when multi-version (or unknown).
    val singleVersionTitle: String? = null,
    // Sparse per-line offset bookkeeping for charLevelData mapping:
    //   absent          -> stored content == raw segment, no prefix
    //   n >= 0          -> unchanged segment behind a generated prefix of length n
    //   n < 0           -> cleaned segment; prefix length is -(n + 1), offsets unusable
    val cleanShiftByLineIndex: Map<Int, Int> = emptyMap(),
    // Sparse keys for segments whose preserved breaks change the rendered text.
    // Computed from the previous cleaning + dashless-repair pipeline, before the
    // generated prefix. Generated headings/authors never pass through cleaning.
    val lineKeyHashOverrides: Map<Int, ByteArray> = emptyMap(),
    // Line indices of siman topic lines split off a first se'if (SefariaSimanTopicLines).
    val simanTopicLines: Set<Int> = emptySet(),
    // All [versionTitle, versionSource] pairs from merged.json's `versions` array
    // (the versions that CONTRIBUTED to the merge). book_version metadata-only
    // fallback when no per-version sibling files exist.
    val versionsMeta: List<VersionMeta> = emptyList(),
    // Book directory (parent of merged.json, where per-version sibling files
    // live) and the resolved schema file — inputs for the versions pass.
    val sourceDirPath: String? = null,
    val schemaFilePath: String? = null,
    // שם קובץ הגרסה המוצהר ב-preferred_versions.txt שממנו נקרא הטקסט הראשי.
    // null = הטקסט הוא merged.json, ורק אז refEntries הם איחוד המהדורות.
    val preferredVersionFileName: String? = null,
    // Raw English `categories` from the index record. Only the whole-unit ref
    // sets need them (Sefaria's own gate is English-category based).
    val categoriesEn: List<String> = emptyList(),
    // Store lines with their dash dibbur bolded — see SefariaTalmudDibburBold.
    val boldDashDibburim: Boolean = false,
    // Top-level Sefaria category is `Reference` (dictionaries, lexicons, bibliographies).
    val isReferenceWork: Boolean = false,
    // Base tractate whose name is dropped after לקמן/לעיל — see SefariaSameTractateRefs.
    val sameTractateHeTitle: String? = null,
) {
    /**
     * Text-only per-line derivations for this book: computed on the parallel
     * parse worker by [precomputeLineData], read by the single-threaded insert
     * loop. Deliberately *not* a constructor property — it is mutable state the
     * loop releases per book, and it must stay out of the data class's identity
     * (`equals`/`hashCode`/`copy`).
     *
     * `null` means "not precomputed": the state tests and the manual-links
     * reader see, neither of which reaches the insert loop.
     */
    var precomputed: LinePrecompute? = null
}

/**
 * The per-line values the serial insert loop used to derive inline, hoisted to
 * the parse phase. The three arrays are indexed by lineIndex and sized to the
 * book's line count.
 *
 * [release] drops them as soon as the book's rows have been queued. That matters:
 * each 20-byte hash is handed to the IdAllocator, which retains it inside its
 * `LineKey` for the rest of the build, so a second reference from the payload
 * buys nothing and only inflates the insert loop's peak heap — and that peak is
 * what sits closest to the generator's `-Xmx`.
 */
internal class LinePrecompute(
    /// `refEntries.associateBy { it.lineIndex - 1 }` — lineIndex → RefEntry.
    /// Kept past [release]: the inline-anchor pass holds it for the whole build.
    val refsByLineIndex: Map<Int, RefEntry>,
    /// `detectTeamimAndNekudot(lines)`, book-level.
    val hasTeamim: Boolean,
    val hasNekudot: Boolean,
    lineKeyHashes: Array<ByteArray>,
    legacyLineKeyHashes: Array<ByteArray>,
    lineCharCounts: IntArray,
    lineIsHeading: BooleanArray,
) {
    /// Per line, the natural-key hash of the raw segment (generated prefixes stripped).
    var lineKeyHashes: Array<ByteArray>? = lineKeyHashes
        private set

    /// Per line, `LegacyLineKey.hash(content, heRef)` — transition shim only.
    var legacyLineKeyHashes: Array<ByteArray>? = legacyLineKeyHashes
        private set

    /// Per line, `countVisibleChars(content)`.
    var lineCharCounts: IntArray? = lineCharCounts
        private set

    /// Per line, whether the content carries an `<h1>`…`<h4>` tag.
    var lineIsHeading: BooleanArray? = lineIsHeading
        private set

    /// Line count the arrays were built for. Survives [release] so the insert
    /// loop can still assert it against `payload.lines.size`.
    val lineCount: Int = lineKeyHashes.size

    fun release() {
        lineKeyHashes = null
        legacyLineKeyHashes = null
        lineCharCounts = null
        lineIsHeading = null
    }
}

/**
 * Computes, on the parallel parse worker that already holds this book's text,
 * every per-line value the serial insert loop used to derive inline: the natural
 * key hash, the visible char count, the heading flag, the lineIndex → RefEntry
 * lookup, and the book-level teamim/nekudot flags.
 *
 * This is pure code motion. Each value is produced by the exact same expression
 * the loop used, over [BookPayload.lines] in ascending index order, so the loop
 * keeps calling `nextLineOccurrence` / `allocator.lineId` with byte-identical
 * arguments in an unchanged sequence — see the ordering invariant in
 * [SefariaDirectImporter].
 */
internal fun BookPayload.precomputeLineData(): BookPayload {
    val refsByLineIndex = refEntries.associateBy { it.lineIndex - 1 }
    val count = lines.size
    val hashes = arrayOfNulls<ByteArray>(count)
    val legacyHashes = arrayOfNulls<ByteArray>(count)
    val charCounts = IntArray(count)
    val isHeading = BooleanArray(count)
    for (idx in 0 until count) {
        val content = lines[idx]
        hashes[idx] = lineKeyHashOverrides[idx] ?: IdAllocatorBindings.lineNaturalKeyHash(rawSegmentForKey(idx, content))
        legacyHashes[idx] = LegacyLineKey.hash(content, refsByLineIndex[idx]?.heRef)
        charCounts[idx] = countVisibleChars(content)
        isHeading[idx] = content.contains("<h1>") || content.contains("<h2>") ||
            content.contains("<h3>") || content.contains("<h4>")
    }
    val (teamim, nekudot) = detectTeamimAndNekudot(lines)
    @Suppress("UNCHECKED_CAST")
    precomputed = LinePrecompute(
        refsByLineIndex = refsByLineIndex,
        hasTeamim = teamim,
        hasNekudot = nekudot,
        lineKeyHashes = hashes as Array<ByteArray>,
        legacyLineKeyHashes = legacyHashes as Array<ByteArray>,
        lineCharCounts = charCounts,
        lineIsHeading = isHeading,
    )
    return this
}

/**
 * The line's text with the generated prefix (`(א) `, daf labels…) stripped off.
 * Inserting one verse reprefixes every later line of the chapter, and hashing
 * the prefixed text would renumber all of their ids (issue #1211).
 * Segments with preserved breaks use the reader's historic-key override above.
 */
private fun BookPayload.rawSegmentForKey(lineIndex: Int, content: String): String =
    rawSegmentForKey(content, cleanShiftByLineIndex[lineIndex])

internal fun rawSegmentForKey(content: String, encodedShift: Int?): String {
    if (encodedShift == null) return content
    val prefixLength = generatedPrefixLength(encodedShift)
    return if (prefixLength in 1..content.length) content.substring(prefixLength) else content
}

internal data class VersionMeta(
    val title: String,
    val source: String?,
)

internal data class RefEntry(
    val ref: String,
    val heRef: String,
    val path: String,
    val lineIndex: Int
)

/**
 * A links-CSV `Char Level Data` cell waiting for offset resolution. Collected
 * while streaming the CSVs (where the ref→line resolution and the stored link
 * direction are known) and resolved after all lines/links exist, where the
 * anchored line's stored content is available (see [SefariaCharLevelAnchors]).
 */
internal data class PendingCharLevelAnchor(
    /** bookPath + 0-based lineIndex of the line the offsets refer to. */
    val path: String,
    val lineIndex0: Int,
    /** The stored link row this anchor belongs to (post direction-swap). */
    val srcLineId: Long,
    val tgtLineId: Long,
    /** 0 = the anchored line is the stored source line, 1 = the stored target. */
    val side: Int,
    val startChar: Int,
    val endChar: Int,
    val versionTitle: String,
    val language: String,
    /** Sefaria uses startWord/endWord (word indices) for Tanakh verse sides. */
    val isWordBased: Boolean,
)

internal data class Heading(
    val title: String,
    val level: Int,
    val lineIndex: Int
)

internal data class AltStructurePayload(
    val key: String,
    val title: String?,
    val heTitle: String?,
    val nodes: List<AltNodePayload>
)

internal data class AltNodePayload(
    val title: String?,
    val heTitle: String?,
    val wholeRef: String?,
    val refs: List<String>,
    val addressTypes: List<String>,
    val childLabel: String?,
    val addresses: List<Int>,
    val skippedAddresses: List<Int>,
    val startingAddress: String?,
    val offset: Int?,
    val children: List<AltNodePayload>,
    // True when a `match_templates` entry has scope "any"/"alone" — Sefaria's
    // own predicate for "this node can be cited on its own" (a whole perek).
    val referenceableAlone: Boolean = false
)

@Serializable
internal data class DefaultCommentatorsEntry(
    val book: String,
    val commentators: List<String>
)

@Serializable
internal data class DefaultTargumEntry(
    val book: String,
    val targumim: List<String>
)
