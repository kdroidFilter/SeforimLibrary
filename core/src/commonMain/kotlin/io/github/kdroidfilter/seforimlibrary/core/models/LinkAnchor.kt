package io.github.kdroidfilter.seforimlibrary.core.models

import kotlinx.serialization.Serializable

/**
 * Word-level anchor of a [Link] inside a line's text.
 *
 * Coordinates are visible-char offsets (HTML tags skipped, each entity counted
 * as one char — the same convention as `line.charCount`), from the start of
 * the anchored line's content. From schema 7 that is the full line: the stored
 * text with each `line_note` body's visible chars at its `charPos`, in note order.
 *
 * @property linkId The link this anchor belongs to
 * @property side 0 = anchor sits in the source line's text (base side of the
 *           stored base→dependant direction); 1 is reserved for target-side anchors
 * @property charStart Anchor point (marker insertion point / range start)
 * @property charEnd Exclusive range end, or `null` for point anchors (inline markers)
 * @property label Printed marker when the source declares one (e.g. "א")
 */
@Serializable
data class LinkAnchor(
    val linkId: Long,
    val side: Int = 0,
    val charStart: Int,
    val charEnd: Int? = null,
    val label: String? = null,
)
