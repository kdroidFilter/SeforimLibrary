package io.github.kdroidfilter.seforimlibrary.core.extensions

import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.LineTocMapping

/**
 * Extensions to facilitate the transition to the new structure without tocEntryId
 */

/**
 * Retrieves the first TOC entry associated with this line
 * (to be used with a list of LineTocMapping)
 *
 * @param mappings The list of line-to-TOC mappings to search in
 * @return The ID of the first TOC entry associated with this line, or null if none found
 */
fun Line.findTocEntryId(mappings: List<LineTocMapping>): Long? {
    return mappings.firstOrNull { it.lineId == this.id }?.tocEntryId
}

/**
 * Checks if this line has an associated TOC entry
 *
 * @param mappings The list of line-to-TOC mappings to search in
 * @return True if this line has an associated TOC entry, false otherwise
 */
fun Line.hasTocEntry(mappings: List<LineTocMapping>): Boolean {
    return mappings.any { it.lineId == this.id }
}
