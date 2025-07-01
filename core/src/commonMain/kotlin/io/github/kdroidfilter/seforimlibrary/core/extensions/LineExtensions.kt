package io.github.kdroidfilter.seforimlibrary.core.extensions

import io.github.kdroidfilter.seforimlibrary.core.models.Line
import io.github.kdroidfilter.seforimlibrary.core.models.LineTocMapping

/**
 * Extensions pour faciliter la transition vers la nouvelle structure sans tocEntryId
 */

/**
 * Permet de récupérer la première entrée TOC associée à cette ligne
 * (à utiliser avec une liste de LineTocMapping)
 */
fun Line.findTocEntryId(mappings: List<LineTocMapping>): Long? {
    return mappings.firstOrNull { it.lineId == this.id }?.tocEntryId
}

/**
 * Permet de savoir si cette ligne a une entrée TOC associée
 */
fun Line.hasTocEntry(mappings: List<LineTocMapping>): Boolean {
    return mappings.any { it.lineId == this.id }
}
