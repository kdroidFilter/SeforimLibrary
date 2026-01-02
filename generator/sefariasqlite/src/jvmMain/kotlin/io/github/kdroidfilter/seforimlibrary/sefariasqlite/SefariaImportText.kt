package io.github.kdroidfilter.seforimlibrary.sefariasqlite

internal fun sanitizeFolder(name: String?): String {
    if (name.isNullOrBlank()) return ""
    return name.replace("\"", "״").trim()
}

internal fun normalizeTitleKey(value: String?): String? {
    if (value.isNullOrBlank()) return null

    // Normalize various quote styles (ASCII and Hebrew) so titles that differ
    // only by גרש/גרשיים or straight quotes map to the same key.
    val withoutQuotes = value
        .replace("\"", "")
        .replace("'", "")
        .replace("\u05F3", "") // Hebrew geresh
        .replace("\u05F4", "") // Hebrew gershayim

    return withoutQuotes
        .lowercase()
        .replace("\\s+".toRegex(), " ")
        .replace('_', ' ')
        .trim()
}
