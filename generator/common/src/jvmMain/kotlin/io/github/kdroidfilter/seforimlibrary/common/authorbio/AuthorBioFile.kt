package io.github.kdroidfilter.seforimlibrary.common.authorbio

/** One biography of kdroidFilter/seforim-author-bio (`authors/NNNN.md`). */
data class AuthorBioFile(
    /** Author name exactly as in the `author` table: the join key. */
    val name: String,
    val confidence: String,
    /** The תקציר section, or null when the file has none. */
    val summary: String?,
    /** Everything after the `# ` title line. */
    val bodyMd: String,
) {
    companion object {
        private const val SUMMARY_HEADING = "## תקציר"

        /** Parses a biography; throws when the front matter or the title is missing. */
        fun parse(text: String): AuthorBioFile {
            val lines = text.replace("\r\n", "\n").lines()
            require(lines.firstOrNull() == "---") { "missing front matter" }
            val end = lines.drop(1).indexOf("---") + 1
            require(end > 0) { "unterminated front matter" }
            val meta = lines.subList(1, end).mapNotNull { line ->
                val colon = line.indexOf(':').takeIf { it > 0 } ?: return@mapNotNull null
                line.substring(0, colon).trim() to unquote(line.substring(colon + 1).trim())
            }.toMap()

            val body = lines.drop(end + 1)
            val titleIndex = body.indexOfFirst { it.startsWith("# ") }
            require(titleIndex >= 0) { "missing # title" }
            val bodyMd = body.drop(titleIndex + 1).joinToString("\n").trim()

            return AuthorBioFile(
                name = meta["name"] ?: error("missing name"),
                confidence = meta["confidence"] ?: error("missing confidence"),
                summary = section(bodyMd, SUMMARY_HEADING),
                bodyMd = bodyMd,
            )
        }

        private fun section(markdown: String, heading: String): String? {
            val lines = markdown.lines()
            val start = lines.indexOf(heading).takeIf { it >= 0 } ?: return null
            return lines.drop(start + 1)
                .takeWhile { !it.startsWith("## ") }
                .joinToString("\n")
                .trim()
                .takeIf { it.isNotEmpty() }
        }

        private fun unquote(value: String): String =
            if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) {
                value.substring(1, value.length - 1).replace("\\\"", "\"").replace("\\\\", "\\")
            } else {
                value
            }
    }
}
