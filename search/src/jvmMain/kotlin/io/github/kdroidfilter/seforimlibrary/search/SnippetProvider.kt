package io.github.kdroidfilter.seforimlibrary.search

/**
 * Metadata about a line needed to fetch its snippet source text.
 *
 * @property lineId Unique identifier of the line
 * @property bookId Book containing this line (used for context fetching)
 * @property lineIndex Zero-based position of the line in the book
 */
data class LineSnippetInfo(
    val lineId: Long,
    val bookId: Long,
    val lineIndex: Int
)

/**
 * Provider interface for fetching snippet source text from a data store.
 *
 * The search engine uses this to retrieve the full text content needed for
 * snippet generation. Implementations typically fetch from a database and
 * may include neighboring lines for better context.
 *
 * ## Implementation Notes
 * - Should return HTML-cleaned text (no raw HTML tags)
 * - May include neighboring lines for context (typically 4 lines before/after)
 * - Should handle missing lines gracefully (omit from result map)
 * - Should be efficient for batch lookups (single DB query for all lines)
 *
 * ## Example Implementation
 * ```kotlin
 * class RepositorySnippetProvider(
 *     private val repository: SeforimRepository
 * ) : SnippetProvider {
 *     override fun getSnippetSources(lines: List<LineSnippetInfo>): Map<Long, String> {
 *         return repository.getSnippetSourcesForLines(
 *             lines.map { it.lineId },
 *             neighborWindow = 4,
 *             minLength = 280
 *         )
 *     }
 * }
 * ```
 *
 * @see SearchEngine for how this provider is used during search
 */
fun interface SnippetProvider {
    /**
     * Fetches snippet source text for multiple lines in a single batch.
     *
     * @param lines List of line metadata for which to fetch text
     * @return Map of lineId to its source text. Missing lines should be omitted.
     */
    fun getSnippetSources(lines: List<LineSnippetInfo>): Map<Long, String>
}
