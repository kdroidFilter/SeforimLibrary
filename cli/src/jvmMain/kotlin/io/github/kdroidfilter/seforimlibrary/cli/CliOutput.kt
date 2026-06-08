package io.github.kdroidfilter.seforimlibrary.cli

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/** Shared pretty-printing JSON instance for `--json` output. */
internal val jsonPretty = Json { prettyPrint = true }

/** Strips HTML tags and unescapes the few entities that show up in snippets/content. */
fun stripHtml(html: String): String =
    html.replace(Regex("<[^>]+>"), "")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("...", "...")
        .trim()

// --- Lucene search output models -------------------------------------------------------------

@Serializable
data class SearchOutput(
    val query: String,
    val totalHits: Long,
    val results: List<SearchResultOutput>,
)

@Serializable
data class SearchResultOutput(
    val bookId: Long,
    val bookTitle: String,
    val lineId: Long,
    val lineIndex: Int,
    val snippet: String,
    val score: Float,
)

@Serializable
data class BookSearchOutput(
    val prefix: String,
    val bookIds: List<Long>,
)

@Serializable
data class FacetsOutput(
    val query: String,
    val totalHits: Long,
    val bookCounts: Map<String, Int>,
    val categoryCounts: Map<String, Int>,
)

// --- DB-navigation output models -------------------------------------------------------------

@Serializable
data class BookOutput(
    val id: Long,
    val title: String,
    val heRef: String?,
    val categoryId: Long,
    val isBaseBook: Boolean,
    val totalLines: Int,
)

@Serializable
data class BookListOutput(
    val query: String,
    val books: List<BookOutput>,
)

@Serializable
data class LineOutput(
    val lineId: Long,
    val bookId: Long,
    val bookTitle: String?,
    val lineIndex: Int,
    val heRef: String?,
    val content: String,
)

@Serializable
data class LinesOutput(
    val count: Int,
    val lines: List<LineOutput>,
)

@Serializable
data class TocOutput(
    val tocId: Long,
    val parentId: Long?,
    val level: Int,
    val text: String,
    val lineId: Long?,
    val hasChildren: Boolean,
)

@Serializable
data class TocListOutput(
    val bookId: Long,
    val entries: List<TocOutput>,
)

@Serializable
data class LinkOutput(
    val connectionType: String,
    val targetBookId: Long,
    val targetBookTitle: String,
    val targetLineId: Long,
    val targetLineIndex: Int,
    val targetHeRef: String?,
    val text: String,
)

@Serializable
data class LinksOutput(
    val lineId: Long,
    val count: Int,
    val links: List<LinkOutput>,
)
