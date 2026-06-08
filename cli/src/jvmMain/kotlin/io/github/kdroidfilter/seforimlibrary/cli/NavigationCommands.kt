package io.github.kdroidfilter.seforimlibrary.cli

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString

/** Strips HTML tags from [content] when [strip] is true; otherwise returns it unchanged. */
private fun maybeStrip(content: String, strip: Boolean): String = if (strip) stripHtml(content) else content

/**
 * `book <substring>` — find books whose title contains the substring (Recipe A).
 * Unlike `books` (Lucene prefix → ids only) this returns full titles + metadata from the DB.
 */
fun runBook(config: CliConfig): Int {
    if (config.query.isBlank()) {
        System.err.println("Error: Book title substring required")
        return 1
    }
    val repository = createRepository(config)
    try {
        val books = runBlocking { repository.findBooksByTitleLike("%${config.query}%", config.limit) }
        val out = books.map { BookOutput(it.id, it.title, it.heRef, it.categoryId, it.isBaseBook, it.totalLines) }
        if (config.jsonOutput) {
            println(jsonPretty.encodeToString(BookListOutput(config.query, out)))
        } else {
            println("Books matching '${config.query}': ${out.size}")
            for (b in out) {
                println("  [${b.id}] ${b.title}${if (b.isBaseBook) " (base)" else ""}  heRef=${b.heRef ?: "-"}")
            }
        }
    } finally {
        repository.close()
    }
    return 0
}

/**
 * `line <lineId>` — full content + heRef of a single line, with its book title (Recipe E).
 * This is how you turn a search/links hit into a quotable source.
 */
fun runLine(config: CliConfig): Int {
    val lineId = config.query.toLongOrNull()
    if (lineId == null) {
        System.err.println("Error: a numeric <lineId> is required")
        return 1
    }
    val repository = createRepository(config)
    try {
        val line = runBlocking { repository.getLine(lineId) }
        if (line == null) {
            if (config.jsonOutput) println("null") else println("No line with id $lineId")
            return 0
        }
        val bookTitle = runBlocking { repository.getBookCore(line.bookId)?.title }
        val out = LineOutput(
            lineId = line.id,
            bookId = line.bookId,
            bookTitle = bookTitle,
            lineIndex = line.lineIndex,
            heRef = line.heRef,
            content = maybeStrip(line.content, config.stripHtml),
        )
        if (config.jsonOutput) {
            println(jsonPretty.encodeToString(out))
        } else {
            println("[${out.lineId}] ${out.bookTitle ?: "book ${out.bookId}"} (line ${out.lineIndex})")
            println("heRef: ${out.heRef ?: "-"}")
            println(out.content)
        }
    } finally {
        repository.close()
    }
    return 0
}

/**
 * `lines <bookId> --from <i> --to <j>` or `lines --toc <tocEntryId>` — a contiguous block of
 * lines (Recipe B). Use `--toc` to get exactly the lines of a TOC section (e.g. one siman).
 */
fun runLines(config: CliConfig): Int {
    val repository = createRepository(config)
    try {
        val lines = runBlocking {
            val tocId = config.tocId
            if (tocId != null) {
                val ids = repository.getLineIdsForTocEntry(tocId)
                repository.getLinesByIds(ids).sortedBy { it.lineIndex }
            } else {
                val bookId = config.query.toLongOrNull()
                    ?: error("a numeric <bookId> is required (or use --toc <tocEntryId>)")
                val from = config.fromIndex ?: 0
                val to = config.toIndex ?: (from + config.limit - 1)
                repository.getLines(bookId, from, to)
            }
        }
        val out = lines.map {
            LineOutput(it.id, it.bookId, null, it.lineIndex, it.heRef, maybeStrip(it.content, config.stripHtml))
        }
        if (config.jsonOutput) {
            println(jsonPretty.encodeToString(LinesOutput(out.size, out)))
        } else {
            println("Lines: ${out.size}")
            for (l in out) {
                println("\n[${l.lineId}] (line ${l.lineIndex}) heRef=${l.heRef ?: "-"}")
                println(l.content)
            }
        }
    } finally {
        repository.close()
    }
    return 0
}

/**
 * `toc <bookId> [--text <label>]` — the table of contents of a book (Recipe B). Each entry exposes
 * its `lineId` (start of the section) so you can pivot to `line`/`lines`. `--text` filters to entries
 * whose label contains the given string (e.g. `--text "סימן תנא"`).
 */
fun runToc(config: CliConfig): Int {
    val bookId = config.query.toLongOrNull()
    if (bookId == null) {
        System.err.println("Error: a numeric <bookId> is required")
        return 1
    }
    val repository = createRepository(config)
    try {
        var entries = runBlocking { repository.getBookToc(bookId) }
        config.tocText?.let { needle -> entries = entries.filter { it.text.contains(needle) } }
        val out = entries.map { TocOutput(it.id, it.parentId, it.level, it.text, it.lineId, it.hasChildren) }
        if (config.jsonOutput) {
            println(jsonPretty.encodeToString(TocListOutput(bookId, out)))
        } else {
            println("TOC entries for book $bookId: ${out.size}")
            for (e in out) {
                val indent = "  ".repeat(e.level.coerceAtLeast(0))
                println("$indent[toc ${e.tocId}] ${e.text}  → lineId=${e.lineId ?: "-"}")
            }
        }
    } finally {
        repository.close()
    }
    return 0
}

/**
 * `links <lineId>` — EVERY link touching the line, in BOTH directions (Recipe C, the killer
 * feature). Forward links are the commentaries/targumim/references on the line; reverse links
 * (connectionType SOURCE) are the base texts this line comments on. Pass `--forward-only` to skip
 * the reverse direction. Each entry carries the target line's heRef so you can cite it directly,
 * and its targetLineId so you can recurse with another `links` call.
 */
fun runLinks(config: CliConfig): Int {
    val lineId = config.query.toLongOrNull()
    if (lineId == null) {
        System.err.println("Error: a numeric <lineId> is required")
        return 1
    }
    val repository = createRepository(config)
    try {
        val commentaries = runBlocking {
            repository.getCommentariesForLines(listOf(lineId), includeSources = !config.forwardOnly)
        }
        val heRefById = runBlocking {
            repository.getLinesByIds(commentaries.map { it.link.targetLineId }.distinct())
                .associate { it.id to it.heRef }
        }
        val out = commentaries.map {
            LinkOutput(
                connectionType = it.link.connectionType.name,
                targetBookId = it.link.targetBookId,
                targetBookTitle = it.targetBookTitle,
                targetLineId = it.link.targetLineId,
                targetLineIndex = it.link.targetLineIndex,
                targetHeRef = heRefById[it.link.targetLineId],
                text = maybeStrip(it.targetText, config.stripHtml),
            )
        }
        if (config.jsonOutput) {
            println(jsonPretty.encodeToString(LinksOutput(lineId, out.size, out)))
        } else {
            println("Links for line $lineId: ${out.size} (both directions${if (config.forwardOnly) " disabled" else ""})")
            for (l in out) {
                println("\n[${l.connectionType}] ${l.targetBookTitle} → lineId=${l.targetLineId} heRef=${l.targetHeRef ?: "-"}")
                println(l.text)
            }
        }
    } finally {
        repository.close()
    }
    return 0
}
