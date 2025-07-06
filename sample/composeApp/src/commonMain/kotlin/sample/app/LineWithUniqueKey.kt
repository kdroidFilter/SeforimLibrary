package sample.app

import io.github.kdroidfilter.seforimlibrary.core.models.Line
import kotlin.random.Random

/**
 * A wrapper class for Line that provides a unique key for use in LazyColumn.
 * This is used to ensure unique keys in LazyColumn even when the same Line object appears multiple times.
 */
data class LineWithUniqueKey(val line: Line) {
    // Generate a truly unique key for this line by combining its ID with a UUID-like string
    // This ensures uniqueness even if the same Line object appears multiple times
    val uniqueKey: String = "${line.id}_${System.nanoTime()}_${Random.nextInt()}"

    // Delegate properties to the wrapped Line object
    val id: Long get() = line.id
    val bookId: Long get() = line.bookId
    val lineIndex: Int get() = line.lineIndex
    val content: String get() = line.content
    val plainText: String get() = line.plainText
}

/**
 * Extension function to convert a Line to a LineWithUniqueKey.
 */
fun Line.withUniqueKey(): LineWithUniqueKey = LineWithUniqueKey(this)

/**
 * Extension function to convert a list of Line objects to a list of LineWithUniqueKey objects.
 */
fun List<Line>.withUniqueKeys(): List<LineWithUniqueKey> = map { it.withUniqueKey() }
