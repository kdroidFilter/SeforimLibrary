package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import io.github.kdroidfilter.seforimlibrary.db.SeforimDb
import kotlinx.coroutines.runBlocking
import java.io.File

/**
 * Diagnostic tool to check links in Sefaria database for Shulchan Aruch
 */
fun main(args: Array<String>) {
    val dbPath = args.getOrNull(0) ?: "build/sefaria.db"

    println("=== Sefaria Database Diagnostics ===")
    println("Database: $dbPath")
    println()

    val dbFile = File(dbPath)
    if (!dbFile.exists()) {
        println("ERROR: Database file does not exist at $dbPath")
        return
    }

    val driver = JdbcSqliteDriver("jdbc:sqlite:$dbPath")
    val repository = SeforimRepository(dbPath, driver)

    runBlocking {
        try {
            // 1. Check if Shulchan Aruch exists
            println("1. Checking for Shulchan Aruch books...")
            val allBooks = repository.getAllBooks()
            val shulchanAruchBooks = allBooks.filter {
                it.title.contains("שולחן ערוך") ||
                it.title.contains("Shulchan Aruch", ignoreCase = true)
            }

            if (shulchanAruchBooks.isEmpty()) {
                println("   ❌ No Shulchan Aruch books found in database!")
                println()
            } else {
                println("   ✓ Found ${shulchanAruchBooks.size} Shulchan Aruch book(s):")
                shulchanAruchBooks.forEach { book ->
                    println("     - ID: ${book.id}, Title: ${book.title}, isBaseBook: ${book.isBaseBook}")
                }
                println()

                // 2. Check lines for each Shulchan Aruch book
                println("2. Checking lines for Shulchan Aruch books...")
                shulchanAruchBooks.forEach { book ->
                    val lineCount = book.totalLines
                    println("   Book '${book.title}' has $lineCount lines")
                }
                println()

                // 3. Check links for Shulchan Aruch
                println("3. Checking links for Shulchan Aruch...")
                shulchanAruchBooks.forEach { book ->
                    val sourceCount = repository.countLinksBySourceBook(book.id)
                    val targetCount = repository.countLinksByTargetBook(book.id)

                    println("   Book '${book.title}':")
                    println("     - As source (base text with commentaries): $sourceCount links")
                    println("     - As target (commentary on other texts): $targetCount links")

                    if (sourceCount > 0) {
                        // Get sample links
                        val sampleLinks = repository.getLinksBySourceBook(book.id).take(5)
                        println("     - Sample source links:")
                        sampleLinks.forEach { link ->
                            val targetBook = repository.getBook(link.targetBookId)
                            println("       → ${targetBook?.title} (type: ${link.connectionType})")
                        }
                    }

                    println()
                }

                // 4. Check for common commentaries on Shulchan Aruch
                println("4. Checking for common Shulchan Aruch commentaries...")
                val commonCommentaries = listOf(
                    "משנה ברורה",
                    "באר הגולה",
                    "טורי זהב",
                    "מגן אברהם",
                    "ביאור הגר\"א",
                    "Mishnah Berurah",
                    "Be'er HaGolah",
                    "Turei Zahav",
                    "Magen Avraham",
                    "Bi'ur HaGra"
                )

                val foundCommentaries = allBooks.filter { book ->
                    commonCommentaries.any { commentary ->
                        book.title.contains(commentary, ignoreCase = true)
                    }
                }

                if (foundCommentaries.isEmpty()) {
                    println("   ❌ No common Shulchan Aruch commentaries found!")
                } else {
                    println("   ✓ Found ${foundCommentaries.size} commentary book(s):")
                    foundCommentaries.forEach { book ->
                        println("     - ID: ${book.id}, Title: ${book.title}, isBaseBook: ${book.isBaseBook}")

                        // Check if it has links to Shulchan Aruch
                        shulchanAruchBooks.forEach { saBook ->
                            val linkCount = repository.countLinksBetweenBooks(book.id, saBook.id)
                            if (linkCount > 0) {
                                println("       → $linkCount links to '${saBook.title}'")
                            }
                        }
                    }
                }
                println()

                // 5. Sample a line from Shulchan Aruch BASE TEXT and check its commentaries
                println("5. Sampling a line from Shulchan Aruch base text to check commentaries...")
                // Choose Orach Chayim specifically (not the introduction)
                val firstBaseBook = shulchanAruchBooks.firstOrNull {
                    it.isBaseBook && (it.title.contains("אורח חיים") || it.title.contains("Orach Chayim"))
                }
                if (firstBaseBook != null) {
                    println("   Using book: ${firstBaseBook.title} (ID: ${firstBaseBook.id})")
                    val sampleLine = repository.getLines(firstBaseBook.id, 0, 0).firstOrNull()
                    if (sampleLine != null) {
                        println("   Sample line ID: ${sampleLine.id}")
                        println("   Content: ${sampleLine.content.take(100)}...")

                        val commentaries = repository.getCommentariesForLines(listOf(sampleLine.id))
                        println("   Found ${commentaries.size} commentaries for this line:")
                        commentaries.take(10).forEach { comm ->
                            println("     - ${comm.targetBookTitle} (type: ${comm.link.connectionType})")
                        }
                    } else {
                        println("   ⚠ Could not retrieve first line")
                    }
                } else {
                    println("   ⚠ Could not find a base Shulchan Aruch book")
                }
                println()

                // 6. Check connection types
                println("6. Checking connection types in database...")
                val connectionTypes = repository.getAllConnectionTypes()
                println("   Available connection types:")
                connectionTypes.forEach { type ->
                    println("     - ${type.name}")
                }
                println()

                // 7. Summary statistics
                println("7. Database summary:")
                println("   - Total books: ${allBooks.size}")
                println("   - Total links: ${repository.countAllLinks()}")
                val baseBooks = allBooks.filter { it.isBaseBook }
                val commentaryBooks = allBooks.filter { !it.isBaseBook }
                println("   - Base books: ${baseBooks.size}")
                println("   - Commentary books: ${commentaryBooks.size}")
            }

        } catch (e: Exception) {
            println("ERROR: ${e.message}")
            e.printStackTrace()
        } finally {
            repository.close()
        }
    }

    println()
    println("=== Diagnostics Complete ===")
}
