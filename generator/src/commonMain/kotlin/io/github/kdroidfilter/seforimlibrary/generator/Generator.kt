package io.github.kdroidfilter.seforimlibrary.generator


import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.extension
import kotlin.io.path.nameWithoutExtension
import kotlin.io.path.readText

class DatabaseGenerator(
    private val sourceDirectory: Path,
    private val repository: SeforimRepository
) {

    private val logger = Logger.withTag("DatabaseGenerator")


    private val json = Json {
        ignoreUnknownKeys = true
        coerceInputValues = true
    }
    private var nextBookId = 1L // Counter for book IDs
    private var nextLineId = 1L // Counter for line IDs
    private var nextTocEntryId = 1L // Counter for TOC entry IDs


    suspend fun generate(): Unit = coroutineScope {
        logger.i { "Démarrage de la génération de la base de données..." }
        logger.i { "Répertoire source: $sourceDirectory" }

        try {
            // Charger les métadonnées
            val metadata = loadMetadata()
            logger.i { "Métadonnées chargées: ${metadata.size} entrées" }

            // Traiter la hiérarchie
            val libraryPath = sourceDirectory.resolve("אוצריא")
            if (!libraryPath.exists()) {
                throw IllegalStateException("Le répertoire אוצריא n'existe pas dans $sourceDirectory")
            }

            logger.i { "🚀 Starting to process library directory: $libraryPath" }
            processDirectory(libraryPath, null, 0, metadata)

            // Traiter les liens
            processLinks()

            logger.i { "Génération terminée avec succès!" }
        } catch (e: Exception) {
            logger.e(e) { "Erreur lors de la génération" }
            throw e
        }
    }

    private suspend fun loadMetadata(): Map<String, BookMetadata> {
        val metadataFile = sourceDirectory.resolve("metadata.json")
        return if (metadataFile.exists()) {
            val content = metadataFile.readText()
            try {
                // Try to parse as Map first (original format)
                json.decodeFromString<Map<String, BookMetadata>>(content)
            } catch (e: Exception) {
                // If that fails, try to parse as List and convert to Map
                try {
                    val metadataList = json.decodeFromString<List<BookMetadata>>(content)
                    logger.i { "Parsed metadata as List with ${metadataList.size} entries" }
                    // Convert list to map using title as key
                    metadataList.associateBy { it.title }
                } catch (e: Exception) {
                    logger.i(e) { "Failed to parse metadata.json" }
                    emptyMap()
                }
            }
        } else {
            logger.w { "Fichier metadata.json introuvable" }
            emptyMap()
        }
    }


    private suspend fun processDirectory(
        directory: Path,
        parentCategoryId: Long?,
        level: Int,
        metadata: Map<String, BookMetadata>
    ) {
        logger.i { "=== Processing directory: ${directory.fileName} with parentCategoryId: $parentCategoryId (level: $level) ===" }

        Files.list(directory).use { stream ->
            val entries = stream.sorted { a, b ->
                a.fileName.toString().compareTo(b.fileName.toString())
            }.toList()

            logger.d { "Found ${entries.size} entries in directory ${directory.fileName}" }

            for (entry in entries) {
                when {
                    Files.isDirectory(entry) -> {
                        logger.d { "Processing subdirectory: ${entry.fileName} with parentId: $parentCategoryId" }
                        val categoryId = createCategory(entry, parentCategoryId, level)
                        logger.i { "✅ Created category '${entry.fileName}' with ID: $categoryId (parent: $parentCategoryId)" }
                        processDirectory(entry, categoryId, level + 1, metadata)
                    }

                    Files.isRegularFile(entry) && entry.extension in listOf("txt", "docx", "pdf") -> {
                        if (parentCategoryId == null) {
                            logger.w { "❌ Livre trouvé sans catégorie: $entry" }
                            continue
                        }
                        logger.i { "📚 Processing book ${entry.fileName} with categoryId: $parentCategoryId" }
                        createAndProcessBook(entry, parentCategoryId, metadata)
                    }

                    else -> {
                        logger.d { "Skipping entry: ${entry.fileName} (not a supported file type)" }
                    }
                }
            }
        }
        logger.i { "=== Finished processing directory: ${directory.fileName} ===" }
    }

    private suspend fun createCategory(
        path: Path,
        parentId: Long?,
        level: Int
    ): Long {
        val title = path.fileName.toString()
        logger.i { "🏗️ Création de la catégorie: '$title' (niveau $level, parent: $parentId)" }

        val category = Category(
            parentId = parentId,
            title = title,
            path = path.toString(),
            level = level,
            createdAt = System.currentTimeMillis()
        )

        val insertedId = repository.insertCategory(category)
        logger.i { "✅ Catégorie '$title' créée avec ID: $insertedId" }

        // Vérification supplémentaire
        val insertedCategory = repository.getCategory(insertedId)
        if (insertedCategory == null) {
            logger.e { "❌ ERREUR: Impossible de récupérer la catégorie qui vient d'être insérée (ID: $insertedId)" }
        } else {
            logger.d { "✅ Vérification: catégorie récupérée avec ID: ${insertedCategory.id}, parent: ${insertedCategory.parentId}" }
        }

        return insertedId
    }


    private suspend fun createAndProcessBook(
        path: Path,
        categoryId: Long,
        metadata: Map<String, BookMetadata>
    ) {
        val filename = path.fileName.toString()
        val title = filename.substringBeforeLast('.')
        val meta = metadata[title]

        logger.i { "Traitement du livre: $title avec categoryId: $categoryId" }

        // Assign a unique ID to this book
        val currentBookId = nextBookId++
        logger.d { "Assigning ID $currentBookId to book '$title' with categoryId: $categoryId" }

        val book = Book(
            id = currentBookId,
            categoryId = categoryId,
            title = title,
            extraTitles = meta?.extraTitles ?: emptyList(),
            author = meta?.author,
            heShortDesc = meta?.heShortDesc,
            pubDate = meta?.pubDate,
            pubPlace = meta?.pubPlace,
            order = meta?.order ?: 999f,
            topics = extractTopics(path),
            path = path.toString(),
            bookType = if (path.extension == "pdf") BookType.PDF else BookType.TEXT,
            createdAt = System.currentTimeMillis()
        )

        logger.d { "Inserting book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}" }
        val insertedBookId = repository.insertBook(book)

        // ✅ Vérification importante : s'assurer que l'ID et categoryId sont corrects
        val insertedBook = repository.getBook(insertedBookId)
        if (insertedBook?.categoryId != categoryId) {
            logger.w { "ATTENTION: Book inserted with wrong categoryId! Expected: $categoryId, Got: ${insertedBook?.categoryId}" }
            // Corriger le categoryId si nécessaire
            repository.updateBookCategoryId(insertedBookId, categoryId)
        }

        logger.d { "Book '${book.title}' inserted with ID: $insertedBookId and categoryId: $categoryId" }

        // Traiter le contenu pour les livres texte
        if (book.bookType == BookType.TEXT) {
            processBookContent(path, insertedBookId)
        }
    }

    private suspend fun processBookContent(path: Path, bookId: Long) = coroutineScope {
        logger.d { "Processing content for book ID: $bookId" }
        logger.i { "Traitement du contenu du livre ID: $bookId (ID généré par la base de données)" }

        val content = when (path.extension) {
            "txt" -> path.readText(Charsets.UTF_8)
            "docx" -> extractDocxText(path)
            else -> {
                logger.w { "Type de fichier non supporté: ${path.extension}" }
                return@coroutineScope
            }
        }

        val lines = content.lines()
        logger.i { "Nombre de lignes: ${lines.size}" }

        // Process each line one by one, handling TOC entries as we go
        processLinesWithTocEntries(bookId, lines)

        // Mettre à jour le nombre total de lignes
        repository.updateBookTotalLines(bookId, lines.size)

        logger.i { "Contenu traité avec succès pour le livre ID: $bookId (ID généré par la base de données)" }
    }


    private suspend fun processLinesWithTocEntries(bookId: Long, lines: List<String>) {
        logger.d { "Processing lines and TOC entries together for book ID: $bookId" }

        // Map to track TOC entry parent relationships by level
        val parentStack = mutableMapOf<Int, Long>()
        var tocOrder = 0

        for ((lineIndex, line) in lines.withIndex()) {
            val plainText = cleanHtml(line)
            val level = detectHeaderLevel(line)

            // Check if this line is a TOC entry
            if (level > 0) {
                // This is a TOC entry, create it first (without lineId)
                val parentId = if (level > 1) {
                    // Find the closest parent
                    (level - 1 downTo 1).firstNotNullOfOrNull { parentStack[it] }
                } else null

                val path = buildTocPath(parentStack, level, tocOrder)

                // Assign explicit IDs
                val currentTocEntryId = nextTocEntryId++
                val currentLineId = nextLineId++

                val tocEntry = TocEntry(
                    id = currentTocEntryId, // Set explicit ID
                    bookId = bookId,
                    parentId = parentId,
                    text = plainText,
                    level = level,
                    lineId = null, // Will be set after line is inserted
                    lineIndex = lineIndex,
                    order = tocOrder++,
                    path = path
                )

                logger.d { "Inserting TOC entry with ID: $currentTocEntryId, bookId: $bookId, lineIndex: $lineIndex" }
                val tocEntryId = repository.insertTocEntry(tocEntry)
                logger.d { "TOC entry inserted with ID: $tocEntryId" }

                // Update parent stack
                parentStack[level] = tocEntryId

                // Now insert the line
                logger.d { "Inserting line with ID: $currentLineId, bookId: $bookId, lineIndex: $lineIndex" }
                val lineId = repository.insertLine(
                    Line(
                        id = currentLineId, // Set explicit ID
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        plainText = plainText
                    )
                )
                logger.d { "Line inserted with ID: $lineId" }

                // Update the TOC entry with the lineId
                logger.d { "Updating TOC entry $tocEntryId with lineId: $lineId" }
                repository.updateTocEntryLineId(tocEntryId, lineId)

                // Update the line with the tocEntryId
                logger.d { "Updating line $lineId with tocEntryId: $tocEntryId" }
                repository.updateLineTocEntry(lineId, tocEntryId)
            } else {
                // This is a regular line, just insert it
                val currentLineId = nextLineId++
                logger.d { "Inserting regular line with ID: $currentLineId, bookId: $bookId, lineIndex: $lineIndex" }
                repository.insertLine(
                    Line(
                        id = currentLineId, // Set explicit ID
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        plainText = plainText
                    )
                )
            }

            // Log progress
            if (lineIndex % 1000 == 0) {
                logger.i { "Progression: $lineIndex/${lines.size} lignes" }
            }
        }
    }


    private fun cleanHtml(html: String): String {
        return Jsoup.clean(html, Safelist.none())
            .trim()
            .replace("\\s+".toRegex(), " ")
    }


    private fun detectHeaderLevel(line: String): Int {
        return when {
            line.startsWith("<h1", ignoreCase = true) -> 1
            line.startsWith("<h2", ignoreCase = true) -> 2
            line.startsWith("<h3", ignoreCase = true) -> 3
            line.startsWith("<h4", ignoreCase = true) -> 4
            line.startsWith("<h5", ignoreCase = true) -> 5
            line.startsWith("<h6", ignoreCase = true) -> 6
            else -> 0
        }
    }

    private fun buildTocPath(parentStack: Map<Int, Long>, level: Int, order: Int): String {
        val path = mutableListOf<Int>()
        for (i in 1..level) {
            if (i == level) {
                path.add(order + 1)
            } else {
                parentStack[i]?.let { path.add(it.toInt()) }
            }
        }
        return path.joinToString(".")
    }

    private suspend fun processLinks() {
        val linksDir = sourceDirectory.resolve("links")
        if (!linksDir.exists()) {
            logger.w { "Répertoire links introuvable" }
            return
        }

        // Count links before processing
        val linksBefore = repository.countLinks()
        logger.d { "Links in database before processing: $linksBefore" }

        logger.i { "Traitement des liens..." }
        var totalLinks = 0

        Files.list(linksDir).use { stream ->
            stream.filter { it.extension == "json" }.forEach { linkFile ->
                runBlocking {
                    val processedLinks = processLinkFile(linkFile)
                    totalLinks += processedLinks
                    logger.d { "Processed $processedLinks links from ${linkFile.fileName}, total so far: $totalLinks" }
                }
            }
        }

        // Count links after processing
        val linksAfter = repository.countLinks()
        logger.d { "Links in database after processing: $linksAfter" }
        logger.d { "Added ${linksAfter - linksBefore} links to the database" }

        logger.i { "Total de $totalLinks liens traités" }
    }

    private suspend fun processLinkFile(linkFile: Path): Int {
        val bookTitle = linkFile.nameWithoutExtension.removeSuffix("_links")
        logger.d { "Processing link file for book: $bookTitle" }

        // Trouver le livre source
        val sourceBook = repository.getBookByTitle(bookTitle)

        if (sourceBook == null) {
            logger.w { "Livre source introuvable pour les liens: $bookTitle" }
            return 0
        }
        logger.d { "Found source book with ID: ${sourceBook.id}" }

        try {
            val content = linkFile.readText()
            logger.d { "Link file content length: ${content.length}" }
            val links = json.decodeFromString<List<LinkData>>(content)
            logger.d { "Decoded ${links.size} links from file" }
            var processed = 0

            for ((index, linkData) in links.withIndex()) {
                try {
                    // Trouver le livre cible
                    // Handle paths with backslashes
                    val path = linkData.path_2
                    val targetTitle = if (path.contains('\\')) {
                        // Extract the last component of a backslash-separated path
                        val lastComponent = path.split('\\').last()
                        // Remove file extension if present
                        lastComponent.substringBeforeLast('.', lastComponent)
                    } else {
                        // Use the standard path handling for forward slash paths
                        val targetPath = Paths.get(path)
                        targetPath.fileName.toString().substringBeforeLast('.')
                    }
                    logger.d { "Link ${index + 1}/${links.size} - Target book title: $targetTitle" }

                    // Try to find the target book
                    val targetBook = repository.getBookByTitle(targetTitle)
                    if (targetBook == null) {
                        // Enhanced logging for debugging
                        logger.i { "Link ${index + 1}/${links.size} - Target book not found: $targetTitle" }
                        logger.i { "Original path: ${linkData.path_2}" }
                        continue
                    }
                    logger.d { "Using target book with ID: ${targetBook.id}" }

                    // Trouver les lignes
                    logger.d { "Looking for source line at index: ${linkData.line_index_1.toInt()} in book ${sourceBook.id}" }

                    // Try to find the source line
                    val sourceLine = repository.getLineByIndex(sourceBook.id, linkData.line_index_1.toInt())
                    if (sourceLine == null) {
                        logger.d { "Source line not found at index: ${linkData.line_index_1.toInt()}, skipping this link but continuing with others" }
                        continue
                    }
                    logger.d { "Using source line with ID: ${sourceLine.id}" }

                    logger.d { "Looking for target line at index: ${linkData.line_index_2.toInt()} in book ${targetBook.id}" }

                    // Try to find the target line
                    val targetLine = repository.getLineByIndex(targetBook.id, linkData.line_index_2.toInt())
                    if (targetLine == null) {
                        logger.d { "Target line not found at index: ${linkData.line_index_2.toInt()}, skipping this link but continuing with others" }
                        continue
                    }
                    logger.d { "Using target line with ID: ${targetLine.id}" }

                    val link = Link(
                        sourceBookId = sourceBook.id,
                        targetBookId = targetBook.id,
                        heRef = linkData.heRef_2,
                        sourceLineId = sourceLine.id,
                        targetLineId = targetLine.id,
                        sourceLineIndex = linkData.line_index_1.toInt(),
                        targetLineIndex = linkData.line_index_2.toInt(),
                        connectionType = ConnectionType.fromString(linkData.connectionType)
                    )

                    logger.d { "Inserting link from book ${sourceBook.id} to book ${targetBook.id}" }
                    val linkId = repository.insertLink(link)
                    logger.d { "Link inserted with ID: $linkId" }
                    processed++
                } catch (e: Exception) {
                    logger.e(e) { "Erreur lors du traitement du lien: ${linkData.heRef_2}" }
                    logger.d { "Error processing link: ${e.message}" }
                }
            }
            logger.d { "Processed $processed links out of ${links.size}" }
            return processed
        } catch (e: Exception) {
            logger.e(e) { "Erreur lors du traitement du fichier de liens: ${linkFile.fileName}" }
            logger.d { "Error processing link file: ${e.message}" }
            return 0
        }
    }

    private fun extractTopics(path: Path): String {
        // Extraire les topics du chemin
        val parts = path.toString().split(File.separator)
        return parts.dropLast(1).takeLast(2).joinToString(", ")
    }

    private fun extractDocxText(path: Path): String {
        // TODO: Implémenter l'extraction DOCX avec Apache POI
        return ""
    }

    // Classes internes

    @Serializable
    private data class LinkData(
        val heRef_2: String,
        val line_index_1: Double,
        val path_2: String,
        val line_index_2: Double,
        @SerialName("Conection Type")
        val connectionType: String = ""
    )
}
