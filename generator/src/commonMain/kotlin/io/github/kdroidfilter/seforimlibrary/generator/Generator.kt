package io.github.kdroidfilter.seforimlibrary.generator


import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.jsoup.Jsoup
import org.jsoup.safety.Safelist
import org.slf4j.LoggerFactory
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
    private val logger = LoggerFactory.getLogger(javaClass)
    private val json = Json { ignoreUnknownKeys = true }
    private var nextBookId = 1L // Counter for book IDs

    suspend fun generate() = coroutineScope {
        logger.info("Démarrage de la génération de la base de données...")
        logger.info("Répertoire source: $sourceDirectory")

        try {
            // Charger les métadonnées
            val metadata = loadMetadata()
            logger.info("Métadonnées chargées: ${metadata.size} entrées")

            // Traiter la hiérarchie
            val libraryPath = sourceDirectory.resolve("אוצריא")
            if (!libraryPath.exists()) {
                throw IllegalStateException("Le répertoire אוצריא n'existe pas dans $sourceDirectory")
            }

            processDirectory(libraryPath, null, 0, metadata)

            // Traiter les liens
            processLinks()

            logger.info("Génération terminée avec succès!")
        } catch (e: Exception) {
            logger.error("Erreur lors de la génération", e)
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
                    logger.info("Parsed metadata as List with ${metadataList.size} entries")
                    // Convert list to map using title as key
                    metadataList.associateBy { it.title }
                } catch (e: Exception) {
                    logger.error("Failed to parse metadata.json", e)
                    emptyMap()
                }
            }
        } else {
            logger.warn("Fichier metadata.json introuvable")
            emptyMap()
        }
    }

    private suspend fun processDirectory(
        directory: Path,
        parentCategoryId: Long?,
        level: Int,
        metadata: Map<String, BookMetadata>
    ) {
        Files.list(directory).use { stream ->
            val entries = stream.sorted { a, b ->
                a.fileName.toString().compareTo(b.fileName.toString())
            }.toList()

            for (entry in entries) {
                when {
                    Files.isDirectory(entry) -> {
                        val categoryId = createCategory(entry, parentCategoryId, level)
                        processDirectory(entry, categoryId, level + 1, metadata)
                    }
                    Files.isRegularFile(entry) && entry.extension in listOf("txt", "docx", "pdf") -> {
                        if (parentCategoryId == null) {
                            logger.warn("Livre trouvé sans catégorie: $entry")
                            continue
                        }
                        createAndProcessBook(entry, parentCategoryId, metadata)
                    }
                }
            }
        }
    }

    private suspend fun createCategory(
        path: Path,
        parentId: Long?,
        level: Int
    ): Long {
        val title = path.fileName.toString()
        logger.info("Création de la catégorie: $title (niveau $level)")

        val category = Category(
            parentId = parentId,
            title = title,
            path = path.toString(),
            level = level,
            createdAt = System.currentTimeMillis()
        )

        return repository.insertCategory(category)
    }

    private suspend fun createAndProcessBook(
        path: Path,
        categoryId: Long,
        metadata: Map<String, BookMetadata>
    ) {
        val filename = path.fileName.toString()
        val title = filename.substringBeforeLast('.')
        val meta = metadata[title]

        logger.info("Traitement du livre: $title")

        // Assign a unique ID to this book
        val currentBookId = nextBookId++
        println("DEBUG: Assigning ID $currentBookId to book '$title'")

        val book = Book(
            id = currentBookId, // Set the ID explicitly
            categoryId = categoryId,
            title = title,
            extraTitles = meta?.extraTitles ?: emptyList(),
            author = meta?.author,
            heShortDesc = meta?.heShortDesc,
            pubDate = meta?.pubDate,
            pubPlace = meta?.pubPlace,
            order = meta?.order?.toFloat() ?: 999f,
            topics = extractTopics(path),
            path = path.toString(),
            bookType = if (path.extension == "pdf") BookType.PDF else BookType.TEXT,
            createdAt = System.currentTimeMillis()
        )

        println("DEBUG: Inserting book '${book.title}' with ID: ${book.id} and categoryId: ${book.categoryId}")
        val bookId = repository.insertBook(book)
        println("DEBUG: Book '${book.title}' inserted with ID: $bookId")
        logger.info("Livre créé avec ID: $bookId (ID généré par la base de données)")

        // Traiter le contenu pour les livres texte
        if (book.bookType == BookType.TEXT) {
            processBookContent(path, bookId)
        }
    }

    private suspend fun processBookContent(path: Path, bookId: Long) = coroutineScope {
        println("DEBUG: Processing content for book ID: $bookId")
        logger.info("Traitement du contenu du livre ID: $bookId (ID généré par la base de données)")

        val content = when (path.extension) {
            "txt" -> path.readText(Charsets.UTF_8)
            "docx" -> extractDocxText(path)
            else -> {
                logger.warn("Type de fichier non supporté: ${path.extension}")
                return@coroutineScope
            }
        }

        val lines = content.lines()
        logger.info("Nombre de lignes: ${lines.size}")

        // Insérer les lignes par batch pour performance
        val lineIds = mutableListOf<Pair<Int, Long>>()
        val batchSize = 1000

        for (batch in lines.chunked(batchSize).withIndex()) {
            val startIndex = batch.index * batchSize

            for ((index, line) in batch.value.withIndex()) {
                val lineIndex = startIndex + index
                val plainText = cleanHtml(line)

                val lineId = repository.insertLine(
                    Line(
                        bookId = bookId,
                        lineIndex = lineIndex,
                        content = line,
                        plainText = plainText
                    )
                )

                lineIds.add(lineIndex to lineId)
            }

            if (batch.index % 10 == 0) {
                logger.info("Progression: ${startIndex + batch.value.size}/${lines.size} lignes")
            }
        }

        // Créer la table des matières
        createTableOfContents(bookId, lines, lineIds)

        // Mettre à jour le nombre total de lignes
        repository.updateBookTotalLines(bookId, lines.size)

        logger.info("Contenu traité avec succès pour le livre ID: $bookId (ID généré par la base de données)")
    }

    private fun cleanHtml(html: String): String {
        return Jsoup.clean(html, Safelist.none())
            .trim()
            .replace("\\s+".toRegex(), " ")
    }

    private suspend fun createTableOfContents(
        bookId: Long,
        lines: List<String>,
        lineIds: List<Pair<Int, Long>>
    ) {
        println("DEBUG: Creating table of contents for book ID: $bookId")
        val tocEntries = mutableListOf<TocInfo>()

        // Détecter les entrées TOC
        lines.forEachIndexed { index, line ->
            val level = detectHeaderLevel(line)
            if (level > 0) {
                val text = cleanHtml(line)
                val lineId = lineIds.find { it.first == index }?.second ?: return@forEachIndexed
                tocEntries.add(TocInfo(index, lineId, level, text))
            }
        }

        if (tocEntries.isEmpty()) return

        logger.info("Création de ${tocEntries.size} entrées TOC")

        // Créer la hiérarchie TOC
        val parentStack = mutableMapOf<Int, Long>()

        for ((order, tocInfo) in tocEntries.withIndex()) {
            val parentId = if (tocInfo.level > 1) {
                // Trouver le parent le plus proche
                (tocInfo.level - 1 downTo 1)
                    .mapNotNull { parentStack[it] }
                    .firstOrNull()
            } else null

            val path = buildTocPath(parentStack, tocInfo.level, order)

            val tocEntry = TocEntry(
                bookId = bookId,
                parentId = parentId,
                text = tocInfo.text,
                level = tocInfo.level,
                lineId = tocInfo.lineId,
                lineIndex = tocInfo.lineIndex,
                order = order,
                path = path
            )

            val tocId = repository.insertTocEntry(tocEntry)
            parentStack[tocInfo.level] = tocId

            // Mettre à jour la ligne avec la référence TOC
            repository.updateLineTocEntry(tocInfo.lineId, tocId)
        }
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
            logger.warn("Répertoire links introuvable")
            return
        }

        logger.info("Traitement des liens...")
        var totalLinks = 0

        Files.list(linksDir).use { stream ->
            stream.filter { it.extension == "json" }.forEach { linkFile ->
                runBlocking {
                    val processedLinks = processLinkFile(linkFile)
                    totalLinks += processedLinks
                }
            }
        }

        logger.info("Total de $totalLinks liens traités")
    }

    private suspend fun processLinkFile(linkFile: Path): Int {
        val bookTitle = linkFile.nameWithoutExtension.removeSuffix("_links")

        // Trouver le livre source
        val sourceBook = repository.getBookByTitle(bookTitle)

        if (sourceBook == null) {
            logger.warn("Livre source introuvable pour les liens: $bookTitle")
            return 0
        }

        val links = json.decodeFromString<List<LinkData>>(linkFile.readText())
        var processed = 0

        for (linkData in links) {
            try {
                // Trouver le livre cible
                val targetPath = Paths.get(linkData.path_2)
                val targetTitle = targetPath.fileName.toString().substringBeforeLast('.')

                val targetBook = repository.getBookByTitle(targetTitle)
                    ?: continue

                // Trouver les lignes
                val sourceLine = repository.getLineByIndex(sourceBook.id, linkData.line_index_1)
                val targetLine = repository.getLineByIndex(targetBook.id, linkData.line_index_2)

                if (sourceLine != null && targetLine != null) {
                    val link = Link(
                        sourceBookId = sourceBook.id,
                        targetBookId = targetBook.id,
                        heRef = linkData.heRef_2,
                        sourceLineId = sourceLine.id,
                        targetLineId = targetLine.id,
                        sourceLineIndex = linkData.line_index_1,
                        targetLineIndex = linkData.line_index_2,
                        connectionType = ConnectionType.fromString(linkData.connectionType)
                    )

                    repository.insertLink(link)
                    processed++
                }
            } catch (e: Exception) {
                logger.error("Erreur lors du traitement du lien: ${linkData.heRef_2}", e)
            }
        }

        return processed
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
    private data class TocInfo(
        val lineIndex: Int,
        val lineId: Long,
        val level: Int,
        val text: String
    )

    @kotlinx.serialization.Serializable
    private data class LinkData(
        val heRef_2: String,
        val line_index_1: Int,
        val path_2: String,
        val line_index_2: Int,
        @kotlinx.serialization.SerialName("Conection Type")
        val connectionType: String
    )
}
