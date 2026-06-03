package io.github.kdroidfilter.seforimlibrary.catalog

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.CatalogBook
import io.github.kdroidfilter.seforimlibrary.core.models.CatalogCategory
import io.github.kdroidfilter.seforimlibrary.core.models.Category
import io.github.kdroidfilter.seforimlibrary.core.models.PrecomputedCatalog
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.protobuf.ProtoBuf
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Entry point for building only the precomputed catalog (catalog.pb) from an existing database.
 *
 * Usage examples:
 *   ./gradlew :catalog:buildCatalog -PseforimDb=/path/to/seforim.db
 *   ./gradlew :catalog:buildCatalog  # Uses default build/seforim.db
 *
 * The catalog.pb file will be created in the same directory as the database.
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildCatalog")

    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()

    val dbFile = File(dbPath)
    require(dbFile.exists() && dbFile.isFile) { "Database file not found: $dbPath" }

    logger.i { "Building precomputed catalog from database: $dbPath" }

    val jdbcUrl = "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repository = SeforimRepository(dbPath, driver)

    try {
        val catalog = buildCatalogTree(repository, logger)
        val catalogFile = dbFile.parentFile.toPath().resolve("catalog.pb")
        saveCatalog(catalog, catalogFile)

        logger.i { "✓ Catalog created: ${catalogFile.toAbsolutePath()} (${Files.size(catalogFile) / 1024} KB)" }
    } finally {
        repository.close()
    }
}

private suspend fun buildCatalogTree(repository: SeforimRepository, logger: Logger): PrecomputedCatalog {
    val allBooks = repository.getAllBooks()
    val booksByCategory = allBooks.groupBy { it.categoryId }

    logger.i { "Building catalog from ${allBooks.size} books" }

    val rootCategories = repository.getRootCategories().sortedBy { it.order }
    var totalCategories = 0
    val roots = rootCategories.mapNotNull { root ->
        buildCatalogCategoryRecursive(root, booksByCategory, repository)?.also {
            totalCategories += countCategories(it)
        }
    }

    logger.i { "Built catalog with $totalCategories categories and ${allBooks.size} books" }

    return PrecomputedCatalog(
        rootCategories = roots,
        version = 1,
        totalBooks = allBooks.size,
        totalCategories = totalCategories
    )
}

private suspend fun buildCatalogCategoryRecursive(
    category: Category,
    booksByCategory: Map<Long, List<Book>>,
    repository: SeforimRepository
): CatalogCategory? {
    val books = booksByCategory[category.id]
        ?.map { book ->
            CatalogBook(
                id = book.id,
                categoryId = book.categoryId,
                title = book.title,
                order = book.order,
                authors = book.authors.map { it.name },
                totalLines = book.totalLines,
                isBaseBook = book.isBaseBook,
                hasTargumConnection = book.hasTargumConnection,
                hasReferenceConnection = book.hasReferenceConnection,
                hasSourceConnection = book.hasSourceConnection,
                hasCommentaryConnection = book.hasCommentaryConnection,
                hasOtherConnection = book.hasOtherConnection,
                hasAltStructures = book.hasAltStructures
            )
        }
        ?.sortedBy { it.order }
        ?: emptyList()

    val subcategories = repository.getCategoryChildren(category.id)
        .sortedBy { it.order }
        .mapNotNull { child -> buildCatalogCategoryRecursive(child, booksByCategory, repository) }

    // Skip empty categories (no books and no non-empty subcategories)
    if (books.isEmpty() && subcategories.isEmpty()) {
        return null
    }

    return CatalogCategory(
        id = category.id,
        title = category.title,
        level = category.level,
        parentId = category.parentId,
        books = books,
        subcategories = subcategories
    )
}

private fun countCategories(root: CatalogCategory): Int =
    1 + root.subcategories.sumOf { countCategories(it) }

@OptIn(ExperimentalSerializationApi::class)
private fun saveCatalog(catalog: PrecomputedCatalog, outputPath: Path) {
    val bytes = ProtoBuf.encodeToByteArray(PrecomputedCatalog.serializer(), catalog)
    outputPath.toFile().parentFile?.mkdirs()
    Files.write(outputPath, bytes)
}

