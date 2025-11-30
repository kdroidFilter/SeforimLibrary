package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.core.models.*
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.protobuf.ProtoBuf
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Entry point for rebuilding only the precomputed catalog (catalog.pb) from an existing Sefaria database.
 *
 * Usage examples:
 *   ./gradlew :sefariasqlite:buildCatalog -PseforimDb=/path/to/seforim.db
 *   ./gradlew :sefariasqlite:buildCatalog  # Uses default build/seforim.db
 *
 * The catalog.pb file will be created in the same directory as the database.
 */
fun main(args: Array<String>) = runBlocking {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("BuildCatalog")

    // Get database path from arguments or system properties
    val dbPath = args.getOrNull(0)
        ?: System.getProperty("seforimDb")
        ?: System.getenv("SEFORIM_DB")
        ?: Paths.get("build", "seforim.db").toString()

    // Verify database exists
    val dbFile = File(dbPath)
    if (!dbFile.exists() || !dbFile.isFile) {
        logger.e { "Database file not found: $dbPath" }
        logger.e { "Please provide a valid database path." }
        throw IllegalArgumentException("Database file not found: $dbPath")
    }

    logger.i { "Building precomputed catalog from Sefaria database: $dbPath" }

    val jdbcUrl = "jdbc:sqlite:$dbPath"
    val driver = JdbcSqliteDriver(url = jdbcUrl)
    val repository = SeforimRepository(dbPath, driver)

    try {
        // Build catalog tree
        logger.i { "Building catalog tree..." }
        val catalog = buildCatalogTree(repository, logger)

        // Save catalog to file
        val catalogFile = File(dbFile.parentFile, "catalog.pb")
        saveCatalog(catalog, catalogFile.toPath(), logger)

        // Verify the catalog was created
        if (catalogFile.exists()) {
            val sizeKB = catalogFile.length() / 1024
            logger.i { "✓ Catalog successfully created: ${catalogFile.absolutePath}" }
            logger.i { "✓ File size: $sizeKB KB" }
        } else {
            logger.e { "✗ Catalog file was not created" }
            throw IllegalStateException("Catalog file was not created")
        }
    } catch (e: Exception) {
        logger.e(e) { "Error building catalog" }
        throw e
    } finally {
        repository.close()
    }
}

/**
 * Builds the catalog tree structure from the database.
 */
private suspend fun buildCatalogTree(repository: SeforimRepository, logger: Logger): PrecomputedCatalog {
    val allBooks = repository.getAllBooks()
    val booksByCategory = allBooks.groupBy { it.categoryId }

    logger.i { "Building catalog from ${allBooks.size} books" }

    // Start from root categories and build recursively, sorted by order
    val rootCategories = repository.getRootCategories().sortedBy { it.order }
    var totalCategories = 0

    val catalogRoots = rootCategories.map { rootCategory ->
        buildCatalogCategoryRecursive(rootCategory, booksByCategory, repository).also {
            totalCategories += countCategories(it)
        }
    }

    logger.i { "Built catalog with $totalCategories categories and ${allBooks.size} books" }

    return PrecomputedCatalog(
        rootCategories = catalogRoots,
        version = 1,
        totalBooks = allBooks.size,
        totalCategories = totalCategories
    )
}

/**
 * Recursively builds a catalog category with its subcategories and books.
 */
private suspend fun buildCatalogCategoryRecursive(
    category: Category,
    booksByCategory: Map<Long, List<Book>>,
    repository: SeforimRepository
): CatalogCategory {
    // Get books in this category and sort by order
    val catBooks = booksByCategory[category.id]?.map { book ->
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
            hasOtherConnection = book.hasOtherConnection
        )
    }?.sortedBy { it.order } ?: emptyList()

    // Get subcategories and build them recursively, sorted by order
    val subCategories = repository.getCategoryChildren(category.id)
        .sortedBy { it.order }
        .map {
            buildCatalogCategoryRecursive(it, booksByCategory, repository)
        }

    return CatalogCategory(
        id = category.id,
        title = category.title,
        level = category.level,      // Use DB value, not calculated!
        parentId = category.parentId, // Use DB value, not calculated!
        books = catBooks,
        subcategories = subCategories
    )
}

/**
 * Counts the total number of categories in the tree.
 */
private fun countCategories(category: CatalogCategory): Int {
    return 1 + category.subcategories.sumOf { countCategories(it) }
}

/**
 * Saves the catalog to a binary file using ProtoBuf serialization.
 */
private fun saveCatalog(catalog: PrecomputedCatalog, outputPath: Path, logger: Logger) {
    logger.i { "Saving catalog to $outputPath..." }
    val bytes = ProtoBuf.encodeToByteArray(PrecomputedCatalog.serializer(), catalog)
    outputPath.toFile().parentFile?.mkdirs()
    Files.write(outputPath, bytes)
    logger.i { "Catalog saved (${bytes.size / 1024} KB)" }
}
