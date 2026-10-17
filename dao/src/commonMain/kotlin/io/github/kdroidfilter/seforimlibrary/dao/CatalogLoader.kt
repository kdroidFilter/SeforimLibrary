package io.github.kdroidfilter.seforimlibrary.dao

import io.github.kdroidfilter.seforimlibrary.core.models.PrecomputedCatalog
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.protobuf.ProtoBuf

/**
 * Loader for the precomputed catalog tree.
 * Provides fast loading of the entire book/category tree from a binary file.
 */
object CatalogLoader {
    private const val CATALOG_FILE = "catalog.pb"

    /**
     * Loads the precomputed catalog from a file next to the database.
     *
     * @param dbPath Path to the database file. The catalog file should be in the same directory.
     * @return The precomputed catalog, or null if the file doesn't exist or can't be loaded
     */
    fun loadCatalog(dbPath: String): PrecomputedCatalog? = load(catalogBeside(dbPath))

    /**
     * Loads the precomputed catalog from a specific path.
     *
     * @param catalogPath Direct path to the catalog.pb file
     * @return The precomputed catalog, or null if the file doesn't exist or can't be loaded
     */
    fun loadCatalogFromPath(catalogPath: String): PrecomputedCatalog? = load(Path(catalogPath))

    /**
     * Checks if a catalog file exists next to the database.
     *
     * @param dbPath Path to the database file
     * @return true if catalog.pb exists, false otherwise
     */
    fun catalogExists(dbPath: String): Boolean = SystemFileSystem.exists(catalogBeside(dbPath))

    private fun catalogBeside(dbPath: String): Path {
        val parent = Path(dbPath).parent
        return if (parent != null) Path(parent, CATALOG_FILE) else Path(CATALOG_FILE)
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun load(catalogFile: Path): PrecomputedCatalog? =
        try {
            if (!SystemFileSystem.exists(catalogFile)) {
                println("Catalog file not found: $catalogFile")
                null
            } else {
                val bytes = SystemFileSystem.source(catalogFile).buffered().use { it.readByteArray() }
                ProtoBuf.decodeFromByteArray(PrecomputedCatalog.serializer(), bytes).also {
                    println("Loaded precomputed catalog: ${it.totalCategories} categories, ${it.totalBooks} books")
                }
            }
        } catch (e: Exception) {
            println("Failed to load precomputed catalog: ${e.message}")
            e.printStackTrace()
            null
        }
}
