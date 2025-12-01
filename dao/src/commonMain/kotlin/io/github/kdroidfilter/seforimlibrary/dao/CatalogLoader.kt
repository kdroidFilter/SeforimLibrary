package io.github.kdroidfilter.seforimlibrary.dao

import io.github.kdroidfilter.seforimlibrary.core.models.PrecomputedCatalog
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.protobuf.ProtoBuf
import java.io.File
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readBytes

/**
 * Loader for the precomputed catalog tree.
 * Provides fast loading of the entire book/category tree from a binary file.
 */
object CatalogLoader {

    /**
     * Loads the precomputed catalog from a file next to the database.
     *
     * @param dbPath Path to the database file. The catalog file should be in the same directory.
     * @return The precomputed catalog, or null if the file doesn't exist or can't be loaded
     */
    @OptIn(ExperimentalSerializationApi::class)
    fun loadCatalog(dbPath: String): PrecomputedCatalog? {
        return try {
            val dbFile = File(dbPath)
            val catalogFile = File(dbFile.parentFile, "catalog.pb")

            if (!catalogFile.exists()) {
                println("Catalog file not found: ${catalogFile.absolutePath}")
                return null
            }

            val bytes = catalogFile.readBytes()
            val catalog = ProtoBuf.decodeFromByteArray(PrecomputedCatalog.serializer(), bytes)

            println("Loaded precomputed catalog: ${catalog.totalCategories} categories, ${catalog.totalBooks} books")
            catalog
        } catch (e: Exception) {
            println("Failed to load precomputed catalog: ${e.message}")
            e.printStackTrace()
            null
        }
    }

    /**
     * Loads the precomputed catalog from a specific Path.
     *
     * @param catalogPath Direct path to the catalog.pb file
     * @return The precomputed catalog, or null if the file doesn't exist or can't be loaded
     */
    fun loadCatalogFromPath(catalogPath: Path): PrecomputedCatalog? {
        return try {
            if (!catalogPath.exists()) {
                println("Catalog file not found: $catalogPath")
                return null
            }

            val bytes = catalogPath.readBytes()
            val catalog = ProtoBuf.decodeFromByteArray(PrecomputedCatalog.serializer(), bytes)

            println("Loaded precomputed catalog: ${catalog.totalCategories} categories, ${catalog.totalBooks} books")
            catalog
        } catch (e: Exception) {
            println("Failed to load precomputed catalog: ${e.message}")
            e.printStackTrace()
            null
        }
    }

    /**
     * Checks if a catalog file exists next to the database.
     *
     * @param dbPath Path to the database file
     * @return true if catalog.pb exists, false otherwise
     */
    fun catalogExists(dbPath: String): Boolean {
        val dbFile = File(dbPath)
        val catalogFile = File(dbFile.parentFile, "catalog.pb")
        return catalogFile.exists()
    }
}
