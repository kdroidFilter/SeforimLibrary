package io.github.kdroidfilter.seforimlibrary.dao

import io.github.kdroidfilter.seforimlibrary.core.models.PrecomputedCatalog
import io.github.vinceglb.filekit.PlatformFile
import io.github.vinceglb.filekit.exists
import io.github.vinceglb.filekit.readBytes
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.protobuf.ProtoBuf

/**
 * Loader for the precomputed catalog tree.
 * Provides fast loading of the entire book/category tree from a binary file.
 */
object CatalogLoader {

    /**
     * Loads the precomputed catalog from a `catalog.pb` file next to the database, using FileKit
     * for cross-platform file access.
     *
     * @param dbPath Path to the database file. The catalog file should be in the same directory.
     * @return The precomputed catalog, or null if the file doesn't exist or can't be loaded
     */
    @OptIn(ExperimentalSerializationApi::class)
    suspend fun loadCatalog(dbPath: String): PrecomputedCatalog? {
        return try {
            // catalog.pb sits next to the db file; handle both '/' and '\' separators.
            val sep = maxOf(dbPath.lastIndexOf('/'), dbPath.lastIndexOf('\\'))
            val catalogPath = if (sep < 0) "catalog.pb" else dbPath.substring(0, sep + 1) + "catalog.pb"

            val file = PlatformFile(catalogPath)
            if (!file.exists()) {
                println("Catalog file not found: $catalogPath")
                return null
            }

            val bytes = file.readBytes()
            val catalog = ProtoBuf.decodeFromByteArray(PrecomputedCatalog.serializer(), bytes)

            println("Loaded precomputed catalog: ${catalog.totalCategories} categories, ${catalog.totalBooks} books")
            catalog
        } catch (e: Exception) {
            println("Failed to load precomputed catalog: ${e.message}")
            e.printStackTrace()
            null
        }
    }
}
