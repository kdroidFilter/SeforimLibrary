package io.github.kdroidfilter.seforimlibrary.core.models

/**
 * Extension functions for working with PrecomputedCatalog
 */

/**
 * Extracts all root categories from the precomputed catalog.
 */
fun PrecomputedCatalog.extractRootCategories(): List<Category> {
    return rootCategories.map { it.toCategory() }
}

/**
 * Extracts all books from the precomputed catalog into a flat set.
 */
fun PrecomputedCatalog.extractAllBooks(): Set<Book> {
    val books = mutableSetOf<Book>()

    fun traverseCategory(cat: CatalogCategory) {
        // Add books from this category
        books.addAll(cat.books.map { it.toBook() })
        // Traverse subcategories
        cat.subcategories.forEach { traverseCategory(it) }
    }

    rootCategories.forEach { traverseCategory(it) }
    return books
}

/**
 * Extracts the category children map from the precomputed catalog.
 * Maps category ID to its direct children.
 */
fun PrecomputedCatalog.extractCategoryChildren(): Map<Long, List<Category>> {
    val childrenMap = mutableMapOf<Long, List<Category>>()

    fun traverseCategory(cat: CatalogCategory) {
        if (cat.subcategories.isNotEmpty()) {
            childrenMap[cat.id] = cat.subcategories.map { it.toCategory() }
        }
        cat.subcategories.forEach { traverseCategory(it) }
    }

    rootCategories.forEach { traverseCategory(it) }
    return childrenMap
}

/**
 * Converts a CatalogCategory to a Category model.
 */
fun CatalogCategory.toCategory(): Category {
    return Category(
        id = id,
        parentId = parentId,
        title = title,
        level = level
    )
}

/**
 * Converts a CatalogBook to a Book model.
 */
fun CatalogBook.toBook(): Book {
    return Book(
        id = id,
        categoryId = categoryId,
        sourceId = 0L, // Will need to be populated separately if needed
        title = title,
        authors = authors.map { Author(name = it) },
        pubPlaces = emptyList(),
        pubDates = emptyList(),
        heShortDesc = null,
        notesContent = null,
        order = order,
        topics = emptyList(),
        totalLines = totalLines,
        isBaseBook = isBaseBook,
        hasTargumConnection = hasTargumConnection,
        hasReferenceConnection = hasReferenceConnection,
        hasCommentaryConnection = hasCommentaryConnection,
        hasOtherConnection = hasOtherConnection,
        hasAltStructures = hasAltStructures
    )
}

/**
 * Finds a category by ID in the catalog tree.
 */
fun PrecomputedCatalog.findCategoryById(categoryId: Long): CatalogCategory? {
    fun searchInCategory(cat: CatalogCategory): CatalogCategory? {
        if (cat.id == categoryId) return cat
        for (subcategory in cat.subcategories) {
            val found = searchInCategory(subcategory)
            if (found != null) return found
        }
        return null
    }

    for (rootCat in rootCategories) {
        val found = searchInCategory(rootCat)
        if (found != null) return found
    }
    return null
}

/**
 * Finds a book by ID in the catalog tree.
 */
fun PrecomputedCatalog.findBookById(bookId: Long): CatalogBook? {
    fun searchInCategory(cat: CatalogCategory): CatalogBook? {
        val book = cat.books.find { it.id == bookId }
        if (book != null) return book

        for (subcategory in cat.subcategories) {
            val found = searchInCategory(subcategory)
            if (found != null) return found
        }
        return null
    }

    for (rootCat in rootCategories) {
        val found = searchInCategory(rootCat)
        if (found != null) return found
    }
    return null
}

/**
 * Gets all books in a specific category (non-recursive).
 */
fun PrecomputedCatalog.getBooksInCategory(categoryId: Long): List<CatalogBook> {
    val category = findCategoryById(categoryId) ?: return emptyList()
    return category.books
}

/**
 * Gets the path from root to a given category.
 */
fun PrecomputedCatalog.getCategoryPath(categoryId: Long): List<CatalogCategory> {
    val path = mutableListOf<CatalogCategory>()

    fun findPathInCategory(cat: CatalogCategory, target: Long): Boolean {
        if (cat.id == target) {
            path.add(cat)
            return true
        }

        for (subcategory in cat.subcategories) {
            if (findPathInCategory(subcategory, target)) {
                path.add(0, cat) // Add to front to build path from root
                return true
            }
        }
        return false
    }

    for (rootCat in rootCategories) {
        if (findPathInCategory(rootCat, categoryId)) {
            break
        }
    }

    return path
}
