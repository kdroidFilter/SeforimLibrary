package sample.app

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.KeyboardArrowRight
import androidx.compose.material.icons.filled.Book
import androidx.compose.material.icons.filled.Folder
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import io.github.kdroidfilter.seforimlibrary.core.models.Book
import io.github.kdroidfilter.seforimlibrary.core.models.Category

@Composable
fun CategoryBookTree(
    rootCategories: List<Category>,
    expandedCategories: Set<Long>,
    categoryChildren: Map<Long, List<Category>>,
    booksInCategory: Set<Book>,
    selectedCategory: Category?,
    selectedBook: Book?,
    onCategoryClick: (Category) -> Unit,
    onBookClick: (Book) -> Unit
) {
    LazyColumn {
        items(rootCategories) { category ->
            CategoryTreeItem(
                category = category,
                level = 0,
                expandedCategories = expandedCategories,
                categoryChildren = categoryChildren,
                booksInCategory = booksInCategory,
                selectedCategory = selectedCategory,
                selectedBook = selectedBook,
                onCategoryClick = onCategoryClick,
                onBookClick = onBookClick,
                maxLevel = 25
            )
        }
    }
}

@Composable
fun CategoryTreeItem(
    category: Category,
    level: Int,
    expandedCategories: Set<Long>,
    categoryChildren: Map<Long, List<Category>>,
    booksInCategory: Set<Book>,
    selectedCategory: Category?,
    selectedBook: Book?,
    onCategoryClick: (Category) -> Unit,
    onBookClick: (Book) -> Unit,
    maxLevel: Int
) {
    val isExpanded = expandedCategories.contains(category.id)
    val isSelected = selectedCategory?.id == category.id

    // Display the category item
    CategoryItem(
        category = category,
        level = level,
        isExpanded = isExpanded,
        isSelected = isSelected,
        onClick = { onCategoryClick(category) }
    )

    // If expanded, show children and/or books
    if (isExpanded) {
        // Show books in this category
        // Filter books that belong to this category
        val booksInThisCategory = booksInCategory.filter { it.categoryId == category.id }
        booksInThisCategory.forEach { book ->
            BookItem(
                book = book,
                level = level + 1,
                isSelected = selectedBook?.id == book.id,
                onClick = { onBookClick(book) }
            )
        }

        // Show child categories if not at max depth
        if (level < maxLevel - 1) {
            val children = categoryChildren[category.id] ?: emptyList()
            children.forEach { childCategory ->
                CategoryTreeItem(
                    category = childCategory,
                    level = level + 1,
                    expandedCategories = expandedCategories,
                    categoryChildren = categoryChildren,
                    booksInCategory = booksInCategory,
                    selectedCategory = selectedCategory,
                    selectedBook = selectedBook,
                    onCategoryClick = onCategoryClick,
                    onBookClick = onBookClick,
                    maxLevel = maxLevel
                )
            }
        }
    }
}

@Composable
fun CategoryItem(
    category: Category,
    level: Int,
    isExpanded: Boolean,
    isSelected: Boolean,
    onClick: () -> Unit
) {
    // Use LaunchedEffect to ensure proper recomposition when category changes
    LaunchedEffect(category.id) {
        // No action needed, just trigger recomposition
    }

    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick)
            .padding(start = (level * 16).dp, top = 4.dp, bottom = 4.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        // Icon for expand/collapse
        Icon(
            imageVector = if (isExpanded) Icons.Default.KeyboardArrowDown else Icons.AutoMirrored.Filled.KeyboardArrowRight,
            contentDescription = if (isExpanded) "Collapse" else "Expand",
            tint = Color.Gray,
            modifier = Modifier.size(24.dp)
        )

        // Icon for folder
        Icon(
            imageVector = Icons.Default.Folder,
            contentDescription = "Folder",
            tint = if (isSelected) Color.Blue else Color.Gray,
            modifier = Modifier.size(24.dp)
        )

        Spacer(modifier = Modifier.width(8.dp))

        Text(
            text = category.title,
            fontWeight = if (isSelected) FontWeight.Bold else FontWeight.Normal,
            color = if (isSelected) Color.Blue else Color.Black
        )
    }
}

@Composable
fun BookItem(
    book: Book,
    level: Int,
    isSelected: Boolean,
    onClick: () -> Unit
) {
    // Use LaunchedEffect to ensure proper recomposition when book changes
    LaunchedEffect(book.id) {
        // No action needed, just trigger recomposition
    }

    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick)
            .padding(start = (level * 16).dp, top = 4.dp, bottom = 4.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Spacer(modifier = Modifier.width(24.dp))

        // Icon for book
        Icon(
            imageVector = Icons.Default.Book,
            contentDescription = "Book",
            tint = if (isSelected) Color.Blue else Color.Gray,
            modifier = Modifier.size(24.dp)
        )

        Spacer(modifier = Modifier.width(8.dp))

        Text(
            text = book.title,
            fontWeight = if (isSelected) FontWeight.Bold else FontWeight.Normal,
            fontSize = 14.sp,
            color = if (isSelected) Color.Blue else Color.Black
        )
    }
}
