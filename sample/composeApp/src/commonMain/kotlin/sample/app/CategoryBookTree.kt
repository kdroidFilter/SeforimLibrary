package sample.app

import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.Text
import androidx.compose.runtime.Composable
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
    booksInCategory: List<Book>,
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
    booksInCategory: List<Book>,
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
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick)
            .padding(start = (level * 16).dp, top = 4.dp, bottom = 4.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        // Simple text indicator instead of icon
        Text(
            text = if (isExpanded) "▼" else "▶",
            color = Color.Gray,
            modifier = Modifier.width(24.dp)
        )

        // Simple text indicator for folder
        Text(
            text = "📁",
            color = if (isSelected) Color.Blue else Color.Gray,
            modifier = Modifier.width(24.dp)
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
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick)
            .padding(start = (level * 16).dp, top = 4.dp, bottom = 4.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Spacer(modifier = Modifier.width(24.dp))

        // Simple text indicator for book
        Text(
            text = "📕",
            color = if (isSelected) Color.Blue else Color.Gray,
            modifier = Modifier.width(24.dp)
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
