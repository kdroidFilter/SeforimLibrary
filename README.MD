# SeforimLibrary

A Kotlin Multiplatform library for converting and accessing the Otzaria database in SQLite format with FTS5 full-text search capabilities.

## Overview

SeforimLibrary is a comprehensive solution for working with Jewish religious texts from the Otzaria database. The project converts the original Otzaria database into a modern SQLite database with full-text search capabilities using FTS5, making it efficient to search through large volumes of text.

The library is structured as a set of modules that can be imported via Maven:

- **core**: Contains data models representing entities like books, authors, categories, and lines of text
- **dao**: Provides database access objects and repositories for interacting with the SQLite database
- **generator**: Handles the conversion of the original Otzaria database to SQLite format

## Features

- Convert Otzaria database to SQLite format
- Efficient full-text search using SQLite's FTS5
- Hierarchical category and book organization
- Table of contents navigation for books
- Support for links between related texts
- Comprehensive data model for Jewish religious texts

## Run Sample App

- Desktop JVM: `./gradlew :sample:composeApp:run`
- Android: `open project in Android Studio and run the sample app`


## Requirements

- JDK 11 or higher
- Kotlin 1.9.0 or higher
- SQLite 3.35.0 or higher (for FTS5 support)

## Usage

### Initializing the Database

```kotlin
// Initialize the database
val dbPath = "path/to/your/database.db"
val driver = JdbcSqliteDriver(url = "jdbc:sqlite:$dbPath")
val repository = SeforimRepository(dbPath, driver)
```

### Searching for Text

```kotlin
// Search in all books
val searchResults = repository.search("your search query", limit = 20, offset = 0)

// Search in a specific book
val bookSearchResults = repository.searchInBook(bookId, "your search query")

// Search by author
val authorSearchResults = repository.searchByAuthor("author name", "your search query")
```

### Browsing Categories and Books

```kotlin
// Get root categories
val rootCategories = repository.getRootCategories()

// Get subcategories
val subcategories = repository.getCategoryChildren(parentId)

// Get books in a category
val books = repository.getBooksByCategory(categoryId)
```

### Reading Book Content

```kotlin
// Get book details
val book = repository.getBook(bookId)

// Get lines of text
val lines = repository.getLines(bookId, startIndex, endIndex)

// Get table of contents
val toc = repository.getBookToc(bookId)
```

## Database Generation

To convert the original Otzaria database to SQLite format:

```kotlin
// Initialize repository
val repository = SeforimRepository(dbPath, driver)

// Create generator with source directory
val sourcePath = Path("/path/to/otzaria_source")
val generator = DatabaseGenerator(sourcePath, repository)

// Generate the database
generator.generate()
```

## Project Structure

- **core**: Contains data models and extensions
  - `models`: Data classes representing entities in the database
  - `extensions`: Utility extensions for working with the models

- **dao**: Database access layer
  - `repository`: Repository classes for accessing the database
  - `extensions`: Extensions for converting between database and model objects
  - `sqldelight`: SQL queries and database schema

- **generator**: Database generation tools
  - `DatabaseGenerator`: Main class for converting Otzaria data to SQLite
  - `Main`: Entry point for running the generator as a standalone application
