# SeforimLibrary

A Kotlin Multiplatform library + JVM tooling to build and query a unified SQLite database of Jewish texts (Sefaria + Otzaria), with Lucene-based full-text search.

## Overview

SeforimLibrary is a comprehensive solution for working with Jewish religious texts from the Otzaria database. The project converts the original Otzaria database into a modern SQLite database with full-text search capabilities using FTS5, making it efficient to search through large volumes of text.

The library is structured as a set of modules that can be imported via Maven:

- **core**: Contains data models representing entities like books, authors, categories, and lines of text
- **dao**: Provides database access objects and repositories for interacting with the SQLite database
- **otzariasqlite**: Otzaria import/enrichment tooling (append into an existing DB)
- **catalog**: Precomputed catalog builder (`catalog.pb`)
- **searchindex**: Lucene index builders (text + lookup)
- **packaging**: Release/bundling tooling (release info + `.tar.zst` bundle)
- **sefariasqlite**: One-step Sefaria export → SQLite importer

Generation tooling modules are grouped under `generator/` on disk (Gradle module names stay `:sefariasqlite`, `:otzariasqlite`, etc.).

## Features

- Convert Otzaria database to SQLite format
- Efficient full-text search using SQLite's FTS5
- Hierarchical category and book organization
- Table of contents navigation for books
- Support for links between related texts
- Comprehensive data model for Jewish religious texts

## Demo / App

SeforimLibrary is typically consumed from the `SeforimApp/` project in the parent repo.


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

### Recommended pipeline (Sefaria → SQLite → Otzaria)

```bash
./gradlew generateSeforimDb
```

Outputs (by default): `build/seforim.db` + `build/catalog.pb`

### Full pipeline (DB + indexes + bundle)

```bash
./gradlew packageSeforimBundle
```

### Manual steps

```bash
# 1) Base DB from Sefaria
./gradlew :sefariasqlite:generateSefariaSqlite

# 2) Append Otzaria (lines + links)
./gradlew :otzariasqlite:appendOtzaria

# 3) Build precomputed catalog
./gradlew :catalog:buildCatalog

# 4) Build Lucene indexes (creates build/seforim.db.lucene + build/seforim.db.lookup.lucene)
./gradlew :searchindex:buildLuceneIndexDefault

# 5) Download lexical.db next to the DB (used by the app; auto-run by packaging)
./gradlew :packaging:downloadLexicalDb

# 6) Package everything into a single .tar.zst (plus split parts)
./gradlew :packaging:packageArtifacts
```

## Project Structure

- **core**: Contains data models and extensions
  - `models`: Data classes representing entities in the database
  - `extensions`: Utility extensions for working with the models

- **dao**: Database access layer
  - `repository`: Repository classes for accessing the database
  - `extensions`: Extensions for converting between database and model objects
  - `sqldelight`: SQL queries and database schema

- **generator/**: Grouping folder for JVM generation tooling modules

- **otzariasqlite**: Otzaria enrichment tools
  - `DatabaseGenerator`: Converts Otzaria sources into SQLite rows
  - `GenerateLines` / `GenerateLinks`: phase tasks (see Gradle tasks)

- **catalog**: Precomputed catalog tools
  - `BuildCatalog`: builds `catalog.pb` from a SQLite DB

- **searchindex**: Lucene indexing tools
  - `LuceneTextIndexWriter` / `LuceneLookupIndexWriter`: JVM Lucene writers
  - `BuildLuceneIndex`: CLI entrypoint used by Gradle tasks

- **packaging**: Release/bundling tools
  - `WriteReleaseInfo` / `PackageArtifacts`: bundle `.tar.zst` for distribution

- **sefariasqlite**: Sefaria direct importer
  - `SefariaDirectImporter` / `GenerateSefariaSqlite`: Sefaria export → SQLite
