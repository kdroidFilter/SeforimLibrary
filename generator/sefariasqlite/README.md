# Sefaria Direct Importer

## Overview

The `SefariaDirectImporter` is a JVM-based data pipeline that transforms Sefaria's database export into a SQLite relational database with cross-references and table of contents (TOC) hierarchies. It processes Hebrew religious texts from their original nested JSON structure into an optimized format for reading and navigation.

Lucene indexes and `catalog.pb` are built as separate steps (see `:searchindex:buildLuceneIndexDefault` and `:catalog:buildCatalog`).

**Key Features:**
- Converts hierarchical JSON texts to flattened SQLite lines
- Generates bidirectional citation links between texts
- Builds hierarchical table of contents with parent-child relationships
- Preserves Hebrew/English metadata, authors, publication dates
- Supports complex schemas with Talmud pagination, Gematria numbering
- Leaves catalog/index generation to dedicated modules

## Source Structure (Sefaria Export)

### Directory Layout
```
database_export/
├── table_of_contents.json          # Category and book ordering metadata
├── json/                            # Book content files
│   └── {BookName}/
│       └── merged.json              # Complete text content
├── schemas/                         # Book structure definitions
│   └── {BookName}.json              # Schema with hierarchy, sections, metadata
└── links/                           # Cross-references between texts
    └── *.csv                        # Format: Citation 1, Citation 2, Connection Type
```

### merged.json Structure
```json
{
  "title": "Genesis",
  "heTitle": "בראשית",
  "categories": ["Tanakh", "Torah"],
  "text": [
    [
      ["In the beginning...", "God created..."],
      ["the heaven", "and the earth"]
    ]
  ]
}
```

### Schema File Structure
```json
{
  "schema": {
    "title": "Genesis",
    "heTitle": "בראשית",
    "heCategories": ["תנ\"ך", "תורה"],
    "authors": [{"he": "משה"}],
    "heDesc": "Description of the book",
    "pubDate": "Ancient",
    "depth": 3,                           // Nesting levels
    "heSectionNames": ["פרק", "פסוק"],    // Section labels (Chapter, Verse)
    "addressTypes": ["Integer", "Integer"], // Numbering format per level
    "referenceableSections": [true, true],  // Which levels get references
    "nodes": [...]                          // Optional hierarchical structure
  }
}
```

#### Schema Fields

**depth**: Number of nesting levels in the text array
- `depth=2`: Book → Chapter → Verse (e.g., Bible)
- `depth=3`: Book → Volume → Page → Line (e.g., Talmud)

**heSectionNames**: Hebrew labels for each level
- `["פרק", "פסוק"]` = Chapter, Verse
- `["דף", "שורה"]` = Page, Line

**addressTypes**: Controls numbering format per level
- `"Integer"`: Standard numbers (1, 2, 3...)
- `"Talmud"`: Daf notation (א:, א., ב:, ב...)
- Default: Gematria (א, ב, ג...)

**referenceableSections**: Boolean array indicating which levels should:
- Generate inline prefixes like `(א)`, `(ב)`
- Create intermediate section headings
- Be included in citation references

**nodes**: Optional array for complex hierarchical structures
- Each node can have `title`, `heTitle`, `key`, `depth`, `addressTypes`
- Nested `nodes` arrays create multi-level hierarchies
- `key="default"` indicates the node doesn't add a reference level

## Destination Structure (SQLite)

### Database Schema

#### 1. source
```sql
CREATE TABLE source (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL  -- "Sefaria"
);
```

#### 2. category (Hierarchical)
```sql
CREATE TABLE category (
    id INTEGER PRIMARY KEY,
    parentId INTEGER,               -- NULL for root categories
    title TEXT NOT NULL,
    level INTEGER NOT NULL,         -- Depth in hierarchy (0 = root)
    order INTEGER DEFAULT 999,      -- Display order from TOC
    FOREIGN KEY (parentId) REFERENCES category(id)
);
```

**Example hierarchy:**
```
תנ"ך (level=0, parentId=NULL)
  └─ תורה (level=1, parentId=תנ"ך)
      └─ בראשית (level=2, parentId=תורה)
```

#### 3. book
```sql
CREATE TABLE book (
    id INTEGER PRIMARY KEY,
    categoryId INTEGER NOT NULL,
    sourceId INTEGER NOT NULL,
    title TEXT NOT NULL,            -- Hebrew title
    authors TEXT,                   -- JSON-serialized Author[]
    pubDates TEXT,                  -- JSON-serialized PubDate[]
    heShortDesc TEXT,               -- Hebrew description
    order REAL DEFAULT 999.0,       -- Display order from TOC
    totalLines INTEGER DEFAULT 0,
    isBaseBook INTEGER DEFAULT 1,
    hasTargumConnection INTEGER DEFAULT 0,
    hasReferenceConnection INTEGER DEFAULT 0,
    hasCommentaryConnection INTEGER DEFAULT 0,
    hasOtherConnection INTEGER DEFAULT 0,
    FOREIGN KEY (categoryId) REFERENCES category(id),
    FOREIGN KEY (sourceId) REFERENCES source(id)
);
```

#### 4. line (Flattened Text Content)
```sql
CREATE TABLE line (
    id INTEGER PRIMARY KEY,
    bookId INTEGER NOT NULL,
    lineIndex INTEGER NOT NULL,     -- Sequential position in book
    content TEXT NOT NULL,          -- HTML-formatted text
    ref TEXT,                       -- English Sefaria reference (e.g., "Genesis 1:1")
    heRef TEXT,                     -- Hebrew Sefaria reference (e.g., "בראשית א, א")
    tocEntryId INTEGER,             -- Associated TOC entry
    FOREIGN KEY (bookId) REFERENCES book(id),
    FOREIGN KEY (tocEntryId) REFERENCES tocEntry(id)
);
```

**Content Format:**
- HTML tags: `<h1>`, `<h2>`, `<h3>`, `<h4>`, `<h5>` for headings
- Inline prefixes: `(א)`, `(ב)`, `(ג)` for numbered segments
- Plain text for content

**Reference Format:**
- `ref`: English citation (e.g., `"Genesis 1:1"`, `"Berakhot 2a:5"`)
- `heRef`: Hebrew citation (e.g., `"בראשית א, א"`, `"ברכות ב., ה"`)
- Only content lines have references; heading lines have `NULL` values

**Example:**
```
lineIndex=0: <h1>בראשית</h1>                    | ref=NULL             | heRef=NULL
lineIndex=1: משה                                | ref=NULL             | heRef=NULL
lineIndex=2: <h2>פרק א</h2>                     | ref=NULL             | heRef=NULL
lineIndex=3: (א) בראשית ברא אלהים                | ref="Genesis 1:1"    | heRef="בראשית א, א"
lineIndex=4: (ב) והארץ היתה תהו                 | ref="Genesis 1:2"    | heRef="בראשית א, ב"
```

#### 5. toc_entry (Table of Contents)
```sql
CREATE TABLE toc_entry (
    id INTEGER PRIMARY KEY,
    bookId INTEGER NOT NULL,
    parentId INTEGER,               -- Parent TOC entry (NULL for root)
    textId TEXT,                    -- Optional external ID
    text TEXT NOT NULL,             -- Display title
    level INTEGER NOT NULL,         -- Depth in TOC hierarchy
    lineId INTEGER,                 -- Associated line in content
    isLastChild INTEGER DEFAULT 0,  -- True if last among siblings
    hasChildren INTEGER DEFAULT 0,  -- True if has child entries
    FOREIGN KEY (bookId) REFERENCES book(id),
    FOREIGN KEY (parentId) REFERENCES toc_entry(id),
    FOREIGN KEY (lineId) REFERENCES line(id)
);
```

#### 6. line_toc (Many-to-Many Junction)
```sql
CREATE TABLE line_toc (
    lineId INTEGER NOT NULL,
    tocEntryId INTEGER NOT NULL,
    PRIMARY KEY (lineId, tocEntryId),
    FOREIGN KEY (lineId) REFERENCES line(id),
    FOREIGN KEY (tocEntryId) REFERENCES toc_entry(id)
);
```

Maps each line to its containing TOC section(s).

#### 7. link (Bidirectional Cross-References)
```sql
CREATE TABLE link (
    id INTEGER PRIMARY KEY,
    sourceBookId INTEGER NOT NULL,
    targetBookId INTEGER NOT NULL,
    sourceLineId INTEGER NOT NULL,
    targetLineId INTEGER NOT NULL,
    connectionTypeId INTEGER NOT NULL,
    FOREIGN KEY (sourceBookId) REFERENCES book(id),
    FOREIGN KEY (targetBookId) REFERENCES book(id),
    FOREIGN KEY (sourceLineId) REFERENCES line(id),
    FOREIGN KEY (targetLineId) REFERENCES line(id),
    FOREIGN KEY xxxxxxctionTypeId) REFERENCES connection_type(id)
);
```

**Connection Types:** `COMMENTARY`, `REFERENCE`, `TARGUM`, `OTHER`

**Bidirectionality:** For every link `A→B`, a reverse link `B→A` is created.

#### 8. book_has_links (Optimization Table)
```sql
CREATE TABLE book_has_links (
    bookId INTEGER PRIMARY KEY,
    hasSourceLinks INTEGER DEFAULT 0,  -- Has outgoing links
    hasTargetLinks INTEGER DEFAULT 0,  -- Has incoming links
    FOREIGN KEY (bookId) REFERENCES book(id)
);
```

## Key Transformations

### 1. Nested JSON → Flattened Lines

**Input (depth=2):**
```json
{
  "text": [
    ["In the beginning", "God created"],
    ["the heaven", "and the earth"]
  ]
}
```

**Output (line table):**
```
lineIndex=0: <h1>Genesis</h1>
lineIndex=1: <h2>Chapter א</h2>
lineIndex=2: (א) In the beginning
lineIndex=3: (ב) God created
lineIndex=4: <h2>Chapter ב</h2>
lineIndex=5: (א) the heaven
lineIndex=6: (ב) and the earth
```

**Implementation:** `recursiveSections()` (lines 588-699)
- Recursively traverses nested JSON arrays
- Decrements `depth` at each level
- Generates headings and inline prefixes based on `referenceableSections`
- Uses `addressTypes` to determine numbering format

### 2. Reference Generation

For each text segment, create English and Hebrew citations:

**English:** Uses book title + numeric indices
- `"Genesis 1:1"` (standard)
- `"Berakhot 2a:5"` (Talmud with Daf notation)

**Hebrew:** Uses Hebrew title + Gematria
- `"בראשית א, א"`
- `"ברכות ב., ה"`

**Storage:** References are stored in two places:

1. **In the line table** (lines 289-307):
   - Each line has `ref` and `heRef` columns
   - Heading lines have `NULL` values
   - Content lines have their specific citations

2. **In RefEntry objects** for link resolution:
   ```kotlin
   RefEntry(
       ref = "Genesis 1:1",              // For link resolution
       heRef = "בראשית א, א",
       path = "Tanakh/Torah/Genesis",    // Book path in category tree
       lineIndex = 42                    // Position in line table (1-indexed)
   )
   ```

**Implementation:**
- Built during `recursiveSections()`, stored in `refEntries` list
- Mapped to line table during insertion via `refsByLineIndex` (converted to 0-indexed)

### 3. TOC Hierarchy Construction

**Process (lines 302-358):**

1. **First Pass:** Insert TOC entries with hierarchy
   - Use `ArrayDeque<Pair<Int, Long>>` to track level stack
   - Pop stack when encountering lower-level headings
   - Set `parentId` to last entry on stack

2. **Second Pass:** Update relational flags
   - Mark `hasChildren=true` for all entries that are parents
   - Mark `isLastChild=true` for last sibling in each group

3. **Create line_toc mappings:**
   - For each line, find the most recent TOC entry at or before its position
   - Insert junction record linking line to TOC section

**Example:**
```
Book Title (level=0, id=1)
  ├─ Chapter 1 (level=1, id=2, parentId=1, hasChildren=1)
  │   ├─ Section A (level=2, id=3, parentId=2)
  │   └─ Section B (level=2, id=4, parentId=2, isLastChild=1)
  └─ Chapter 2 (level=1, id=5, parentId=1, isLastChild=1, hasChildren=1)
      └─ Section C (level=2, id=6, parentId=5, isLastChild=1)
```

### 4. Link Resolution & Insertion

**Process (lines 372-430):**

1. **Parse CSV files:**
   ```csv
   Citation 1,Citation 2,Connection Type
   "Genesis 1:1","Exodus 2:3","Commentary"
   "Berakhot 2a","Shabbat 15b","Reference"
   ```

2. **Normalize citations:**
   - Trim whitespace and quotes
   - Convert to lowercase
   - Remove commas: `"Genesis 1:1"` → `"genesis 1:1"`

3. **Resolve to RefEntry:**
   - Try exact match: `refsByCanonical[canonical]`
   - Try range start: `"Gen 1:1-5"` → `"Gen 1:1"`
   - Fallback to base: `canonicalBase()` removes trailing numbers

4. **Create bidirectional links:**
   ```kotlin
   // Forward: source → target
   Link(sourceLineId=A, targetLineId=B, connectionType=...)

   // Reverse: target → source
   Link(sourceLineId=B, targetLineId=A, connectionType=...)
   ```

**Lookup Optimization:**
- `refsByCanonical: Map<String, List<RefEntry>>` for exact matches
- `refsByBase: Map<String, RefEntry>` for prefix fallback

### 5. Category & Book Ordering

**Source:** `table_of_contents.json`
```json
[
  {
    "category": "Tanakh",
    "heCategory": "תנ\"ך",
    "order": 1,
    "contents": [
      {
        "category": "Torah",
        "heCategory": "תורה",
        "order": 1,
        "contents": [
          {
            "title": "Genesis",
            "heTitle": "בראשית",
            "order": 1
          }
        ]
      }
    ]
  }
]
```

**Implementation (lines 70-152):**
- Recursively parse TOC structure
- Store orders with multiple keys:
  - English name: `"Tanakh"` → 1
  - Hebrew name: `"תנ\"ך"` → 1
  - Sanitized: `sanitizeFolder("תנ\"ך")` → 1
  - Full path: `"Tanakh/Torah"` → combined order
- Apply during category/book insertion (default: 999)

## Special Numbering Systems

### Gematria (Hebrew Numerals)

**Implementation:** `toGematria()` (lines 812-879)

| Number | Gematria | Special Cases |
|--------|----------|---------------|
| 1      | א        |               |
| 5      | ה        |               |
| 10     | י        |               |
| 15     | טו       | Avoid יה (divine name) |
| 16     | טז       | Avoid יו (divine name) |
| 20     | כ        |               |
| 100    | ק        |               |
| 1000+  | א' קכג   | Thousands prefix |

**Algorithm:**
1. Extract thousands → recursive Gematria + space
2. Map hundreds (100=ק, 200=ר, 300=ש, 400=ת)
3. Handle special cases (15=טו, 16=טז)
4. Map tens (10-90)
5. Map units (1-9)

### Talmud Daf Notation

**Implementation:** `toDaf()` and `toEnglishDaf()` (lines 881-889)

Talmud pages have two sides (a/b or :/. notation):

| Index | Hebrew | English |
|-------|--------|---------|
| 1     | א:     | 1a      |
| 2     | א.     | 1b      |
| 3     | ב:     | 2a      |
| 4     | ב.     | 2b      |

**Logic:**
- Odd index → `:` (a-side): `(index+1)/2` with `:`
- Even index → `.` (b-side): `(index+1)/2` with `.`

### Title Sanitization

**Implementation:** `sanitizeFolder()` (lines 777-782)

Converts ASCII quotes to Hebrew guersayim:
```kotlin
"Book \"Title\"" → "Book ״Title״"
```

Reason: Sefaria JSON uses ASCII `"` instead of proper Hebrew punctuation `״`.

## Querying References

### Accessing Line References

Once imported, you can query lines with their Sefaria references:

```kotlin
// Get a specific line with its references
val line = repository.getLineById(lineId)
println("English: ${line.ref}")   // "Genesis 1:1"
println("Hebrew: ${line.heRef}")  // "בראשית א, א"

// Find lines by reference
val lines = repository.executeRawQuery(
    "SELECT * FROM line WHERE ref = 'Genesis 1:1'"
)

// Search for lines in a specific chapter
val chapter1Lines = repository.executeRawQuery(
    "SELECT * FROM line WHERE ref LIKE 'Genesis 1:%'"
)
```

### Index Performance

The schema includes indexes on both reference columns for fast lookups:
- `idx_line_ref` - Index on English references
- `idx_line_heref` - Index on Hebrew references

These enable efficient queries like:
- Finding all lines for a specific citation
- Range queries (e.g., all lines in a chapter)
- Reverse lookup from citation to content

## Usage

### Basic Import

```kotlin
import io.github.kdroidfilter.seforimlibrary.sefariasqlite.SefariaDirectImporter
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import java.nio.file.Paths

val exportRoot = Paths.get("/path/to/sefaria/database_export")
val repository = SeforimRepository(driver = sqlDriver)

val importer = SefariaDirectImporter(
    exportRoot = exportRoot,
    repository = repository
)

// Run import (suspend function)
importer.import()

// Build the catalog/indexes as separate steps:
//   ./gradlew :catalog:buildCatalog
//   ./gradlew :searchindex:buildLuceneIndexDefault
```

### Expected Directory Structure

```
/path/to/sefaria/
└── database_export/           # Auto-detected
    ├── table_of_contents.json
    ├── json/
    │   ├── Genesis/
    │   │   └── merged.json
    │   └── Exodus/
    │       └── merged.json
    ├── schemas/
    │   ├── Genesis.json
    │   └── Exodus.json
    └── links/
        ├── Genesis_links.csv
        └── Exodus_links.csv
```

The importer will:
1. Auto-detect `database_export` folder (direct or one level deep)
2. Validate presence of `json/` and `schemas/` directories
3. Process all `merged.json` files
4. Match schemas by title/heTitle/folderName
5. Parse all `.csv` files in `links/`

### Output

**SQLite Database:**
- Populated via `SeforimRepository` methods
- All tables created (schema) + SQLite indexes applied
- Category closure table rebuilt
- Connection type flags updated

**Catalog File (built separately):**
- Generated by `./gradlew :catalog:buildCatalog` (or `./gradlew generateSeforimDb`)
- Written as `catalog.pb` next to the DB file

**Lucene indexes (built separately):**
- Generated by `./gradlew :searchindex:buildLuceneIndexDefault`
- Written next to the DB file (e.g. `seforim.db.lucene/` + `seforim.db.lookup.lucene/`)

## Advanced Features

### Schema Node Processing

Complex texts use `nodes` for multi-level hierarchies:

```json
{
  "schema": {
    "nodes": [
      {
        "key": "introduction",
        "title": "Introduction",
        "heTitle": "הקדמה",
        "depth": 1,
        "heSectionNames": ["פסקה"]
      },
      {
        "key": "default",
        "depth": 2,
        "heSectionNames": ["פרק", "פסוק"]
      }
    ]
  }
}
```

**Processing (lines 473-541):**
- Recursively traverse `nodes` array
- Match node keys to text object keys
- `key="default"` uses empty string or falls back to title
- Generate separate TOC entries for each node
- Maintain reference prefix chain through recursion

### Citation Matching Strategies

**1. Exact Canonical Match**
```kotlin
refsByCanonical["genesis 1:1"] → List<RefEntry>
```

**2. Range Start Extraction**
```kotlin
"Genesis 1:1-5" → "Genesis 1:1"
refsByCanonical["genesis 1:1"] → List<RefEntry>
```

**3. Base Fallback**
```kotlin
canonicalBase("Genesis 1:1") → "genesis"
refsByBase["genesis"] → RefEntry (lowest lineIndex)
```

**Implementation:** `resolveRefs()` (lines 955-970)

### Memory Optimization

**Streaming Processing:**
- Uses `Files.walk()` with `.use { }` for auto-close
- Processes books in parallel with bounded concurrency
- Only keeps required lookup maps in memory

**Batch Operations:**
- Lines and line→TOC mappings are inserted in batches (see `SefariaImportTuning`)
- Links are processed in parallel and inserted in batches

## Error Handling

**Safe Parsing:**
```kotlin
runCatching {
    // Parse and process book
}.onFailure { e ->
    logger.w(e) { "Failed to prepare book from $textPath" }
}
```

**Schema Resolution:**
- Tries multiple candidates (title, heTitle, folderName)
- Returns `null` if no schema found (skips book)

**Link Resolution:**
- Silently skips invalid citations
- Logs warnings for unparseable CSV lines
- Continues processing on errors

## Performance Characteristics

**Typical Import Stats:**
- 6,000+ books
- 1,000,000+ lines
- 500,000+ links
- 15,000+ TOC entries
- Import time: ~10-30 minutes (depending on hardware)

**Bottlenecks:**
1. Link insertion (bidirectional = 2× operations)
2. TOC entry updates (2-pass algorithm)
3. Category closure rebuild

**Optimization Opportunities:**
1. Batch inserts with transactions
2. Parallel book processing (requires thread-safe repository)
3. Precomputed citation lookup index

## Logging

Uses Kermit logger with tag `"SefariaDirectImporter"`:

```kotlin
logger.i { "Prepared book $hebrewTitle with ${lines.size} lines" }
logger.w { "table_of_contents.json not found" }
logger.e(e) { "Error parsing table_of_contents.json" }
```

**Key Log Points:**
- TOC parsing results
- Per-book preparation stats
- Import completion summary
- Catalog and Lucene indexes are built separately (see `:catalog:buildCatalog`, `:searchindex:buildLuceneIndexDefault`)

## Dependencies

**Required:**
- `SeforimRepository` (DAO layer)
- SQLDelight driver (for database operations)
- `kotlinx-serialization-json` (JSON parsing)
- Kermit (logging)

**Platform:**
- JVM only (uses `java.nio.file.*`)

## Related Components

**SeforimRepository** (`SeforimLibrary/dao`)
- Provides all database operations
- Manages SQLDelight queries
- Handles transactions

**Legacy Sefaria→Otzaria converter** (removed)
- The old intermediate “Otzaria-like folder” step was removed in favor of this direct SQLite importer

**PrecomputedCatalog** (`core/models`)
- Protobuf-serialized catalog structure
- Generated by `./gradlew :catalog:buildCatalog` (or `./gradlew generateSeforimDb`)

## License

Part of SeforimLibrary project.
