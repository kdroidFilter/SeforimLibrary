# LLM Guide: Understanding the Sefaria Links System

## Overview

This dataset contains **~3.97 million cross-references** between Jewish texts (Torah, Talmud, Midrash, commentaries, dictionaries, and more). The links are organized across 15 CSV files in two distinct formats.

## File Structure

### Detailed Link Files: `links0.csv` through `links12.csv`

These 13 files contain **individual citation-level links** with full metadata.

**Schema (7 columns):**

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `Citation 1` | string | Precise source reference with section/verse | "Beit Yosef, Orach Chayim 325:34:1" |
| `Citation 2` | string | Precise target reference with section/verse | "Tur, Orach Chayim 325:1" |
| `Connection Type` | string (optional) | Relationship type between texts | "commentary", "quotation", or empty |
| `Text 1` | string | Source book name | "Beit Yosef, Orach Chayim" |
| `Text 2` | string | Target book name | "Tur, Orach Chayim" |
| `Category 1` | string | Source text category | "Halakhah" |
| `Category 2` | string | Target text category | "Talmud" |

**Connection Types:**
- `"commentary"` - Text 1 provides commentary on Text 2
- `"quotation"` - Text 1 quotes from Text 2
- `""` (empty) - Generic reference or unspecified relationship

**Categories:**
- `Reference` - Dictionaries (Jastrow, Klein, BDB)
- `Tanakh` - Hebrew Bible
- `Mishnah` - Early rabbinic law
- `Talmud` - Rabbinic discussions
- `Tosefta` - Supplementary Mishnah
- `Midrash` - Exegetical literature
- `Halakhah` - Legal codes and commentaries

**Volume:** ~3.87 million links total (~300k lines per file, except links12.csv with ~186k)

### Summary Files

#### `links_by_book.csv` (96,121 rows)
**Aggregated statistics of ALL links between books**

**Schema (3 columns):**
| Column | Description | Example |
|--------|-------------|---------|
| `Text 1` | Source book | "Jastrow" |
| `Text 2` | Target book | "Shabbat" |
| `Link Count` | Total number of links | 3739 |

**Use case:** Quickly identify which books reference each other most frequently without examining individual citations.

#### `links_by_book_without_commentary.csv` (88,510 rows)
**Same as above but EXCLUDING links where Connection Type = "commentary"**

**Use case:** Focus on direct quotations and references while filtering out commentary relationships.

## Data Patterns & Insights

### Self-References
Dictionaries often link to themselves:
```csv
Jastrow,Jastrow,56060
Klein Dictionary,Klein Dictionary,45937
```
This represents internal cross-references (e.g., "see also..." entries).

### Commentary Chains
Halakhic texts form layered commentary structures:
```csv
Beit Yosef → Tur (18,369 links)
Mishnah Berurah → Shulchan Arukh (17,521 links)
```

### Biblical References
Dictionaries heavily reference biblical texts:
```csv
BDB,Psalms,12295
BDB,Isaiah,10329
BDB,Genesis,10153
```

## Usage Guidelines for LLMs

### When to Use Which File

**Use `links_by_book.csv` when:**
- User asks "Which books reference X the most?"
- Need overview of relationship strength between texts
- Want to identify major commentaries on a work
- Performing high-level network analysis

**Use `links_by_book_without_commentary.csv` when:**
- User wants to exclude commentary relationships
- Focus on quotations and direct references only
- Analyzing citation patterns vs. commentary patterns

**Use `links0-12.csv` when:**
- Need specific citation references
- User asks "Where does X cite passage Y?"
- Building precise cross-reference navigation
- Analyzing specific connection types

### Query Patterns

**Example 1: "What are the main commentaries on Shabbat?"**
```
Search links_by_book.csv where:
  - Text 2 = "Shabbat"
  - Connection Type = "commentary"
  - Sort by Link Count descending
```

**Example 2: "Find all places where Beit Yosef quotes the Talmud"**
```
Search links0-12.csv where:
  - Text 1 contains "Beit Yosef"
  - Category 2 = "Talmud"
  - Connection Type = "quotation"
```

**Example 3: "Which books most reference Genesis?"**
```
Search links_by_book.csv where:
  - Text 2 = "Genesis"
  - Sort by Link Count descending
```

### Important Notes

1. **File Organization**: links0-12 are likely split alphabetically or by size for manageability. Search across all files for complete results.

2. **Bidirectionality**: Links are directional (source → target). If you need bidirectional relationships, query both directions.

3. **Citation Precision**: Citation format varies by text type (Talmud uses page:section, Torah uses chapter:verse, etc.)

4. **Empty Connection Types**: Many links have no specified connection type. These are general references.

5. **Book Name Variations**: Some books appear with section names (e.g., "Shulchan Arukh, Orach Chayim" vs. "Shulchan Arukh"). Account for this in queries.

## Connecting Links to JSON Corpus

This section explains how to bridge the link data (CSV files) with the actual text content (JSON files in the corpus).

### Citation Format in Links

Citations in the CSV files follow these general patterns:

**Pattern 1: Simple Works with Section Numbers**
```
"Book Title chapter:verse"
Examples:
  "Genesis 1:1"          → Chapter 1, Verse 1
  "Mishnah Peah 6:6"     → Chapter 6, Mishnah 6
  "Psalms 119:28"        → Psalm 119, Verse 28
```

**Pattern 2: Talmud (Page:Section)**
```
"Tractate page:section"
Examples:
  "Shabbat 45b:3"        → Page 45b, Section 3
  "Sanhedrin 74b:9"      → Page 74b, Section 9
  "Berakhot 6b:11"       → Page 6b, Section 11
```

**Pattern 3: Structured Legal Codes**
```
"Work, Section siman:seif:subsection"
Examples:
  "Beit Yosef, Orach Chayim 325:34:1"
    → Work: "Beit Yosef"
    → Section: "Orach Chayim"
    → Siman: 325, Seif: 34, Subsection: 1

  "Shulchan Arukh, Yoreh De'ah 116:5"
    → Work: "Shulchan Arukh"
    → Section: "Yoreh De'ah"
    → Siman: 116, Seif: 5
```

**Pattern 4: Dictionary Entries**
```
"Dictionary, Entry index"
Examples:
  "A Dictionary of the Talmud, אֱגוֹד 1"
  "Jastrow, אֲבִילָה 3"
```

### Mapping Citations to JSON Files

#### Step 1: Locate the JSON File

From a citation like `"Beit Yosef, Orach Chayim 325:34:1"`:

1. Extract the base work name: `"Beit Yosef"`
2. Navigate to the corpus directory structure (typically organized by category)
3. Find the corresponding `merged.json` file

Example paths:
- `"Shulchan Arukh"` → `/Halakhah/Shulchan Arukh/merged.json`
- `"Genesis"` → `/Tanakh/Torah/Genesis/merged.json`
- `"Mishnah Peah"` → `/Mishnah/Seder Zeraim/Mishnah Peah/merged.json`

#### Step 2: Parse the Citation Components

Extract numerical/structural components:

```python
# Example: "Beit Yosef, Orach Chayim 325:34:1"
work = "Beit Yosef"
section = "Orach Chayim"
indices = [325, 34, 1]  # Siman, Seif, Subsection

# Example: "Genesis 1:1"
work = "Genesis"
indices = [1, 1]  # Chapter, Verse

# Example: "Shabbat 45b:3"
work = "Shabbat"
page = "45b"
section = 3
```

#### Step 3: Navigate the JSON Structure

**For Simple Model (sectionNames + nested arrays):**

```json
{
  "title": "Genesis",
  "sectionNames": ["Chapter", "Verse"],
  "text": [
    [...],  // Chapter 1
    [
      "verse 1 text",
      "verse 2 text",
      ...
    ]
  ]
}
```

Citation `"Genesis 1:1"` maps to:
- `text[0][0]` (index 0 = Chapter 1, index 0 = Verse 1)
- Remember: **zero-based indexing**, so subtract 1 from citation numbers

**For Complex Model (schema + section-keyed text):**

```json
{
  "title": "Beit Yosef",
  "schema": {
    "nodes": [
      {
        "enTitle": "Orach Chayim",
        "heTitle": "אורח חיים"
      }
    ]
  },
  "text": {
    "Orach Chayim": [
      [...],  // Siman 1
      [...],  // Siman 2
      // ...
      [      // Siman 325
        [...],  // Seif 1
        [...],  // Seif 34
        [
          "subsection 1 text",
          ...
        ]
      ]
    ]
  }
}
```

Citation `"Beit Yosef, Orach Chayim 325:34:1"` maps to:
1. Access section: `text["Orach Chayim"]`
2. Navigate indices: `text["Orach Chayim"][324][33][0]`
   - 324 = Siman 325 - 1
   - 33 = Seif 34 - 1
   - 0 = Subsection 1 - 1

### Special Cases

#### Talmud Page References

Talmud pages use format like `"45b"` (page 45, side b):
- Side "a" (recto) and side "b" (verso)
- Each side divided into numbered sections
- JSON structure typically uses page as first index, section as second

```json
{
  "title": "Shabbat",
  "text": [
    [...],  // Page 2a
    [...],  // Page 2b
    // ...
  ]
}
```

Citation `"Shabbat 45b:3"` requires:
1. Calculate page index from "45b" (implementation-specific)
2. Access section 3 within that page

#### Works with Subsections

Some works have named subsections in citations:
- `"Bereshit Rabbah 4:4"` → Parashah 4, Section 4
- `"Zohar 1:15a"` → Volume 1, Page 15a

Consult the JSON's `schema` or `sectionNames` to understand the hierarchy.

### Practical Workflow for LLMs

**Task: Display text referenced in a link**

1. **Parse the link CSV row:**
   ```csv
   "Beit Yosef, Orach Chayim 325:7:1","Shabbat 45b:3",commentary,...
   ```

2. **For Citation 1 ("Beit Yosef, Orach Chayim 325:7:1"):**
   - Locate JSON: `/Halakhah/Beit Yosef/merged.json`
   - Check `schema` → it's a complex model
   - Access: `text["Orach Chayim"][324][6][0]`

3. **For Citation 2 ("Shabbat 45b:3"):**
   - Locate JSON: `/Talmud/Bavli/Shabbat/merged.json`
   - Check `sectionNames` → ["Page", "Section"]
   - Calculate page index for "45b"
   - Access: `text[pageIndex][2]`

4. **Display both passages with context:**
   - Show the commentator's text (Citation 1)
   - Show the source being commented on (Citation 2)
   - Use `Connection Type` to describe relationship

### Reverse Lookup: Finding Links from JSON Content

**Scenario:** User is reading a specific passage and wants to see all links.

1. **Identify the current location:**
   - Book: `"Genesis"`
   - Indices: `[1, 1]` (Chapter 1, Verse 1)

2. **Construct citation string:**
   - Format: `"Genesis 1:1"`

3. **Search links CSV files:**
   ```
   WHERE Citation 1 = "Genesis 1:1" OR Citation 2 = "Genesis 1:1"
   ```

4. **Return matching links** with metadata (connection type, related texts)

### Integration Example

**User Query:** "Show me commentaries on Genesis 1:1"

```
1. Search links_by_book.csv:
   Text 2 = "Genesis" AND Connection Type = "commentary"
   → Find top commentators: Rashi, Ramban, etc.

2. Search links0-12.csv:
   Citation 2 = "Genesis 1:1" AND Connection Type = "commentary"
   → Get specific commentary citations

3. For each result:
   - Parse Citation 1 (e.g., "Rashi on Genesis 1:1:2")
   - Load JSON: /Tanakh/Rishonim on Torah/Rashi/Genesis/merged.json
   - Navigate to text[0][0] (or corresponding structure)
   - Display commentary text

4. Present results with:
   - Commentator name
   - Full citation reference
   - Actual text content
   - Link to original source (versionSource)
```

## Performance Tips

- For counting questions, use summary files (`links_by_book*.csv`)
- For specific citations, search detailed files (`links0-12.csv`)
- Filter by Category to narrow corpus (e.g., only Talmudic texts)
- Use Connection Type to distinguish quotations from commentaries
- **When displaying text**: Load JSON files on-demand rather than preloading entire corpus
- **For navigation**: Build an index mapping book names to file paths for faster lookup

## Example Use Cases

1. **Citation Networks**: Build graph of which texts cite which others
2. **Commentary Hierarchies**: Map layers of commentary on primary sources
3. **Influence Analysis**: Identify most-referenced works by category
4. **Cross-Reference Navigation**: Enable "see also" functionality
5. **Textual Analysis**: Study how different traditions interact through citations

---

**Total Dataset Size:** ~4 million links across Jewish canonical literature spanning 2,000+ years of scholarship.
