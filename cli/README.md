# Seforim CLI

Command-line tool for searching the Seforim database.

## Building

### Fat JAR (development)

```bash
./gradlew :cli:fatJar
```

The JAR is generated at `cli/build/libs/seforim-cli-1.0.0-all.jar`.

### Direct execution

```bash
java -jar cli/build/libs/seforim-cli-1.0.0-all.jar <command> [options]
```

### Native package (jpackage)

```bash
# Installer (.deb, .dmg, .exe depending on OS)
./gradlew :cli:jpackage

# Portable app image (no installer)
./gradlew :cli:jpackageAppImage

# Optimized version with ProGuard
./gradlew :cli:jpackageOptimized
```

Packages are generated in `cli/build/jpackage/` or `cli/build/jpackage-image/`.

## Usage

### Commands

```bash
seforim-cli search <query>    # Search for text
seforim-cli books <prefix>    # Search books by title prefix
seforim-cli facets <query>    # Get facets (counts by book/category)
seforim-cli help              # Show help
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--db <path>` | Path to seforim.db | Same location as SeforimApp |
| `--index <path>` | Path to Lucene index | `<db>.lucene` |
| `--dict <path>` | Path to lexical.db dictionary | `<db>/../lexical.db` |
| `--limit <n>` | Results per page | 25 |
| `--near <n>` | Proximity slop for phrases (0=exact) | 5 |
| `--book <id>` | Filter by book ID | - |
| `--category <id>` | Filter by category ID | - |
| `--base-only` | Search base books only (not commentaries) | false |
| `--json` | Output as JSON | false |
| `--no-snippets` | Disable snippets (faster) | false |
| `--all` | Fetch all results (not just first page) | false |

### Examples

```bash
# Simple search
seforim-cli search "בראשית ברא" --limit 10

# Search with filter and JSON output
seforim-cli search "אברהם" --book 123 --json

# Search books by prefix
seforim-cli books "בראש" --limit 5

# Get facets
seforim-cli facets "משה" --base-only

# With custom database path
seforim-cli search "תורה" --db /path/to/seforim.db --index /path/to/seforim.db.lucene
```

## Requirements

- JDK 21+ (JetBrains Runtime recommended)
- `seforim.db` database with its Lucene index
- Optional: `lexical.db` for search expansion

## File structure

The CLI uses the same default paths as the SeforimApp:
- Database: `~/.local/share/io.github.kdroidfilter.seforimapp/databases/seforim.db`
- Lucene index: `seforim.db.lucene` (next to the DB)
- Dictionary: `lexical.db` (next to the DB)
