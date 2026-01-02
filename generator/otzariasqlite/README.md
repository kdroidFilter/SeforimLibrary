# Otzaria → SQLite (append)

This module appends **Otzaria** content into an existing `seforim.db` that was generated from **Sefaria** (see `:sefariasqlite`).

Scope:
- ✅ Insert/append categories, books, TOC entries, lines, links.
- ✅ Enrich the DB with book acronyms (table `book_acronym`) using the **SeforimAcronymizer** database.
- ❌ Does **not** build `catalog.pb` (see `:catalog:buildCatalog`).
- ❌ Does **not** build Lucene indexes (see `:searchindex:buildLuceneIndexDefault`).

## Key tasks

### Download helpers
- `./gradlew :otzariasqlite:downloadOtzaria`: download Otzaria source into `generator/otzariasqlite/build/otzaria/source`.
- `./gradlew :otzariasqlite:downloadAcronymizer` (alias: `:otzariasqlite:downloadAcronymizerDb`): download the Acronymizer DB into `generator/otzariasqlite/build/acronymizer/acronymizer.db`.

The Acronymizer DB is used during import to populate `book_acronym` (it’s not related to Lucene indexing).

### Recommended pipeline (append into existing DB)
- `./gradlew :otzariasqlite:appendOtzaria`: wrapper that runs:
  - `:otzariasqlite:appendOtzariaLines`
  - `:otzariasqlite:appendOtzariaLinks`

Default input/output DB is `build/seforim.db` at the repo root (override with `-PseforimDb=/path/to/seforim.db`).

### Advanced / manual phases
- `./gradlew :otzariasqlite:appendOtzariaLines`: append categories/books/TOCs/lines into an existing DB.
- `./gradlew :otzariasqlite:appendOtzariaLinks`: append links (requires that lines/books exist).
- `./gradlew :otzariasqlite:generateLines`: phase 1 generation (creates/persists `build/seforim.db` by default).
- `./gradlew :otzariasqlite:generateLinks`: phase 2 link processing.

## Common properties / env vars

- DB path:
  - `-PseforimDb=/path/to/seforim.db` or env `SEFORIM_DB`
- Otzaria source dir:
  - `-PsourceDir=/path/to/otzaria` or env `OTZARIA_SOURCE_DIR` (must contain `metadata.json` and `אוצריא/`, not the `אוצריא/` folder itself)
- Acronymizer DB:
  - `-PacronymDb=/path/to/acronymizer.db` or env `ACRONYM_DB`
- In-memory mode:
  - default is in-memory for speed; set `-PinMemoryDb=false` to work directly on disk.
