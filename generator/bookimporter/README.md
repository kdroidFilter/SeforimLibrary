# Seforim Book Importer (Desktop GUI)

Desktop tool (Compose for Desktop, JVM) for appending new books into an existing `seforim.db`.

## Features

- Select an existing SQLite DB (`.db`) and one or more library root folders.
- Scan + preview before execution (detected books/categories, invalid roots).
- Execute import flow with progress + logs:
  1. Backup DB (`*.before-import.bak`)
  2. Append lines/books via Otzaria pipeline
  3. Append links
  4. Optional post-steps: rebuild `catalog.pb`, rebuild Lucene indexes
- Rollback on failure (restore DB from backup).

## Run (developer)

```bash
./gradlew :bookimporter:run
```

## Build distributables

Cross-platform artifacts (on matching OS):

```bash
./gradlew :bookimporter:packageReleaseDistribution
```

Windows EXE (run on Windows with `jpackage`):

```bash
./gradlew :bookimporter:packageWindowsExe
# or from root
./gradlew packageBookImporterWindowsExe
```

## User install (Windows)

1. Download the generated EXE from `generator/bookimporter/build/compose/binaries/main-release/exe/`.
2. Run installer.
3. Open **Seforim DB Book Importer**.
4. Select DB + book folder(s), run **Scan + Preview**, then **Execute Import**.
