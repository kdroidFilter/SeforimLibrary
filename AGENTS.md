# Repository Guidelines

## Project Structure & Module Organization
- `core/`: shared models and extensions (`src/commonMain/kotlin/...`).
- `dao/`: SQLDelight schema + repositories (`src/commonMain/sqldelight/...`).
- `generator/`: grouping folder for generation tooling modules:
  - `generator/sefariasqlite/`: one-step Sefaria → SQLite pipeline.
  - `generator/otzariasqlite/`: JVM tools to download/import Otzaria and append it into an existing DB.
  - `generator/catalog/`: JVM tool to build the precomputed `catalog.pb` from a SQLite DB.
  - `generator/searchindex/`: JVM tooling to build Lucene indexes from a SQLite DB.
  - `generator/packaging/`: JVM tooling to write release info and package DB + indexes into a bundle.
- `SeforimMagicIndexer/`: included build with indexing tooling (see `settings.gradle.kts`).
- Build outputs live under `*/build/` (gitignored).

## Build, Test, and Development Commands
- `./gradlew build`: build all modules.
- `./gradlew :core:jvmTest`: run JVM tests for a single module (replace `core` as needed).
- `./gradlew :core:allTests`: run tests for all KMP targets for that module.
- `./gradlew generateSeforimDb`: Sefaria → SQLite, then append Otzaria + rebuild catalog.
- `./gradlew :otzariasqlite:generateLines`: phase 1 DB generation (creates/persists `build/seforim.db` by default).
- `./gradlew :otzariasqlite:generateLinks`: phase 2 link processing.
- `./gradlew :otzariasqlite:appendOtzaria`: append Otzaria lines + links into an existing DB (wrapper task).
- `./gradlew :catalog:buildCatalog`: rebuild `catalog.pb` from `build/seforim.db` (override with `-PseforimDb`).
- `./gradlew :searchindex:buildLuceneIndexDefault`: build Lucene indexes next to `build/seforim.db` (override with `-PseforimDb`).
- `./gradlew :packaging:downloadLexicalDb`: download `lexical.db` next to `build/seforim.db` (override with `-PseforimDb`).
- `./gradlew :packaging:packageArtifacts`: bundle DB + catalog + indexes + release info into a `.tar.zst`.
- `./gradlew :sefariasqlite:generateSefariaSqlite`: generate SQLite DB directly from Sefaria.
- `./gradlew packageSeforimBundle`: full pipeline (DB + catalog + indexes + bundle).

## Coding Style & Naming Conventions
- Kotlin style is “official” (`kotlin.code.style=official`); 4-space indent, no tabs.
- Names: packages `lowercase`, classes `PascalCase`, functions/properties `camelCase`.
- Keep public API documented with KDoc; prefer immutable `data class`es.
- SQLDelight: keep queries in `dao/src/commonMain/sqldelight/...` and name files `*Queries.sq`.

## Testing Guidelines
- Tests use `kotlin("test")` (multiplatform). Add tests under `src/commonTest/kotlin` or `src/jvmTest/kotlin`.
- Don’t commit large generated artifacts (DBs, Lucene indexes); use `build/` and existing `.gitignore` patterns (`*.db`, `otzaria_latest`, `*/build`).

## Commit & Pull Request Guidelines
- History mixes plain imperative messages and Conventional Commits; prefer `type(scope): summary` (e.g., `feat(searchindex): add lucene index step`).
- PRs should include: what changed, the Gradle command(s) to verify, and notes on schema/API changes (including any required regeneration steps).

## Configuration Notes
- Android targets require an Android SDK; set `sdk.dir` in `local.properties` (ignored) or `ANDROID_HOME`.
- JVM toolchain version is defined in `gradle/libs.versions.toml` (`jvmToolchain`).
