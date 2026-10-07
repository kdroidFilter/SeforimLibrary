plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

// Generator forked-JVM heap. Honors -PgeneratorHeap=… (CI lowers it on 16 GB runners).
// Default 10g matches local workstation use; CI sets 5g via the workflow.
val generatorHeap: String = (project.findProperty("generatorHeap") as String?)
    ?: System.getenv("SEFORIM_GENERATOR_HEAP")
    ?: "10g"

// The full Phase-2 LINKER import holds the large sidecar/ref resolution indexes
// while resolving the contextual payload. Keep its heap independently tunable
// so increasing this one memory-bound stage does not inflate every generator
// fork. Stable link IDs themselves are allocated through build_state.db.
val linkerHeap: String = (project.findProperty("linkerHeap") as String?)
    ?: System.getenv("SEFORIM_LINKER_HEAP")
    ?: generatorHeap


kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":dao"))

            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kermit)
            implementation(libs.kotlinx.serialization.json)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(project(":generator-common"))
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.commons.compress)
            implementation(libs.zstd)
            implementation(libs.jackson.core)
            implementation(libs.jackson.databind)
        }
    }
}

tasks.register<JavaExec>("refreshManualLinks") {
    group = "application"
    description = "Refresh manual Otzaria link indices from stable Sefaria refs."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks.RefreshManualLinksMainKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val properties = listOf(
        "manualLinksMode",
        "manualLinksRepo",
        "manualLinksConfig",
        "manualLinksLineage",
        "expectedOldConfigSha256",
        "expectedOldToolCommit",
        "sefariaExport",
        "sefariaReleaseMetadata",
        "sefariaReleaseMetadataSha256",
        "sefariaChangelogDir",
        "seforimToolCommit",
        "manualLinksOutput",
        "anchorUnrelocatableCap",
    )
    properties.forEach { name ->
        if (project.hasProperty(name)) systemProperty(name, project.property(name) as String)
    }

    jvmArgs = listOf("-Xmx4g", "-XX:+UseG1GC")
}

tasks.register<JavaExec>("manualLinksCorpusTest") {
    group = "verification"
    description = "Run the pinned full-corpus manual-links gate with explicit expected counts."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.manuallinks.ManualLinksCorpusMainKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val properties = listOf(
        "manualLinksMode", "manualLinksRepo", "manualLinksConfig", "manualLinksLineage",
        "expectedOldConfigSha256", "expectedOldToolCommit", "sefariaExport",
        "sefariaReleaseMetadata", "sefariaReleaseMetadataSha256", "sefariaChangelogDir",
        "seforimToolCommit", "manualLinksOutput", "expectedTargetSefariaRecords",
        "expectedSourceSefariaRecords", "expectedAnchors", "expectedUnrelocatableAnchors",
        "anchorUnrelocatableCap",
    )
    properties.forEach { name ->
        if (project.hasProperty(name)) systemProperty(name, project.property(name) as String)
    }
    jvmArgs = listOf("-Xmx4g", "-XX:+UseG1GC")
}

tasks.register<JavaExec>("generateSefariaSqlite") {
    group = "application"
    description = "Convert Sefaria export directly into a SQLite DB (one-step pipeline)."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.GenerateSefariaSqliteKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    val dbPath = if (project.hasProperty("seforimDb")) {
        project.property("seforimDb") as String
    } else {
        defaultDbPath
    }
    val exportDir = if (project.hasProperty("exportDir")) {
        project.property("exportDir") as String
    } else {
        null
    }
    args = listOfNotNull(dbPath, exportDir)

    // Optional overrides (the Kotlin entrypoint also supports -D / env)
    if (project.hasProperty("persistDb")) {
        systemProperty("persistDb", project.property("persistDb") as String)
    }
    if (project.hasProperty("inMemoryDb")) {
        systemProperty("inMemoryDb", project.property("inMemoryDb") as String)
    }
    // When set, dump the linker sidecar during the Sefaria import (RefEntry → lineId),
    // so the Phase-2 LINKER importer can run against this build. No effect when unset.
    if (project.hasProperty("linkerSidecar")) {
        systemProperty("linkerSidecarPath", project.property("linkerSidecar") as String)
    }
    // Durable textimages.sefaria.org cache (see SefariaImageEmbedder.defaultCacheDir).
    if (project.hasProperty("imageCacheDir")) {
        systemProperty("imageCacheDir", project.property("imageCacheDir") as String)
    }
    // Where the embedder reports what it could NOT inline; the workflow gates
    // on it (see SefariaImageEmbedder.defaultReportPath). Absolute path please —
    // this JavaExec runs with the project dir as its working directory.
    if (project.hasProperty("imageEmbedReport")) {
        systemProperty("imageEmbedReport", project.property("imageEmbedReport") as String)
    }

    // Optional JVM tuning (similar to generator)
    jvmArgs = listOf(
        "-Xmx$generatorHeap",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200"
    )
}

// Generation seeding — runs after appendOtzaria so Otzaria books are linked too.
// Usage:
//   ./gradlew :sefariasqlite:seedGenerations
//   ./gradlew :sefariasqlite:seedGenerations -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("seedGenerations") {
    group = "application"
    description = "Seed generation table and book_generation links from otzaria-library/ForDB/סדר הדורות.csv."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.SeedGenerationsPostProcessKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    jvmArgs = listOf("-Xmx512m")
}

// Seifim alt-TOC synthesis — runs after all book- and link-writing stages so
// the COMMENTARY links from the Shulchan Aruch are already present.
// Usage:
//   ./gradlew :sefariasqlite:synthesizeSeifimAltToc
//   ./gradlew :sefariasqlite:synthesizeSeifimAltToc -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("synthesizeSeifimAltToc") {
    group = "application"
    description = "Synthesize a Seifim alt-TOC for nosei-kelim on the Shulchan Aruch from COMMENTARY links."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.SynthesizeSeifimAltTocPostProcessKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }
    if (project.hasProperty("buildStatePath")) {
        systemProperty("buildStatePath", project.property("buildStatePath") as String)
    }

    jvmArgs = listOf("-Xmx1g")
}

// Siman names in the Topic alt-TOC ("כותרות") — reads the Topic structures
// written by generateSefariaSqlite and the COMMENTARY links, so it runs with
// the other alt-TOC post-processes.
// Usage:
//   ./gradlew :sefariasqlite:synthesizeSimanNamesAltToc -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("synthesizeSimanNamesAltToc") {
    group = "application"
    description = "Add Shulchan Aruch siman names under the Topic alt-TOC entries."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.SynthesizeSimanNamesAltTocPostProcessKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }
    if (project.hasProperty("buildStatePath")) {
        systemProperty("buildStatePath", project.property("buildStatePath") as String)
    }

    jvmArgs = listOf("-Xmx1g")
}

// Chapters alt-TOC inheritance — runs after synthesizeSeifimAltToc so every
// book-, link- and alt-TOC-writing stage is done before it picks candidates.
// Usage:
//   ./gradlew :sefariasqlite:inheritChaptersAltToc -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("inheritChaptersAltToc") {
    group = "application"
    description = "Give commentaries without an alt-TOC the Chapters structure of their base book."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.InheritChaptersAltTocPostProcessKt")
    // Both post-processes write seforim.db and the attached build-state counters; with
    // org.gradle.parallel they must not overlap. Matched by name: the task may not exist.
    mustRunAfter(tasks.matching { it.name == "synthesizeSimanNamesAltToc" })
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }
    if (project.hasProperty("buildStatePath")) {
        systemProperty("buildStatePath", project.property("buildStatePath") as String)
    }

    jvmArgs = listOf("-Xmx1g")
}

// Phase-2 LINKER importer: resolve ref-based artifacts (LinkerToOtzaria) into clickable links.
// Usage:
//   ./gradlew :sefariasqlite:generateLinkerLinks -PseforimDb=/path/seforim.db \
//       -PlinkerArtifacts=/unpacked/artifacts -PlinkerSidecar=/sidecar.tsv
tasks.register<JavaExec>("generateLinkerLinks") {
    group = "application"
    description = "Resolve LinkerToOtzaria ref-based artifacts into LINKER links + word anchors."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.GenerateLinkerLinksKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        systemProperty("seforimDb", rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath)
    }
    for (p in listOf("linkerArtifacts", "linkerSidecar", "buildStatePath", "linkerStrict")) {
        if (project.hasProperty(p)) systemProperty(p, project.property(p) as String)
    }

    // Phase-2 holds the corpus-wide sidecar/ref indexes. It has its own measured
    // budget while other generator forks remain unchanged; the stable-ID
    // lineage is disk-backed and no longer consumes this heap.
    jvmArgs = listOf("-Xmx$linkerHeap", "-XX:+UseG1GC")
    // Phase-2 rewrites every LINKER link, so the planner statistics are rebuilt after it.
    finalizedBy(":generator-common:analyzeSeforimDb")
}

// Post-processing step to rename categories after all generation is complete
// Usage:
//   ./gradlew :sefariasqlite:renameCategories
//   ./gradlew :sefariasqlite:renameCategories -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("renameCategories") {
    group = "application"
    description = "Apply category renames, book renames, and book moves from otzaria-library/ForDB/."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.RenameCategoriesPostProcessKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Pass DB path if provided
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }
    // Destination leaves of book_moves.csv take their ids from the build state,
    // which is ATTACHed (not loaded), so the small heap stays enough.
    if (project.hasProperty("buildStatePath")) {
        systemProperty("buildStatePath", project.property("buildStatePath") as String)
    }

    jvmArgs = listOf("-Xmx1g", "-XX:+UseG1GC")
}

// Metadata enrichment — sets sourceId and pub dates/places from ForDB/all_metadata.json
// and replaces heShortDesc/heDesc from ForDB/sefaria_metadata_changes.csv. Both assets
// come from the immutable release selected by fordb_latest_pointer.json. Runs after
// the other post-process seeders.
// pub_date/pub_place are created through the IdAllocator, so this loads build_state and
// needs the generator heap (not 512m).
// Usage:
//   ./gradlew :sefariasqlite:seedAllMetadata
//   ./gradlew :sefariasqlite:seedAllMetadata -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("seedAllMetadata") {
    group = "application"
    description = "Seed book descriptions, pub dates/places, and source from all_metadata.json + the changes CSV."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.SeedAllMetadataPostProcessKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    jvmArgs = listOf(
        "-Xmx$generatorHeap",
        "-XX:+UseG1GC",
    )
}

// Publisher banners and protection levels — book_banner / book_protection from the
// optional ForDB book_banners.csv / book_protection.csv. Runs after every stage that
// writes or renames books (it matches final titles).
// Usage:
//   ./gradlew :sefariasqlite:seedBookNotices -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("seedBookNotices") {
    group = "application"
    description = "Seed book_banner and book_protection from ForDB book_banners.csv / book_protection.csv."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.SeedBookNoticesPostProcessKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    jvmArgs = listOf("-Xmx512m")
}

// Dry-run validation of EVERY ForDB rename/move rule against a real seforim.db:
// the exact appliers in the exact build order, in a rolled-back transaction, and
// a complete all-failures report instead of a first-row crash. The DB is never
// mutated. update-fordb runs this on the CANDIDATE archive + the last released DB
// before advancing the immutable pointer; the build's renameCategories pass runs the same
// collector as a pre-mutation preflight.
// Usage:
//   ./gradlew :sefariasqlite:validateForDbInputs -PseforimDb=/path/to/seforim.db \
//       [-PforDbArchive=/path/to/fordb_latest.zip -PforDbSha256=<64-hex>]
//       [-PlibraryTreeBase=/path/base.tree -PlibraryTreeCandidate=/path/candidate.tree]
tasks.register<JavaExec>("validateForDbInputs") {
    group = "verification"
    description = "Dry-run all ForDB rename/move rules against a seforim.db (rollback, all-failures report)."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.ValidateForDbInputsKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }
    // NUL-separated library file lists (release commit, candidate commit); both or neither.
    for (p in listOf("libraryTreeBase", "libraryTreeCandidate")) {
        if (project.hasProperty(p)) systemProperty(p, project.property(p) as String)
    }

    jvmArgs = listOf("-Xmx$generatorHeap", "-XX:+UseG1GC")
}

// Each ForDB post-process runs as its own JVM. When the release build pins the
// ForDB archive (-PforDbArchive + -PforDbSha256), forward both properties to all
// of them so they read byte-identical inputs instead of resolving the repository
// pointer independently. No effect for a local run that omits them.
// generateSefariaSqlite is in the set because the Sefaria import now reads
// sefaria_author_changes.csv while it creates the author rows — too early for a
// post-process to fix without merging rows the id allocator has already handed out.
tasks.matching {
    it.name in setOf(
        "generateSefariaSqlite",
        "renameCategories",
        "seedGenerations",
        "seedAllMetadata",
        "seedBookNotices",
        "validateForDbInputs",
    )
}
    .configureEach {
        this as JavaExec
        for (p in listOf("forDbArchive", "forDbSha256")) {
            if (project.hasProperty(p)) systemProperty(p, project.property(p) as String)
        }
    }
