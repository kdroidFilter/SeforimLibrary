
plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(21)

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            api(project(":dao"))

            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.coroutines.test)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kermit)
            implementation("org.jsoup:jsoup:1.17.2")
            implementation("org.slf4j:slf4j-simple:2.0.17")
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }


        jvmMain.dependencies {
            implementation(libs.kotlinx.coroutines.swing)
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(project(":analysis"))
            implementation(libs.lucene.core)
            implementation(libs.lucene.analysis.common)
            implementation(libs.lucene.queryparser)
            implementation(libs.lucene.highlighter)
            // HebMorph Lucene integration (substituted by included build SeforimLibrary/HebMorph/java)
            api("com.code972.hebmorph:hebmorph-lucene:10.3.1")
            implementation("com.github.luben:zstd-jni:1.5.7-6")
            implementation("org.apache.commons:commons-compress:1.26.2")
        }

    }

}

// Download latest Acronymizer DB into build/acronymizer/acronymizer.db
tasks.register<JavaExec>("downloadAcronymizer") {
    group = "application"
    description = "Download latest SeforimAcronymizer .db into build/acronymizer/acronymizer.db"

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.DownloadAcronymizerKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    jvmArgs = listOf("-Xmx512m")
}

// Download latest Otzaria source into build/otzaria/source
tasks.register<JavaExec>("downloadOtzaria") {
    group = "application"
    description = "Download latest otzaria-library zip and extract to build/otzaria/source"

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.DownloadOtzariaKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    jvmArgs = listOf("-Xmx512m")
}

// Package DB (.zst) and Lucene indexes (.tar.zst)
tasks.register<JavaExec>("packageArtifacts") {
    group = "application"
    description = "Compress seforim.db to .zst and Lucene indexes to .tar.zst with zstd."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.PackageArtifactsKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Pass optional properties if provided
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    }
    // New properties for separate outputs
    if (project.hasProperty("dbOutput")) {
        systemProperty("dbOutput", project.property("dbOutput") as String)
    }
    if (project.hasProperty("indexesOutput")) {
        systemProperty("indexesOutput", project.property("indexesOutput") as String)
    }
    // Backward-compatible: if -Poutput was provided, map it to indexesOutput
    if (project.hasProperty("output")) {
        systemProperty("output", project.property("output") as String)
    }
    if (project.hasProperty("zstdLevel")) {
        systemProperty("zstdLevel", project.property("zstdLevel") as String)
    }

    jvmArgs = listOf("-Xmx512m")
}

// Build Lucene index using StandardAnalyzer (no HebMorph)
// Usage:
//   ./gradlew :generator:buildLuceneIndexDefault -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("buildLuceneIndexDefault") {
    group = "application"
    description = "Build Lucene index using StandardAnalyzer (no HebMorph). Requires -PseforimDb."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.BuildLuceneIndexKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Pass DB path as system property recognized by the Kotlin entrypoint
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("SEFORIM_DB", System.getenv("SEFORIM_DB"))
    } else {
        // Default to DB under build/
        val defaultDbPath = layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    // Prefer in-memory DB for faster reads (override with -PinMemoryDb=false)
    val inMemory = project.findProperty("inMemoryDb") != "false"
    if (inMemory) {
        systemProperty("inMemoryDb", "true")
    }

    jvmArgs = listOf(
        "-Xmx48g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}


// Phase 1: generate categories/books/lines only
// Usage:
//   ./gradlew :generator:generateLines -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria [-PacronymDb=/path/acronymizer.db]
tasks.register<JavaExec>("generateLines") {
    group = "application"
    description = "Phase 1: categories/books/lines only."

    dependsOn("jvmJar")
    dependsOn("downloadAcronymizer")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.GenerateLinesKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Always provide a default DB path under build/ so no -PseforimDb is needed
    val defaultDbPath = layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    val defaultAcronymDb = layout.buildDirectory.file("acronymizer/acronymizer.db").get().asFile.absolutePath
    // In-memory DB generation enabled by default (override with -PinMemoryDb=false)
    val inMemory = project.findProperty("inMemoryDb") != "false"
    val cliDbPath = if (inMemory) ":memory:" else defaultDbPath
    // arg0: DB path only; sourceDir omitted so Kotlin will auto-download Otzaria
    args(cliDbPath)

    // Provide acronym DB via system property so Kotlin picks it up
    if (project.hasProperty("acronymDb")) {
        systemProperty("acronymDb", project.property("acronymDb") as String)
    } else {
        systemProperty("acronymDb", defaultAcronymDb)
    }

    // If in-memory DB is used, persist destination (default to build/seforim.db)
    if (inMemory) {
        if (project.hasProperty("persistDb")) {
            systemProperty("persistDb", project.property("persistDb") as String)
        } else {
            systemProperty("persistDb", defaultDbPath)
        }
    }

    jvmArgs = listOf(
        "-Xmx48g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Phase 2: process links only
// Usage:
//   ./gradlew :generator:generateLinks -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria
tasks.register<JavaExec>("generateLinks") {
    group = "application"
    description = "Phase 2: process links only."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.GenerateLinksKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Default DB path in build/
    val defaultDbPath = layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    // In-memory DB generation enabled by default (override with -PinMemoryDb=false)
    val inMemory = project.findProperty("inMemoryDb") != "false"
    val cliDbPath = if (inMemory) ":memory:" else defaultDbPath
    args(cliDbPath)

    if (inMemory) {
        if (project.hasProperty("persistDb")) {
            systemProperty("persistDb", project.property("persistDb") as String)
        } else {
            systemProperty("persistDb", defaultDbPath)
        }
        // Seed from base DB on disk by default
        if (project.hasProperty("seforimDb")) {
            systemProperty("baseDb", project.property("seforimDb") as String)
        } else {
            systemProperty("baseDb", defaultDbPath)
        }
    }

    jvmArgs = listOf(
        "-Xmx48g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Build HebMorph-based Lucene index (uses HebrewExactAnalyzer/LegacyIndexingAnalyzer)
// Usage:
//   ./gradlew :generator:buildHebMorphIndex -PseforimDb=/path/to/seforim.db \
//       [-Phebmorph.hspell.path=/path/to/hspell-data-files] \
//       [-PinMemoryDb=true] [-PuseTmpfsForIndex=true] [-PtmpfsDir=/dev/shm]
tasks.register<JavaExec>("buildHebMorphIndex") {
    group = "application"
    description = "Build Lucene text+lookup indexes using HebMorph (field text_he). Requires -PseforimDb."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.BuildHebMorphIndexKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Database path
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("SEFORIM_DB", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    // Optional: hspell path for HebMorph
    if (project.hasProperty("hebmorph.hspell.path")) {
        systemProperty("hebmorph.hspell.path", project.property("hebmorph.hspell.path") as String)
    } else if (System.getenv("HEBMORPH_HSPELL_PATH") != null) {
        systemProperty("hebmorph.hspell.path", System.getenv("HEBMORPH_HSPELL_PATH"))
    }

    // Use in-memory DB for faster reads by default (override with -PinMemoryDb=false)
    val inMemory = project.findProperty("inMemoryDb") != "false"
    if (inMemory) systemProperty("inMemoryDb", "true")

    // Optional tmpfs usage for faster index writes
    if (project.hasProperty("useTmpfsForIndex")) {
        systemProperty("useTmpfsForIndex", project.property("useTmpfsForIndex") as String)
    }
    if (project.hasProperty("tmpfsDir")) {
        systemProperty("tmpfsDir", project.property("tmpfsDir") as String)
    }
    if (project.hasProperty("copyDbToTmpfs")) {
        systemProperty("copyDbToTmpfs", project.property("copyDbToTmpfs") as String)
    }
    if (project.hasProperty("indexThreads")) {
        systemProperty("indexThreads", project.property("indexThreads") as String)
    }

    jvmArgs = listOf(
        "-Xmx48g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}
