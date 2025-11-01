
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
            implementation("org.apache.lucene:lucene-core:10.3.1")
            implementation("org.apache.lucene:lucene-analysis-common:10.3.1")
            implementation("org.apache.lucene:lucene-queryparser:10.3.1")
            implementation("org.apache.lucene:lucene-highlighter:10.3.1")
            // HebMorph Lucene integration (substituted by included build SeforimLibrary/HebMorph/java)
            api("com.code972.hebmorph:hebmorph-lucene:10.3.1")
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

// Build Lucene index using HebMorph analyzer against an existing SQLite DB
// Usage:
//   ./gradlew :generator:buildHebMorphIndex -PseforimDb=/path/to/seforim.db \
//     [-Phebmorph.hspell.path=/path/to/hspell-data-files]
tasks.register<JavaExec>("buildHebMorphIndex") {
    group = "application"
    description = "Build Lucene index using HebMorph analyzer. Requires -PseforimDb and hspell data path."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.BuildHebMorphIndexKt")
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

    // Optional: pass explicit hspell path
    if (project.hasProperty("hebmorph.hspell.path")) {
        systemProperty("hebmorph.hspell.path", project.property("hebmorph.hspell.path") as String)
    } else if (System.getenv("HEBMORPH_HSPELL_PATH") != null) {
        systemProperty("HEBMORPH_HSPELL_PATH", System.getenv("HEBMORPH_HSPELL_PATH"))
    }

    // Generous heap for indexing
    jvmArgs = listOf(
        "-Xmx4g",
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
    // arg0: DB path only; sourceDir omitted so Kotlin will auto-download Otzaria
    args(defaultDbPath)

    // Provide acronym DB via system property so Kotlin picks it up
    if (project.hasProperty("acronymDb")) {
        systemProperty("acronymDb", project.property("acronymDb") as String)
    } else {
        systemProperty("acronymDb", defaultAcronymDb)
    }

    jvmArgs = listOf(
        "-Xmx4g",
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
    args(defaultDbPath)

    jvmArgs = listOf(
        "-Xmx2g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Phase 3: build Lucene indexes only
// Usage:
//   ./gradlew :generator:buildLuceneIndex -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("buildLuceneIndex") {
    group = "application"
    description = "Phase 3: build Lucene index next to the DB. Use -PseforimDb."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.BuildLuceneIndexKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Default DB path in build/
    val defaultDbPath = layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("seforimDb", defaultDbPath)

    jvmArgs = listOf(
        "-Xmx4g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}
// (no imports at end)
