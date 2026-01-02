plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            api(project(":dao"))

            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.coroutines.test)
            implementation(libs.kotlinx.serialization.json)
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
        }
    }
}

// Download latest Acronymizer DB into build/acronymizer/acronymizer.db
tasks.register<JavaExec>("downloadAcronymizer") {
    group = "application"
    description = "Download latest SeforimAcronymizer .db (used to populate book acronyms) into build/acronymizer/acronymizer.db"

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.otzariasqlite.DownloadAcronymizerKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    jvmArgs = listOf("-Xmx512m")
}

// Alias: clearer name for the Acronymizer DB download task
tasks.register("downloadAcronymizerDb") {
    group = "application"
    description = "Alias for :otzariasqlite:downloadAcronymizer (downloads acronymizer.db used for book acronyms)."
    dependsOn("downloadAcronymizer")
}

// Download latest Otzaria source into build/otzaria/source
tasks.register<JavaExec>("downloadOtzaria") {
    group = "application"
    description = "Download latest otzaria-library zip and extract to build/otzaria/source"

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.otzariasqlite.DownloadOtzariaKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    jvmArgs = listOf("-Xmx512m")
}

// Phase 1: generate categories/books/lines only
// Usage:
//   ./gradlew :otzariasqlite:generateLines -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria [-PacronymDb=/path/acronymizer.db]
tasks.register<JavaExec>("generateLines") {
    group = "application"
    description = "Phase 1: categories/books/lines only."

    dependsOn("jvmJar")
    dependsOn("downloadAcronymizer")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.otzariasqlite.GenerateLinesKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Always provide a default DB path under build/ so no -PseforimDb is needed
    val defaultDbPath = if (project.hasProperty("seforimDb")) {
        project.property("seforimDb") as String
    } else {
        rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    }
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

    if (project.hasProperty("sourceDir")) {
        systemProperty("sourceDir", project.property("sourceDir") as String)
    }
    if (project.hasProperty("appendExistingDb")) {
        systemProperty("appendExistingDb", project.property("appendExistingDb") as String)
    }
    if (project.hasProperty("baseDb")) {
        systemProperty("baseDb", project.property("baseDb") as String)
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
        "-Xmx10g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Phase 2: process links only
// Usage:
//   ./gradlew :otzariasqlite:generateLinks -PseforimDb=/path/to.db -PsourceDir=/path/to/otzaria
tasks.register<JavaExec>("generateLinks") {
    group = "application"
    description = "Phase 2: process links only."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.otzariasqlite.GenerateLinksKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Default DB path in build/
    val defaultDbPath = if (project.hasProperty("seforimDb")) {
        project.property("seforimDb") as String
    } else {
        rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    }
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
        systemProperty(
            "baseDb",
            when {
                project.hasProperty("baseDb") -> project.property("baseDb") as String
                project.hasProperty("seforimDb") -> project.property("seforimDb") as String
                else -> defaultDbPath
            }
        )
    }

    if (project.hasProperty("sourceDir")) {
        systemProperty("sourceDir", project.property("sourceDir") as String)
    }

    jvmArgs = listOf(
        "-Xmx10g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Append Otzaria content into an existing DB (seeded from the DB itself).
// This is the recommended phase-1 task after :sefariasqlite:generateSefariaSqlite.
tasks.register<JavaExec>("appendOtzariaLines") {
    group = "application"
    description = "Phase 1: append Otzaria categories/books/lines into an existing SQLite DB."

    dependsOn("jvmJar")
    dependsOn("downloadAcronymizer")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.otzariasqlite.GenerateLinesKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    val baseDb = when {
        project.hasProperty("baseDb") -> project.property("baseDb") as String
        project.hasProperty("seforimDb") -> project.property("seforimDb") as String
        else -> defaultDbPath
    }
    val persistDb = if (project.hasProperty("persistDb")) project.property("persistDb") as String else baseDb

    // Use in-memory DB for speed; seed from baseDb; persist to persistDb (can equal baseDb)
    args(":memory:")
    systemProperty("appendExistingDb", "true")
    systemProperty("baseDb", baseDb)
    systemProperty("persistDb", persistDb)

    val defaultAcronymDb = layout.buildDirectory.file("acronymizer/acronymizer.db").get().asFile.absolutePath
    if (project.hasProperty("acronymDb")) {
        systemProperty("acronymDb", project.property("acronymDb") as String)
    } else {
        systemProperty("acronymDb", defaultAcronymDb)
    }
    if (project.hasProperty("sourceDir")) {
        systemProperty("sourceDir", project.property("sourceDir") as String)
    }

    jvmArgs = listOf(
        "-Xmx10g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Append Otzaria links into an existing DB (seeded from the DB itself).
tasks.register<JavaExec>("appendOtzariaLinks") {
    group = "application"
    description = "Phase 2: append Otzaria links into an existing SQLite DB (requires lines/books to exist)."

    dependsOn("jvmJar")
    dependsOn("appendOtzariaLines")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.otzariasqlite.GenerateLinksKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    val baseDb = when {
        project.hasProperty("baseDb") -> project.property("baseDb") as String
        project.hasProperty("seforimDb") -> project.property("seforimDb") as String
        else -> defaultDbPath
    }
    val persistDb = if (project.hasProperty("persistDb")) project.property("persistDb") as String else baseDb

    args(":memory:")
    systemProperty("baseDb", persistDb)
    systemProperty("persistDb", persistDb)

    if (project.hasProperty("sourceDir")) {
        systemProperty("sourceDir", project.property("sourceDir") as String)
    }

    jvmArgs = listOf(
        "-Xmx10g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

tasks.register("appendOtzaria") {
    group = "application"
    description = "Append Otzaria lines + links into an existing DB (wrapper task)."
    dependsOn("appendOtzariaLinks")
}
