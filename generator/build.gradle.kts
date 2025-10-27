
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
        }

    }

}


// Run the generator's main (BuildFromScratch.kt) on the JVM target
// Usage (from repo root):
//   SEFORIM_DB=/path/to/seforim.db \
//   OTZARIA_SOURCE_DIR=/path/to/otzaria_source \
//   ./gradlew :generator:runBuildFromScratch --no-daemon
tasks.register<JavaExec>("runBuildFromScratch") {
    group = "application"
    description = "Run the Otzaria DB generator (BuildFromScratch). Requires SEFORIM_DB and OTZARIA_SOURCE_DIR."

    // Ensure JVM classes/jar are built
    dependsOn("jvmJar")

    // Top-level main() in BuildFromScratch.kt compiles to this class name
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.BuildFromScratchKt")

    // Use the JVM runtime classpath for the multiplatform 'jvm' target
    // and include the project's jar itself
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Allow passing args: db, source, optional acronym DB
    if (project.hasProperty("seforimDb")) {
        args(project.property("seforimDb") as String)
    }
    if (project.hasProperty("sourceDir")) {
        if (!project.hasProperty("seforimDb")) args("")
        args(project.property("sourceDir") as String)
    }
    if (project.hasProperty("acronymDb")) {
        if (!project.hasProperty("seforimDb")) args("")
        if (!project.hasProperty("sourceDir")) args("")
        args(project.property("acronymDb") as String)
    }

    // Give the process a reasonable heap and GC for heavy indexing
    jvmArgs = listOf("-Xmx4g", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200")
}

// Utility: rebuild category_closure + FTS5 for an existing DB
// Usage:
//   ./gradlew :generator:rebuildClosureAndFts -PseforimDb=/path/to/seforim.db
// or set env var SEFORIM_DB and run the task without -P.
tasks.register<JavaExec>("rebuildClosureAndFts") {
    group = "application"
    description = "Rebuild category_closure and FTS5 index for an existing DB. Use -PseforimDb=/path or SEFORIM_DB env."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.RebuildClosureAndFtsKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Propagate DB path to the launched JVM
    if (project.hasProperty("seforimDb")) {
        systemProperty("SEFORIM_DB", project.property("seforimDb") as String)
    }

    // Give the process a reasonable heap
    jvmArgs = listOf("-Xmx1g", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200")
}

// Utility: sanitize plainText (remove nikud + teamim, replace maqaf), then rebuild FTS
// Usage:
//   ./gradlew :generator:sanitizePlainText -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("sanitizePlainText") {
    group = "application"
    description = "Sanitize line.plainText in an existing DB (remove diacritics), then rebuild FTS. Use -PseforimDb=/path or SEFORIM_DB env."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.SanitizePlainTextKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        systemProperty("SEFORIM_DB", project.property("seforimDb") as String)
    }

    jvmArgs = listOf("-Xmx2g", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200")
}

// Utility: migrate acronyms from external Acronymizer DB into the main DB table book_acronym
// Usage:
//   ./gradlew :generator:migrateAcronyms -PseforimDb=/path/to/seforim.db -PacronymDb=/path/to/acronymizer.db
tasks.register<JavaExec>("migrateAcronyms") {
    group = "application"
    description = "Populate book_acronym from Acronymizer DB. Use -PseforimDb and -PacronymDb or env vars SEFORIM_DB/ACRONYM_DB."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.AcronymMigrationKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    if (project.hasProperty("seforimDb")) {
        args(project.property("seforimDb") as String)
    }
    if (project.hasProperty("acronymDb")) {
        if (!project.hasProperty("seforimDb")) {
            // placeholder to satisfy arg order
            args("")
        }
        args(project.property("acronymDb") as String)
    }

    jvmArgs = listOf("-Xmx1g")
}
