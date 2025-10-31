
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
            implementation("com.code972.hebmorph:hebmorph-lucene:10.3.1")
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

    // Top-level main() in BuildFromScratch.kt (now in jvmMain) compiles to this class name
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
    jvmArgs = listOf(
        "-Xmx4g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Removed legacy FTS rebuild task (replaced by upgradeToLucene)

// Upgrade existing DB: drop legacy FTS objects and build Lucene index next to the DB
// Usage:
//   ./gradlew :generator:upgradeToLucene -PseforimDb=/path/to/seforim.db [-PdropPlainText=true]
tasks.register<JavaExec>("upgradeToLucene") {
    group = "application"
    description = "Upgrade an existing DB: drop FTS, build Lucene index. Use -PseforimDb=/path and optional -PdropPlainText=true."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.UpgradeToLuceneKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Pass DB path and optional drop flag via system properties
    if (project.hasProperty("seforimDb")) {
        systemProperty("SEFORIM_DB", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("SEFORIM_DB", System.getenv("SEFORIM_DB"))
    }
    if (project.findProperty("dropPlainText")?.toString()?.equals("true", ignoreCase = true) == true) {
        systemProperty("DROP_PLAINTEXT", "true")
    }

    jvmArgs = listOf(
        "-Xmx2g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

// Removed legacy FTS sanitization task (replaced by Lucene indexer)

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
