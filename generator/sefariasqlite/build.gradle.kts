plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

// Generator forked-JVM heap. Honors -PgeneratorHeap=… (CI lowers it on 16 GB runners).
// Default 10g matches local workstation use; CI sets 5g via the workflow.
val generatorHeap: String = (project.findProperty("generatorHeap") as String?)
    ?: System.getenv("SEFORIM_GENERATOR_HEAP")
    ?: "10g"


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
        }
    }
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

    // Optional JVM tuning (similar to generator)
    jvmArgs = listOf(
        "-Xmx$generatorHeap",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200"
    )
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

    jvmArgs = listOf("-Xmx256m")
}
