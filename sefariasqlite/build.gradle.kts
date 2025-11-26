plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":generator"))
            api(project(":sefaria"))

            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kermit)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.protobuf)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}

tasks.register<JavaExec>("generateSefariaSqlite") {
    group = "application"
    description = "Convert Sefaria export directly into a SQLite DB (one-step pipeline)."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.GenerateSefariaSqliteKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Optional JVM tuning (similar to generator)
    jvmArgs = listOf(
        "-Xmx8g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200"
    )
}

// Build only the precomputed catalog (catalog.pb) from an existing Sefaria database
// Usage:
//   ./gradlew :sefariasqlite:buildCatalog -PseforimDb=/path/to/seforim.db
//   ./gradlew :sefariasqlite:buildCatalog  # Uses default build/seforim.db
tasks.register<JavaExec>("buildCatalog") {
    group = "application"
    description = "Build precomputed catalog (catalog.pb) from an existing Sefaria database."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefariasqlite.BuildCatalogKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Default DB path in build/
    val defaultDbPath = layout.buildDirectory.file("seforim.db").get().asFile.absolutePath

    // Allow override via -PseforimDb
    val dbPath = if (project.hasProperty("seforimDb")) {
        project.property("seforimDb") as String
    } else {
        defaultDbPath
    }

    args(dbPath)

    jvmArgs = listOf("-Xmx2g")
}
