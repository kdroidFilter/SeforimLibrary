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
            implementation(libs.kotlinx.serialization.protobuf)
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}

// Build only the precomputed catalog (catalog.pb) from an existing database
// Usage:
//   ./gradlew :catalog:buildCatalog -PseforimDb=/path/to/seforim.db
//   ./gradlew :catalog:buildCatalog  # Uses default build/seforim.db
tasks.register<JavaExec>("buildCatalog") {
    group = "application"
    description = "Build precomputed catalog (catalog.pb) from an existing database."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.catalog.BuildCatalogKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Default DB path in build/
    val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath

    // Allow override via -PseforimDb
    val dbPath = if (project.hasProperty("seforimDb")) {
        project.property("seforimDb") as String
    } else {
        defaultDbPath
    }

    args(dbPath)

    jvmArgs = listOf("-Xmx2g")
}
