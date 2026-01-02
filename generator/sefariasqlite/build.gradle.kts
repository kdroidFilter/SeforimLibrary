plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

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
        "-Xmx8g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200"
    )
}
