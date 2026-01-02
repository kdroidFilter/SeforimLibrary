plugins {
    alias(libs.plugins.multiplatform)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(project(":core"))
            implementation(project(":dao"))
            implementation(libs.sqlDelight.driver.sqlite)
            implementation("org.jsoup:jsoup:1.17.2")

            api(libs.lucene.core)
            api(libs.lucene.analysis.common)
        }
    }
}

// Build Lucene index using StandardAnalyzer
// Usage:
//   ./gradlew :searchindex:buildLuceneIndexDefault -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("buildLuceneIndexDefault") {
    group = "application"
    description = "Build Lucene index using StandardAnalyzer. Requires -PseforimDb."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.searchindex.BuildLuceneIndexKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Pass DB path as system property recognized by the Kotlin entrypoint
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        // Default to DB under root build/
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    // Prefer in-memory DB for faster reads (override with -PinMemoryDb=false)
    val inMemory = project.findProperty("inMemoryDb") != "false"
    if (inMemory) {
        systemProperty("inMemoryDb", "true")
    }

    jvmArgs = listOf(
        "-Xmx10g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

