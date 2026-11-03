plugins {
    alias(libs.plugins.multiplatform)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmTest.dependencies {
            implementation(libs.kotlinx.coroutines.core)
        }

        jvmMain.dependencies {
            implementation(project(":generator-common"))
            implementation(project(":dao"))
            implementation(libs.sqlite.jdbc)
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.zstd)
            implementation(libs.commons.compress)
        }
    }
}

// Write release information to release_info.txt file
// Usage:
//   ./gradlew :packaging:writeReleaseInfo
//   ./gradlew :packaging:writeReleaseInfo -PreleaseName=20251108195010
//   ./gradlew :packaging:writeReleaseInfo -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("writeReleaseInfo") {
    group = "application"
    description = "Write release information (timestamp) to release_info.txt file."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.packaging.WriteReleaseInfoKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Ensure release_info.txt is written next to the same DB that will be packaged.
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    // Pass release name if provided
    if (project.hasProperty("releaseName")) {
        systemProperty("releaseName", project.property("releaseName") as String)
    }

    jvmArgs = listOf("-Xmx256m")
}

// Download lexical.db (from latest SeforimMagicIndexer release) next to seforim.db
// Usage:
//   ./gradlew :packaging:downloadLexicalDb
//   ./gradlew :packaging:downloadLexicalDb -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("downloadLexicalDb") {
    group = "application"
    description = "Download lexical.db from latest SeforimMagicIndexer release next to seforim.db."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.packaging.DownloadLexicalDbKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Place lexical.db next to the same DB that will be packaged.
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    jvmArgs = listOf("-Xmx512m")
}

// Package DB + Lucene indexes into single tar.zst and split
tasks.register<JavaExec>("packageArtifacts") {
    group = "application"
    description = "Create seforim_bundle.tar.zst (DB + indexes + release info) with zstd and split into ~1.9GiB parts."

    dependsOn("jvmJar", "writeReleaseInfo", "downloadLexicalDb")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.packaging.PackageArtifactsKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Pass optional properties if provided
    if (project.hasProperty("seforimDb")) {
        systemProperty("seforimDb", project.property("seforimDb") as String)
    } else if (System.getenv("SEFORIM_DB") != null) {
        systemProperty("seforimDb", System.getenv("SEFORIM_DB"))
    } else {
        // Default to DB under root build/
        val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        systemProperty("seforimDb", defaultDbPath)
    }

    // Output bundle
    if (project.hasProperty("bundleOutput")) {
        systemProperty("bundleOutput", project.property("bundleOutput") as String)
    }
    // Backward-compatible: if -Poutput was provided, pass it through as legacy
    if (project.hasProperty("output")) {
        systemProperty("output", project.property("output") as String)
    }
    if (project.hasProperty("zstdLevel")) {
        systemProperty("zstdLevel", project.property("zstdLevel") as String)
    }
    if (project.hasProperty("zstdWorkers")) {
        systemProperty("zstdWorkers", project.property("zstdWorkers") as String)
    }
    if (project.hasProperty("splitPartBytes")) {
        systemProperty("splitPartBytes", project.property("splitPartBytes") as String)
    }

    jvmArgs = listOf("-Xmx512m")
}

// Write the shipped database from the generator's: line texts compressed, dev-only indexes dropped, 4 KiB pages
// Usage:
//   ./gradlew :packaging:compactDb
//   ./gradlew :packaging:compactDb -PseforimDb=/path/to/seforim.db -PcompactDb=/path/to/seforim.compact.db
//   ./gradlew :packaging:compactDb -PtrainDictionaries=src/jvmMain/resources/line_codec_dict   (new dictionaries)
tasks.register<JavaExec>("compactDb") {
    group = "application"
    description = "Write the compact seforim.db that ships (compressed line texts, no dev-only indexes)."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.packaging.CompactDbKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val defaultDbPath = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("seforimDb", (project.findProperty("seforimDb") as String?) ?: System.getenv("SEFORIM_DB") ?: defaultDbPath)
    listOf("compactDb", "trainDictionaries", "pageSize").forEach { name ->
        project.findProperty(name)?.let { systemProperty(name, it as String) }
    }

    jvmArgs = listOf("-Xmx2g")
}
