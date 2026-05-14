plugins {
    alias(libs.plugins.multiplatform)
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
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.jdbc)
            implementation(libs.kotlinx.coroutines.core)
            implementation(project(":dao"))
            implementation(libs.jsoup)
        }

        jvmTest.dependencies {
            implementation(kotlin("test-junit"))
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}

tasks.register<JavaExec>("producePatchAndVerify") {
    group = "application"
    description = "Produce patch.db from (prevDb, newDb) and verify apply reproduces newDb's logical hash."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.PatchPipelineCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Skip cleanly when no previous release was supplied — this is the
    // first-release path, where there's no prior DB to diff against.
    // Without this, publishRelease for a v1 release would fail at the
    // finalizedBy step with a confusing "prev db missing" error.
    // Capture properties at config time so onlyIf {} stays config-cache safe.
    val hasPrevReleaseDb = project.findProperty("prevDb") != null ||
        project.findProperty("prevReleaseDb") != null ||
        rootProject.findProperty("prevReleaseDb") != null
    onlyIf {
        if (!hasPrevReleaseDb) {
            logger.lifecycle(
                "producePatchAndVerify: no prevDb / prevReleaseDb supplied — skipping " +
                    "(first release? ship the full bundle and re-run with -PprevReleaseDb " +
                    "next time)."
            )
        }
        hasPrevReleaseDb
    }

    val prev = project.findProperty("prevDb") as String?
    val new = project.findProperty("newDb") as String?
    val out = project.findProperty("out") as String?
        ?: rootProject.layout.buildDirectory.file("patch.db").get().asFile.absolutePath
    if (prev != null) systemProperty("prevDb", prev)
    if (new != null) systemProperty("newDb", new)
    systemProperty("out", out)
    project.findProperty("fromVersion")?.let { systemProperty("fromVersion", it as String) }
    project.findProperty("toVersion")?.let { systemProperty("toVersion", it as String) }
    // Optional release-meta knobs + the new catalog.pb path (defaults to
    // <out>.resolveSibling("catalog.pb") when unset, which matches the
    // standard layout produced by :catalog:buildCatalog).
    listOf(
        "releaseMeta", "fullBundleUrl", "fullBundleSha", "fullBundleSize",
        "manifestBaseUrl", "fromSchemaVersion", "toSchemaVersion",
        "catalogPb",
    ).forEach { key ->
        project.findProperty(key)?.let { systemProperty(key, it as String) }
    }

    jvmArgs = listOf("-Xmx$generatorHeap", "-XX:+UseG1GC")
}

tasks.register<JavaExec>("compareLogicalContent") {
    group = "verification"
    description = "Per-table LogicalContentHasher comparison between two seforim.db files."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.CompareLogicalContentCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val left = project.findProperty("leftDb") as String? ?: error("-PleftDb= missing")
    val right = project.findProperty("rightDb") as String? ?: error("-PrightDb= missing")
    systemProperty("leftDb", left)
    systemProperty("rightDb", right)
    jvmArgs = listOf("-Xmx$generatorHeap", "-XX:+UseG1GC")
}

tasks.register<JavaExec>("stampSchemaVersion") {
    group = "application"
    description = "Stamps schema_meta.db_version + db_schema_version into the freshly-built seforim.db so the client can read the current release version."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.StampSchemaVersionCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    // Default the dbPath to <root>/build/seforim.db (where generateSeforimDb
    // emits) so the operator only needs to pass -PdbVersion in the common case.
    val dbPath = project.findProperty("dbPath") as String?
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    // dbVersion falls back to toVersion (release timeline) for convenience
    // when called through publishRelease, then to "1" for the first release.
    val dbVersion = (project.findProperty("dbVersion") as String?)
        ?: (project.findProperty("toVersion") as String?)
        ?: "1"
    systemProperty("dbPath", dbPath)
    systemProperty("dbVersion", dbVersion)
    project.findProperty("dbSchemaVersion")?.let { systemProperty("dbSchemaVersion", it as String) }
    jvmArgs = listOf("-Xmx512m")
}

tasks.register<JavaExec>("diagnoseHashMismatch") {
    group = "verification"
    description = "Apply a patch.db onto a copy of prevDb and report which tables hash-differ from newDb."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.DiagnoseHashMismatchCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val prev = project.findProperty("prevDb") as String? ?: error("-PprevDb= missing")
    val new = project.findProperty("newDb") as String? ?: error("-PnewDb= missing")
    val patch = project.findProperty("patch") as String? ?: error("-Ppatch= missing")
    systemProperty("prevDb", prev)
    systemProperty("newDb", new)
    systemProperty("patch", patch)
    jvmArgs = listOf("-Xmx$generatorHeap", "-XX:+UseG1GC")
}
