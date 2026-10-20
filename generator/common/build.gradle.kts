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
            implementation(libs.zstd)
        }

        jvmTest.dependencies {
            implementation(kotlin("test-junit"))
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.kotlinx.serialization.json)
            implementation(project(":core"))
        }
    }
}

// ─── One definition of how the patch pipeline is launched ──────────────────
// `producePatchAndVerify` below is the Gradle entry point. The release
// workflow's patch fan runs the SAME CLI directly with `java`, because it
// produces several anchors at once and two concurrent Gradle builds in one
// project directory would contend on the project cache dir, the configuration
// cache and jvmJar's own output file — which a running fork holds open.
// Both paths read the three definitions below and `patchPipelineLauncher`
// publishes them verbatim, so the direct invocation cannot drift from the task
// it stands in for.
val patchPipelineMainClass = "io.github.kdroidfilter.seforimlibrary.common.patch.PatchPipelineCliKt"
// The fan writes a schema barrier (writeSchemaBarrier) instead of a delta for an
// anchor no delta can leave, on the same classpath and JVM args.
val schemaBarrierMainClass = "io.github.kdroidfilter.seforimlibrary.common.patch.SchemaBarrierCliKt"
// --enable-native-access: the fan runs this CLI through `java` directly (see
// the launcher below), so it never passes through the root build's JavaExec
// argument provider; without the flag every fan fork re-prints JDK 25's
// four-line sqlite-jdbc restricted-method warning (16 log lines across the 4
// anchors of run 34024655297). It changes no bytecode and no output.
val patchPipelineJvmArgs =
    listOf("-Xmx$generatorHeap", "-XX:+UseG1GC", "--enable-native-access=ALL-UNNAMED")
// A function, not a val: like every other JavaExec here it must resolve inside
// a task's configuration block, never at script-evaluation time.
fun patchPipelineClasspath() = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

tasks.register<JavaExec>("producePatchAndVerify") {
    group = "application"
    description = "Produce patch.db from (prevDb, newDb) and verify apply reproduces newDb's logical hash."

    dependsOn("jvmJar")
    mainClass.set(patchPipelineMainClass)
    classpath = patchPipelineClasspath()

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
        "catalogPb", "zstdLevel", "maxDeltaUncompressedRatio",
    ).forEach { key ->
        project.findProperty(key)?.let { systemProperty(key, it as String) }
    }

    jvmArgs = patchPipelineJvmArgs

    // Exit-code contract with PatchPipelineCli:
    //   0 → produced + verified.
    //   3 → UnpatchableAnchor: this (prev, new) pair cannot be expressed as a
    //       delta at all (missing PK column / column dropped without a
    //       db_schema_version bump). The CLI wrote "<out>.unpatchable" with
    //       the reason; the build SUCCEEDS so the release fan can skip just
    //       this anchor by testing for that marker.
    //   * → a real failure; rethrown so the build fails as before.
    // Gradle itself always exits 1 on a failed build, so the marker file — not
    // Gradle's own exit code — is what carries the typed outcome to the caller.
    isIgnoreExitValue = true
    val unpatchableMarker = file("$out.unpatchable")
    val execOutcome = executionResult
    doLast {
        when (val rc = execOutcome.get().exitValue) {
            0 -> Unit
            3 -> logger.warn(
                "producePatchAndVerify: anchor is NOT patchable (exit 3) — " +
                    (unpatchableMarker.takeIf { it.isFile }?.readText()?.trim()
                        ?: "see PatchPipelineCli output") +
                    " — skipping this anchor; marker at $unpatchableMarker"
            )
            else -> throw GradleException("producePatchAndVerify failed with exit code $rc")
        }
    }
}

// Publishes exactly what `producePatchAndVerify` would fork, so a caller can
// run the CLI itself: the release workflow's patch fan produces several
// anchors concurrently, which Gradle cannot do inside one project directory.
// Everything here comes from the same definitions the task above uses, so the
// two can never disagree about main class, heap or classpath;
// `javaVersion` lets the caller assert the `java` it is about to run is the
// toolchain Gradle would have forked.
tasks.register("patchPipelineLauncher") {
    group = "application"
    description = "Write build/patch-pipeline-launcher.properties — the main class, JVM args, toolchain version and runtime classpath producePatchAndVerify forks with."
    dependsOn("jvmJar")
    val launcherMainClass = patchPipelineMainClass
    val launcherBarrierMainClass = schemaBarrierMainClass
    val launcherJvmArgs = patchPipelineJvmArgs
    val launcherClasspath = patchPipelineClasspath()
    val launcherJavaVersion = libs.versions.jvmToolchain.get()
    val specFile = layout.buildDirectory.file("patch-pipeline-launcher.properties")
    inputs.property("mainClass", launcherMainClass)
    inputs.property("barrierMainClass", launcherBarrierMainClass)
    inputs.property("jvmArgs", launcherJvmArgs)
    inputs.property("javaVersion", launcherJavaVersion)
    inputs.files(launcherClasspath)
    outputs.file(specFile)
    doLast {
        val target = specFile.get().asFile
        target.parentFile.mkdirs()
        target.writeText(
            "mainClass=$launcherMainClass\n" +
                "jvmArgs=${launcherJvmArgs.joinToString(" ")}\n" +
                "javaVersion=$launcherJavaVersion\n" +
                "barrierMainClass=$launcherBarrierMainClass\n" +
                "classpath=${launcherClasspath.asPath}\n",
        )
    }
}

tasks.register<JavaExec>("compareLogicalContent") {
    group = "verification"
    description = "Per-table LogicalContentHasher comparison between two seforim.db files."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.CompareLogicalContentCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    // Defer validation to execution — IntelliJ's Gradle sync materialises
    // every registered task, so any error() at config-time breaks sync.
    val left = project.findProperty("leftDb") as String?
    val right = project.findProperty("rightDb") as String?
    if (left != null) systemProperty("leftDb", left)
    if (right != null) systemProperty("rightDb", right)
    doFirst {
        checkNotNull(left) { "-PleftDb= missing" }
        checkNotNull(right) { "-PrightDb= missing" }
    }
    jvmArgs = listOf("-Xmx$generatorHeap", "-XX:+UseG1GC")
}

tasks.register<JavaExec>("splitLineContent") {
    group = "application"
    description = "Converts the finished seforim.db to schema 6: line text into line_content, version_line.content NULL when identical."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.SplitLineContentCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("dbPath", dbPath)
    listOf("chunkRows", "vacuumInto").forEach { key ->
        project.findProperty(key)?.let { systemProperty(key, it as String) }
    }
    jvmArgs = listOf("-Xmx1g")
}

tasks.register<JavaExec>("compressLineContent") {
    group = "application"
    description = "Compresses line_content/version_line text of the finished seforim.db into zstd frames with the bundled dictionary."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.CompressLineContentCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("dbPath", dbPath)
    listOf("chunkRows", "threads").forEach { key ->
        project.findProperty(key)?.let { systemProperty(key, it as String) }
    }
    jvmArgs = listOf("-Xmx2g")
}

tasks.register<JavaExec>("compactSeforimDb") {
    group = "application"
    description = "Drops the freelist of the finished seforim.db via VACUUM INTO on -PscratchDir (needs ~one compacted DB free there)."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.CompactDbCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("dbPath", dbPath)
    project.findProperty("scratchDir")?.let { systemProperty("scratchDir", it as String) }
    jvmArgs = listOf("-Xmx512m")
}

tasks.register<JavaExec>("writeSchemaBarrier") {
    group = "application"
    description = "Writes the full-rebase barrier patch-v<from>-v<to>.db.zst + manifest for an anchor older than the split schema."
    dependsOn("jvmJar")
    mainClass.set(schemaBarrierMainClass)
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    listOf("out", "fromVersion", "toVersion", "fromSchemaVersion", "toSchemaVersion").forEach { key ->
        project.findProperty(key)?.let { systemProperty(key, it as String) }
    }
    jvmArgs = listOf("-Xmx256m")
}

tasks.register<JavaExec>("stampSchemaVersion") {
    group = "application"
    description = "Stamps schema_meta.db_version + db_schema_version into the freshly-built seforim.db so the client can read the current release version."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.StampSchemaVersionCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    // Default the dbPath to <root>/build/seforim.db (where generateSeforimDb
    // emits) so the operator only needs to pass -PdbVersion in the common case.
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
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

tasks.register<JavaExec>("analyzeSeforimDb") {
    group = "application"
    description = "Runs ANALYZE on the finished seforim.db so the app's query planner has sqlite_stat1."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.AnalyzeDbCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("dbPath", dbPath)
    jvmArgs = listOf("-Xmx512m")
}

tasks.register<JavaExec>("buildLineRefIndex") {
    group = "application"
    description = "Rebuild line_ref — the canonical (bookId, refKeyHash) -> lineIndex reference index."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.refs.BuildLineRefIndexCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("dbPath", dbPath)
    jvmArgs = listOf("-Xmx2g")
}

tasks.register<JavaExec>("buildLineDhIndex") {
    group = "application"
    description = "Rebuild line_dh — the (bookId, dhText) -> lineIndex dibbur-hamatchil index."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.dh.BuildLineDhIndexCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("dbPath", dbPath)
    jvmArgs = listOf("-Xmx2g")
}

tasks.register<JavaExec>("buildLineNoteIndex") {
    group = "application"
    description = "Move inline footnote bodies out of line text into line_note; link_anchor offsets keep counting them."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.notes.BuildLineNoteIndexCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    val dbPath = (project.findProperty("dbPath") ?: project.findProperty("seforimDb")) as String?
        ?: System.getenv("SEFORIM_DB")
        ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
    systemProperty("dbPath", dbPath)
    jvmArgs = listOf("-Xmx2g")
}

tasks.register<JavaExec>("diagnoseHashMismatch") {
    group = "verification"
    description = "Apply a patch.db onto a copy of prevDb and report which tables hash-differ from newDb."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.DiagnoseHashMismatchCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    // Defer validation to execution — IntelliJ's Gradle sync materialises
    // every registered task, so any error() at config-time breaks sync.
    val prev = project.findProperty("prevDb") as String?
    val new = project.findProperty("newDb") as String?
    val patch = project.findProperty("patch") as String?
    if (prev != null) systemProperty("prevDb", prev)
    if (new != null) systemProperty("newDb", new)
    if (patch != null) systemProperty("patch", patch)
    doFirst {
        checkNotNull(prev) { "-PprevDb= missing" }
        checkNotNull(new) { "-PnewDb= missing" }
        checkNotNull(patch) { "-Ppatch= missing" }
    }
    jvmArgs = listOf("-Xmx$generatorHeap", "-XX:+UseG1GC")
}
