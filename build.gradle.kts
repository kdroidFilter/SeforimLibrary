plugins {
    alias(libs.plugins.multiplatform).apply(false)
    alias(libs.plugins.android.library).apply(false)
    alias(libs.plugins.maven.publish).apply(false)
    alias(libs.plugins.kotlinx.serialization).apply(false)
    alias(libs.plugins.sqlDelight).apply(false)
    alias(libs.plugins.android.application).apply(false)
}

// ─── JDK 25: pre-approve the native access sqlite-jdbc already performs ────
// Every JVM that opens a seforim.db makes sqlite-jdbc call System::load, and
// JDK 25 answers on stderr with
//   WARNING: A restricted method in java.lang.System has been called
//   WARNING: java.lang.System::load has been called by org.sqlite.SQLiteJDBCLoader …
//   WARNING: Use --enable-native-access=ALL-UNNAMED to avoid a warning …
//   WARNING: Restricted methods will be blocked in a future release …
// Run 34024655297 printed that four-line block 15 times — 60 log lines — from
// "Generate Seforim Database" (×9), "Dump lines snapshot for the linker",
// "Apply LINKER links (Phase-2)" and "Produce + verify patch fan" (×4). The
// flag grants exactly the access the code already takes: no bytecode, no
// behaviour and no artifact change. The last line is also the reason not to
// leave it: the same call becomes an error in a future JDK.
//
// Added as an argument PROVIDER rather than by appending to `jvmArgs`: ~25
// JavaExec tasks assign `jvmArgs = listOf(…)` in their own configuration
// blocks (manual-generate-release.yml greps two of those lines verbatim), and
// an assignment discards anything appended here. jvmArgumentProviders is a
// separate list that no assignment can clear.
//
// The patch fan does not fork through Gradle at all — it runs PatchPipelineCli
// with `java` from generator/common's published launcher spec — so its copy of
// the flag lives in `patchPipelineJvmArgs` there.
class EnableNativeAccess : org.gradle.process.CommandLineArgumentProvider {
    override fun asArguments(): Iterable<String> = listOf("--enable-native-access=ALL-UNNAMED")
}

allprojects {
    tasks.withType<JavaExec>().configureEach {
        jvmArgumentProviders.add(EnableNativeAccess())
    }
    tasks.withType<Test>().configureEach {
        jvmArgumentProviders.add(EnableNativeAccess())
    }
}

tasks.register("generateSeforimDb") {
    group = "application"
    description = "Generate build/seforim.db from Sefaria, append Otzaria, and release info."

    dependsOn(":sefariasqlite:generateSefariaSqlite")
    dependsOn(":otzariasqlite:appendOtzaria")
    dependsOn(":otzariasqlite:generateHavroutaLinks")
    dependsOn(":sefariasqlite:renameCategories")
    dependsOn(":sefariasqlite:seedGenerations")
    dependsOn(":sefariasqlite:seedAllMetadata")
    dependsOn(":sefariasqlite:seedBookNotices")
    dependsOn(":generator-common:buildLineRefIndex")
    dependsOn(":generator-common:buildLineDhIndex")
    dependsOn(":sefariasqlite:synthesizeSeifimAltToc")
    dependsOn(":sefariasqlite:synthesizeSimanNamesAltToc")
    dependsOn(":sefariasqlite:inheritChaptersAltToc")
    dependsOn(":packaging:writeReleaseInfo")
    dependsOn(":packaging:downloadLexicalDb")
    // Stamps schema_meta.db_version into the produced seforim.db so the
    // delta client can read it. Without this, every client reads
    // db_version=0 (default) and the path chooser always picks FullBundle
    // instead of the incremental chain.
    // Every stage writes the schema-5 working shape; the split converts the
    // finished DB to schema 6 before the stamp asserts that shape.
    finalizedBy(":generator-common:splitLineContent")
    finalizedBy(":generator-common:stampSchemaVersion")
    finalizedBy(":generator-common:analyzeSeforimDb")
}
// Every stage that writes seforim.db in the working shape; the split and the
// stamp follow all of them.
val lineContentWriters = listOf(
    ":packaging:writeReleaseInfo",
    ":catalog:buildCatalog",
    ":sefariasqlite:seedGenerations",
    ":sefariasqlite:seedAllMetadata",
    ":sefariasqlite:seedBookNotices",
    ":sefariasqlite:synthesizeSeifimAltToc",
    ":sefariasqlite:synthesizeSimanNamesAltToc",
    ":sefariasqlite:inheritChaptersAltToc",
    ":generator-common:buildLineRefIndex",
    ":generator-common:buildLineDhIndex",
)
project(":generator-common").tasks.matching { it.name == "splitLineContent" }.configureEach {
    lineContentWriters.forEach { mustRunAfter(it) }
}
// Force stamp ordering after every step that writes to seforim.db, so the
// stamp runs at the very end of the pipeline (not concurrently with content
// inserts).
project(":generator-common").tasks.matching { it.name == "stampSchemaVersion" }.configureEach {
    lineContentWriters.forEach { mustRunAfter(it) }
    mustRunAfter(":generator-common:splitLineContent")
}
// sqlite_stat1 must describe the final rows, so ANALYZE follows the split, the
// stamp and Phase-2 (generateLinkerLinks), the last writer in the release workflow.
project(":generator-common").tasks.matching { it.name == "analyzeSeforimDb" }.configureEach {
    mustRunAfter(":generator-common:splitLineContent")
    mustRunAfter(":generator-common:stampSchemaVersion")
    mustRunAfter(":sefariasqlite:generateLinkerLinks")
}
// Compression follows the last reader of plain text (Phase-2), and compaction
// reclaims the space the shorter rows free.
project(":generator-common").tasks.matching { it.name == "compressLineContent" }.configureEach {
    lineContentWriters.forEach { mustRunAfter(it) }
    mustRunAfter(":generator-common:splitLineContent")
    mustRunAfter(":generator-common:stampSchemaVersion")
    mustRunAfter(":sefariasqlite:generateLinkerLinks")
    mustRunAfter(":generator-common:analyzeSeforimDb")
}
// VACUUM INTO carries sqlite_stat1 over, so compaction comes after every writer
// including ANALYZE; the producer then reads the compacted DB's stat1.
project(":generator-common").tasks.matching { it.name == "compactSeforimDb" }.configureEach {
    mustRunAfter(":generator-common:splitLineContent")
    mustRunAfter(":generator-common:stampSchemaVersion")
    mustRunAfter(":sefariasqlite:generateLinkerLinks")
    mustRunAfter(":generator-common:analyzeSeforimDb")
    mustRunAfter(":generator-common:compressLineContent")
}

// Generator diagnostics side-channel (see GeneratorReport). Findings that are
// too long for the build log — the missing priority entries, the metadata
// records that matched no book, the books with no source hash, the ambiguous
// line_ref keys — log one bounded summary line and write the full list here.
//
// Every generator stage is a forked JavaExec whose working directory is its OWN
// subproject, so GeneratorReport's relative default would scatter the files
// across generator/*/build/generator-reports and none of them would be under
// the root build/ that the workflow's tmpfs and its release staging address.
// Pin all writers to one absolute directory in the ROOT build dir.
// -PgeneratorReportDir overrides it.
//
// NOTE: nothing collects this directory off the runner today — build/ is a
// tmpfs the job unmounts, and manual-generate-release.yml deliberately has no
// Actions artifact upload to hook into (see
// test_weekly_workflow_has_no_actions_artifact_handoffs and
// test_generator_reports_are_not_collected_off_the_runner_yet). Every finding's
// counts and its first names are in the build log regardless; only the tail of
// each list dies with the run. Publishing them is an operator decision.
val generatorReportDir: String =
    (findProperty("generatorReportDir") as String?)
        ?: layout.buildDirectory.dir("generator-reports").get().asFile.absolutePath
listOf(
    ":sefariasqlite" to "generateSefariaSqlite",
    ":sefariasqlite" to "seedAllMetadata",
    ":sefariasqlite" to "seedBookNotices",
    ":otzariasqlite" to "generateLines",
    ":otzariasqlite" to "generateLinks",
    ":otzariasqlite" to "appendOtzariaLines",
    ":otzariasqlite" to "appendOtzariaLinks",
    ":generator-common" to "buildLineRefIndex",
).forEach { (projectPath, taskName) ->
    project(projectPath).tasks.matching { it.name == taskName }.configureEach {
        (this as JavaExec).systemProperty("generatorReportDir", generatorReportDir)
    }
}

// line_ref is derived from line.heRef + book.title, so it must be rebuilt
// after every stage that writes or renames books and lines.
project(":generator-common").tasks.matching { it.name == "buildLineRefIndex" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
    mustRunAfter(":sefariasqlite:seedGenerations")
    mustRunAfter(":sefariasqlite:seedAllMetadata")
    mustRunAfter(":sefariasqlite:seedBookNotices")
    mustRunAfter(":sefariasqlite:synthesizeSeifimAltToc")
    mustRunAfter(":sefariasqlite:synthesizeSimanNamesAltToc")
    mustRunAfter(":sefariasqlite:inheritChaptersAltToc")
}

// line_dh is derived from line.content, so it must be rebuilt after every
// stage that writes lines. Ordered after buildLineRefIndex as well so the
// two index builders write to seforim.db sequentially rather than concurrently.
project(":generator-common").tasks.matching { it.name == "buildLineDhIndex" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
    mustRunAfter(":sefariasqlite:seedGenerations")
    mustRunAfter(":sefariasqlite:seedAllMetadata")
    mustRunAfter(":sefariasqlite:seedBookNotices")
    mustRunAfter(":generator-common:buildLineRefIndex")
}

// Ensure ordering inside the pipeline task graph
project(":otzariasqlite").tasks.matching {
    it.name in setOf(
        // Use strict ordering on the actual DB-mutating task, not only the wrapper,
        // otherwise Gradle may run dependencies early and overlap IO (downloads + DB writes).
        "downloadAcronymizer",
        "appendOtzariaLines",
        "appendOtzariaLinks",
        "appendOtzaria"
    )
}.configureEach {
    mustRunAfter(":sefariasqlite:renameCategories")
}
project(":sefariasqlite").tasks.matching { it.name == "renameCategories" }.configureEach {
    mustRunAfter(":sefariasqlite:generateSefariaSqlite")
}
project(":otzariasqlite").tasks.matching { it.name == "generateHavroutaLinks" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
}
// seedGenerations runs after all book-writing stages so it can link both
// Sefaria- and Otzaria-sourced books in a single pass.
project(":sefariasqlite").tasks.matching { it.name == "seedGenerations" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
}
// seedAllMetadata matches by final book title, so it must run after appendOtzaria
// and renameCategories. Ordered after seedGenerations as well so the post-process
// seeders write to seforim.db sequentially rather than concurrently.
project(":sefariasqlite").tasks.matching { it.name == "seedAllMetadata" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
    mustRunAfter(":sefariasqlite:seedGenerations")
}
// seedBookNotices matches final book titles and sources, so it follows the
// seeders that rename books or set sourceId; ordered before the alt-TOC writers
// only so post-process seeders write to seforim.db sequentially.
project(":sefariasqlite").tasks.matching { it.name == "seedBookNotices" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
    mustRunAfter(":sefariasqlite:seedGenerations")
    mustRunAfter(":sefariasqlite:seedAllMetadata")
}
// synthesizeSeifimAltToc reads COMMENTARY links and the final main TOC, so it
// runs after every book- and link-writing stage. Ordered after the other
// post-process seeders so they write to seforim.db sequentially.
project(":sefariasqlite").tasks.matching { it.name == "synthesizeSeifimAltToc" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
    mustRunAfter(":sefariasqlite:seedGenerations")
    mustRunAfter(":sefariasqlite:seedAllMetadata")
    mustRunAfter(":sefariasqlite:seedBookNotices")
}
// synthesizeSimanNamesAltToc reads the Sefaria Topic structures and COMMENTARY
// links; ordered after synthesizeSeifimAltToc only so the two write sequentially.
project(":sefariasqlite").tasks.matching { it.name == "synthesizeSimanNamesAltToc" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
    mustRunAfter(":sefariasqlite:seedGenerations")
    mustRunAfter(":sefariasqlite:seedAllMetadata")
    mustRunAfter(":sefariasqlite:seedBookNotices")
    mustRunAfter(":sefariasqlite:synthesizeSeifimAltToc")
}
// inheritChaptersAltToc skips any book that already has an alt structure, so
// it must see every other alt-TOC writer's output, Seifim included.
project(":sefariasqlite").tasks.matching { it.name == "inheritChaptersAltToc" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:renameCategories")
    mustRunAfter(":sefariasqlite:seedGenerations")
    mustRunAfter(":sefariasqlite:seedAllMetadata")
    mustRunAfter(":sefariasqlite:seedBookNotices")
    mustRunAfter(":sefariasqlite:synthesizeSeifimAltToc")
    mustRunAfter(":sefariasqlite:synthesizeSimanNamesAltToc")
}
project(":catalog").tasks.matching { it.name == "buildCatalog" }.configureEach {
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":sefariasqlite:seedGenerations")
    mustRunAfter(":sefariasqlite:seedAllMetadata")
    mustRunAfter(":sefariasqlite:seedBookNotices")
    mustRunAfter(":sefariasqlite:synthesizeSeifimAltToc")
    mustRunAfter(":sefariasqlite:synthesizeSimanNamesAltToc")
    mustRunAfter(":sefariasqlite:inheritChaptersAltToc")
    mustRunAfter(":generator-common:buildLineDhIndex")
}
project(":packaging").tasks.matching { it.name == "writeReleaseInfo" }.configureEach {
    mustRunAfter(":catalog:buildCatalog")
}

tasks.register("packageSeforimBundle") {
    group = "application"
    description = "Generate DB + release info, then package a bundle (.tar.zst)."

//    dependsOn("generateSeforimDb")
    dependsOn(":packaging:packageArtifacts")
}

//project(":packaging").tasks.matching { it.name == "packageArtifacts" }.configureEach {
//    mustRunAfter("generateSeforimDb")
//}

/**
 * Push-button release task — produces the seforim.db, then
 * derives a delta against a configured previous release and
 * emits the JSON artefacts the client polls.
 *
 * Required when invoking:
 *   -PprevReleaseDb=/path/to/previous/seforim.db
 *   -PfromVersion=<int>           current release of the prev DB
 *   -PtoVersion=<int>             version label this build will publish
 *
 * Optional (writes release_meta.json when all four are set):
 *   -PreleaseMeta=/path/to/release_meta.json
 *   -PfullBundleUrl=https://.../full-vN.tar.zst
 *   -PfullBundleSha=<sha256>
 *   -PfullBundleSize=<bytes>
 *   -PmanifestBaseUrl=https://.../deltas
 *
 * Output (under <root>/build/):
 *   - seforim.db                   freshly-built DB
 *   - seforim.db.buildstate        IdAllocator snapshot for the next build
 *   - patch-v<from>-v<to>.db                  binary delta
 *   - patch-v<from>-v<to>.db.manifest.json    per-delta manifest
 *   - release_meta.json (optional)            release-level index
 */
tasks.register("publishRelease") {
    group = "application"
    description = "generateSeforimDb + producePatchAndVerify (+ release_meta.json upsert)."
    dependsOn("generateSeforimDb")
    finalizedBy(":generator-common:producePatchAndVerify")
    val prevReleaseDb = providers.gradleProperty("prevReleaseDb")
    val buildStatePath = providers.systemProperty("buildStatePath")
        .orElse(providers.environmentVariable("BUILD_STATE_PATH"))
        .orElse(layout.buildDirectory.file("seforim.db.buildstate").map { it.asFile.absolutePath })
    // Operator footgun guard: if -PprevReleaseDb is set we're producing a
    // delta against a previous release, which requires the IdAllocator to
    // be seeded from that release's build_state. Without it, the allocator
    // starts fresh and assigns brand-new ids to every row — the producer
    // would then emit a "delta" containing the entire corpus, useless as
    // an incremental patch. Fail fast with the path the operator forgot.
    doFirst {
        val prev = prevReleaseDb.orNull
        if (prev != null) {
            val buildStateFile = java.io.File(buildStatePath.get())
            check(buildStateFile.exists()) {
                "publishRelease: -PprevReleaseDb=$prev was set but " +
                    "$buildStateFile is missing. Copy the previous release's " +
                    "seforim.db.buildstate into place (see RELEASE.md) — without it, " +
                    "the IdAllocator restarts from scratch and produces a patch with " +
                    "every id renumbered."
            }
        }
    }
}
project(":generator-common").tasks.matching { it.name == "producePatchAndVerify" }.configureEach {
    // Use the absolute task path so the lookup succeeds when this task is
    // invoked directly (e.g. for a producer-only e2e), without requiring
    // generateSeforimDb to exist in :generator-common.
    mustRunAfter(rootProject.tasks.named("generateSeforimDb"))
    // The stamp writes schema_meta.db_schema_version, which resolveSchemaVersion
    // reads — must land before the producer runs in a single-invocation build.
    mustRunAfter(project(":generator-common").tasks.matching { it.name == "splitLineContent" })
    mustRunAfter(project(":generator-common").tasks.matching { it.name == "stampSchemaVersion" })
    mustRunAfter(project(":generator-common").tasks.matching { it.name == "analyzeSeforimDb" })
    mustRunAfter(project(":generator-common").tasks.matching { it.name == "compressLineContent" })
    mustRunAfter(project(":generator-common").tasks.matching { it.name == "compactSeforimDb" })
    // Map the umbrella task's -P props onto the CLI's gradle props.
    val prev = providers.gradleProperty("prevReleaseDb").orNull
    val from = providers.gradleProperty("fromVersion").orNull
    val to = providers.gradleProperty("toVersion").orNull
    if (prev != null && from != null && to != null) {
        val out = rootProject.layout.buildDirectory
            .file("patch-v${from}-v${to}.db").get().asFile.absolutePath
        val new = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        (this as JavaExec).systemProperty("prevDb", prev)
        (this as JavaExec).systemProperty("newDb", new)
        (this as JavaExec).systemProperty("out", out)
    }
}

tasks.register<Delete>("cleanGeneratedData") {
    group = "application"
    description = "Delete all downloaded sources and generated databases/indexes."

    // Downloaded sources (in submodule build directories)
    delete(project(":sefariasqlite").layout.buildDirectory.dir("sefaria"))
    delete(project(":otzariasqlite").layout.buildDirectory.dir("otzaria"))
    delete(project(":otzariasqlite").layout.buildDirectory.dir("acronymizer"))

    // Generated databases
    delete(layout.buildDirectory.file("seforim.db"))
    delete(layout.buildDirectory.file("seforim.db.bak"))
    delete(layout.buildDirectory.file("seforim.db-shm"))
    delete(layout.buildDirectory.file("seforim.db-wal"))
    delete(layout.buildDirectory.file("lexical.db"))
    delete(layout.buildDirectory.file("catalog.pb"))
    delete(layout.buildDirectory.file("release_info.txt"))

    // Stale index directories from older builds
    delete(layout.buildDirectory.dir("seforim.db.lucene"))
    delete(layout.buildDirectory.dir("seforim.db.lookup.lucene"))

    // Packaged bundle
    delete(layout.buildDirectory.dir("package"))
}
