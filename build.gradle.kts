plugins {
    alias(libs.plugins.multiplatform).apply(false)
    alias(libs.plugins.android.library).apply(false)
    alias(libs.plugins.maven.publish).apply(false)
    alias(libs.plugins.kotlinx.serialization).apply(false)
    alias(libs.plugins.sqlDelight).apply(false)
    alias(libs.plugins.android.application).apply(false)
}

tasks.register("generateSeforimDb") {
    group = "application"
    description = "Generate build/seforim.db from Sefaria, append Otzaria, build catalog, Lucene indexes, and release info."

    dependsOn(":sefariasqlite:generateSefariaSqlite")
    dependsOn(":otzariasqlite:appendOtzaria")
    dependsOn(":otzariasqlite:generateHavroutaLinks")
    dependsOn(":sefariasqlite:renameCategories")
    dependsOn(":catalog:buildCatalog")
    dependsOn(":searchindex:buildLuceneIndexDefault")
    dependsOn(":packaging:writeReleaseInfo")
    dependsOn(":packaging:downloadLexicalDb")
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
project(":catalog").tasks.matching { it.name == "buildCatalog" }.configureEach {
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
}
project(":searchindex").tasks.matching { it.name == "buildLuceneIndexDefault" }.configureEach {
    mustRunAfter(":catalog:buildCatalog")
}
project(":packaging").tasks.matching { it.name == "writeReleaseInfo" }.configureEach {
    mustRunAfter(":searchindex:buildLuceneIndexDefault")
}

tasks.register("packageSeforimBundle") {
    group = "application"
    description = "Generate DB + catalog + indexes + release info, then package a bundle (.tar.zst)."

//    dependsOn("generateSeforimDb")
    dependsOn(":packaging:packageArtifacts")
}

//project(":packaging").tasks.matching { it.name == "packageArtifacts" }.configureEach {
//    mustRunAfter("generateSeforimDb")
//}

/**
 * Push-button release task — produces the seforim.db + catalog.pb + Lucene
 * indexes, then derives a delta against a configured previous release and
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
 *   - catalog.pb / *.lucene/       app artefacts
 *   - patch-v<from>-v<to>.db                  binary delta
 *   - patch-v<from>-v<to>.db.manifest.json    per-delta manifest
 *   - release_meta.json (optional)            release-level index
 */
tasks.register("publishRelease") {
    group = "application"
    description = "generateSeforimDb + producePatchAndVerify (+ release_meta.json upsert)."
    dependsOn("generateSeforimDb")
    finalizedBy(":generator-common:producePatchAndVerify")
}
project(":generator-common").tasks.matching { it.name == "producePatchAndVerify" }.configureEach {
    mustRunAfter("generateSeforimDb")
    // Map the umbrella task's -P props onto the CLI's gradle props.
    val prev = project.findProperty("prevReleaseDb") as String?
    val from = project.findProperty("fromVersion") as String?
    val to = project.findProperty("toVersion") as String?
    if (prev != null && from != null && to != null) {
        val out = rootProject.layout.buildDirectory
            .file("patch-v${from}-v${to}.db").get().asFile.absolutePath
        val new = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
        this.extensions.extraProperties.set("prevDb", prev)
        this.extensions.extraProperties.set("newDb", new)
        this.extensions.extraProperties.set("out", out)
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

    // Lucene indexes
    delete(layout.buildDirectory.dir("seforim.db.lucene"))
    delete(layout.buildDirectory.dir("seforim.db.lookup.lucene"))

    // Packaged bundle
    delete(layout.buildDirectory.dir("package"))
}
