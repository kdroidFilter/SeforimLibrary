import java.net.URI
import java.nio.file.Files
import java.nio.file.StandardCopyOption

plugins {
    alias(libs.plugins.multiplatform).apply(false)
    alias(libs.plugins.android.library).apply(false)
    alias(libs.plugins.maven.publish).apply(false)
    alias(libs.plugins.kotlinx.serialization).apply(false)
    alias(libs.plugins.sqlDelight).apply(false)
    alias(libs.plugins.android.application).apply(false)
}

// Seeds the IdAllocator with the build_state of the latest GitHub release that published one, so the ids
// (and the user notes on them) carry over. Each run starts again from that file: a previous local run rewrites
// build/seforim.db.buildstate, and seeding from it would drift from the published ids.
// -PbuildStateTag=<tag> picks the release; -PfreshBuildState=true starts from empty ids instead.
val downloadBuildState = tasks.register("downloadBuildState") {
    group = "application"
    description = "Copies the build_state of the latest GitHub release into build/seforim.db.buildstate."

    val repo = "kdroidFilter/SeforimLibrary"
    val asset = "seforim.db.buildstate"
    val target = layout.buildDirectory.file("seforim.db.buildstate").get().asFile
    val cacheDir = layout.buildDirectory.dir("buildstate-cache").get().asFile
    val fresh = providers.gradleProperty("freshBuildState").orNull == "true"
    val explicitTag = providers.gradleProperty("buildStateTag").orNull
    val token = providers.environmentVariable("GITHUB_TOKEN").orNull

    outputs.upToDateWhen { false }
    doLast {
        if (fresh) {
            target.delete()
            logger.lifecycle("freshBuildState: starting from empty ids")
            return@doLast
        }

        fun openApi(path: String) = (URI("https://api.github.com/repos/$repo/$path").toURL().openConnection())
            .apply {
                setRequestProperty("Accept", "application/vnd.github+json")
                token?.let { setRequestProperty("Authorization", "Bearer $it") }
            }
            .getInputStream()

        // Releases come newest first
        @Suppress("UNCHECKED_CAST")
        val releases = openApi("releases?per_page=100").use { groovy.json.JsonSlurper().parse(it) } as List<Map<String, Any?>>
        val withAsset = releases.filter { release ->
            (release["assets"] as List<Map<String, Any?>>).any { it["name"] == asset }
        }
        val release = if (explicitTag != null) {
            withAsset.firstOrNull { it["tag_name"] == explicitTag }
                ?: error("Release $explicitTag has no $asset")
        } else {
            withAsset.firstOrNull() ?: error("No release of $repo publishes $asset (-PfreshBuildState=true to start from empty ids)")
        }
        val tag = release["tag_name"] as String
        val latestTag = releases.firstOrNull()?.get("tag_name")
        if (explicitTag == null && tag != latestTag) {
            logger.warn("The latest release $latestTag has no $asset: seeding from $tag instead")
        }

        val cached = cacheDir.resolve("$tag/$asset")
        if (!cached.exists()) {
            val url = (release["assets"] as List<Map<String, Any?>>).first { it["name"] == asset }["browser_download_url"] as String
            logger.lifecycle("Downloading $asset of $tag")
            cached.parentFile.mkdirs()
            val partial = cached.resolveSibling("$asset.part")
            URI(url).toURL().openStream().use { Files.copy(it, partial.toPath(), StandardCopyOption.REPLACE_EXISTING) }
            Files.move(partial.toPath(), cached.toPath(), StandardCopyOption.ATOMIC_MOVE)
        }
        Files.copy(cached.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        logger.lifecycle("Seeded ${target.name} from release $tag")
    }
}

tasks.register("generateSeforimDb") {
    group = "application"
    description = "Generate build/seforim.db from Sefaria, append Otzaria, build catalog, Lucene indexes, and release info."

    dependsOn(downloadBuildState)
    dependsOn(":sefariasqlite:generateSefariaSqlite")
    dependsOn(":otzariasqlite:appendOtzaria")
    dependsOn(":otzariasqlite:generateHavroutaLinks")
    dependsOn(":generator-common:importAuthorBios")
    dependsOn(":sefariasqlite:renameCategories")
    dependsOn(":catalog:buildCatalog")
    dependsOn(":searchindex:buildLuceneIndexDefault")
    dependsOn(":packaging:writeReleaseInfo")
    dependsOn(":packaging:downloadLexicalDb")
    // Stamps schema_meta.db_version into the produced seforim.db so the
    // delta client can read it. Without this, every client reads
    // db_version=0 (default) and the path chooser always picks FullBundle
    // instead of the incremental chain.
    finalizedBy(":generator-common:stampSchemaVersion")
}
// Force stamp ordering after every step that writes to seforim.db, so the
// stamp runs at the very end of the pipeline (not concurrently with content
// inserts).
project(":generator-common").tasks.matching { it.name == "stampSchemaVersion" }.configureEach {
    mustRunAfter(":packaging:writeReleaseInfo")
    mustRunAfter(":catalog:buildCatalog")
    mustRunAfter(":searchindex:buildLuceneIndexDefault")
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
project(":sefariasqlite").tasks.matching { it.name == "generateSefariaSqlite" }.configureEach {
    mustRunAfter(downloadBuildState)
}
project(":sefariasqlite").tasks.matching { it.name == "renameCategories" }.configureEach {
    mustRunAfter(":sefariasqlite:generateSefariaSqlite")
}
project(":otzariasqlite").tasks.matching { it.name == "generateHavroutaLinks" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
}
// Biographies join on author names, so they go in once every author exists.
project(":generator-common").tasks.matching { it.name == "importAuthorBios" }.configureEach {
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
}
project(":catalog").tasks.matching { it.name == "buildCatalog" }.configureEach {
    mustRunAfter(":otzariasqlite:generateHavroutaLinks")
    mustRunAfter(":generator-common:importAuthorBios")
}
project(":searchindex").tasks.matching { it.name in setOf("embedCorpus", "buildLuceneIndexDefault") }.configureEach {
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
    // Operator footgun guard: if -PprevReleaseDb is set we're producing a
    // delta against a previous release, which requires the IdAllocator to
    // be seeded from that release's build_state. Without it, the allocator
    // starts fresh and assigns brand-new ids to every row — the producer
    // would then emit a "delta" containing the entire corpus, useless as
    // an incremental patch. Fail fast with the path the operator forgot.
    doFirst {
        val prev = project.findProperty("prevReleaseDb") as String?
        if (prev != null) {
            val explicit = System.getProperty("buildStatePath")
                ?: System.getenv("BUILD_STATE_PATH")
            val buildStateFile = if (explicit != null) {
                file(explicit)
            } else {
                layout.buildDirectory.file("seforim.db.buildstate").get().asFile
            }
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
