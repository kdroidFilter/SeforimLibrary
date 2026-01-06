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
    description = "Generate build/seforim.db from Sefaria, then append Otzaria and rebuild catalog.pb."

    dependsOn(":sefariasqlite:generateSefariaSqlite")
    dependsOn(":otzariasqlite:appendOtzaria")
    dependsOn(":catalog:buildCatalog")
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
    mustRunAfter(":sefariasqlite:generateSefariaSqlite")
}
project(":catalog").tasks.matching { it.name == "buildCatalog" }.configureEach {
    mustRunAfter(":otzariasqlite:appendOtzaria")
}

tasks.register("packageSeforimBundle") {
    group = "application"
    description = "Generate DB + catalog, build Lucene indexes, then package a bundle (.tar.zst)."

    dependsOn("generateSeforimDb")
    dependsOn(":searchindex:buildLuceneIndexDefault")
    dependsOn(":packaging:packageArtifacts")
}

project(":searchindex").tasks.matching { it.name == "buildLuceneIndexDefault" }.configureEach {
    mustRunAfter(":generateSeforimDb")
}
project(":packaging").tasks.matching { it.name == "packageArtifacts" }.configureEach {
    mustRunAfter(":searchindex:buildLuceneIndexDefault")
}

tasks.register<Delete>("cleanGeneratedData") {
    group = "application"
    description = "Delete all downloaded sources and generated databases/indexes."

    // Downloaded sources
    delete(layout.buildDirectory.dir("sefaria"))
    delete(layout.buildDirectory.dir("otzaria"))
    delete(layout.buildDirectory.dir("acronymizer"))

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
