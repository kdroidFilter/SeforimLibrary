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
