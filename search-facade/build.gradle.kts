plugins {
    id("org.jetbrains.kotlin.jvm")
    alias(libs.plugins.nja.facade)
}

group = "io.github.kdroidfilter.seforimlibrary"

// The search's Lucene side, its façade for Nucleus JVM Access: it runs on a JVM (the desktop's own, the mobile apps'
// embedded one), and search's common code uses it through the proxies NJA generates (see JVM_RUNTIME.md)
kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())
}

njaFacade {
    libName = "search"
    // The package the common code knows these types by (the façade's own is ...search.lucene: the desktop has both)
    clientPackage = "io.github.kdroidfilter.seforimlibrary.search.index"
}

dependencies {
    implementation(libs.lucene.core)
    testImplementation(kotlin("test"))
}
