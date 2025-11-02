plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.maven.publish)
}

group = "io.github.kdroidfilter.seforimlibrary"

kotlin {
    jvmToolchain(21)
    jvm()

    sourceSets {
        jvmMain.dependencies {
            implementation(libs.lucene.core)
            implementation(libs.lucene.analysis.common)
        }
        commonTest.dependencies { implementation(kotlin("test")) }
    }
}
