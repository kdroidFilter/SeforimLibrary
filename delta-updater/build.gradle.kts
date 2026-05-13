plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.maven.publish)
}

group = "io.github.kdroidfilter.seforimlibrary"

mavenPublishing {
    publishToMavenCentral()
    coordinates("io.github.kdroidfilter.seforimlibrary", "delta-updater", "1.0.0")
    pom {
        name = "SeforimLibraryDeltaUpdater"
        description = "Client-side delta updater for seforim.db"
        url = "github url"
        licenses {
            license { name = "MIT"; url = "https://opensource.org/licenses/MIT" }
        }
        developers { developer { id = ""; name = ""; email = "" } }
        scm { url = "github url" }
    }
    if (project.hasProperty("signing.keyId")) signAllPublications()
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":dao"))
            api(project(":generator-common"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.jdbc)
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.zstd)
        }

        jvmTest.dependencies {
            implementation(kotlin("test-junit"))
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}
