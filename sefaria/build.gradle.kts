
plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.android.library)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.kotlinx.serialization)
}

group = "io.github.kdroidfilter.seforimlibrary"

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    androidLibrary {
        namespace = "io.github.kdroidfilter.seforimlibrary.sefaria"
        compileSdk = 35
        minSdk = 21
    }
    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            implementation(libs.kotlinx.serialization.json)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        androidMain.dependencies {
            // Android-specific deps (if any) can be added here later
        }

        jvmMain.dependencies {
            implementation(libs.kermit)
            implementation(libs.commons.compress)
            implementation(libs.zstd)
        }
    }
}

tasks.register<JavaExec>("generateSefariaOtzaria") {
    group = "application"
    description = "Download Sefaria export and convert it to an Otzaria-like folder under build/sefaria/otzaria"

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.sefaria.DownloadSefariaKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    jvmArgs = listOf("-Xmx4g")
}

// Optional: Publishing configuration (mirrors other modules)
mavenPublishing {
    publishToMavenCentral()
    coordinates("io.github.kdroidfilter.seforimlibrary", "sefaria", "1.0.0")

    pom {
        name = "SeforimLibrarySefaria"
        description = "Sefaria integration utilities for SeforimLibrary (KMP)"
        url = "github url" // todo

        licenses {
            license {
                name = "MIT"
                url = "https://opensource.org/licenses/MIT"
            }
        }

        developers {
            developer {
                id = "" // todo
                name = "" // todo
                email = "" // todo
            }
        }

        scm {
            url = "github url" // todo
        }
    }
    if (project.hasProperty("signing.keyId")) signAllPublications()
}
