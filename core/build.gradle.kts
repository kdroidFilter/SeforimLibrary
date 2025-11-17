
plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.android.kotlin.multiplatform.library)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.kotlinx.serialization)
}

group = "io.github.kdroidfilter.seforimlibrary"

kotlin {
    jvmToolchain(21)

    androidLibrary {
        namespace = "io.github.kdroidfilter.seforimlibrary"
        compileSdk = 35
        minSdk = 21
    }
    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.protobuf)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        androidMain.dependencies {

        }

        jvmMain.dependencies {

        }

    }

}

//Publishing your Kotlin Multiplatform library to Maven Central
//https://www.jetbrains.com/help/kotlin-multiplatform-dev/multiplatform-publish-libraries.html
mavenPublishing {
    publishToMavenCentral()
    coordinates("io.github.kdroidfilter.seforimlibrary", "core", "1.0.0")

    pom {
        name = "SeforimLibrary"
        description = "Kotlin Multiplatform library"
        url = "github url" //todo

        licenses {
            license {
                name = "MIT"
                url = "https://opensource.org/licenses/MIT"
            }
        }

        developers {
            developer {
                id = "" //todo
                name = "" //todo
                email = "" //todo
            }
        }

        scm {
            url = "github url" //todo
        }
    }
    if (project.hasProperty("signing.keyId")) signAllPublications()
}
