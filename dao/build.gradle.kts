
plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.android.library)
    alias(libs.plugins.maven.publish)
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.sqlDelight)
}

group = "io.github.kdroidfilter.seforimlibrary"


kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    android {
        namespace = "io.github.kdroidfilter.seforimlibrary.dao"
        compileSdk = 35
        minSdk = 21
        // The reads on a real database, on a device or an emulator (AndroidDbReadBenchmark)
        withDeviceTestBuilder { sourceSetTreeName = "test" }.configure {
            instrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        }
    }
    jvm()
    iosArm64()
    iosSimulatorArm64()

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            implementation(project(":cache"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.protobuf)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kermit)
            implementation(libs.kotlinx.io.core)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmTest.dependencies {
            implementation(libs.sqlDelight.driver.sqlite)
        }

        // The JVM and Android share java.util.zip's codec for the line texts
        val androidJvmMain by creating { dependsOn(commonMain.get()) }

        androidMain {
            dependsOn(androidJvmMain)
            dependencies {
                implementation(libs.kotlinx.coroutines.android)
                implementation(libs.sqlDelight.driver.android)
            }
        }

        jvmMain {
            dependsOn(androidJvmMain)
            dependencies {
                implementation(libs.kotlinx.coroutines.swing)
                implementation(libs.sqlDelight.driver.sqlite)
            }
        }

        // The manual source set above turns off the default hierarchy: iOS shares its own one by hand
        val iosMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                implementation(libs.sqlDelight.driver.native)
            }
        }
        getByName("androidDeviceTest").dependencies {
            implementation(kotlin("test"))
            implementation(libs.androidx.test.runner)
            implementation(libs.androidx.test.junit)
        }

        iosArm64Main { dependsOn(iosMain) }
        iosSimulatorArm64Main { dependsOn(iosMain) }

    }

}


//Publishing your Kotlin Multiplatform library to Maven Central
//https://www.jetbrains.com/help/kotlin-multiplatform-dev/multiplatform-publish-libraries.html
mavenPublishing {
    publishToMavenCentral()
    coordinates("io.github.kdroidfilter.seforimlibrary", "dao", "1.0.0")

    pom {
        name = "SeforimLibraryDao"
        description = "Kotlin Multiplatform library"
        url = "github url" //todo

        licenses {
            license {
                name = "Zayit Public License 1.0"
                url = "https://github.com/kdroidFilter/SeforimLibrary/blob/master/LICENSE"
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

sqldelight {
    databases {
        create("SeforimDb") {
            // Database configuration here.
            // https://cashapp.github.io/sqldelight
            packageName.set("io.github.kdroidfilter.seforimlibrary.db")
            dialect("app.cash.sqldelight:sqlite-3-24-dialect:${libs.versions.sqlDelight.get()}")
        }
    }
}
